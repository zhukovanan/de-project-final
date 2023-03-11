from lib.config import Config
import boto3
import logging
from datetime import datetime, timedelta
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator

config = Config()

#Для логирования
task_logger = logging.getLogger('airflow.task')

def download_s3_file(bucket: str) -> None:

    session = boto3.session.Session()
    s3_client = session.client(
        service_name=config.s3_service_name,
        endpoint_url=config.s3_endpoint_url,
        aws_access_key_id=config.s3_aws_access_key_id,
        aws_secret_access_key=config.s3_secret_access_key
    )

    objects_s3 = s3_client.list_objects_v2(Bucket=bucket)

    for item in objects_s3['Contents']:
        task_logger.info(f"Started downloading file {item['Key']} from S3 bucket {bucket}")
        s3_client.download_file(
            Bucket=bucket,
            Key=item['Key'],
            Filename=f"/data/{item['Key']}"
        )
        task_logger.info(f"File {item['Key']} is saved to /data/{item['Key']}")


def load_file(path: str, schema = 'ZHUKOVANANYANDEXRU__STAGING', client = config.vertica_connection(),**kwargs) -> None:
    files = [i for i in os.listdir(path) if 'csv' in i]
    business_dt = kwargs['ds']

    with client.connection() as de_conn:
        with de_conn.cursor() as cur:
            for name in files:
                table = 'transactions' if 'transactions' in name else 'currencies'
                date_column = 'transaction_dt' if 'transactions' in name else 'date_update'
                file = pd.read_csv(f"{path}/{name}")

                file[date_column] = pd.to_datetime(file[date_column])
                actual_file = file.loc[file[date_column].dt.date == datetime.strptime(business_dt,"%Y-%m-%d").date()]
                placeholders = ["%s",] * len(actual_file.columns)

                task_logger.info(f"Loading data {name} to {schema}.{table} for {business_dt}")
                cur.executemany(f"INSERT INTO {schema}.{table} ({', '.join(actual_file.columns)}) VALUES ({', '.join(placeholders)})", list(zip(*map(actual_file.get, actual_file))))
                task_logger.info(f"{name} for {business_dt} is successfully uploaded to {schema}.{table}")


def run_sql_command_vertica(file_name_path: str, client = config.vertica_connection()) -> None:
    with open(file_name_path, 'r') as sql_script:
        sqlCommands = sql_script.read().split(';')

    with client.connection() as de_conn:
        with de_conn.cursor() as cur:
            for command in sqlCommands:
                try:
                    cur.execute(command)
                except Exception as e:
                    task_logger.error(f'Not managed to run command {command} due to {str(e)}')

default_args={
        "owner": "zhukov-an-an",
        "email": ["zhukov.an.an@gmail.com"],
        "email_on_failure": True,
        "email_on_retry": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=10)}

with DAG(
    dag_id="data_import",
    description="Dag in charge of data import",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2022, 10, 1),
    max_active_runs=3,
    end_date=datetime(2022,11,1),
    catchup=True,
    tags=["Yandex Praktikum"],
) as dag: 
    
    create_stg_currencies_mart = PythonOperator(
                                task_id="create_stg_currencies_mart",
                                python_callable=run_sql_command_vertica,
                                op_kwargs={'file_name_path':"/lessons/sql/stg_currencies.sql"},
                                dag=dag
                            )
    

    create_stg_transactions_mart = PythonOperator(
                                task_id="create_stg_transactions_mart",
                                python_callable=run_sql_command_vertica,
                                op_kwargs={'file_name_path':"/lessons/sql/stg_transactions.sql"},
                                dag=dag
                            )
    
    create_dwh_global_metrics_mart = PythonOperator(
                                task_id="create_dwh_global_metrics_mart",
                                python_callable=run_sql_command_vertica,
                                op_kwargs={'file_name_path':"/lessons/sql/dwh_global_metrics.sql"},
                                dag=dag
                            )
    
    upload_data_from_s3 = PythonOperator(
        task_id="upload_data_from_s3",
        python_callable=download_s3_file,
        op_kwargs={'bucket':"final-project"},
        dag=dag
    )


    upload_files_to_vertica = PythonOperator(
        task_id="upload_files_to_vertica",
        python_callable=load_file,
        op_kwargs={'path':'/data'},
        dag=dag
    )

(create_stg_currencies_mart >>
 create_stg_transactions_mart >>
 create_dwh_global_metrics_mart >>
 upload_data_from_s3 >>
 upload_files_to_vertica)