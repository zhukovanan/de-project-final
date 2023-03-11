from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
from lib.config import Config
import logging

config = Config()

#Для логирования
task_logger = logging.getLogger('airflow.task')


def run_vertica_upload_mart(file_name_path: str, client = config.vertica_connection(), **kwargs) -> None:
    business_dt = kwargs['ds']

    with open(file_name_path, 'r') as sql_script:
        sqlCommands = sql_script.read().split(';')
        sqlCommands = list(map(lambda st: str.replace(st, "{{ds}}", business_dt), sqlCommands))

    with client.connection() as de_conn:
        with de_conn.cursor() as cur:
            for command in sqlCommands:
                print(command)
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
    dag_id="datamart_update_",
    description="Dag in charge of data mart update",
    default_args=default_args,
    schedule_interval="00 01 * * *",
    start_date=datetime(2022, 10, 1),
    max_active_runs=3,
    end_date=datetime(2022,11,1),
    catchup=True,
    tags=["Yandex Praktikum"],
) as dag:
    

    external_task_sensor = ExternalTaskSensor(
        task_id="external_task_sensor",
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        external_task_id="upload_files_to_vertica",
        external_dag_id="data_import",
        execution_delta=timedelta(minutes=60),
        dag=dag,
    )
    

    update_datamart = PythonOperator(
                                task_id="create_dwh_global_metrics_mart",
                                python_callable=run_vertica_upload_mart,
                                op_kwargs={'file_name_path':"/lessons/sql/cdm_global_metrics.sql"},
                                dag=dag
                            )
    

    (external_task_sensor >>
     update_datamart)
