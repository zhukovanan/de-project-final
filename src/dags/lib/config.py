from lib.vertica.vertica_connect import VerticaConnect
from airflow.models import Variable

class Config:

    def __init__(self) -> None:

        self.vertica_warehouse_host = Variable.get('VERTICA_HOST')
        self.vertica_warehouse_port = 5433
        self.vertica_warehouse_dbname = Variable.get('VERTICA_DB')
        self.vertica_warehouse_user = Variable.get('VERTICA_USER')
        self.vertica_warehouse_password = Variable.get('VERTICA_SECRET_PASSWORD')

        self.s3_aws_access_key_id = Variable.get('AWS_ACCESS_KEY_ID')
        self.s3_secret_access_key = Variable.get('AWS_SECRET_ACCESS_KEY')
        self.s3_service_name = 's3'
        self.s3_endpoint_url = 'https://storage.yandexcloud.net'

    def vertica_connection(self):
        return VerticaConnect(
            self.vertica_warehouse_host,
            self.vertica_warehouse_port,
            self.vertica_warehouse_dbname,
            self.vertica_warehouse_user,
            self.vertica_warehouse_password
        )