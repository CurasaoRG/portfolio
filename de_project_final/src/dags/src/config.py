from src.connectors import PgConnect, VerticaConnector, S3Connector, KafkaConsumer
from src.loader.FromS3 import FromS3
from src.repository.ToVertica import ToVertica
from dotenv import dotenv_values
from json import loads

class Config:
    def __init__(self,
                 env_path):
        """
        Класс для считывания параметров и подключения к системам. 
        Парсит файл .env по заданному адресу и инициализирует подключения к базам данных с полученными параметрами.
        Реализованы подключения к системе-источнику - S3 и системе-приемнику - Vertica.
        """
        parameters = dotenv_values(env_path)
        self.vertica_conn_info = {'host': parameters['VERTICA_HOST'],
                    'port': parameters['VERTICA_PORT'],
                    'user': parameters['VERTICA_USER'],
                    'password': parameters['VERTICA_PASSWORD'],
                    'ssl': False,
                    'autocommit': True}
        self.s3_conn_info = {
            "aws_access_key_id": parameters['S3_AWS_ACCESS_KEY_ID'],
            "aws_secret_access_key": parameters['S3_AWS_SECRET_ACCESS_KEY_ID'],
            "endpoint_url":parameters['S3_ENDPOINTURL'],
            "bucket": parameters['S3_BUCKET']
        }
       
        self._repository = ToVertica(VerticaConnector(**self.vertica_conn_info),
                                     schemas=loads(parameters['VERTICA_SCHEMAS_JSON']),
                                     tables=loads(parameters['VERTICA_TABLES_JSON']))
        self._loader = FromS3(S3Connector(**self.s3_conn_info))
        self._consumer = KafkaConsumer(host=parameters['KAFKA_HOST'],
                                       port=parameters['KAFKA_PORT'],
                                       user=parameters['KAFKA_CONSUMER_USER_NAME'],
                                       password=parameters['KAFKA_CONSUMER_PASSWORD'],
                                        topic = parameters['KAFKA_CONSUMER_TOPIC'],
                                        group=parameters['KAFKA_CONSUMER_GROUP'],
                                        cert_path=parameters['KAFKA_CERT_PATH'] 
                                       )