import boto3
from botocore.exceptions import ClientError

class S3Connector:
    """
    Класс для инициализации подключения к хранилищу S3.
    Использует методы библиотеки boto3 для получения списка файлов и получения файла по имени. 
    """
    def __init__(self,
                endpoint_url,
                bucket,
                aws_access_key_id,
                aws_secret_access_key):
        self._url = endpoint_url
        self._bucket = bucket
        self._key_id = aws_access_key_id
        self._key = aws_secret_access_key
        try:
            self._client = boto3.session.Session().client(
                service_name = 's3',
                endpoint_url = self._url,
                aws_access_key_id = self._key_id,
                aws_secret_access_key = self._key
            )
        except ClientError:
            print('Cannot connect to S3')
            raise ClientError

    def get_object_list(self):
        return self._client.list_objects(Bucket=self._bucket)['Contents']
    
    def get_file(self, file_name):
        return self._client.get_object(Bucket=self._bucket, Key=file_name)['Body']