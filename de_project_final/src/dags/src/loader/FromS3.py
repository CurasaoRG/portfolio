from src.connectors import S3Connector
import datetime

import pandas as pd

class FromS3:
    """
    Класс для загрузки данных из хранилища S3, для подключения к S3 использует класс S3Connector. 
    Содержит методы для работы с файлами. 
    """
    def __init__(self, 
                 conn:S3Connector) -> None: 
        self._conn = conn
    

    def get_files_list(self, table_name):
        """
        Метод для получения списка файлов из хранилища, на выходе - словарь: имя файла - время изменения файла.
        """
        res = {}
        for item in self._conn.get_object_list():
                if table_name in item['Key']: 
                    res[item['Key']] = str(item['LastModified'])
        return res

    def load_file(self, file_name):
        """
        Метод для загрузки файла из S3, возвращает дополненные данные в CSV формате, удобном для записи в Vertica.
        """
        df = pd.read_table(self._conn.get_file(file_name), delimiter=',')
        df['load_dt'] = datetime.datetime.now()
        df['load_src'] = 'S3'
        if 'currencies' in file_name:
            df = df.loc[:, ['date_update', 'currency_code', 'currency_code_with', 'currency_with_div', 'load_dt', 'load_src']]
        return df.to_csv(index=False, header=False, sep='|')