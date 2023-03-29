import datetime
from dateutil import parser
from src.config import Config
from json import loads, dumps
import sys
from src.loader.FromS3 import FromS3
from src.repository.ToVertica import ToVertica

# класс для всех операций с данными

class VerticaLoad:

    def __init__(self,
                 loader:FromS3,
                 repository:ToVertica):
        self._loader = loader
        self._repository = repository

    
    def load_data_to_staging_pg(self, table_name, load_to_dt=datetime.date.today()):
        """
        метод для инкрементальной загрузки данных из Postgres в Vertica
        """
        dt = self._repository.get_increment_settings(table_name)
        if not dt: 
            dt = self._loader.get_dt(table_name)
        else: 
            dt = dt[0][0]
        dt = parser.parse(dt).date()
        while dt != load_to_dt:
            data = self._loader.get_data(table_name, dt)
            self._repository.load_to_stg(table_name, data)
            self._repository.update_increment_settings(table_name, dt)
            dt = dt + datetime.timedelta(days=1)
    
    def load_data_to_staging_s3(self, table_name):
        """
        Метод для инкрементальной загрузки данных из хранилища S3 в Vertica. 
        Логика работы: 
        1 получаю список файлов с датами обновления - метод класса ToVertica - get_files_list
        2 сравниваю полученный список с технической таблицей wf_settings, нахожу файлы, которые нужно обновить
        3 Загружаю обновленные файлы в соответствующие таблицы
        4 обновляю таблицу wf_settings
        """
        files_list = self._loader.get_files_list(table_name)
        loaded = self._repository.get_increment_settings(table_name, source="S3")
        if loaded: 
            loaded = loads(loaded[0][0])
            for key, val in files_list.items():
                if val>loaded.get(key, '2022-01-01'):
                    data = self._loader.load_file(key)
                    self._repository.load_to_stg(table_name, data)
            self._repository.update_increment_settings(table_name, dumps(files_list), source="S3")         
        else:
            for key in files_list.keys():
                data = self._loader.load_file(key)
                self._repository.load_to_stg(table_name, data)
            self._repository.update_increment_settings(table_name, dumps(files_list), source="S3")

    def load_data_to_dwh(self, table_name='global_metrics'):
        """
        Метод для загрузки данных из STAGING в DWH, логика реализована в классе ToVertica
        """
        self._repository.load_to_dwh(table_name)


if __name__ == "__main__":
    # логика работы на случай, если файл запускается
    # созданы различные сценарии работы
    configuration = Config(env_path='.env')
    ETL = VerticaLoad(loader=configuration.loader(),
                      repository=configuration.repository())
    # считываем входные параметры и задаем различные результаты выполнения скрипта в зависимости от входных параметров
    try:
        action = sys.argv[1]
    except IndexError:
        action = 'stg'
    try:
        table_name = sys.argv[2]
    except IndexError:
        table_name = None

    if action == 'stg':
        if not table_name:
            ETL.load_data_to_staging_s3('currencies')
            ETL.load_data_to_staging_s3('transactions')
        elif table_name in ['currencies', 'transactions']:
            ETL.load_data_to_staging_s3(table_name)
        else:
            pass
    elif action == 'dwh':
        if table_name == 'global_metrics':
            ETL.load_data_to_dwh('global_metrics')
        else:
            pass
    elif action == 'stg-ddl':
        ETL._repository.staging_DDL()
    elif action == "dwh-ddl":
        ETL._repository.DWH_DDL()
    else:
        print('please specify correct action: stg or dwh for data load (table name is required), stg-ddl or dwh-ddl for table creation')