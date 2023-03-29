import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel
# from psycopg2.errors import InvalidTextRepresentation

class SQLscripts:
    def __init__(self):
        # SQL_PATH = "/home/rg/Documents/Study/de-project-sprint-9/solution/service_dds/src/dds_loader/repository/SQL/DDS_scripts.sql"
        SQL_PATH = "/src/dds_loader/repository/SQL/DDS_scripts.sql"
        with open(SQL_PATH, "r") as f:
            sql_list = f.read().split("---")[1:]
            self.scripts = {}
            for i in range(len(sql_list)//2):
                self.scripts[sql_list[i*2].strip()] = sql_list[i*2+1].strip()
            # self.scripts = {key.strip('\n'):val.strip('\n')  for key in sql_list[::2] for val in sql_list[1::2]}




class DdsRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self.DDS_sql_scripts = SQLscripts().scripts


    def simple_insert(self, 
                    table_name: str, 
                    table_data: Dict) -> None:
        # print(self.DDS_sql_scripts)
        sql = self.DDS_sql_scripts[table_name]
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                # print(table_name)
                # print(sql)
                # print(table_data)
                cur.execute(sql, table_data)



    

    # можно добавить генерацию SQL сюда,   
    # def table_insert(self,
    #                         table_name: Str,
    #                         table_fields: List[str],
    #                         table_data: Dict
    #                         ) -> None:
    #     with self._db.connection() as conn:
    #         with conn.cursor() as cur:
    #             unique_key = table_fields[0]
    #             field_names = ', '.join(table_fields)
    #             fields_values = f'{")s, %(".join(table_fields)}'
    #             table_type = 'h'
    #             sql = f"""INSERT INTO TABLE dds.h_{table_name}
    #                             (h_{table_name}_pk,
    #                             {field_names},
    #                             load_dt, 
    #                             load_src)
    #                             values
    #                             ( %({table_type }_{table_name}_pk)s, %({fields_values})s, %(load_dt)s, %(load_src)s)
    #                             ON CONFLICT ({unique_key})
    #                             DO NOTHING;"""
    #             table_data.update({
    #                 "load_dt":str(datetime.now()),
    #                 "load_src":"Kafka-stg-service-orders", 
    #                 f"h_{table_name}_pk":uuid.uuid3(uuid.NAMESPACE_OID, str(table_data[unique_key])) })
    #             cur.execute(sql, table_data)

    # def link_table_insert(self,
    #                         table_name: Str,
    #                         table_data: Dict
    #                         ) -> None:
    #     with self._db.connection() as conn:
    #         with conn.cursor() as cur:
    #             table_type, table_fields = table_name.split('_', 1) 
    #             unique_key = f"hk_{table_fields}_pk"
    #             field_names = ', '.join(table_fields.split('_'))
    #             fields_values = f'{")s, %(".join(table_fields)}'

    #             # l_product_restaurant
    #             # hk_product_restaurant_pk
    #             sql = f"""INSERT INTO TABLE dds.h_{table_name}
    #                             ({unique_key},
    #                             {field_names},
    #                             load_dt, 
    #                             load_src)
    #                             values
    #                             ( %({table_type }_{table_name}_pk)s, %({fields_values})s, %(load_dt)s, %(load_src)s)
    #                             ON CONFLICT ({unique_key})
    #                             DO NOTHING;"""
    #             table_data.update({
    #                 "load_dt":str(datetime.now()),
    #                 "load_src":"Kafka-stg-service-orders", 
    #                 f"h_{table_name}_pk":uuid.uuid3(uuid.NAMESPACE_OID, str(table_data[unique_key])) })
    #             cur.execute(sql, table_data)