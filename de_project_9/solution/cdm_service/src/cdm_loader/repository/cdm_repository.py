import uuid
from datetime import datetime
from typing import Any, Dict, List

from lib.pg import PgConnect
from pydantic import BaseModel

class SQLscripts:
    def __init__(self):
        self.scripts = {"user_product_counters":"""
            INSERT INTO cdm.user_product_counters as upc
            (user_id, product_id, product_name, order_cnt)
            VALUES
            (%(user_id)s, %(product_id)s, %(product_name)s, %(order_cnt)s)
            ON CONFLICT (user_id, product_id) 
            DO UPDATE set order_cnt =  EXCLUDED.order_cnt + upc.order_cnt;
             """,
             "user_category_counters":"""
            INSERT INTO cdm.user_category_counters as ucc
            (user_id, category_id, category_name, order_cnt)
            VALUES
            (%(user_id)s, %(category_id)s, %(category_name)s, %(order_cnt)s)
            ON CONFLICT (user_id, category_id) 
            DO UPDATE set order_cnt =  EXCLUDED.order_cnt + ucc.order_cnt;
            """}



class CDMRepository:
    def __init__(self, db: PgConnect) -> None:
        self._db = db
        self.CDM_sql_scripts = SQLscripts().scripts


    def simple_insert(self, 
                    table_name: str, 
                    table_data: Dict) -> None:
        sql = self.CDM_sql_scripts[table_name]
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, table_data)