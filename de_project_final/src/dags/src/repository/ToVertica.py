from src.connectors.Vertica import VerticaConnector
from typing import Any, Dict
import datetime
from dateutil import parser

class ToVertica:
    """
    Класс ToVertica содержит всю логику для работы с Vertica. 
    """
    def __init__(self,
                 db: VerticaConnector,
                 schemas:Dict,
                 tables:Dict) -> None:
        self._db = db
        self.settings_table = "srv_wf_settings"
        self.sql = SQLscripts()
        self.schemas = schemas
        self.tables = tables
    
    def staging_DDL(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql.stg_ddl)
            conn.commit()

    def DWH_DDL(self):
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(self.sql.dwh_ddl)
            conn.commit()
    
    def get_increment_settings(self, 
                               table_name:str,
                               source:str,
                               schema = None) -> str:
        if not schema: schema = self.schemas['stg']
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT workflow_settings from {schema}.{self.settings_table} where workflow_key='{schema}_loads_{source}_{table_name}'")
                res = cur.fetchall()
        return res

    def update_increment_settings(self, 
                                  table_name:str, 
                                  data:Any,
                                  source:str,
                                  schema=None) -> None:
        if not schema: schema = self.schemas['stg']
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"delete from {schema}.{self.settings_table} where workflow_key='{schema}_loads_{source}_{table_name}';")
                cur.execute(f"""insert into {schema}.{self.settings_table} 
                            (workflow_key, workflow_settings) 
                            values ('{schema}_loads_{source}_{table_name}', '{data}');""")
            conn.commit()
    
    def check_tables_creation(self, schema):
        sql = f"""SELECT distinct table_name FROM all_tables
          WHERE table_name in ({', '.join(self.tables[schema])})
         and schema_name like {self.schemas[schema]}"""
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql)
                res = cur.fetchall()
        if not res:
            return False
        else:
            return len(res[0]) == len(self.tables[schema])
        
    
    def load_to_stg(self,
                    table_name,
                    data) -> None:
         if data:
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.copy(f'''COPY {self.schemas['stg']}.{table_name} FROM STDIN REJECTED DATA AS TABLE {self.schemas['stg']}.{table_name}_rej;''', data)
                conn.commit()
   
    def load_to_dwh(self, 
                    table_name):
            inc = self.get_increment_settings(table_name, schema=self.schemas['dwh'], source="STG")[0][0]
            if not inc:
                inc = datetime.date.today() - datetime.timedelta(days=1)
            sql = f"""
MERGE INTO {self.schemas['dwh']}.{table_name} AS gm USING
    (SELECT *,
            now()::date AS load_dt,
            'STG' AS load_src
    FROM
        (SELECT t.date_update,
                t.cnt_transactions,
                t.cnt_accounts_make_transactions,
                t.currency_from,
                t.avg_transactions_per_account,
                t.amount_total_local*c.currency_with_div AS amount_total
    FROM
        (SELECT count(operation_id) AS cnt_transactions,
                count(DISTINCT account_number_from) AS cnt_accounts_make_transactions,
                currency_code AS currency_from,
                sum(amount)/count(DISTINCT account_number_from) AS avg_transactions_per_account,
                sum(amount) AS amount_total_local,
                transaction_dt::date AS date_update
            FROM {self.schemas['stg']}.transactions t
            WHERE status = 'done' AND account_number_from>0
            AND load_dt > '{inc}'::date
            GROUP BY currency_code,
                    date_update) t
    LEFT JOIN {self.schemas['stg']}.currencies c ON t.currency_from = c.currency_code
        AND t.date_update = c.date_update::date
    WHERE c.currency_code_with = 420) ctt) ct ON (gm.date_update = ct.date_update
                                                    AND gm.currency_from = ct.currency_from) 
    WHEN NOT MATCHED THEN
        INSERT (date_update,
                currency_from,
                amount_total,
                cnt_transactions,
                avg_transactions_per_account,
                cnt_accounts_make_transactions,
                load_dt,
                load_src)
        VALUES (ct.date_update, 
                ct.currency_from, 
                ct.amount_total, 
                ct.cnt_transactions, 
                ct.avg_transactions_per_account, 
                ct.cnt_accounts_make_transactions, 
                ct.load_dt, 
                ct.load_src);
                    """
            inc = parser.parse(inc).date()
            with self._db.connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql)
                conn.commit()
            self.update_increment_settings(table_name, inc + datetime.timedelta(days=1), schema=self.schemas['dwh'], source='STG')


class SQLscripts:
    def __init__(self):
        self.stg_ddl = """CREATE TABLE IF NOT EXISTS RASHITGAFAROVYANDEXRU__STAGING.transactions (
	operation_id varchar(60) NULL,
	account_number_from int NULL,
	account_number_to int NULL,
	currency_code int NULL,
	country varchar(30) NULL,
	status varchar(30) NULL,
	transaction_type varchar(30) NULL,
	amount int NULL,
	transaction_dt timestamp NULL,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(operation_id) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS RASHITGAFAROVYANDEXRU__STAGING.currencies (
	date_update timestamp NULL,
	currency_code int NULL,
	currency_code_with int NULL,
	currency_with_div numeric(5, 3) NULL,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(date_update, currency_code) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);
--srv_wf_settings
CREATE TABLE if not exists RASHITGAFAROVYANDEXRU__STAGING.srv_wf_settings
(
    workflow_key varchar(80),
    workflow_settings varchar(80)
);
--transactions_proj
CREATE PROJECTION IF NOT EXISTS transactions_proj as 
select 
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	"status",
	transaction_type,
	amount,
	transaction_dt,
	load_dt,
    load_src
FROM
RASHITGAFAROVYANDEXRU__STAGING.transactions t
ORDER BY transaction_dt 
SEGMENTED BY hash(operation_id) all nodes;
--currencies_proj
CREATE PROJECTION IF NOT EXISTS currencies_proj
AS SELECT 
	date_update,
	currency_code,
	currency_code_with,	
	currency_with_div,
	load_dt ,
    load_src
FROM RASHITGAFAROVYANDEXRU__STAGING.currencies
ORDER BY date_update 
SEGMENTED BY hash(date_update, currency_code) all nodes;

"""
        self.dwh_ddl = """create table if not exists
RASHITGAFAROVYANDEXRU__DWH.global_metrics
(    date_update date, 
    currency_from int, 
    amount_total numeric(7,3),
    cnt_transactions int, 
    avg_transactions_per_account numeric(7,3),
    cnt_accounts_make_transactions int,
	load_dt datetime,
    load_src varchar(20)
)
ORDER BY load_dt 
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);"""
        
        self.check_stg_ddl = """SELECT distinct table_name FROM all_tables
          WHERE table_name in ('transactions', 'currencies')
         and schema_name like '%STAGING%'"""