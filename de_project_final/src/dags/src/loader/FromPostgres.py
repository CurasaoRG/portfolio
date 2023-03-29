from connectors.Postgres import PgConnect
import pandas as pd
from sqlalchemy import text


class FromPostgres:
    def __init__(self,
                 db: PgConnect):
        self._db = db
        self.filter_cols = {'transactions':'transaction_dt', 'currencies':'date_update'}
    
    # def get_data_io(self, 
    #              table_name:str):
    #     input = io.StringIO()        
    #     with self._db.connection() as conn:
    #         with conn.cursor() as cur:
    #             query = f"SELECT * FROM public.transactions limit 0;"
    #             df = pd.read_sql(text(query), self._db)
    #             cur.copy_expert(f"""COPY (select {cols},
    #                 now() as load_dt, 
    #                 'postgres' as load_src
    #                 from public.transactions where transaction_dt>'{dt}' limit {limit}) TO STDOUT DELIMITER '|';""", input)
    #             query = f"SELECT * FROM public.transactions where transaction_dt>'{dt}' limit {limit};"
    #     return input
 
    def get_data(self, 
                 table_name:str,
                 dt:str):
        filter_col = self.filter_cols[table_name]
        if not dt:
            dt = pd.read_sql(text(f"select min({filter_col}) from public.{table_name};"), self._db.connection())\
                .to_string(header=False, index=False)
        df = pd.read_sql(text(f"""select *,
                    now() as load_dt, 
                    'testing' as load_src
                    from public.{table_name} where {filter_col} between '{dt}' and '{dt}'::date+1;"""), self._db.connection())
        return df.to_csv(index=False, header=False, sep='|')

    def get_dt(self, table_name):
        filter_col = self.filter_cols[table_name]
        return pd.read_sql(text(f"select min({filter_col}) from public.{table_name};"), self._db.connection())\
                .to_string(header=False, index=False)