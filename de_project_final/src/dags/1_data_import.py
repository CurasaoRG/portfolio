from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator, ShortCircuitOperator
from airflow.decorators import dag
import pendulum
from src.config import Config
from src.main import VerticaLoad



configuration = Config(env_path='src/.env')
ETL = VerticaLoad(loader=configuration.loader(),
                      repository=configuration.repository())
def DDL_check():
    if ETL._repository.check_tables_creation('stg'):
        return 'STG_tables_created'
    else:
        "STG_DDL"

def S3_check():
    return bool(ETL._loader._conn.get_object_list())
        


@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-01-01'))
def final_project_load_data():

    begin = EmptyOperator(task_id = "Begin")

    chk_DDL = BranchPythonOperator(task_id = "STG_DDL_Check", 
                                   python_callable=DDL_check, 
                                   dag=dag)    
    
    stg_ddl = PythonOperator(task_id = "STG_DDL",
                             python_callable=ETL._repository.staging_DDL,
                             dag=dag)
    
    stg_ddl_ok = EmptyOperator(task_id = "STG_tables_created")


    check_s3 = ShortCircuitOperator(task_id="Check_S3_files",
                                    python_callable = S3_check,
                                    dag=dag)

    load_stg = [
        PythonOperator(
        task_id = f"Load_{table_name}_to_STG",
        python_callable = ETL.load_data_to_staging_s3,
        op_kwargs = {"table_name":table_name},
        dag=dag
        )

    for table_name in ('currencies', 'transactions')
    ]
    
    end = EmptyOperator(task_id = "End")



    begin >> chk_DDL >> [stg_ddl, stg_ddl_ok] >> check_s3 >> load_stg  >>  end

_ = final_project_load_data()