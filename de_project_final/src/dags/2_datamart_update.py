from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.decorators import dag
import pendulum
from src.config import Config
from src.main import VerticaLoad



configuration = Config(env_path='src/.env')
ETL = VerticaLoad(loader=configuration.loader(),
                      repository=configuration.repository())
def DDL_check():
    if ETL._repository.check_tables_creation('dwh'):
        return 'DWH_tables_created'
    else:
        "DWH_DDL"


@dag(schedule_interval='0 0 * * *', start_date=pendulum.parse('2022-01-01'))
def final_project_load_dwh():

    begin = EmptyOperator(task_id = "Begin")

    chk_DDL = BranchPythonOperator(task_id = "DWH_DDL_Check", 
                                   python_callable=DDL_check, 
                                   dag=dag)  
    
    dwh_ddl_ok = EmptyOperator(task_id = "DWH_tables_created")

    dwh_ddl = PythonOperator(
        task_id = "DWH_DDL",
        python_callable = ETL._repository.DWH_DDL,
        dag=dag
    ) 

    load_dwh = PythonOperator(
        task_id = "Load_global_metrics_to_DWH",
        python_callable = ETL.load_data_to_dwh,
        dag=dag
    )

    end = EmptyOperator(task_id = "End")

    begin >> chk_DDL >> [dwh_ddl, dwh_ddl_ok]  >> load_dwh >> end

_ = final_project_load_dwh()