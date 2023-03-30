from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import pendulum
import boto3
import vertica_python
import json


cols_dict = {'users':['id', 'chat_name', 'registration_dt', 'country', 'age'], \
             'groups':['id', 'admin_id', 'group_name', 'registration_dt', 'is_private'], \
             'dialogs':['message_id', 'message_ts', 'message_from', 'message_to', 'message', 'message_group'],\
             'group_log':['group_id', 'user_id', 'user_id_from', 'event', 'datetime'] \
             }
sql_taks = ['DDS_DLL', 'Insert_into_l_user_group_activity', 'Insert_into_s_auth_history', 'Create_view_for_business_answer']
sql_files_dict = {'DDS_DLL':'DDS_DLL.sql', 'Insert_into_l_user_group_activity':'l_user_group_activity.sql', \
                    'Insert_into_s_auth_history':'s_auth_history.sql', 'Create_view_for_business_answer':'users_conversion.sql'}

S3_conn = Variable.get('S3_SETTINGS')
S3_conn = json.loads(S3_conn)
Vertica_conn = Variable.get('VERTICA_CONNECTION_INFO')
conn_info = json.loads(Vertica_conn)

def fetch_s3_file(bucket: str, key: str):
    session = boto3.session.Session()
    s3_client = session.client(**S3_conn)
    s3_client.download_file(
        Bucket=bucket,
        Key=key,
        Filename=f"/data/{key}")
    

def load_staging_to_vertica(table_name):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cols = ', '.join(cols_dict[table_name])
        sql = f"""
            COPY RASHITGAFAROVYANDEXRU__STAGING.{table_name} ({cols})
            FROM local '/data/{table_name}.csv'
            DELIMITER ','
            REJECTED DATA AS TABLE {table_name}_rej;
        """
        cur.execute(sql)
        conn.commit()

def execute_sql_from_file(file_name):
    with open(file_name, 'r') as f:
        commands_list = f.read().split(';')[:-1]
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        for command in commands_list:
            cur.execute(command)
            conn.commit()
    

@dag(schedule_interval=None, start_date=pendulum.parse('2022-01-01'))
def sprint6_project():

    begin = EmptyOperator(task_id = "Begin")

    end = EmptyOperator(task_id = "End")

    @task(task_id = "Fetch_file_from_S3")
    def fetch_group_log_file():
        fetch_s3_file(bucket='sprint6', key = 'group_log.csv')

    @task(task_id = "Staging_DDL")
    def staging_ddl():
        execute_sql_from_file(file='/sql/STG_DDL.sql')
    
    @task(task_id="load_group_log_to_staging")
    def load_group_log_stg():
        load_staging_to_vertica('group_log')
    
    sql_tasks = []
    for task_id in ['DDS_DLL', 'Insert_into_l_user_group_activity', 'Insert_into_s_auth_history', 'Create_view_for_business_answer']:
        sql_tasks.append(
           PythonOperator(task_id=task_id,
                    python_callable=execute_sql_from_file,
                    op_kwargs = {'file':'/sql/'+sql_files_dict[task_id]}))

    begin >> fetch_group_log_file() >> staging_ddl() >> load_group_log_stg() >> sql_tasks >> end


_ = sprint6_project()