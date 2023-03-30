from datetime import datetime
from airflow import DAG
import os
from airflow.operators.bash import BashOperator

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date':datetime.today(),
                                }

dag_spark = DAG(
                        dag_id = "Project_7_dashboards",
                        default_args=default_args,
                        schedule_interval=None,
                        )


scripts_location = '/home/src/scripts'
geo_data_location = '/user/crashmosco/data/geoinfo'
events_location = '/user/master/data/geo/events/'
analytics_location = '/user/crashmosco/analytics'
file_names =['users_stats_dashboard', 'zone_stats_dashboard', 'friends_recommendations_dashboard']


bash_command = f"spark-submit --master yarn --deploy-mode cluster {scripts_location}/user_stats_dashboard.py {events_location} {geo_data_location} {analytics_location} {str(datetime.now().date())}"

# создаем таски для аналитики
analytics_tasks = []
for file_name in file_names:
    analytics_tasks.append(
        BashOperator(
            task_id = f"analytics_task_{file_name}"
            ,bash_command = f"spark-submit --master yarn --deploy-mode cluster {scripts_location}/{file_name}.py {events_location} {geo_data_location} {analytics_location} {str(datetime.now().date())}"
            ,retries = 3
            ,dag=dag_spark
        )
    )


analytics_tasks