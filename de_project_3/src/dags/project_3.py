from curses import update_lines_cols
import time
from venv import create
import requests
import json
import pandas as pd

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
import psycopg2

http_conn_id = HttpHook.get_connection('http_conn_id')
api_key = http_conn_id.extra_dejson.get('api_key')
base_url = http_conn_id.host

postgres_conn_id = 'postgresql_de'

nickname = 'RG'
cohort = '5'

headers = {
    'X-Nickname': nickname,
    'X-Cohort': cohort,
    'X-Project': 'True',
    'X-API-KEY': api_key,
    'Content-Type': 'application/x-www-form-urlencoded'
}


def generate_report(ti):
    print('Making request generate_report')

    response = requests.post(f'{base_url}/generate_report', headers=headers)
    response.raise_for_status()
    task_id = json.loads(response.content)['task_id']
    ti.xcom_push(key='task_id', value=task_id)
    print(f'Response is {response.content}')


def get_report(ti):
    print('Making request get_report')
    task_id = ti.xcom_pull(key='task_id')

    report_id = None

    for i in range(20):
        response = requests.get(f'{base_url}/get_report?task_id={task_id}', headers=headers)
        response.raise_for_status()
        print(f'Response is {response.content}')
        status = json.loads(response.content)['status']
        if status == 'SUCCESS':
            report_id = json.loads(response.content)['data']['report_id']
            break
        else:
            time.sleep(10)

    if not report_id:
        raise TimeoutError()

    ti.xcom_push(key='report_id', value=report_id)
    print(f'Report_id={report_id}')


def get_increment(date, ti):
    print('Making request get_increment')
    report_id = ti.xcom_pull(key='report_id')
    response = requests.get(
        f'{base_url}/get_increment?report_id={report_id}&date={str(date)}T00:00:00',
        headers=headers)
    response.raise_for_status()
    print(f'Response is {response.content}')

    increment_id = json.loads(response.content)['data']['increment_id']
    ti.xcom_push(key='increment_id', value=increment_id)
    print(f'increment_id={increment_id}')

def upload_data_to_staging(filename, date, pg_table, pg_schema, increment_load, ti):
    if increment_load:
        increment_id = ti.xcom_pull(key='increment_id')
        s3_filename = f'https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{increment_id}/{filename}'
    else:
        report_id = ti.xcom_pull(key='report_id')
        s3_filename = f"https://storage.yandexcloud.net/s3-sprint3/cohort_{cohort}/{nickname}/project/{report_id}/{filename}"
    try:
        if ti.xcom_pull('initial_load_status')  == 'Completed':
            return None 
    except KeyError:
        pass

    local_filename = date.replace('-', '') + '_' + filename

    response = requests.get(s3_filename)
    open(f"{local_filename}", "wb").write(response.content)

    df = pd.read_csv(local_filename)
    df.drop_duplicates(subset=['id'])

    postgres_hook = PostgresHook(postgres_conn_id)
    engine = postgres_hook.get_sqlalchemy_engine()
    df.to_sql(pg_table, engine, schema=pg_schema, if_exists='append', index=False)
    if not increment_load:
        ti.xcom_push('initial_load_status','Completed')

def incorporate_status_column(table, ti):
    pg_conn = BaseHook.get_connection('pg_connection')
    alter_table_sql_script = f"ALTER TABLE {table} ADD COLUMN if not exists status varchar(15) NOT NULL Default 'shipped';"
    with psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'") as conn:
        cur = conn.cursor()
        cur.execute(alter_table_sql_script)
        conn.commit()

def update_f_customer_retention(ti):
    pg_conn = BaseHook.get_connection('pg_connection')
    # скрипт для создания структуры таблицы mart._f_customer_retention
    create_f_retention_table_script = """
    create table if not exists mart.f_customer_retention (
        new_customers_count bigint default 0,
        returning_customers_count bigint default 0,
        refunded_customers_count bigint default 0,
        period_name varchar(7) default 'weekly',
        period_id bigint,
        item_id int4,
        new_customers_revenue numeric(10,2) default 0,
        returning_customers_revenue numeric (10,2) default 0,
        customers_refunded bigint default 0
        );
    """
    # скрипт для создания вспомогательного представления с количеством заказов
    create_customers_count_view = """create or replace view staging.customers_count as (
        select week_id, item_id, customer_id, sum(case 
        when status = 'shipped' then 1
        else 0
        end) as shipped_qty, 
        sum(case 
        when status = 'refunded' then 1
        else 0
        end) as refunded_qty,
        sum(case
        when status = 'refunded' then payment_amount
        else 0
        end) as refunded,
        sum(case 
            when status = 'shipped' then payment_amount
            else 0
        end) as payment_amount
        from (
        select 
        s.customer_id, s.payment_amount, s.item_id, 
        100*c.year_actual+c.week_of_year as week_id, 
        status
        from mart.f_sales s
        left join mart.d_calendar c
        on s.date_id = c.date_id) all_cust
        group by week_id, customer_id, item_id);"""
    # Скрипты для представлений для отдельных компонентов витрины: данные по новым заказчикам, данные по возвращающимся заказчикам, данные по возвратам
    create_f_new_customers_view = """
    CREATE or replace VIEW mart.f_new_customers as
        select week_id, item_id, count(customer_id) as new_customers_count, sum(payment_amount) as new_customers_revenue from staging.customers_count where shipped_qty = 1
        group by week_id, item_id;"""
    create_f_returning_customers_view = """
    CREATE VIEW mart.f_returning_customers as
        select week_id, item_id, count(customer_id) as returning_customers_count, sum(payment_amount) as returning_customers_revenue from staging.customers_count where shipped_qty > 1
        group by week_id, item_id;
    """
    create_f_refunded_customers_view = """
    CREATE or replace VIEW mart.f_refunded_customers as
        select week_id, item_id, count(customer_id) as refunded_customers_count, sum(payment_amount) as customers_refunded from staging.customers_count where refunded_qty > 0
        group by week_id, item_id;
    """
    #Скрипт для объединения данных из представлений в витрину
    fill_f_customers_retention_table="""
    with wi as (select 
        distinct 100*c.year_actual+c.week_of_year as week_id, item_id
        from mart.f_sales s
        left join mart.d_calendar c
        on s.date_id = c.date_id)
    insert into  mart.f_customer_retention 
        (new_customers_count, returning_customers_count, refunded_customers_count, 
        period_id, item_id, 
        new_customers_revenue, returning_customers_revenue, customers_refunded)
    (select nc.new_customers_count, rc.returning_customers_count, rfc.refunded_customers_count, wi.week_id as period_id, wi.item_id, 
        nc.new_customers_revenue, rc.returning_customers_revenue, rfc.customers_refunded
    from wi left join mart.f_new_customers nc 
        on wi.week_id = nc.week_id and wi.item_id = nc.item_id
    left join mart.f_returning_customers rc
        on wi.week_id = rc.week_id and wi.item_id = rc.item_id
    left join mart.f_refunded_customers rfc
        on wi.week_id = rfc.week_id and wi.item_id = rfc.item_id);
    """
    all_scripts = [create_f_retention_table_script, create_customers_count_view, create_f_new_customers_view, create_f_returning_customers_view, create_f_refunded_customers_view, fill_f_customers_retention_table]
    with psycopg2.connect(f"dbname='de' port='{pg_conn.port}' user='{pg_conn.login}' host='{pg_conn.host}' password='{pg_conn.password}'") as conn:
        cur = conn.cursor()
        for script in all_scripts:
            cur.execute(script)
            conn.commit()
        
args = {
    "owner": "student",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

business_dt = '{{ ds }}'

with DAG(
        'customer_retention',
        default_args=args,
        description='Provide default dag for sprint3',
        catchup=True,
        start_date=datetime.today() - timedelta(days=8),
        end_date=datetime.today() - timedelta(days=1),
) as dag:
    generate_report = PythonOperator(\
        task_id='generate_report',
        python_callable=generate_report)

    get_report = PythonOperator(
        task_id='get_report',
        python_callable=get_report)
    
    upload_user_order = PythonOperator(
        task_id='upload_user_order',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging', 
                   'increment_load':'False'})

    get_increment = PythonOperator(
        task_id='get_increment',
        python_callable=get_increment,
        op_kwargs={'date': business_dt})

    upload_user_order_inc = PythonOperator(
        task_id='upload_user_order_inc',
        python_callable=upload_data_to_staging,
        op_kwargs={'date': business_dt,
                   'filename': 'user_orders_log_inc.csv',
                   'pg_table': 'user_order_log',
                   'pg_schema': 'staging', 
                   'increment_load':'True'})
    
    incorporate_status_column_user_orders = PythonOperator(
        task_id='incorporate_status_column',
        python_callable=incorporate_status_column,
        op_kwargs={'table': 'staging.user_order_log'})

    update_d_item_table = PostgresOperator(
        task_id='update_d_item',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_item.sql")

    update_d_customer_table = PostgresOperator(
        task_id='update_d_customer',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_customer.sql")

    update_d_city_table = PostgresOperator(
        task_id='update_d_city',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.d_city.sql")

    update_f_sales = PostgresOperator(
        task_id='update_f_sales',
        postgres_conn_id=postgres_conn_id,
        sql="sql/mart.f_sales.sql",
        parameters={"date": {business_dt}}
    )

    update_f_customer_retention = PythonOperator(
        task_id='update_f_customer_retention',
        python_callable=update_f_customer_retention
        )

    (
            generate_report
            >> get_report
            >> get_increment
            >> upload_user_order
            >> incorporate_status_column_user_orders 
            >> upload_user_order_inc 
            >> [update_d_item_table, update_d_city_table, update_d_customer_table]
            >> update_f_sales
            >> update_f_customer_retention
    )

