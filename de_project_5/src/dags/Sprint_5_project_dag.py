from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import psycopg2
import pendulum
from bson.json_util import dumps
from bson.json_util import loads
from airflow.models import Variable
from airflow.decorators import task_group
import requests
import json



origin_conn = 'PG_ORIGIN_BONUS_SYSTEM_CONNECTION'
dest_conn = 'PG_WAREHOUSE_CONNECTION'
api_headers = {
    'X-Nickname': 'RG',
    'X-Cohort': '5',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'   
    }
api_parameters = {'sort_field':'_id',
        'sort_direction':'desc',
        'limit':50,
        'offset':0
        }

api_base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net//"

def get_parameter(cur, table, schema, is_date=None):
    get_latest_ts = f"""select WORKFLOW_SETTINGS from {schema}.srv_wf_settings 
                            where workflow_key = '{schema}_loads_{table}';"""
    cur.execute(get_latest_ts)
    latest_ts = cur.fetchall()
    if not latest_ts:
        if is_date:
            return datetime(2022, 1, 1)
        else:
            return 0
    else:
        if is_date:
            return str(latest_ts[0][0])
        else:
            return int(latest_ts[0][0])

def add_wf_setting(cur, table, schema, value, update=None):
    if update:
        value = value + int(get_parameter(cur, table, schema, is_date=False))
    sql_remove_previous = f"delete from {schema}.srv_wf_settings where workflow_key = '{schema}_loads_{table}'"
    sql_update_wf_settings = f"INSERT INTO {schema}.srv_wf_settings (workflow_settings, workflow_key) values(%(value)s, %(workflow_key)s)"
    cur.execute(sql_remove_previous)
    cur.execute(sql_update_wf_settings, {"workflow_key":f"{schema}_loads_{table}", "value":value})


def api_load(api_method, warehouse_connection):
    postgres_hook_dest = BaseHook.get_connection(warehouse_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    api_url = api_base_url + api_method
    response = requests.get(api_url, headers=api_headers)
    content = json.loads(response.content)
    for line in content:
        sql_insert_request = """insert into stg.api_{table} (content, load_ts) values(
            %(content)s, %(ts)s) on conflict (content) do nothing;""".replace('{table}',api_method)
        cur_dest.execute(sql_insert_request, {"content":json.dumps(line), "ts": datetime.now()})
    cur_dest.close()
    conn_dest.close()

def api_inc_load(api_method, warehouse_connection, incremental_load = None):
    postgres_hook_dest = BaseHook.get_connection(warehouse_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    date_from = get_parameter(cur_dest, 'api_'+ api_method, 'stg', is_date=True)
    api_url = api_base_url + api_method + f"?from={date_from}"
    response = requests.get(api_url, headers=api_headers)
    content = json.loads(response.content)
    for line in content:
        sql_insert_request = """insert into stg.api_{table} (content, load_ts) values(
            %(content)s, %(ts)s) on conflict (content) do nothing;""".replace('{table}',api_method)
        cur_dest.execute(sql_insert_request, {"content":json.dumps(line), "ts": datetime.now()})
        latest_ts = line['delivery_ts']
    else:
        add_wf_setting(cur=cur_dest, table='api_' + api_method, schema='stg', value=latest_ts.split('.')[0])
        conn_dest.commit()
    cur_dest.close()
    conn_dest.close()

def dds_api_dm_load(table_name, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, table_name, 'DDS', is_date=False)
    limit = 500
    sql_get_data = """select content
                    from stg.{table_name}
                    where id>%(latest_id)s
                    limit %(limit)s;""".replace('{table_name}',table_name)
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for item in data:
            values = loads(item[0])
            id = values['_id']
            name = values['name']
            sql_insert_data = f"insert into dds.dm_{table_name} ({table_name[4:-1]}_id, name) values (%(id)s, %(name)s);"
            cur_dest.execute(sql_insert_data, {"id":id, "name":name})
        else:
            add_wf_setting(cur_dest, table_name, 'DDS', len(data), update=True)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_api_dm_address_load_func(table_name="api_address", destination_connection=dest_conn):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, table_name, 'DDS', is_date=False)
    limit = 500
    sql_get_data = """select content
                    from stg.api_deliveries
                    where id>%(latest_id)s
                    limit %(limit)s;""".replace('{table_name}',table_name)
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for item in data:
            values = loads(item[0])
            full_address = values['address']
            address = full_address.split(', ')
            street = address[0]
            house = address[1]
            apartment = address[2].split()[1]
            sql_insert_data = """insert into dds.dm_{table_name} (full_address, street, house_number, apartment) 
                                values (%(full_address)s, %(street)s, %(house)s, %(apartment)s)
                                on conflict(full_address) do nothing;""".replace("{table_name}", table_name)
            cur_dest.execute(sql_insert_data, {'full_address': full_address, 'street': street, 'house': house, 'apartment': apartment})
        else:
            add_wf_setting(cur_dest, table_name, 'DDS', len(data), update=True)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_api_dm_orders_load_func(table_name="api_orders", destination_connection=dest_conn):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, table_name, 'DDS', is_date=False)
    limit = 500
    sql_get_data = """select content
                    from stg.api_deliveries
                    where id>%(latest_id)s
                    limit %(limit)s;""".replace('{table_name}',table_name)
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for item in data:
            values = loads(item[0])
            order_id = values['order_id']
            order_ts = values['order_ts']
            sql_insert_data = "insert into dds.dm_{table_name} (order_id, order_ts) values (%(order_id)s, %(order_ts)s);".replace("{table_name}", table_name)
            cur_dest.execute(sql_insert_data, {'order_id': order_id, 'order_ts': order_ts})
        else:
            add_wf_setting(cur_dest, table_name, 'DDS', len(data), update=True)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()


def dds_api_dm_delivery_details_load_func(table_name="api_delivery_details", destination_connection=dest_conn):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, table_name, 'DDS', is_date=False)
    limit = 500
    sql_get_data = """select content
                    from stg.api_deliveries
                    where id>%(latest_id)s
                    limit %(limit)s;""".replace('{table_name}',table_name)
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for item in data:
            values = loads(item[0])
            courier_key = values['courier_id']
            sql_get_courier_id = """select id from dds.dm_api_couriers where courier_id = %(key)s"""
            cur_dest.execute(sql_get_courier_id, {"key":courier_key})
            # на случай, если API не отдал все ID курьеров, добавляем новый ID в таблицу, генерим имя
            courier_id = cur_dest.fetchall()
            if not courier_id:
                cur_dest.execute("select count(1) from dds.dm_api_couriers;")
                courier_id = cur_dest.fetchall()[0][0]+1
                sql_insert_courier = """insert into dds.dm_api_couriers (courier_id, name) values (%(courier_key)s, %(name)s);"""
                cur_dest.execute(sql_insert_courier, {"courier_key":courier_key, "name":"Курьер-нелегал #" + str(courier_id)})
                conn_dest.commit()
            else:
                courier_id = courier_id[0][0]

            full_address = values['address']
            sql_get_address_id = """select id from dds.dm_api_address where full_address = %(key)s"""
            cur_dest.execute(sql_get_address_id, {"key":full_address})
            address_id = cur_dest.fetchall()[0][0]
           
            delivery_id = values['delivery_id']
            delivery_ts = values['delivery_ts']
            rate = values['rate']
            sql_insert_data = """insert into dds.dm_{table_name} (delivery_id, courier_id, address_id, delivery_ts, rate) 
                                values (%(delivery_id)s, %(courier_id)s, %(address_id)s, %(delivery_ts)s, %(rate)s);""".replace("{table_name}", table_name)
            cur_dest.execute(sql_insert_data, {'delivery_id': delivery_id, 'courier_id': courier_id, 'address_id': address_id, 'delivery_ts': delivery_ts, 'rate': rate})
        else:
            add_wf_setting(cur_dest, table_name, 'DDS', len(data), update=True)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_fct_api_sales_load_func(table_name="fct_api_sales", destination_connection=dest_conn):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, table_name, 'DDS', is_date=False)
    limit = 100
    sql_get_data = """select content
                    from stg.api_deliveries
                    where id>%(latest_id)s
                    limit %(limit)s;""".replace('{table_name}',table_name)
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for item in data:
            values = loads(item[0])
            order_key = values['order_id']
            sql_get_order_id = """select id from dds.dm_api_orders where order_id = %(key)s"""
            cur_dest.execute(sql_get_order_id, {"key":order_key})
            order_id = cur_dest.fetchall()[0][0]

            delivery_key = values['delivery_id']
            sql_get_delivery_id = """select id from dds.dm_api_delivery_details where delivery_id = %(key)s"""
            cur_dest.execute(sql_get_delivery_id, {"key":delivery_key})
            delivery_id = cur_dest.fetchall()[0][0]

            sum = values['sum']
            tip_sum = values['tip_sum']
            sql_insert_data = "insert into dds.{table_name} (order_id, delivery_id, order_sum, tip_sum) values (%(order_id)s, %(delivery_id)s, %(sum)s, %(tip_sum)s);".replace("{table_name}", table_name)
            cur_dest.execute(sql_insert_data, {'order_id': order_id, 'delivery_id': delivery_id, "sum":sum, "tip_sum":tip_sum})
        else:
            add_wf_setting(cur_dest, table_name, 'DDS', len(data), update=True)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def cdm_load_dm_courier_ledger_func(destination_connection=dest_conn):
    sql_create_view ="""
            create or replace view cdm.courier_agg_with_rates as 
        (select courier_id, name, orders_count, orders_total_sum, case 
            when rate<4 then greatest(0.05*orders_total_sum,100.0)
            when rate<4.5 then greatest(0.07*orders_total_sum,150.0)
            when rate<4.9 then greatest(0.08*orders_total_sum,175.0)
            when rate>4.9 then greatest(0.1*orders_total_sum,200.0)
        end as courier_order_sum, tip_sum, year, month, rate

        from (
        select courier_id, name, count(order_id) as orders_count, sum(order_sum) as orders_total_sum,  
        sum(tip_sum) as tip_sum, year, month, avg(rate) as rate
        from cdm.courier_agg
        group by year, month, courier_id, name
        order by courier_id) agg);"""    
    sql_add_data_to_cdm = """delete from cdm.dm_courier_ledger;
    alter sequence cdm.dm_settlement_report_id_seq restart;
    with cc as (
    select courier_id, name, orders_count, orders_total_sum, courier_order_sum,
        tip_sum, year, month, rate
        from cdm.courier_agg_with_rates)
    insert into cdm.dm_courier_ledger (courier_id, courier_name, settlement_year, settlement_month,
        orders_count, orders_total_sum, rate_avg, order_processing_fee, courier_order_sum, courier_tips_sum, courier_reward_sum)
    (select courier_id, name as courier_name, year as settlement_year, month as settlement_month, orders_count, orders_total_sum, rate as rate_avg, 
        0.25*orders_total_sum as order_processing_fee, courier_order_sum, tip_sum as courier_tips_sum, courier_order_sum + tip_sum * 0.95 as courier_reward_sum 
        from cc);"""
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    cur_dest.execute(sql_create_view)
    conn_dest.commit()
    cur_dest.execute(sql_add_data_to_cdm)
    conn_dest.commit()

@dag(
    schedule_interval='0/7 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5'],
    is_paused_upon_creation=False)
def project5_dag():

#creating tasks

    @task(task_id = "api_load_restaurants_stg")
    def api_load_restaurants_task():
        api_load('restaurants', dest_conn)
    
    @task(task_id = "api_load_couriers_stg")
    def api_load_couriers_task():
        api_load('couriers', dest_conn)
    
    @task(task_id = "api_load_deliveries_stg")
    def api_load_deliveries_task():
        api_inc_load('deliveries', dest_conn)

    @task(task_id = "api_load_dm_couriers")
    def dds_api_dm_couriers_load():
        dds_api_dm_load('api_couriers', dest_conn)
    
    @task(task_id = "api_load_dm_restaurants")
    def dds_api_dm_restaurants_load():
        dds_api_dm_load('api_restaurants', dest_conn)

    @task(task_id = "api_load_dm_address")
    def dds_api_dm_address_load():
        dds_api_dm_address_load_func()
    
    @task(task_id = "api_load_dm_orders")
    def dds_api_dm_orders_load():
        dds_api_dm_orders_load_func()

    @task(task_id = "api_load_dm_delivery_details")
    def dds_api_dm_delivery_details_load():
        dds_api_dm_delivery_details_load_func()

    @task(task_id = "fct_api_sales_load")
    def dds_fct_api_sales_load():
        dds_fct_api_sales_load_func()

    @task(task_id = "load_cdm_dm_courier_ledger")
    def cdm_load_dm_courier_ledger():
        cdm_load_dm_courier_ledger_func()

# creating task groups
    @task_group
    def api_load_to_stg():
        api_load_restaurants_task()
        api_load_couriers_task()
        api_load_deliveries_task()

    @task_group
    def api_load_to_independent_dms():
        dds_api_dm_couriers_load()
        dds_api_dm_restaurants_load()
        dds_api_dm_orders_load()
        dds_api_dm_address_load()

    api_load_to_stg() >> api_load_to_independent_dms() >> dds_api_dm_delivery_details_load() >> dds_fct_api_sales_load() >> cdm_load_dm_courier_ledger()


sprint_5_dag = project5_dag()