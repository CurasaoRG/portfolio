from datetime import datetime
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
import psycopg2
import pandas as pd
import numpy as np
import pendulum
from sqlalchemy import create_engine
from sqlalchemy.types import Float
from pymongo import MongoClient
from urllib.parse import quote_plus as quote
from bson.json_util import dumps
from bson.json_util import loads
import psycopg2
from airflow.models import Variable
from dateutil import parser
from airflow.decorators import task_group
from dotenv import dotenv_values
import json

parameters = dotenv_values('.env')
origin_conn = parameters['ORIGIN_CONNECTION']
dest_conn = parameters['DESTINATION_CONNECTION']
cols = json.loads(parameters['COLS'])
col_ranks = cols['RANKS']
col_users = cols['USERS']
MONGO_VARIABLES_NAMES = json.loads(parameters['MONGO_VARIABLES_NAMES'])
api_headers = json.loads(parameters['API_HEADERS'])
api_parameters = json.loads(parameters['API_PARAMETERS'])
api_base_url = parameters['API_URL']


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
        value = value + int(get_parameter(cur, table, schema))
    sql_remove_previous = f"delete from {schema}.srv_wf_settings where workflow_key = '{schema}_loads_{table}'"
    sql_update_wf_settings = f"INSERT INTO {schema}.srv_wf_settings (workflow_settings, workflow_key) values(%(value)s, %(workflow_key)s)"
    cur.execute(sql_remove_previous)
    cur.execute(sql_update_wf_settings, {"workflow_key":f"{schema}_loads_{table}", "value":value})


def extract_func(origin_connection, destination_connection, orig_schema, orig_table, dest_schema, dest_table, extra):
    postgres_hook_org  = PostgresHook(origin_connection)
    postgres_hook_dest = PostgresHook(destination_connection)
    engine_org  = postgres_hook_org.get_sqlalchemy_engine()
    engine_dest = postgres_hook_dest.get_sqlalchemy_engine()
    sql_get_all_from_org = """select id, name, bonus_percent, min_payment_threshold from public.ranks;"""

    if extra:
        with psycopg2.connect("dbname='de-public' port='6432' user='student' host='rc1a-1kn18k47wuzaks6h.mdb.yandexcloud.net' password='student1'") as conn_org:
            cur = conn_org.cursor()
            cur.execute(sql_get_all_from_org)
            data = cur.fetchall()
        with psycopg2.connect("dbname='de' port='5432' user='jovyan' host='localhost' password='jovyan'") as conn_dest:
            cur = conn_dest.cursor()    
            for line in data:
                id, name, bonus_percent, min_payment_threshold = line
                sql_insert_line = "insert into stg.bonussystem_ranks (id, name, bonus_percent, min_payment_threshold) values(%(id)s, %(name)s, %(bonus_percent)s, %(min_payment_threshold)s)"
                cur.execute(sql_insert_line, {"id":id, "name":name, "bonus_percent":bonus_percent, "min_payment_threshold":min_payment_threshold})
                conn_dest.commit()
        conn_org.close()
        conn_dest.close()
    else:
        df = pd.read_sql_table(table_name=orig_table, con=engine_org, schema='public')
        df.to_sql(name=dest_table, con=engine_dest, schema='stg', if_exists='replace', index=False)


def events_load_func(origin_connection, destination_connection):
    postgres_hook_org = BaseHook.get_connection(origin_connection)
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_org =   psycopg2.connect(f"dbname='de-public' port={postgres_hook_org.port} user={postgres_hook_org.login} host={postgres_hook_org.host} password={postgres_hook_org.password}")
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_org = conn_org.cursor()
    cur_dest = conn_dest.cursor()
    data = get_parameter(cur_dest, 'events', 'stg', is_date=None)
    sql_fetch_data_from_outbox = f"""SELECT id, event_ts, event_type, event_value
                                    from public.outbox
                                    where id>{data}
                                    order by id asc;"""
    cur_org.execute(sql_fetch_data_from_outbox)
    outbox_data = cur_org.fetchall()
    if outbox_data:
        for line in outbox_data:
            id, event_ts, event_type, event_value = line
            sql_insert_line = """insert into stg.bonussystem_events (id, event_ts, event_type, event_value) values(%(id)s, %(event_ts)s, %(event_type)s, %(event_value)s)
            on conflict(id) do nothing;
            """
            cur_dest.execute(sql_insert_line, {"id":id, "event_ts":event_ts, "event_type":event_type, "event_value":event_value})
        else:
            add_wf_setting(cur_dest, 'events', 'stg', id)
    conn_dest.commit()
    cur_org.close()
    cur_dest.close()
    conn_org.close()
    conn_dest.close()


def mongodb_collection_load(collection_name, destination_connection):
    MONGO_DB_CERTIFICATE_PATH, MONGO_DB_USER, MONGO_DB_PASSWORD,MONGO_DB_HOST,MONGO_DB_REPLICA_SET, MONGO_DB_DATABASE_NAME = list(Variable.get(x) for x in MONGO_VARIABLES_NAMES)
    mongo_connection = f"mongodb://{MONGO_DB_USER}:{MONGO_DB_PASSWORD}@{MONGO_DB_HOST}/{MONGO_DB_DATABASE_NAME}?authMechanism=DEFAULT&authSource={MONGO_DB_DATABASE_NAME}&tls=true&tlsCAFile={quote(MONGO_DB_CERTIFICATE_PATH)}&replicaSet={MONGO_DB_REPLICA_SET}"
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    last_update = get_parameter(cur_dest, table='MongoDB_'+collection_name, schema='stg', is_date=True)
    mongo_connect = MongoClient(mongo_connection)
    db = mongo_connect[MONGO_DB_DATABASE_NAME]
    collection = db[collection_name]
    filter = {'update_ts': {'$gt': parser.parse(last_update)}}
    sort = [('update_ts', 1)]
    limit = 1000
    docs = list(collection.find(filter=filter, sort=sort, limit=limit))
    dest_database = 'ordersystem_'+ collection_name
    if docs: 
        for single_doc in docs:
            id = str(single_doc['_id'])
            object_value = dumps(single_doc)
            update_ts = single_doc['update_ts']
            sql_insert_line = """insert into stg.database (object_id, object_value, update_ts) values(%(object_id)s, %(object_value)s, %(update_ts)s)
                                 on conflict (object_id) do nothing;
            """.replace('database', dest_database)
            cur_dest.execute(sql_insert_line, {"object_id":id, "object_value":object_value, "update_ts":update_ts})
        else:
            add_wf_setting(cur_dest, 'MongoDB_'+ collection_name, 'stg', update_ts)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()


def dds_dm_load_users(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_id = get_parameter(cur_dest, dds_table, 'DDS', is_date=False)
    limit = 100
    sql_get_data = """select id, object_id, object_value
                    from stg.ordersystem_users
                    where id>%(latest_id)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"latest_id":latest_id, "limit":limit})
    data = cur_dest.fetchall()
    if data:
        for line in data:
            id = line[0]
            user_id = line[1]
            values = loads(line[2])
            sql_insert_data = "insert into dds.dm_users (user_id, user_name, user_login) values (%(user_id)s,%(user_name)s,%(user_login)s);"
            cur_dest.execute(sql_insert_data, {"user_id":user_id, "user_name":values['name'], "user_login":values['login']})
        else:
            add_wf_setting(cur_dest, 'DDS', dds_table, id)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_dm_restaurants_load(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_ts = get_parameter(cur_dest, dds_table, 'DDS', is_date=True)
    limit = 100
    sql_get_data = """select object_id, object_value, update_ts
                    from stg.ordersystem_restaurants
                    where update_ts>%(latest_ts)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"limit":limit, "latest_ts":str(latest_ts)})
    data = cur_dest.fetchall()
    if data:
        for line in data:
            rest_id = line[0]
            values = loads(line[1])
            sql_insert_data = "insert into dds.dm_restaurants (restaurant_id, restaurant_name, active_from, active_to) values (%(rest_id)s,%(rest_name)s,TIMESTAMP WITH TIME ZONE %(act_from)s,TIMESTAMP WITH TIME ZONE %(act_to)s);"
            cur_dest.execute(sql_insert_data, {"rest_id":rest_id, "rest_name":values['name'], "act_from":'2022-09-18 18:30:14+03:00', "act_to":'2099-12-31 03:00:00+03:00'})
        else:
            add_wf_setting(cur_dest, 'DDS', dds_table, values['update_ts'])
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_dm_timestamps_load(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_ts = get_parameter(cur_dest, dds_table, 'DDS', is_date=True)
    limit = 1000
    sql_get_data = """select object_id, object_value, update_ts
                    from stg.ordersystem_orders
                    where update_ts>%(latest_ts)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"limit":limit, "latest_ts":latest_ts})
    data = cur_dest.fetchall()
    if data:
        for line in data:
            values = loads(line[1])
            if values['final_status'] == 'CLOSED' or values['final_status'] == 'CANCELLED':
                ts = values['update_ts']
                sql_insert_data = "insert into dds.dm_timestamps (ts, year, month, day, time, date) values (%(ts)s, %(year)s, %(month)s, %(day)s, %(time)s, %(date)s);"
                cur_dest.execute(sql_insert_data, {"ts":ts, "year":ts.year, "month":ts.month, "day":ts.day, "time":ts.time(), "date":ts.date()})
                conn_dest.commit() 
        else:
            add_wf_setting(cur_dest, dds_table,'DDS', values['update_ts'])
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_dm_products_load(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_ts = get_parameter(cur_dest, dds_table, 'DDS', is_date=True)
    limit = 1000
    sql_get_data = """select id, object_value, update_ts
                    from stg.ordersystem_restaurants
                    where update_ts>%(latest_ts)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"limit":limit, "latest_ts":latest_ts})
    data = cur_dest.fetchall()
    if data:
        for line in data:
            rest_id, values, update_ts = line
            values = loads(values)
            menu = values['menu']
            for item in menu:
                product_id = item['_id']
                product_name = item['name']
                product_price = item['price']
                sql_insert_data = """insert into dds.dm_products (restaurant_id, product_id, product_name, product_price, active_from, active_to) values (
                    %(rest_id)s,%(product_id)s, %(product_name)s, %(product_price)s, %(act_from)s,%(act_to)s);"""
                cur_dest.execute(sql_insert_data, {"rest_id":str(rest_id), "product_id":str(product_id), "product_name":product_name, "product_price":product_price,"act_from":update_ts, "act_to":str(datetime(2099,12,31))})
                conn_dest.commit()
        else:
            add_wf_setting(cur_dest, dds_table, 'DDS', update_ts)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_dm_orders_load(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_ts = get_parameter(cur_dest, dds_table, 'DDS', is_date=True)
    limit = 1000
    sql_get_data = """select object_id, object_value, update_ts
                    from stg.ordersystem_orders
                    where update_ts>%(latest_ts)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"limit":limit, "latest_ts":latest_ts})
    data = cur_dest.fetchall()
    if data:
        for line in data:
            order_key, values, update_ts = line
            values = loads(values)
            status = values['final_status']

            user_key = str(values['user']['id'])
            rest_key = str(values['restaurant']['id'])
            timestamp_key = values['update_ts']
            
            if status == 'CLOSED' or status == 'CANCELLED':
                sql_get_user_id = "SELECT ID FROM dds.dm_users where user_id = %(key)s"
                sql_get_restaurant_id = "SELECT ID FROM dds.dm_restaurants where restaurant_id = %(key)s"
                sql_get_timestamp_id = "SELECT ID FROM dds.dm_timestamps where ts = %(key)s"
                cur_dest.execute(sql_get_user_id, {"key":user_key})
                user_id = cur_dest.fetchall()[0][0]
                cur_dest.execute(sql_get_restaurant_id, {"key":rest_key})
                restaurant_id = cur_dest.fetchall()[0][0]
                cur_dest.execute(sql_get_timestamp_id, {"key":timestamp_key})
                timestamp_id = cur_dest.fetchall()[0][0]
                sql_insert_data = """insert into dds.dm_orders (order_key, order_status, user_id, restaurant_id, timestamp_id) values 
                (%(order_key)s, %(order_status)s, %(user_id)s, %(restaurant_id)s, %(timestamp_id)s);"""
                cur_dest.execute(sql_insert_data, {"order_key":order_key, "order_status":status, "user_id":user_id, "restaurant_id":restaurant_id, "timestamp_id":timestamp_id})
                conn_dest.commit() 
        else:
            add_wf_setting(cur_dest, dds_table,'DDS', update_ts)
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def dds_fct_product_sales_load(dds_table, destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn_dest =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur_dest = conn_dest.cursor()
    latest_ts = get_parameter(cur_dest, dds_table, 'DDS', is_date=True)
    limit = 1000
    sql_get_data = """select object_id, object_value, update_ts
                    from stg.ordersystem_orders
                    where update_ts>%(latest_ts)s
                    limit %(limit)s;"""
    cur_dest.execute(sql_get_data, {"limit":limit, "latest_ts":latest_ts})
    data = cur_dest.fetchall()
    if data:
        for order_line in data:
            order_key, values, update_ts = order_line
            status = loads(values)['final_status']
            if status == 'CLOSED':
                get_order_id = """select id from dds.dm_orders where order_key = %(order_key)s"""
                cur_dest.execute(get_order_id, {"order_key":order_key})
                order_id = cur_dest.fetchall()[0][0]
                sql_get_data_from_bonus_system = """select event_value from stg.bonussystem_events
                                                    where event_type = 'bonus_transaction' and event_value like %(order_key)s"""
                cur_dest.execute(sql_get_data_from_bonus_system, {"order_key":('%' + order_key + '%')})
                bonus_data = cur_dest.fetchall()
                if bonus_data:
                    bonus_data = loads(bonus_data[0][0])
                    for line in bonus_data['product_payments']:
                        product_key = line['product_id']
                        price = line['price']
                        count = line['quantity']
                        total_sum = line['product_cost']
                        bonus_payment = line['bonus_payment']
                        bonus_grant = line['bonus_grant']
                        sql_get_product_id = "select id from dds.dm_products where product_id = %(product_key)s"
                        cur_dest.execute(sql_get_product_id, {"product_key":product_key})
                        product_id = cur_dest.fetchall()[0][0]
                        sql_insert_data = """insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) 
                                            values (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);"""
                        cur_dest.execute(sql_insert_data, {"product_id":product_id, "order_id":order_id, "count":count, "price":price, "total_sum":total_sum, "bonus_payment":bonus_payment, "bonus_grant":bonus_grant})
                else:
                    sql_get_data_from_order_system = """SELECT object_value from stg.ordersystem_orders where object_id = %(order_key)s;"""
                    cur_dest.execute(sql_get_data_from_order_system, {"order_key":order_key})
                    order_data = cur_dest.fetchall()
                    order_data = loads(order_data[0][0])
                    for item in order_data['order_items']:
                        product_key = str(item['id'])
                        price = item['price']
                        count = item['quantity']
                        total_sum = price*count
                        bonus_payment = 0
                        bonus_grant = 0
                        sql_get_product_id = "select id from dds.dm_products where product_id = %(product_key)s"
                        cur_dest.execute(sql_get_product_id, {"product_key":product_key})
                        product_id = cur_dest.fetchall()[0][0]
                        sql_insert_data = """insert into dds.fct_product_sales (product_id, order_id, count, price, total_sum, bonus_payment, bonus_grant) 
                                            values (%(product_id)s, %(order_id)s, %(count)s, %(price)s, %(total_sum)s, %(bonus_payment)s, %(bonus_grant)s);"""
                        cur_dest.execute(sql_insert_data, {"product_id":product_id, "order_id":order_id, "count":count, "price":price, "total_sum":total_sum, "bonus_payment":bonus_payment, "bonus_grant":bonus_grant})
        else:
            add_wf_setting(cur_dest, 'fct_product_sales','DDS', update_ts)
            conn_dest.commit()
    conn_dest.commit()    
    cur_dest.close()
    conn_dest.close()

def cdm_dm_settlement_report_load(destination_connection):
    postgres_hook_dest = BaseHook.get_connection(destination_connection)
    conn =  psycopg2.connect(f"dbname='de' port={postgres_hook_dest.port} user={postgres_hook_dest.login} host={postgres_hook_dest.host} password={postgres_hook_dest.password}")
    cur = conn.cursor() 
    sql_delete_old_report = """delete from cdm.dm_settlement_report; 
                               alter sequence cdm.dm_settlement_report_id_seq restart;"""
    cur.execute(sql_delete_old_report)
    conn.commit()
    sql_data_request = """with gr as (select t.date, o.restaurant_id, count(fps.order_id) as orders_count, sum(fps.order_sum) as orders_total_sum, 
                        sum(fps.bonus_sum) as bonus_payment_sum, sum(fps.bonus_grant_sum) as bonus_granted_sum
                        from 
                        (select order_id, sum(total_sum) as order_sum, sum(bonus_payment) as bonus_sum, sum(bonus_grant) as bonus_grant_sum
                        from dds.fct_product_sales fps 
                        group by order_id) fps
                        left join 
                        dds.dm_orders o on
                        fps.order_id = o.id
                        left join 
                        dds.dm_timestamps t
                        on o.timestamp_id = t.id 
                        where o.order_status ='CLOSED'
                        group by t.date, o.restaurant_id
                        order by t.date desc)
                        select gr.restaurant_id, r.restaurant_name, gr.date as settlement_date, gr.orders_count, gr.orders_total_sum, gr.bonus_payment_sum,
                        gr.bonus_granted_sum, gr.orders_total_sum * 0.25 as order_processing_fee, 0.75*gr.orders_total_sum - gr.bonus_payment_sum as restaurant_reward_sum 
                        from gr
                        left join dds.dm_restaurants r 
                        on gr.restaurant_id = r.id
                        where gr.date<%(current_date)s
                        """
    cur.execute(sql_data_request, {"current_date":datetime.today().date()})
    data = cur.fetchall()
    for line in data:
        restaurant_id, restaurant_name, settlement_date,  orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum = line
        sql_data_insert = """insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date,  orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
                            values(%(restaurant_id)s, %(restaurant_name)s, %(settlement_date)s,  %(orders_count)s, %(orders_total_sum)s, %(orders_bonus_payment_sum)s, %(orders_bonus_granted_sum)s, %(order_processing_fee)s, %(restaurant_reward_sum)s);"""
        cur.execute(sql_data_insert, {"restaurant_id":restaurant_id,"restaurant_name":restaurant_name, "settlement_date":settlement_date, "orders_count": orders_count, "orders_total_sum":orders_total_sum, "orders_bonus_payment_sum":orders_bonus_payment_sum, "orders_bonus_granted_sum":orders_bonus_granted_sum, "order_processing_fee":order_processing_fee, "restaurant_reward_sum":restaurant_reward_sum})
        conn.commit()
    cur.close()
    conn.close()                     

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

            order_ts = values['order_ts']
            delivery_key = values['delivery_id']
            sql_get_delivery_id = """select id from dds.dm_api_delivery_details where delivery_id = %(key)s"""
            cur_dest.execute(sql_get_delivery_id, {"key":delivery_key})
            delivery_id = cur_dest.fetchall()[0][0]
            courier_id = values['courier_id']
            full_address = values['address']
            delivery_ts = values['delivery_ts']
            rate = values['rate']
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
    alter sequence cdm.dm_courier_ledger_id_seq restart;
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
    schedule_interval='0/15 * * * *',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['sprint5'],
    is_paused_upon_creation=True)
def full_dag():

#creating tasks

    @task(task_id = "extract_ranks")
    def extract_ranks_task():
        extract_func(origin_conn, dest_conn, 'public', 'ranks', 'stg', 'bonussystem_ranks', extra=False)

    @task(task_id = "extract_users")
    def extract_users_task():
        extract_func(origin_conn, dest_conn, 'public', 'users', 'stg', 'bonussystem_users',extra=False)

    @task(task_id = "events_load")
    def events_load_task():
        events_load_func(origin_conn, dest_conn)

    @task(task_id = "Mongo_load_restaurants")
    def mongodb_restaurants():
        mongodb_collection_load('restaurants', dest_conn)
    
    @task(task_id = "Mongo_load_users")
    def mongodb_users():
        mongodb_collection_load('users', dest_conn)
    
    @task(task_id = "Mongo_load_orders")
    def mongodb_orders():
        mongodb_collection_load('orders', dest_conn)

    @task(task_id = "DDS_dm_users")
    def dds_dm_users():
        dds_dm_load_users('dm_users', dest_conn)
    
    @task(task_id = "DDS_dm_restaurants")
    def dds_dm_restaurants():
        dds_dm_restaurants_load('dm_restaurants', dest_conn)

    @task(task_id="DDS_dm_timestamps")
    def dds_dm_timestamps():
        dds_dm_timestamps_load('dm_timestamps', dest_conn)

    @task(task_id = "DDS_dm_products")
    def dds_dm_products():
        dds_dm_products_load('dm_products', dest_conn)

    @task(task_id = "DDS_dm_orders")
    def dds_dm_orders():
        dds_dm_orders_load('dm_orders', dest_conn)

    @task(task_id = "DDS_fct_product_sales")
    def dds_fct_product_sales():
        dds_fct_product_sales_load('fct_product_sales', dest_conn)

    @task(task_id = "CDM_dm_settlement_report")
    def cdm_dm_settlement_report():
        cdm_dm_settlement_report_load(dest_conn)

#project_tasks
    
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

#creating task_groups

    @task_group
    def load_to_stg():
        extract_ranks_task()
        extract_users_task()
        events_load_task()
        mongodb_restaurants()
        mongodb_users()
        mongodb_orders()
        api_load_restaurants_task()
        api_load_couriers_task()
        api_load_deliveries_task()

    @task_group
    def load_independent_dms():
        dds_dm_restaurants()
        dds_dm_timestamps()
        dds_dm_users()
        dds_api_dm_couriers_load()
        dds_api_dm_restaurants_load()
        dds_api_dm_orders_load()
        dds_api_dm_address_load()

    @task_group
    def load_dependent_dms():
        dds_dm_products()
        dds_dm_orders()
        dds_api_dm_delivery_details_load()
    
    @task_group
    def load_facts():
        dds_fct_product_sales()
        dds_fct_api_sales_load()
    
    @task_group
    def load_cdm():
        cdm_dm_settlement_report()
        cdm_load_dm_courier_ledger()



    load_to_stg() >> load_independent_dms() >> load_dependent_dms() >> load_facts() >> load_cdm()
sprint_5_dag = full_dag()