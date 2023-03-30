# standard lib imports
import sys

# spark related imports 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# local imports
from aux_functions import get_geo_data, distance, add_cities, add_local_time, get_combinations

# В sub_channels есть все комбинации пар подписчиков одного канала (Василий - Мария, Мария - Василий), 
# а в user_contacts только те пары, в которых элементы отсортированы (Василий - Мария). 
# Если Василий писал Марии, а она не отвечала, то пара (Василий - Мария) исключится, а вот (Мария - Василий) останется.
# 
# В предыдущей версии кода ты эту логику обрабатывал через union обоих комбинаций. 
# В текущем случае предлагаю в функции get_user_contacts сделать оба варианта массива - один отсортирован по возрастанию, 
# а второй по убыванию (параметр asc=False, https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.functions.sort_array.html).

# sub_channels
def get_user_channels(events):

    return events.where("event_type = 'subscription'")\
                .selectExpr('event.subscription_channel as channel', 'event.user as user')\
                .distinct()\
                .groupBy('channel')\
                .agg(F.collect_set('user').alias('channel_users'))\
                .withColumn('pairs_list', get_combinations('channel_users'))\
                .selectExpr('explode(pairs_list) as user_pairs')\
                .withColumn('user_pairs', F.array_sort(F.col('user_pairs')))\
                .distinct()


def get_user_contacts(events):
    return  events\
                .where('event.message_from is not null')\
                .withColumn('user_pairs', F.array_sort(F.array('event.message_to', 'event.message_from')))\
                .select('user_pairs').distinct()


def get_final_coordinates(events):
# забираю координаты последнего события пользователя, если они есть
    return events\
            .select(F.coalesce(F.col('event.message_from'), F.col('event.user')).alias('user_id')\
                ,F.coalesce(F.col('event.message_ts'), F.col('event.datetime')).alias('ts')
                , 'lat', 'lon'
                    )\
            .sort(F.asc('ts')).groupBy('user_id').agg(F.last('lat',ignorenulls = True).alias('final_lat')\
                                                    , F.last('lon',ignorenulls = True).alias('final_lng')\
                                                    , F.last('ts').alias('ts'))


def fill_dashboard(events):
# получаю список пар пользователей, подписанных на один канал => user_pairs
    sub_channels = get_user_channels(events)
# получаю список пользователей с координатами их последнего события 
    users_with_final_coordinates = get_final_coordinates(events)
# собираю контакты пользователей => user_pairs
    user_contacts = get_user_contacts(events)
# получаю финальный результат: 
# - исключаю пары друзей из списка подписчиков одних каналов
# - добавляю координаты для первого пользователя
#   и для второго пользователя
# - вычисляю расстояния 
# - отбираю пары пользователей согласно требованиям
    res = sub_channels.join(user_contacts, 'user_pairs', 'left_anti')\
                            .withColumn('user', F.element_at(F.col('user_pairs'), 1))\
                            .withColumn('user2', F.element_at(F.col('user_pairs'), 2))\
                            .drop('user_pairs')\
                            .join(users_with_final_coordinates, F.col('user_id') == F.col('user'), 'left')\
                            .selectExpr('user_id as user_left'\
                                        , 'user2 as user_right'\
                                        , 'final_lat as final_lat_left'\
                                        , 'final_lng as final_lon_left')\
                            .join(users_with_final_coordinates, F.col('user_right') == F.col('user_id'), 'left')\
                            .withColumnRenamed('final_lat', 'final_lat_right')\
                            .withColumnRenamed('final_lng', 'final_lon_right')\
                            .withColumn('distance', distance(\
                                    F.col("final_lat_left"),\
                                    F.col("final_lon_left"),\
                                    F.col("final_lat_right"),\
                                    F.col("final_lon_right")))\
                            .where('distance<=1')
    return res


def main():
# забираю параметры запуска из командной строки
    events_loc = sys.argv[1]
    geo_data_loc = sys.argv[2]
    analytics_loc = sys.argv[3]
    dt = sys.argv[4]
# запускаю Spark
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"User_recommendations_dashboard_date={dt}") \
                    .getOrCreate()
# читаю датафрейм с сообщениями
    events = spark.read.option("mergeSchema", "true")\
        .parquet(events_loc)
# читаю датафрейм с геоданными
    geo_data = get_geo_data(geo_data_loc, spark)
# собираю датафрейм с рекомендациями
    users_final_cities = add_cities(get_final_coordinates(events), geo_data, 'final', prt='user')
    dashboard = fill_dashboard(events)\
                    .select('user_left', 'user_right')\
                    .join(users_final_cities, F.col('user_left')==F.col('user_id'), 'left')\
                    .withColumn('processed_dttm', F.lit(dt))
# добавляю локальное время 
    dashboard = add_local_time(dashboard)\
        .select(['user_left', 'user_right', 'zone_id', 'processed_dttm', 'local_time'])
# сохраняю дэшборд в заданную локацию
    dashboard.write.mode('overwrite').format('parquet').save(analytics_loc)

if __name__=="__main__":
    main()