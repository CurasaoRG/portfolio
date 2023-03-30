# standard lib imports
import sys

# spark related imports 
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.window import Window 

# local imports
from aux_functions import get_geo_data, get_events_with_cities, add_local_time

# Шаг 2. Создать витрину в разрезе пользователей

#     Определите, в каком городе было совершено событие. С этим вам поможет список городов из файла geo.csv. 
#     В нём указаны координаты центра города. Найдите расстояние от координаты отправленного сообщения до центра города. 
#     Событие относится к тому городу, расстояние до которого наименьшее. 

#     Когда вы вычислите геопозицию каждого отправленного сообщения, создайте витрину с тремя полями:
#         user_id — идентификатор пользователя.
#         act_city — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
#         home_city — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
#     Выясните, сколько пользователь путешествует. Добавьте в витрину два поля:
#         travel_count — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
#         travel_array — список городов в порядке посещения.
#     Добавьте в витрину последний атрибут — местное время (local_time) события (сообщения или других событий, если вы их разметите на основе сообщений). Местное время события — время последнего события пользователя, о котором у нас есть данные с учётом таймзоны геопозициии этого события. Данные, которые вам при этом пригодятся:
#         TIME_UTC — время в таблице событий. Указано в UTC+0.
#         timezone — актуальный адрес. Атрибуты содержатся в виде Australia/Sydney.




# Готовим датафрейм для поиска домашнего города и списка городов - находим количество дней, проведенных в городе:
# - запоминаем дату самого позднего события, можно брать по всем пользователям - это отсечка уже загруженных данных
# - добавляем даты и города предыдущих событий пользователя
# - оставляем только те события, где текущий город отличается от предыдущего, либо предыдущей записи не было
# - убираем лишние колонки
# - добавляем колонку с самой поздней датой
# - считаем количество дней, проведенных в городе как разницу между текущей датой и будущей либо текущей и максимальной датой - 
#   предполагая, что юзер оставался в последнем городе после отправки последнего сообщения

def get_messages_with_days(df):
    max_ts = df.select(F.max('ts')).collect()
    return df\
            .withColumn('prev_city', F.lag('city').over(Window().partitionBy('user_id').orderBy('ts')))\
            .withColumn('prev_ts', F.lag('ts').over(Window().partitionBy('user_id').orderBy('ts')))\
            .where('city<>prev_city or prev_city is null')\
            .select('user_id', 'message_id', 'city', 'ts')\
            .withColumn('max_ts', F.lit(max_ts[0][0]))\
            .withColumn('next_ts', F.lead('ts').over(Window().partitionBy('user_id').orderBy('ts')))\
            .withColumn('duration', F.coalesce(F.datediff(F.col('next_ts'),F.col('ts')), \
                                            F.datediff(F.col('max_ts'),F.col('ts'))))
    
# Получаем домашние города юзеров: 
# - добавляем колонку max_days с максимальным пребыванием пользователя в определённом городе
# - добавляем колонку latest_date_in_city в которой будет храниться самая поздняя дата пользователя в городе
# - добавляем колонку case_col для итоговой сортировки: 
#             оставляем города, в которых пользователь был дольше заданного значения (27 дней по умолчанию)
#             если пользователь нигде не останавливался дольше заданного значения, то оставляем города с максимальным пребыванием
#             иначе null - остальные значения не нужны
# - отфильтровываем лишние строки
# - добавляем колонку home_city согласно условиям


def get_home_city(df, home_days=27):
    df = get_messages_with_days(df)
    return df\
            .withColumn('max_days', F.max('duration').over(Window().partitionBy('user_id')))\
            .withColumn('latest_date_in_city', F.max('ts').over(Window().partitionBy('user_id', 'city')))\
            .withColumn('case_col', F.expr(f"""
                case 
                    when duration >= {home_days} then city
                    when max_days < {home_days} and duration=max_days then city
                    else null
                end
                """))\
            .where('case_col is not null')\
            .withColumn('home_city', F.first('case_col')\
                                .over(
                                        Window().partitionBy('user_id').orderBy(F.col('latest_date_in_city').desc()))
                                        )\
            .select('user_id', 'home_city').distinct()

# Собираем список посещенных городов, количество поездок, последний город и время последнего события


def get_travels(df):
    df = get_messages_with_days(df)
    df1 = df.select('user_id', 'city', 'ts')\
            .groupBy('user_id')\
                .agg(F.collect_list(F.col('city')).alias('travel_array'),\
                    F.count(F.col('city')).alias('travel_count'))
    df2 = df.select('user_id', 'city', 'ts')\
         .withColumn('rank', F.rank().over(Window().partitionBy('user_id').orderBy('ts')))\
         .where('rank=1')\
         .drop('rank')   
                        
    return df1.join(df2.select('user_id', 'act_city', 'ts'), 'user_id', 'left')      






def main():
    # забираю параметры из командной строки
    events_loc = sys.argv[1]
    geo_data_loc = sys.argv[2]
    analytics_loc = sys.argv[3]
    dt = sys.argv[4]
    # запускаю Spark
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"User_stats_dashboard_date={dt}") \
                    .getOrCreate()
    # читаю датафрейм с сообщениями
    messages = spark.read.option("mergeSchema", "true")\
        .parquet(events_loc).where("event_type = 'message' and event.message_id is not null")
    # добавляю геоданные к датафрейму с сообщениями 
    messages_with_cities = get_events_with_cities(messages, geo_data_loc, spark)
    # нахожу домашние города
    messages_with_home_cities = get_home_city(messages_with_cities, home_days=27)
    # нахожу список поездок, их количество и актуальный город
    messages_with_travels = get_travels (messages_with_cities)
    # объединяю датафреймы в один
    dashboard =  messages_with_home_cities.join(messages_with_travels, 'user_id', 'left')
    # добавляю локальное время
    dashboard = add_local_time(dashboard)
    # записываю витрину по заданному адресу
    dashboard.write.mode('overwrite').format('parquet').save(analytics_loc)


if __name__=="__main__":
    main()