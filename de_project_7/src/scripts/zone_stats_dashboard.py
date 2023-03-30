from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
from pyspark.sql.window import Window 

from aux_functions import get_geo_data, get_events_with_cities

# ##########
# Шаг 3. Создать витрину в разрезе зон
# Вы создадите геослой — найдёте распределение атрибутов, связанных с событиями, по географическим зонам (городам). Если проанализировать этот слой, то можно понять поведение пользователей по различным регионам.
# Итак, вам нужно посчитать количество событий в конкретном городе за неделю и месяц. Значит, витрина будет содержать следующие поля:
# ##########
#     month — месяц расчёта;
#     week — неделя расчёта;
#     zone_id — идентификатор зоны (города);
#     week_message — количество сообщений за неделю;
#     week_reaction — количество реакций за неделю;
#     week_subscription — количество подписок за неделю;
#     week_user — количество регистраций за неделю;
#     month_message — количество сообщений за месяц;
#     month_reaction — количество реакций за месяц;
#     month_subscription — количество подписок за месяц;
#     month_user — количество регистраций за месяц.

# В этой витрине вы учитываете не только отправленные сообщения, но и другие действия — подписки, реакции, 
# регистрации (рассчитываются по первым сообщениям). 
# Пока присвойте таким событиям координаты последнего отправленного сообщения конкретного пользователя.


def get_events_count(df, period):
    assert type(period) == str
    return df\
            .withColumn('reg_date', F.first('ts').over(Window().partitionBy('user_id').orderBy(F.col('ts').asc())))\
            .withColumn('reg_date', F.expr('reg_date == ts'))\
            .withColumn(period, F.date_trunc(period, 'ts'))\
            .groupBy('zone_id', period)\
            .agg(F.count('message_id').alias(f'{period}_message'),\
                F.count('reaction').alias(f'{period}_reaction'),\
                F.count('subs').alias(f'{period}_subscription'),\
                F.sum(F.col('reg_date').cast('long')).alias(f'{period}_user')
    )

def fill_dashboard(events, geo_data_loc, spark):
    events_with_cities = get_events_with_cities(events, geo_data_loc, spark)
    df = get_events_count(events_with_cities,'week').withColumn('month', F.date_trunc('month', 'week'))
    df2 = get_events_count(events_with_cities,'month')
    return df2.join(df, ['zone_id','month'], 'left')\
              .select('month', 'week', 'zone_id', 'week_message',  'week_reaction', 'week_subscription',
                     'week_user', 'month_message', 'month_reaction', 'month_subscription', 'month_user')


def main():
    # забираю параметры из командной строки
    events_loc = sys.argv[1]
    geo_data_loc = sys.argv[2]
    analytics_loc = sys.argv[3]
    dt = sys.argv[4]
    # запускаю Spark
    spark = SparkSession.builder \
                    .master("yarn") \
                    .appName(f"Zone_stats_dashboard_date={dt}") \
                    .getOrCreate()
    events = spark.read.option("mergeSchema", "true")\
                  .parquet(events_loc)
    dashboard = fill_dashboard(events, geo_data_loc, spark)
    dashboard.write.mode('overwrite').format('parquet').save(analytics_loc)


if __name__=="__main__":
    main()