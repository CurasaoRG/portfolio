# Импорт стандартных библиотек
from datetime import datetime
import sys
# Импорт компонентов pyspark
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

# Обработка входных параметров скрипта
try:
    TOPIC_NAME_IN = sys.argv[1]
    TOPIC_NAME_OUT = sys.argv[2]
except IndexError: 
    TOPIC_NAME_IN = 'student.topic.cohort5.GRR.in' # топик, из которого читаем сообщения
    TOPIC_NAME_OUT = 'student.topic.cohort5.GRR.out' # топик, в который отправляем сообщения

# Перечисление констант

# Общие колонки для исходящих данных
OUT_COLS = ['client_id' 
            ,'restaurant_id'
            , 'adv_campaign_id'
            , 'adv_campaign_content'
            , 'adv_campaign_owner'
            , 'adv_campaign_owner_contact'
            , 'adv_campaign_datetime_start'
            , 'adv_campaign_datetime_end'
            , 'datetime_created'
            , 'trigger_datetime_created'
            ]

# Параметры подключения Postgres
POSTGRES_URL = 'jdbc:postgresql://localhost:5432/jovyan'
POSTGRES_TABLE_IN = 'public.subscribers_restaurants'
POSTGRES_TABLE_OUT = 'public.subscribers_feedback'
POSTGRES_DETAILS = {'user':'jovyan', 'password':'jovyan', 'driver': 'org.postgresql.Driver'}

# Подключаемые пакеты spark 
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.5.1",
        ]
    )

# Параметры подключения Kafka 
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config':'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";',
}
SERVER = "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091"
CHECKPOINT_LOC = "/data/checkpoint"

# Процедуры 

# Запуск Spark
def spark_init(app_name) -> SparkSession:
    return SparkSession.builder\
            .master("local")\
            .appName(app_name)\
            .config("spark.jars.packages", spark_jars_packages)\
            .getOrCreate()

# Загрузка данных из Kafka
def load_stream(spark: SparkSession) -> DataFrame:
    df = spark.readStream\
        .format('kafka')\
        .option('kafka.bootstrap.servers', SERVER)\
        .options(**kafka_security_options)\
        .option('subscribe', TOPIC_NAME_IN)\
        .load()
    return transform(df)

# Процедура для обработки датафрейма: извлечение колонок из json, фильтр по времени
def transform(df: DataFrame) -> DataFrame:
    message_example = '{"restaurant_id": "123e4567-e89b-12d3-a456-426614174000","adv_campaign_id": "123e4567-e89b-12d3-a456-426614174003","adv_campaign_content": "first campaign","adv_campaign_owner": "Ivanov Ivan Ivanovich","adv_campaign_owner_contact": "iiivanov@restaurant.ru","adv_campaign_datetime_start": 1659203516,"adv_campaign_datetime_end": 2659207116,"datetime_created": 1659131516}' 
    schema = F.schema_of_json(message_example)
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    df = df.withColumn('value', F.col('value').cast(StringType()))\
            .withColumnRenamed('timestamp', 'org_timestamp')\
            .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc))\
            .withColumn('json_value',F.from_json(F.col('value'),schema=schema))\
            .selectExpr("*","json_value.*")\
            .drop('json_value')\
            .dropDuplicates(['restaurant_id', 'adv_campaign_id']).withWatermark('org_timestamp', "1 day")

    return df.where(f'adv_campaign_datetime_start < {current_timestamp_utc} and adv_campaign_datetime_end>{current_timestamp_utc} ')

# Загрузка данных из Postgres
def load_static_df(spark: SparkSession) -> DataFrame:
    return (spark.read.jdbc(url=POSTGRES_URL, \
        table=POSTGRES_TABLE_IN, \
        properties=POSTGRES_DETAILS)\
        )\
        .dropDuplicates(['client_id', 'restaurant_id'])


# Объединение датафреймов
def join_dfs(df1, df2, field='restaurant_id'):
    return df1.join((df2), field, 'inner')\
       
# Вывод в Kafka
def output_to_kafka(df):
    df = df\
        .withColumn('value', F.to_json(F.struct(OUT_COLS)))\
        .select('value')
    df.write.format("kafka")\
            .option('kafka.bootstrap.servers', SERVER)\
            .options(**kafka_security_options)\
            .option('topic', TOPIC_NAME_OUT)\
            .save()

# Вывод в Postgres
def output_to_ps(df):
    df = df.withColumn('feedback', F.lit(''))
    df.write.format('jdbc')\
            .mode('append')\
            .option('url', POSTGRES_URL)\
            .option('dbtable', 'subscribers_feedback')\
            .options(**POSTGRES_DETAILS)\
            .save()

# Общая процедура вывода данных
def write_to_multiple_sinks(df, batch_id):
    df = df.select(OUT_COLS)
    df.cache()
    output_to_kafka(df)
    output_to_ps(df)


def main():
    # запускаем spark
    spark = spark_init('project_8')
    # загружаем стрим, парсим json
    incoming_df = load_stream(spark)
    # получаем список подписчиков из Postgres
    sub_data = load_static_df(spark)
    # соединяем датафреймы
    joined_df = join_dfs(incoming_df, sub_data)
    # запускаем отправку в разные стоки
    joined_df.writeStream\
                .foreachBatch(write_to_multiple_sinks)\
                .option("checkpointLocation", CHECKPOINT_LOC)\
                .start()\
                .awaitTermination()


if __name__ == "__main__":
    main()