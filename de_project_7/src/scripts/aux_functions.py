# spark related imports 
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import ArrayType, IntegerType
from itertools import combinations



# distance calculation
def distance(lat1, lon1, lat2, lon2):
    lat1, lon1, lat2, lon2 = map(lambda x: F.regexp_replace(x,",","."), [lat1, lon1, lat2, lon2])
    lat1, lon1, lat2, lon2 = map(lambda x: F.cast(float, x), [lat1, lon1, lat2, lon2])
    lat1, lon1, lat2, lon2 = map(F.radians, [lat1, lon1, lat2, lon2])
    r = 6371
    return 2*r*F.acos(
        F.sqrt(
    (F.sin((lat2-lat1)/2))**2 + F.cos(lon1)*F.cos(lon2)*(F.sin((lon2-lon1)/2))**2
              )
                     )

# retrieving geodata df, adding timezones
def get_geo_data(geo_data_loc, spark):
        return spark.read.parquet(geo_data_loc)\
        .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city')))\
        .withColumn('lat', F.regexp_replace("lat",",",".").cast('float'))\
        .withColumn('lng', F.regexp_replace("lng",",",".").cast('float'))\
        .withColumn('timezone', F.regexp_replace('timezone', "(Rockhampton|Mackay|Gold Coast|Townsville|Ipswich|Cairns|Toowoomba)", "Brisbane"))\
        .withColumn('timezone', F.regexp_replace('timezone', "(Wollongong|Newcastle|Maitland)", "Sydney"))\
        .withColumn('timezone', F.regexp_replace('timezone', "(Cranbourne|Geelong|Ballarat|Bendigo)", "Melbourne"))\
        .withColumn('timezone', F.regexp_replace('timezone', "(Launceston)", "Hobart"))\
        .withColumn('timezone', F.regexp_replace('timezone', "(Bunbury)", "Perth"))\
        .withColumnRenamed('id', 'zone_id')

def add_cities(events, geo_data, prefix, prt="message"):
        return events.join(geo_data)\
                    .withColumn('distance', distance(F.col(f"{prefix}_lat"),\
                                                     F.col(f"{prefix}_lng"),\
                                                     F.col("lat"),\
                                                     F.col("lng")))\
                    .withColumn('rank', F.rank().over(Window().partitionBy(f'{prt}_id').orderBy(F.col('distance').asc())))\
                    .where('rank=1')
    
    
# adding city data to df based on events schema
def get_events_with_cities(events, geo_data_loc, spark):
    geo_data = get_geo_data(geo_data_loc, spark)
    df =  events\
    .withColumn('subs', F.coalesce(F.col('event.subscription_channel'), F.col('event.subscription_user')))\
    .withColumn('ts', F.coalesce(F.col('event.message_ts'), F.col('event.datetime')))\
    .withColumn('user_id', F.coalesce(F.col('event.message_from'), F.col('event.user')))\
    .selectExpr(['event.message_id',\
                'user_id',\
                'ts',\
                'event.reaction_type as reaction',\
                'subs',\
                'lat as m_lat', \
                'lon as m_lng'])
    return add_cities(df, geo_data, prefix = 'm')\
        .select('user_id', 'message_id', 'reaction', 'subs', 'ts', 'city', 'zone_id')



def add_local_time(df):
    return df.withColumn('ts', F.date_format('ts', 'HH:mm:ss'))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("ts"),F.col('timezone')))\
            .drop('ts')


@F.udf(returnType=ArrayType(ArrayType(IntegerType())))
def get_combinations(arr):
    return [x for x in combinations(arr, 2)]