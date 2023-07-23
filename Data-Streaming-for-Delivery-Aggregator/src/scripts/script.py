from datetime import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType, LongType

TOPIC_IN = 'yc-user_in'
TOPIC_OUT = 'yc-user_out'

# required libraries for integrating Spark with Kafka and PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

postgresql_settings = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
    'user': 'student',
    'password': 'de-student',
}

# method for writing data to 2 targets: in PostgreSQL for feedback and in Kafka for triggers
def foreach_batch_function(df, epoch_id):
    # keep the df in memory so we don't have to re-create the df before sending it to Kafka
    df.persist()
    df.write.format('parquet').mode('overwrite').save('/root/datas8/result_df')
    # write df to PostgreSQL with feedback field
    df.withColumn('feedback', F.lit(None)) \
    .write.format('jdbc') \
    .options(url=postgresql_settings['url'], driver=postgresql_settings['driver'], dbtable='public.subscribers_feedback', user=postgresql_settings['user'], password=postgresql_settings['password']) \
    .mode('append') \
    .save() 
    # create df to send to Kafka. Serialization to json
    send_df = df.withColumn('value', F.to_json(F.struct(F.col('restaurant_id'),
                                                   F.col('adv_campaign_id'),
                                                   F.col('adv_campaign_content'),
                                                   F.col('adv_campaign_owner'),
                                                   F.col('adv_campaign_owner_contact'),
                                                   F.col('adv_campaign_datetime_start'),
                                                   F.col('adv_campaign_datetime_end'),
                                                   F.col('datetime_created'),
                                                   F.col('client_id'),
                                                   F.col('trigger_datetime_created'))))
    # send messages to the resulting Kafka topic without the feedback field
    send_df.write \
            .mode("append") \
            .format("kafka") \
            .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option("topic", TOPIC_OUT) \
            .option("checkpointLocation", "query") \
            .save()
    # clear memory from df
    df.unpersist()



# create a spark session with the necessary libraries in spark_jars_packages for integration with Kafka and PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# read from the Kafka topic messages with promotions from restaurants
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"de-student\" password=\"ltcneltyn\";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', TOPIC_IN) \
    .load()

# define input message schema for json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), nullable=True),
    StructField("adv_campaign_id", StringType(), nullable=True),
    StructField("adv_campaign_content", StringType(), nullable=True),
    StructField("adv_campaign_owner", StringType(), nullable=True),
    StructField("adv_campaign_owner_contact", StringType(), nullable=True),
    StructField("adv_campaign_datetime_start", LongType(), nullable=True),
    StructField("adv_campaign_datetime_end", LongType(), nullable=True),
    StructField("datetime_created", LongType(), nullable=True)
    ])

# determine the current time in UTC in milliseconds
current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

# deserialize json message from value and filter by promotion start and end time
restaurant_read_stream_df = restaurant_read_stream_df.withColumn('value', restaurant_read_stream_df.value.cast("string")) \
    .withColumn('key', restaurant_read_stream_df.key.cast("string"))

restaurant_read_stream_df = restaurant_read_stream_df.withColumn('value_json', F.from_json(restaurant_read_stream_df.value, incomming_message_schema)) \
    .selectExpr('value_json.*', '*') \
    .drop('value_json')
    
filtered_read_stream_df = restaurant_read_stream_df \
    .filter(F.col('adv_campaign_datetime_start') < current_timestamp_utc) \
    .filter(F.col('adv_campaign_datetime_end') > current_timestamp_utc)

# subtract all users with a subscription to restaurants
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .options(**postgresql_settings) \
                    .load() \
                    .dropDuplicates(['client_id', 'restaurant_id']) \
                    .cache()

# join data from a Kafka message with subscribers by restaurant_id (uuid). Add the event creation time.
result_df = filtered_read_stream_df.join(subscribers_restaurant_df, 'restaurant_id', 'inner') \
    .select(['restaurant_id', 'adv_campaign_id','adv_campaign_content','adv_campaign_owner','adv_campaign_owner_contact','adv_campaign_datetime_start','adv_campaign_datetime_end','datetime_created','client_id']) \
    .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc))

# start streaming
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 