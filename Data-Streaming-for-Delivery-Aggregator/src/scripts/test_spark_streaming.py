from datetime import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_IN = 'yc-user_in'

# create a spark session with the necessary libraries in spark_jars_packages for integration with Kafka and PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
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

incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), nullable=True),
    StructField("adv_campaign_id", StringType(), nullable=True),
    StructField("adv_campaign_content", StringType(), nullable=True),
    StructField("adv_campaign_owner", StringType(), nullable=True),
    StructField("adv_campaign_owner_contact", StringType(), nullable=True),
    StructField("adv_campaign_datetime_start", IntegerType(), nullable=True),
    StructField("adv_campaign_datetime_end", IntegerType(), nullable=True),
    StructField("datetime_created", IntegerType(), nullable=True)
    ])

restaurant_read_stream_df = restaurant_read_stream_df.withColumn('value', restaurant_read_stream_df.value.cast("string")) \
    .withColumn('key', restaurant_read_stream_df.key.cast("string"))
restaurant_read_stream_df = restaurant_read_stream_df.withColumn('value_json', F.from_json(restaurant_read_stream_df.value, incomming_message_schema)) \
    .selectExpr('value_json.*', '*') \
    .drop('value_json')

restaurant_read_stream_df \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination() \
    .show()