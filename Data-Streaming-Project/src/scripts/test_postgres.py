from datetime import datetime
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F 
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, TimestampType, IntegerType

TOPIC_IN = 'yc-user_in'

spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

postgresql_settings = {
    'url': 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'subscribers_restaurants',
    'user': 'student',
    'password': 'de-student',
}

# subtract all users with a subscription to restaurants
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .options(**postgresql_settings) \
                    .load().cache()



subscribers_restaurant_df.show(20)

"""  
launch
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.4.0 /root/datas8/test_postgres.py
gives:
+---+--------------------+--------------------+
| id|           client_id|       restaurant_id|
+---+--------------------+--------------------+
|  1|223e4567-e89b-12d...|123e4567-e89b-12d...|
|  2|323e4567-e89b-12d...|123e4567-e89b-12d...|
|  3|423e4567-e89b-12d...|123e4567-e89b-12d...|
|  4|523e4567-e89b-12d...|123e4567-e89b-12d...|
|  5|623e4567-e89b-12d...|123e4567-e89b-12d...|
|  6|723e4567-e89b-12d...|123e4567-e89b-12d...|
|  7|823e4567-e89b-12d...|123e4567-e89b-12d...|
|  8|923e4567-e89b-12d...|123e4567-e89b-12d...|
|  9|923e4567-e89b-12d...|123e4567-e89b-12d...|
| 10|023e4567-e89b-12d...|123e4567-e89b-12d...|
+---+--------------------+--------------------+
"""