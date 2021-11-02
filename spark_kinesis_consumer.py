from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, lit, to_timestamp, window
import configparser
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

env_config = configparser.ConfigParser()
env_config.read("config.ini")

AWS_CONFIG = {
    'aws_region': env_config['AWS']['AWS_REGION'],
    'aws_access_key_id': env_config['AWS']['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': env_config['AWS']['AWS_SECRET_ACCESS_KEY'],
    'aws_kinesis_stream_name': env_config['KINESIS']['AWS_KINESIS_STREAM_NAME'],
    'aws_kinesis_partition_key': env_config['KINESIS']['AWS_KINESIS_PARTITION_KEY']
}

ss = SparkSession \
    .builder \
    .appName("Kinesis consumer") \
    .getOrCreate()

endpointUrl = 'https://kinesis.ap-northeast-2.amazonaws.com'

kinesisDF = ss.readStream \
    .format('kinesis') \
    .option('endpointUrl', endpointUrl) \
    .option('awsAccessKeyId', AWS_CONFIG['aws_access_key_id']) \
    .option('awsSecretKey', AWS_CONFIG['aws_secret_access_key']) \
    .option('streamName', AWS_CONFIG['aws_kinesis_stream_name']) \
    .option('initialPosition', 'earliest') \
    .load()

dataSchema = StructType([
    StructField("구분명", StringType(), True),
    StructField("집화일자", TimestampType(), True),
    StructField("집배일자", TimestampType(), True),
    StructField("운임명", StringType(), True),
    StructField("수량(BOX)", IntegerType(), True),
    StructField("운임", IntegerType(), True),
    StructField("집화여부", StringType(), True),
    StructField("집배시간", IntegerType(), True),
    StructField("배달일자", TimestampType(), True),
    StructField("장비구분", StringType(), True),
    StructField("품목", StringType(), True),
    StructField("SM명", StringType(), True),
    StructField("받는분주소", StringType(), True),
])

jsonParsedDF = kinesisDF.selectExpr("CAST(data AS STRING)") \
    .select(from_json(col("data"), dataSchema).alias('parsed_data')) \
    .select('parsed_data.*') \
    .withColumn('unix_timestamp', to_timestamp(lit(datetime.now()))) \
    .select("집화일자", "품목", "받는분주소") \
    .groupBy("품목").count()
    
# print(jsonParsedDF.count())

jsonParsedDF \
    .writeStream \
    .format("console") \
    .option("checkpointLocation", "checkpoint/") \
    .option("startingOffsets", 'latest') \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .start() \
    .awaitTermination()
