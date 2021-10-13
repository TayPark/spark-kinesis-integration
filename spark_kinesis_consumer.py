from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, window
import configparser
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

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
    StructField("timestamp", DoubleType(), True),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("price", IntegerType(), True),
])

jsonParsedDF = kinesisDF.selectExpr("CAST(data AS STRING)") \
    .select(from_json(col("data"), dataSchema).alias('parsed_data')) \
    .select('parsed_data.*') \
    .withColumn('unix_timestamp', to_timestamp(col('timestamp'))) \
    .drop(col('timestamp'))

sumPriceDF = jsonParsedDF \
    .withWatermark('unix_timestamp', '5 minutes') \
    .groupBy(window(jsonParsedDF.unix_timestamp, '10 minutes', '5 minutes')) \
    .sum('price')

sumPriceDF.writeStream \
    .format("console") \
    .outputMode("update") \
    .trigger(processingTime='5 seconds') \
    .option("checkpointLocation", "checkpoint/") \
    .start() \
    .awaitTermination()
