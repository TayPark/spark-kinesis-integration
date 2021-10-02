from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, from_unixtime
import configparser

from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

env_config = configparser.ConfigParser()
env_config.read("config.ini")

AWS_CONFIG = {
    'aws_access_key_id': env_config['AWS']['AWS_ACCESS_KEY_ID'],
    'aws_secret_access_key': env_config['AWS']['AWS_SECRET_ACCESS_KEY'],
    'aws_kinesis_stream_name': env_config['Kinesis']['AWS_KINESIS_STREAM_NAME']
}

sc = SparkSession \
    .builder \
    .appName("Kinesis consumer") \
    .getOrCreate()

endpoitUrl = 'https://kinesis.ap-northeast-2.amazonaws.com'

kinesisDF = sc.readStream \
    .format('kinesis') \
    .option('endpointUrl', endpoitUrl) \
    .option('awsAccessKeyId', AWS_CONFIG['aws_access_key_id']) \
    .option('awsSecretKey', AWS_CONFIG['aws_secret_access_key']) \
    .option('streamName', AWS_CONFIG['aws_kinesis_stream_name']) \
    .option('startingposition', 'latest') \
    .load()\


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
    .withColumn('unix_timestamp', from_unixtime(col('timestamp').alias('unix_timestamp')))

jsonParsedDF.writeStream \
    .format("console") \
    .trigger(processingTime='1 seconds') \
    .outputMode("append")\
    .option("checkpointLocation", "checkpoint/") \
    .option("path", "kinesis_write.csv") \
    .start()\
    .awaitTermination()
