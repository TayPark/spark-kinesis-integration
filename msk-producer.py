from kafka import KafkaProducer
from datetime import datetime
import json
import os
import boto3
import configparser

env_config = configparser.ConfigParser()
env_config.read("config.ini")

AWS_CONFIG = {
    "aws_region": env_config["S3"]["AWS_S3_BUCKET_REGION"],
    "aws_access_key_id": env_config["S3"]["AWS_S3_BUCKET_ACCESS_KEY_ID"],
    "aws_secret_access_key": env_config["S3"]["AWS_S3_BUCKET_SECRET_ACCESS_KEY"],
    "aws_bucket_name": env_config["S3"]["AWS_S3_BUCKET_NAME"],
}

s3_client = boto3.client("s3", aws_access_key_id=AWS_CONFIG["aws_access_key_id"],
                               aws_secret_access_key=AWS_CONFIG["aws_secret_access_key"])

filename = "delivery_refined.csv"
object_key = f"testdata/{filename}"

kafka_brokers = env_config["KAFKA"]["KAFKA_BROKER_LIST"].split(",")
kafka_topic = env_config["KAFKA"]["KAFKA_TOPIC"]

# download file if not exists
if not os.path.isfile(filename):
    print("[MSK_PRODUCER] Cannot find target file. Download from S3...")
    s3_client.download_file(AWS_CONFIG["aws_bucket_name"], object_key, filename)

producer = KafkaProducer(bootstrap_servers=kafka_brokers,
                         value_serializer=lambda v: json.dumps(v).encode("utf-8"))

if __name__ == "__main__":
    msg_count = 0
    while True:
        with open("delivery_refined.csv", "r") as csv_file:
            for i, row in enumerate(csv_file):
                if i == 0: continue

                # 순번 제외
                parsed = row.split(",")[1:]
                
                payload = {
                    "구분명": parsed[0],
                    "집화일자": parsed[1],
                    "집배일자": parsed[2],
                    "수량(BOX)": parsed[4],
                    "운임": parsed[5],
                    "집화여부": parsed[6],
                    "집배시간": parsed[7],
                    "배달일자": parsed[8],
                    "장비구분": parsed[9],
                    "품목": parsed[10],
                    "SM명": parsed[11],
                    "받는분주소": parsed[12].strip(),
                }

                producer.send(kafka_topic, payload)

                msg_count += 1
                if msg_count % 1000 == 0:
                    print(f"[KAFKA_PRODUCER] {datetime.today().strftime('%Y-%m-%d-%H:%M:%S')} {msg_count} messages sent")
