from datetime import datetime
import random
from datetime import datetime
from typing import Dict, Tuple
import boto3
import configparser
import json


env_config = configparser.ConfigParser()
env_config.read("config.ini")

AWS_CONFIG = {
    "aws_region": env_config['AWS']['AWS_REGION'],
    "aws_access_key_id": env_config['AWS']['AWS_ACCESS_KEY_ID'],
    "aws_secret_access_key": env_config['AWS']['AWS_SECRET_ACCESS_KEY'],
    "stream_name": env_config['KINESIS']['AWS_KINESIS_STREAM_NAME'],
    "partition_key": env_config['KINESIS']['AWS_KINESIS_PARTITION_KEY'],
}


def get_kinesis_client_connection(config):
    """Get connection with AWS Kinesis with given config.

    Example
    -------

    >>> config  # config format
    {
        "aws_region": YOUR_AWS_REGION,
        "aws_access_key_id": YOUR_ACCESS_KEY_ID,
        "aws_secret_access_key": YOUR_SECRET_ACCESS_KEY,
        "stream_name": KINESIS_STREAM_NAME
    }
    """
    return boto3.client(service_name='kinesis', aws_access_key_id=config['aws_access_key_id'],
                        aws_secret_access_key=['aws_secret_access_key'], region_name=config['aws_region'])


class KinesisClient:
    def __init__(self, config):
        self._config = config
        self.stream_name = config['stream_name']
        self.client = get_kinesis_client_connection(config)

    def put_record(self, record: Tuple):
        """
        Put record to Kinesis connection. Parameter `record` should be a Tuple type like `(PartitionKey, Data)`.

        Example
        -------

        >>> kc = KinesisClient(config)   # Create a Kinesis Client.
        >>> kc.put_record(('part_key', 'hello_data')) # If you invoke .put_record() method, if successed, returns ShardId and SequenceNumber.
        {
            "ShardId": "shardId-000000000000",
            "SequenceNumber": "49622591066282461377338739270427554170387450219917737986"
        }
        """
        return self.client.put_record(StreamName=self.stream_name, PartitionKey=record[0], Data=record[1])

    def get_records(self, shardIterator: str):
        return self.client.get_records(StreamName=self.stream_name, ShardIterator=shardIterator)

    def describe_stream_summary(self) -> Dict:
        return self.client.describe_stream_summary(StreamName=self.stream_name)

    def list_streams(self):
        return self.list_streams()

    def get_shard_iterator(self, shardIteratorType):
        """
        Get shard iterator to get_record startpoint. Parameter `shardIteratorType` *MUST* be one of below.

        - `AT_SEQUENCE_NUMBER` - Start reading exactly from the position denoted by a specific sequence number.
        - `AFTER_SEQUENCE_NUMBER` - Start reading right after the position denoted by a specific sequence number.
        - `TRIM_HORIZON` - Start reading at the last untrimmed record in the shard in the system, which is the oldest data record in the shard.
        - `LATEST` - Start reading just after the most recent record in the shard, so that you always read the most recent data in the shard.
        """
        self.shards = self.client.list_shards(
            StreamName=self.stream_name)  # Update currently available shard lists
        self.client.get_shard_iterator(
            StreamName=self.stream_name, ShardId=self.Shards[0], ShardIteratorType=shardIteratorType)['ShardIterator']

    @property
    def client_config(self):
        return self._config


# kc = get_kinesis_client_connection(AWS_CONFIG).list_streams()
# print(kc)

"""
2021.10.03 03:55 파일로 credential 주고 제어하고 싶었는데 내부 오류인지 auth가 되지 않아 중도 포기
"""

client = boto3.Session().client('kinesis')
stream_name = client.list_streams()['StreamNames'][0]

# Get random ShardId
shard_id = random.choice(client.list_shards(
    StreamName=stream_name)['Shards'])['ShardId']

shard_iterator = client.get_shard_iterator(StreamName=stream_name,
                                           ShardId=shard_id,
                                           ShardIteratorType='LATEST')['ShardIterator']

msg_count = 0
filename = 'delivery_refined.csv'

while True:
    with open(filename, 'r') as csv_file:
        for i, row in enumerate(csv_file):
            # header 제외
            if i == 0:
                continue

            parsed = row.split(',')[1:]

            payload = json.dumps({
                "구분명": parsed[0],
                "집화일자": parsed[1],
                "집배일자": parsed[2],
                "운임명": parsed[3],
                "수량(BOX)": int(parsed[4]),
                "운임": int(parsed[5]),
                "집화여부": parsed[6],
                "집배시간": parsed[7],
                "배달일자": parsed[8],
                "장비구분": parsed[9],
                "품목": parsed[10],
                "SM명": parsed[11],
                "받는분주소": parsed[12].strip(),
            })

            kinesis_response = client.put_record(StreamName=stream_name,
                                                 Data=payload,
                                                 PartitionKey=AWS_CONFIG['partition_key'])

            if kinesis_response['ResponseMetadata']['HTTPStatusCode'] == 200:
                msg_count += 1

                if msg_count % 100 == 0:
                    print(datetime.utcnow(), msg_count)

            """
            Release comment print() below to check whether message sent well to kinesis or not.
            """
            # print(client.get_records(ShardIterator=shard_iterator))
