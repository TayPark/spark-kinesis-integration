from datetime import datetime
from typing import Dict, Tuple
from faker import Faker
import boto3
import configparser
import time
import json

fake = Faker()


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


def generate_fake_data():
    return json.dumps({
        "timestamp": time.time(),
        "name": fake.name(),
        "address": fake.address(),
        "lat": float(fake.latitude()),
        "lon": float(fake.longitude()),
        "price": fake.random_int(min=1, max=10000),
    })


client = boto3.Session().client('kinesis')
stream_name = client.list_streams()['StreamNames'][0]
shard_id = client.list_shards(StreamName=stream_name)['Shards'][0]['ShardId']
shard_iterator = client.get_shard_iterator(
    StreamName=stream_name, ShardId=shard_id, ShardIteratorType='LATEST')['ShardIterator']

total_message_amount = 0
NUM_INSERT = 1000
while True:

    payload = generate_fake_data()
    kinesis_response = client.put_record(StreamName=stream_name,
        Data=payload, PartitionKey=AWS_CONFIG['partition_key'])

    if kinesis_response['ResponseMetadata']['HTTPStatusCode'] == 200:
        total_message_amount += 1
        
        if total_message_amount % 100 == 0:
            print(datetime.utcnow(), total_message_amount)

    """
    Release comment print() below to check whether message sent well to kinesis or not.
    """
    # print(client.get_records(ShardIterator=shard_iterator))
