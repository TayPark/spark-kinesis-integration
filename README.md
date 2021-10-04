# spark-kinesis-integration

PySpark 3 + Kinesis integration

## Overview

[Kinesis](https://aws.amazon.com/kinesis/?nc1=h_ls) is a fully managed message broker service on AWS. This repo contains consumer and producer implemented with python3. 

**Producer** is a data source which push messages to message broker. **Consumer** pulls messages from message broker for their usage.

In this example, producer generates 10 JSON string messages on every seconds and push to Kinesis server, whereas consumer pulls it. Also, consumer implemented with [Apache Spark](https://spark.apache.org/) which supports [Spark Structued Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html). Spark Structued Streaming can process data very quickly with data type name **DataFrame**, a table-like data, and can easily get lookup aggregation on every stream intervals. As a result, when consumer got data, parse it, process it, then show it as table-like. 

## Kinesis Producer

Configure producer with [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html). AWS credentials MUST BE PLACED IN `~/.aws/config` or configure with `AWS CLI`.

## Kinesis Consumer

An Apache Spark Structured Streaming application. Configure AWS credentials with `config.ini` file and [configparser](https://pypi.org/project/configparser/) package. 
