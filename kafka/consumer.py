from kafka import KafkaConsumer
from time import sleep
from json import dumps, loads
import json
import boto3

consumer = KafkaConsumer(
    'spotify_topic',
    bootstrap_servers=['52.91.46.51'],
    value_deserializer=lambda x: loads(x.decode('utf-8')))

session = boto3.Session()
s3 = session.client('s3')


def upload_data_to_s3(data, bucket_name, key):
    data_json = json.dumps(data)

    response = s3.put_object(
        Body=data_json,
        Bucket=bucket_name,
        Key=key)
    print(f"Data uploaded to S3 with response: {response}")


bucket_name = "spotifydatasetteam02"

data_count = 500
for count, i in enumerate(consumer):
    data_count += 1
    data = i.value  # Consumed data
    print(data)
    upload_data_to_s3(data, bucket_name, "track_{}.json".format(data_count))
