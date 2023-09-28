import json
import os
from kafka import KafkaProducer
import boto3
from time import sleep

# Define your topic
KAFKA_TOPIC = "check_kafka_ingest"

# Initialize Kafka producer
producer = KafkaProducer(bootstrap_servers=['18.119.19.35:9092'])

# Initialize S3 client
s3 = boto3.client('s3')

def lambda_handler(event, context):
    for record in event['Records']:
        # Get the S3 bucket name and object key from the event
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']

        # Construct the S3 object URL
        s3_object_url = f"s3://{bucket_name}/{object_key}"



        response = s3.get_object(Bucket=bucket_name, Key=object_key)
        image_object = response['Body'].read()

        # Create a message to send to Kafka
        kafka_message = {
            "s3_object_url": s3_object_url,
            "image_object": image_object
        }


        # Send the message to Kafka
        producer.send(KAFKA_TOPIC, value=kafka_message)
        sleep(1.5)

        print("Successfully sent data to Kafla Topic")
    
    producer.flush()





