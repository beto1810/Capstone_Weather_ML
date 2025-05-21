import requests
import json
from confluent_kafka import Producer
import time
import os 
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaConsumer


def consume_messages():
    consumer = KafkaConsumer(
        'data-weather',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print("Starting consumer...", flush=True)
    try:
        for message in consumer:
            data_weather = message.value
            print(f"Consumed: {data_weather}", flush=True)
    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
