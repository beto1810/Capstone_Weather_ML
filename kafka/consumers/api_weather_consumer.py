import requests
import json
import time
import os
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaConsumer
from Weather_ML.snowflake_utils.snowflake_loader import load_to_snowflake

load_dotenv()

BATCH_SIZE = 50  # You can adjust this
timeout_ms = 5000  # Default timeout for Kafka polling

def transform_to_row(data_weather):
    """Convert a raw message into a tuple row for Snowflake."""
    return (
        data_weather['province_name'],
        data_weather['last_updated'],
        data_weather['temp_c'],
        data_weather['temp_f'],
        data_weather['is_day'],
        data_weather['condition']['text'],
        data_weather['condition']['icon'],
        data_weather['condition']['code'],
        data_weather['wind_mph'],
        data_weather['wind_kph'],
        data_weather['wind_degree'],
        data_weather['wind_dir'],
        data_weather['pressure_mb'],
        data_weather['pressure_in'],
        data_weather['precip_mm'],
        data_weather['precip_in'],
        data_weather['humidity'],
        data_weather['cloud'],
        data_weather['feelslike_c'],
        data_weather['feelslike_f'],
        data_weather['vis_km'],
        data_weather['vis_miles'],
        data_weather['uv'],
        data_weather['gust_mph'],
        data_weather['gust_kph']
    )

def consume_messages(timeout_ms=5000):
    consumer = KafkaConsumer(
        'data-weather',
        bootstrap_servers='kafka:9092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=timeout_ms
    )

    print("Starting consumer...", flush=True)
    messages_processed = 0
    batch = []

    try:
        for message in consumer:
            data_weather = message.value
            if data_weather:
                row = transform_to_row(data_weather)
                batch.append(row)
                messages_processed += 1

            if len(batch) >= BATCH_SIZE:
                print(f"Processing batch of {len(batch)} messages...", flush=True)
                load_to_snowflake(batch)
                batch.clear()  # Clear the batch after processing

        if batch:
            print(f"Processing final batch of {len(batch)} messages...", flush=True)
            load_to_snowflake(batch)
            batch.clear()

        print(f"Processed {messages_processed} messages in total.", flush=True)

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    except Exception as e:
        print(f"Error consuming messages: {str(e)}")
        raise
    finally:
        consumer.close()
        print("Consumer closed successfully.", flush=True)

    return {"messages_processed": messages_processed}

if __name__ == "__main__":
    consume_messages()
