import requests
import json
import time
import os 
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaConsumer
from Weather_ML.snowflake_utils.snowflake_loader import load_to_snowflake

def consume_messages(timeout_ms=5000):  # Add timeout parameter
    consumer = KafkaConsumer(
        'data-weather',
        bootstrap_servers='kafka:9092',  # Use localhost for local development
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='weather-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=timeout_ms  # Add timeout
    )

    print("Starting consumer...", flush=True)
    messages_processed = 0
    try:
        for message in consumer:
            data_weather = message.value
            print(f"Consumed: {data_weather}", flush=True)
            if data_weather:
                load_to_snowflake(data_weather)
                messages_processed += 1
            else:
                print("No data received, skipping...", flush=True)
        
        print(f"Processed {messages_processed} messages. No more messages available.", flush=True)
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
