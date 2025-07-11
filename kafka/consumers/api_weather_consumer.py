import requests
import json
import time
import os
import logging
from dotenv import load_dotenv
from pathlib import Path
from kafka import KafkaConsumer
from snowflake.connector import connect

load_dotenv()

BATCH_SIZE = 50  # You can adjust this
timeout_ms = 5000  # Default timeout for Kafka polling

load_dotenv()

def load_to_snowflake(rows):
    """Load weather data into Snowflake."""
    try:
        print("Entered load_to_snowflake", flush=True)
        # Debug print for connection parameters
        print("Connecting to Snowflake with:", flush=True)
        print(f"  user={os.getenv('SNOWFLAKE_USER')}", flush=True)
        print(f"  account={os.getenv('SNOWFLAKE_ACCOUNT')}", flush=True)
        print(f"  warehouse={os.getenv('SNOWFLAKE_WAREHOUSE')}", flush=True)
        print(f"  database={os.getenv('SNOWFLAKE_DATABASE')}", flush=True)
        print(f"  schema={os.getenv('SNOWFLAKE_SCHEMA_RAW_DATA')}", flush=True)
        conn = connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA_RAW_DATA')
        )

        cursor = conn.cursor()

        # Create a more detailed table schema matching the Weather API response
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather_data (
                province_name VARCHAR,
                last_updated TIMESTAMP_NTZ,
                temp_c FLOAT,
                temp_f FLOAT,
                is_day BOOLEAN,
                condition_text VARCHAR,
                condition_icon VARCHAR,
                condition_code INTEGER,
                wind_mph FLOAT,
                wind_kph FLOAT,
                wind_degree INTEGER,
                wind_dir VARCHAR,
                pressure_mb FLOAT,
                pressure_in FLOAT,
                precip_mm FLOAT,
                precip_in FLOAT,
                humidity INTEGER,
                cloud INTEGER,
                feelslike_c FLOAT,
                feelslike_f FLOAT,
                vis_km FLOAT,
                vis_miles FLOAT,
                uv FLOAT,
                gust_mph FLOAT,
                gust_kph FLOAT,
                loaded_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)

        # Insert data with all available fields
        insert_query = """
            INSERT INTO raw_weather_data (
                province_name, last_updated, temp_c, temp_f, is_day,
                condition_text, condition_icon, condition_code,
                wind_mph, wind_kph, wind_degree, wind_dir,
                pressure_mb, pressure_in, precip_mm, precip_in,
                humidity, cloud, feelslike_c, feelslike_f,
                vis_km, vis_miles, uv, gust_mph, gust_kph
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s)
        """

        print(f"First row to insert: {rows[0] if rows else 'No rows'}", flush=True)
        cursor.executemany(insert_query, rows)
        conn.commit()
        print(f"✅ Successfully loaded {len(rows)} rows into Snowflake", flush=True)

    except Exception as e:
        logging.error(f"❌ Error loading to Snowflake: {e}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

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

def consume_messages():
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
    retry_count = 0
    max_retries = 100  # Set to None for infinite retries
    while True:
        try:
            consume_messages()
            retry_count = 0  # Reset on successful run
        except Exception as e:
            print(f"Consumer crashed with error: {e}. Restarting in 5 seconds...", flush=True)
            retry_count += 1
            if max_retries is not None and retry_count >= max_retries:
                print("Max retries reached. Exiting.", flush=True)
                break
            time.sleep(5)

