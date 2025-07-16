"""Kafka consumer for weather data, loads messages into Snowflake."""
import json
import logging
import os
import time
from dotenv import load_dotenv
from snowflake.connector import connect
from kafka import KafkaConsumer


BATCH_SIZE = 50  # You can adjust this
TIMEOUT_MS = 5000  # Default timeout for Kafka polling

load_dotenv()


def load_to_snowflake(rows):
    """Load weather data into Snowflake."""
    try:
        print("Entered load_to_snowflake", flush=True)

        conn = connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA_RAW_DATA"),
        )
        print("Connected to snowflake", flush=True)

        cursor = conn.cursor()

        # Create a more detailed table schema matching the Weather API response
        cursor.execute(
            """
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
        """
        )

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

        print("First row to insert: %s", rows[0] if rows else 'No rows', flush=True)
        cursor.executemany(insert_query, rows)
        conn.commit()
        print("✅ Successfully loaded %d rows into Snowflake", len(rows), flush=True)

    except Exception as e:
        logging.error("❌ Error loading to Snowflake: %s", e)
        raise
    finally:
        if "conn" in locals():
            conn.close()


def transform_to_row(data_weather):
    """Convert a raw message into a tuple row for Snowflake."""
    return (
        data_weather["province_name"],
        data_weather["last_updated"],
        data_weather["temp_c"],
        data_weather["temp_f"],
        data_weather["is_day"],
        data_weather["condition"]["text"],
        data_weather["condition"]["icon"],
        data_weather["condition"]["code"],
        data_weather["wind_mph"],
        data_weather["wind_kph"],
        data_weather["wind_degree"],
        data_weather["wind_dir"],
        data_weather["pressure_mb"],
        data_weather["pressure_in"],
        data_weather["precip_mm"],
        data_weather["precip_in"],
        data_weather["humidity"],
        data_weather["cloud"],
        data_weather["feelslike_c"],
        data_weather["feelslike_f"],
        data_weather["vis_km"],
        data_weather["vis_miles"],
        data_weather["uv"],
        data_weather["gust_mph"],
        data_weather["gust_kph"],
    )


def consume_messages():
    """Consume weather data messages from Kafka and load them into Snowflake."""
    consumer = KafkaConsumer(
        "data-weather",
        bootstrap_servers="kafka:9092",
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="weather-consumer-group",
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        consumer_timeout_ms=TIMEOUT_MS,
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
                print("Processing batch of %d messages...", len(batch), flush=True)
                load_to_snowflake(batch)
                batch.clear()  # Clear the batch after processing

        if batch:
            print("Processing final batch of %d messages...", len(batch), flush=True)
            load_to_snowflake(batch)
            batch.clear()

        print("Processed %d messages in total.", messages_processed, flush=True)

    except KeyboardInterrupt:
        print("Consumer stopped by user.")
    except (json.JSONDecodeError) as e:
        logging.error("Error consuming messages: %s", e)
        raise
    except Exception as e:
        logging.error("Unexpected error consuming messages: %s", e)
        raise
    finally:
        consumer.close()
        print("Consumer closed successfully.", flush=True)

    return {"messages_processed": messages_processed}


if __name__ == "__main__":
    RETRY_COUNT = 0
    MAX_RETRIES = 100  # Set to None for infinite retries
    while True:
        try:
            consume_messages()
            RETRY_COUNT = 0  # Reset on successful run
        except Exception as e:
            logging.error(
                "Consumer crashed with error: %s. Restarting in 5 seconds...",
                e,
            )
            RETRY_COUNT += 1
            if MAX_RETRIES is not None and RETRY_COUNT >= MAX_RETRIES:
                logging.error("Max retries reached. Exiting.")
                break
            time.sleep(5)
