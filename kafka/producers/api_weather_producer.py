import json
import logging
import os
import time

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

from kafka import KafkaProducer
from kafka.errors import KafkaError

load_dotenv()
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "data"
)

API_KEY = os.getenv("WEATHER_API_KEY")
if not API_KEY:
    raise ValueError(
        "API key not found. Please set the WEATHER_API_KEY environment variable."
    )


def get_cities_from_snowflake():
    """Fetch cities data from Snowflake."""
    required = [
        "SNOWFLAKE_USER",
        "SNOWFLAKE_PASSWORD",
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_WAREHOUSE",
        "SNOWFLAKE_DATABASE",
        "SNOWFLAKE_SCHEMA_ANALYSIS",
    ]
    missing = [v for v in required if not os.getenv(v)]
    if missing:
        raise ValueError(f"Missing required env vars: {missing}")
    try:
        conn = snowflake.connector.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA_ANALYSIS"),
            role="USER_DBT_ROLE",  # Ensure this role has access to the required tables
        )

        cursor = conn.cursor()

        print(user := os.getenv("SNOWFLAKE_USER"))
        print(account := os.getenv("SNOWFLAKE_ACCOUNT"))
        print(warehouse := os.getenv("SNOWFLAKE_WAREHOUSE"))
        print(database := os.getenv("SNOWFLAKE_DATABASE"))
        print(schema := os.getenv("SNOWFLAKE_SCHEMA_ANALYSIS"))
        print(role := "USER_DBT_ROLE")
        logger.info("Connecting to Snowflake as user %s on account %s", user, account)

        print("connected to Snowflake successfully")

        # Fetch cities data
        query = """
        SELECT province_name, latitude, longitude
        FROM dim_vietnam_provinces
        """

        cursor.execute(query)
        cities_df = pd.DataFrame(
            cursor.fetchall(), columns=["province_name", "latitude", "longitude"]
        )

        logger.info(f"Loaded {len(cities_df['province_name'])} cities from Snowflake")
        return cities_df

    except Exception as e:
        logger.error(f"Failed to fetch cities from Snowflake: {str(e)}")
        raise
    finally:
        if "conn" in locals():
            conn.close()


def fetch_data_from_api(latitude, longitude):
    API_KEY = os.getenv("WEATHER_API_KEY")
    if not API_KEY:
        raise ValueError(
            "API key not found. Please set the WEATHER_API_KEY environment variable."
        )

    weather_url = f"https://api.weatherapi.com/v1/current.json?key={API_KEY}&q={latitude},{longitude}&aqi=no"
    response = requests.get(weather_url)
    response.raise_for_status()
    return response.json()  # Expecting a list of events


def produce_messages(cities, max_retries=3, timeout=10):
    bootstrap_servers = os.getenv("KAFKA_BROKER", "kafka:9092")

    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retry_backoff_ms=1000,
        request_timeout_ms=timeout * 1000,
        max_block_ms=30000,
        compression_type="gzip",
    )
    messages_processed = 0
    logger.info("Starting producer...")
    try:
        for province, lat, lon in zip(
            cities["province_name"], cities["latitude"], cities["longitude"]
        ):
            retries = 0
            while retries < max_retries:
                try:
                    data = fetch_data_from_api(lat, lon)
                    data_weather = data["current"]
                    data_weather["province_name"] = province

                    logger.info(
                        f"Sending data for {data_weather['province_name']}..."
                    )

                    # Send with increased timeout
                    future = producer.send("data-weather", data_weather)
                    record_metadata = future.get(timeout=timeout)

                    logger.info(
                        f"Successfully sent data for {data_weather['province_name']} "
                        f"to partition {record_metadata.partition} "
                        f"at offset {record_metadata.offset}"
                    )

                    messages_processed += 1
                    break  # Success, exit retry loop

                except requests.exceptions.RequestException as e:
                    logger.error(f"API error for {province}: {str(e)}")
                    break  # Don't retry API errors

                except KafkaError as e:
                    logger.error(f"Error sending data for {province}: {str(e)}")
                    retries += 1
                    if retries == max_retries:
                        logger.error(
                            f"Failed to send data for {province} after {max_retries} attempts"
                        )
                    else:
                        logger.info(f"Retrying... ({retries}/{max_retries})")
                        time.sleep(1)  # Wait before retry

            time.sleep(0.3)  # Wait between cities

        logger.info("Finished processing all cities.")
        return messages_processed

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer closed")


# def send_to_dlq(producer, data, error_msg):
#     """Send failed messages to DLQ with error context"""
#     dlq_message = {
#         'original_data': data,
#         'error': str(error_msg),
#         'timestamp': time.time()
#     }
#     try:
#         producer.send('weather-dlq', dlq_message).get()
#         logger.info(f"Message sent to DLQ: {error_msg}")
#     except Exception as e:
#         logger.error(f"Failed to send to DLQ: {str(e)}")

if __name__ == "__main__":
    while True:
        try:
            cities_df = get_cities_from_snowflake()
            if cities_df.empty:
                logger.error("No cities found in Snowflake. Exiting.")
            else:
                logger.info(
                    f"Found {len(cities_df['province_name'])} cities.
                    Starting to produce messages..."
                )
                messages_processed = produce_messages(cities_df)
                logger.info(f"Total messages produced: {messages_processed}")
        except Exception as e:
            logger.error(f"Failed to produce messages: {str(e)}")
            exit(1)
        time.sleep(900)
