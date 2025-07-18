"""
Kafka producer for weather data ingestion.
Fetches city data from Snowflake, queries weather API, and produces messages to Kafka.
"""
import json
import logging
import os
import time

import pandas as pd
import requests
import snowflake.connector
from dotenv import load_dotenv

from kafka import KafkaProducer # type: ignore
from kafka.errors import KafkaError

load_dotenv()
# Configure logging
#test git revert
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
        "SNOWFLAKE_SCHEMA",
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
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            role="USER_DBT_ROLE",  # Ensure this role has access to the required tables
        )

        cursor = conn.cursor()

        logger.info(
            "Connecting to Snowflake as user %s on account %s", os.getenv("SNOWFLAKE_USER"), os.getenv("SNOWFLAKE_ACCOUNT")
        )
        logger.info("Connected to Snowflake successfully")

        # Fetch cities data
        query = """
        SELECT province_name, latitude, longitude
        FROM dim_vietnam_provinces
        """

        cursor.execute(query)
        cities_df = pd.DataFrame(
            cursor.fetchall(), columns=["province_name", "latitude", "longitude"] # type: ignore
        )

        logger.info("Loaded %d cities from Snowflake", len(cities_df["province_name"]))
        return cities_df

    except Exception as e:
        logger.error("Failed to fetch cities from Snowflake: %s", str(e))
        raise
    finally:
        if "conn" in locals():
            conn.close()


def fetch_data_from_api(latitude, longitude):
    """Fetch weather data from the API for given latitude and longitude."""
    api_key = os.getenv("WEATHER_API_KEY")
    if not api_key:
        raise ValueError(
            "API key not found. Please set the WEATHER_API_KEY environment variable."
        )

    weather_url = (
        f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={latitude},{longitude}&aqi=no"
    )
    response = requests.get(weather_url, timeout=10)
    response.raise_for_status()
    return response.json()  # Expecting a list of events


def produce_messages(cities, max_retries=3, timeout=10):
    """Produce weather data messages to Kafka for each city."""
    import datetime
    try:
        from zoneinfo import ZoneInfo
        tz = ZoneInfo("Asia/Bangkok")
    except ImportError:
        tz = None
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
    messages_count = 0
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

                    # Add current_time in GMT+7 for logging only
                    if tz:
                        current_time = datetime.datetime.now(tz).strftime("%Y-%m-%d %H:%M:%S")
                    else:
                        current_time = (
                            datetime.datetime.utcnow() + datetime.timedelta(hours=7)
                        ).strftime("%Y-%m-%d %H:%M:%S")

                    logger.info(
                        "Sending data for %s at %s (GMT+7)...",
                        data_weather["province_name"],
                        current_time,
                    )

                    # Send with increased timeout
                    future = producer.send("data-weather", data_weather)
                    record_metadata = future.get(timeout=timeout)

                    logger.info(
                        "Successfully sent data for %s to partition %s at offset %s",
                        data_weather["province_name"],
                        record_metadata.partition,
                        record_metadata.offset,
                    )

                    messages_count += 1
                    break  # Success, exit retry loop

                except requests.exceptions.RequestException as e:
                    logger.error("API error for %s: %s", province, str(e))
                    break  # Don't retry API errors

                except KafkaError as e:
                    logger.error("Error sending data for %s: %s", province, str(e))
                    retries += 1
                    if retries == max_retries:
                        logger.error(
                            "Failed to send data for %s after %d attempts",
                            province,
                            max_retries,
                        )
                    else:
                        logger.info("Retrying... (%d/%d)", retries, max_retries)
                        time.sleep(1)  # Wait before retry

            time.sleep(1)  # Wait between cities

        logger.info("Finished processing all cities.")
        return messages_count

    except KeyboardInterrupt:
        logger.info("Producer stopped by user")
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
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
    """Main entry point for the weather data Kafka producer."""
    import sys
    while True:
        try:
            cities = get_cities_from_snowflake()
            if cities.empty:
                logger.error("No cities found in Snowflake. Exiting.")
            else:
                logger.info(
                    "Found %d cities. Starting to produce messages...",
                    len(cities["province_name"]),
                )
                total_messages = produce_messages(cities)
                logger.info("Total messages produced: %d", total_messages)
        except Exception as e:
            logger.error("Failed to produce messages: %s", str(e))
        time.sleep(900)
