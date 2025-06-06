# import dependencies
from airflow import DAG
from datetime import datetime, timedelta
import logging
import os
from dotenv import load_dotenv
from pathlib import Path
import requests
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from Weather_ML.kafka.producers.api_weather_producer import produce_messages
from Weather_ML.kafka.producers.api_weather_producer import get_cities_from_snowflake
from Weather_ML.kafka.consumers.api_weather_consumer import consume_messages
# Load environment variables from .env file
load_dotenv()

@task
def get_snowflake_hook():

    hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
    try:
        # Attempt to get a connection to test it
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")  # Simple test query
        version = cursor.fetchone()
        logging.info(f"✅ Successfully connected to Snowflake. Version: {version[0]}")
        result = hook.get_pandas_df("SELECT CURRENT_VERSION()")
    except Exception as e:
        logging.error("❌ Failed to connect to Snowflake", exc_info=True)
        raise
    return result.to_dict()

@task
def fetch_weather_data():
    try:
        cities_df = get_cities_from_snowflake()
        if cities_df.empty:
            logging.warning("No active cities found in Snowflake. Skipping data production.")
            return 0
        logging.info(f"Fetched {len(cities_df['province_name'])} active cities from Snowflake")
        result = produce_messages(cities_df)
        logging.info(f"Successfully produced {result} messages to Kafka")
        return result
    except Exception as e:
        logging.error("Failed to produce messages to Kafka", exc_info=True)
        raise

@task
def consume_weather():
    return  consume_messages()

@dag(
    schedule_interval='5 * * * *',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    }
)
def weather_data_pipeline():
    test_connection  = get_snowflake_hook()
    producer_weather_data = fetch_weather_data()
    consume_weather_data = consume_weather()

    dbt_stg_weather_data = BashOperator(
        task_id='dbt_stg_weather_data',
        bash_command="""
        cd /opt/dbt && \
        dbt run --select stg_weather_data --profiles-dir /opt/dbt/.dbt
    """

    )


    test_connection >> producer_weather_data
    test_connection >> consume_weather_data
    consume_weather_data >> dbt_stg_weather_data

dag = weather_data_pipeline()