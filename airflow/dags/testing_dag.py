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
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from Weather_ML.kafka.producers.api_weather_producer import produce_messages
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
        result = produce_messages()
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

    test_connection >> producer_weather_data
    test_connection >> consume_weather_data 

dag = weather_data_pipeline()