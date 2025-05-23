# import dependencies
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging  
import os
from dotenv import load_dotenv
from pathlib import Path
import requests
import snowflake.connector
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sensors.base import PokeReturnValue
from airflow.hooks.base import BaseHook

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


@dag(
    schedule_interval='@daily',
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
    test_connection

dag = weather_data_pipeline()