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

from airflow.decorators import task
import snowflake.connector
import logging
import os
import pandas as pd
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization

import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))
from Weather_ML.kafka.producers.api_weather_producer import produce_messages
from Weather_ML.kafka.producers.api_weather_producer import get_cities_from_snowflake
from Weather_ML.kafka.consumers.api_weather_consumer import consume_messages
# Load environment variables from .env file
load_dotenv()

@task
def get_snowflake_hook():
    try:
        # Load PEM private key and convert to DER bytes
        private_key_path = "/opt/airflow/keys/rsa_key.p8"  # 🔁 Thay bằng đường dẫn thực tế
        with open(private_key_path, "rb") as key_file:
            p_key = serialization.load_pem_private_key(
                key_file.read(),
                password=None,  # hoặc b"your_password" nếu key được mã hóa
                backend=default_backend()
            )
        pkb = p_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )

        # Thiết lập kết nối Snowflake
        conn = snowflake.connector.connect(
            user="DATNGUYEN1810",
            account="ZFLRNBO-YT37108",
            private_key=pkb,
            warehouse="DBT_WH",
            database="KAFKA_AIRFLOW_WEATHER",
            schema="PUBLIC",
            role="USER_DBT_ROLE"
        )

        # Truy vấn test
        cursor = conn.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        version = cursor.fetchone()
        logging.info(f"✅ Successfully connected to Snowflake. Version: {version[0]}")

        # Truy vấn trả kết quả dưới dạng pandas dataframe
        df = pd.read_sql("SELECT CURRENT_VERSION()", conn)
        return df.to_dict()

    except Exception as e:
        logging.error("❌ Failed to connect to Snowflake", exc_info=True)
        raise

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

    test_connection >> producer_weather_data >> consume_weather_data >> dbt_stg_weather_data

dag = weather_data_pipeline()