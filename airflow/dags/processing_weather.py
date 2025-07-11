from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import dotenv_values
import logging
import snowflake.connector
import pandas as pd

dbt_env_vars = dotenv_values("/opt/airflow/.env")




print("✅ ENV VARS LOADED:")
print(dbt_env_vars)



@dag(
    schedule_interval='15 * * * *',
    start_date=datetime(2023, 10, 1),
    catchup=False,
    default_args={
        'owner': 'airflow',
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    tags=["dbt", "weather","aggregate", "prediction"]
)
def dbt_current_weather():

    dbt_stg_weather_data = BashOperator(
        task_id='dbt_stg_weather_data',
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --select stg_weather_data
    """,
    env=dbt_env_vars
    )

    update_int_current_weather_province = BashOperator(
        task_id="dbt_int_current_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --select int_current_weather_province
    """,
    env=dbt_env_vars
    )


    update_fct_current_weather_province = BashOperator(
        task_id ='dbt_fct_current_weather_province',
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --select fct_current_weather_province
    """,
    env=dbt_env_vars
    )


    dbt_stg_weather_data >> update_int_current_weather_province >> update_fct_current_weather_province

dag = dbt_current_weather()