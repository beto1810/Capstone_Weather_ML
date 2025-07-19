from datetime import datetime, timedelta
from dotenv import dotenv_values
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
import os

# Load environment variables once at module level
# Try the correct path first based on debug findings
env_file_path = "/opt/airflow/.env"

# Load environment variables and filter out None values
raw_env_vars = dotenv_values(env_file_path)
dbt_env_vars = {k: v for k, v in raw_env_vars.items() if v is not None}


@dag(
    schedule_interval="20,35,50,5 * * * *",
    start_date=datetime(2023, 10, 1),
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
        "email_on_failure": False,
        "email_on_retry": False,
    },
    tags=["dbt", "weather", "aggregate", "prediction"],
)
def dbt_current_weather():

    dbt_stg_weather_data = BashOperator(
        task_id="dbt_stg_weather_data",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select stg_weather_data
    """,
        env=dbt_env_vars,
    )

    update_int_current_weather_province = BashOperator(
        task_id="dbt_int_current_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select int_current_weather_province
    """,
        env=dbt_env_vars,
    )

    update_fct_current_weather_province = BashOperator(
        task_id="dbt_fct_current_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select fct_current_weather_province
    """,
        env=dbt_env_vars,
    )

    (
        dbt_stg_weather_data
        >> update_int_current_weather_province
        >> update_fct_current_weather_province
    )


dag = dbt_current_weather()
