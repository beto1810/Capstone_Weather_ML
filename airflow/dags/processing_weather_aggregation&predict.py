from datetime import datetime, timedelta

from dotenv import load_dotenv
from predict_weather import predict_weather_7day

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dbt_env_vars = load_dotenv("/opt/airflow/.env")

print("✅ ENV VARS LOADED:")


@dag(
    schedule_interval="45 16 * * *",
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
def dbt_weather_aggregate_prediction():

    update_int_lag_weather_province = BashOperator(
        task_id="dbt_int_lag_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select int_lag_weather_province
    """,
    )

    update_int_weather_province = BashOperator(
        task_id="dbt_int_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select int_weather_province
    """,
    )

    update_fct_weather_province = BashOperator(
        task_id="dbt_fct_weather_province",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select fct_weather_province
    """,
    )

    update_fct_weather_region = BashOperator(
        task_id="dbt_fct_weather_region",
        bash_command="""
        cd /opt/dbt && \
        /home/airflow/.local/bin/dbt run --target airflow --select fct_weather_region
    """,
    )

    predict_weather_7days = PythonOperator(
        task_id="predict_weather_7day",
        python_callable=predict_weather_7day,
    )

    update_int_lag_weather_province >> predict_weather_7days

    (
        update_int_weather_province
        >> update_fct_weather_province
        >> update_fct_weather_region
    )


dag = dbt_weather_aggregate_prediction()
