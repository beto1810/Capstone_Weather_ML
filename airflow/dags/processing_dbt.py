from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

@dag(
    schedule_interval='5 16 * * *',
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
def dbt_weather_aggregate_prediction():
    update_int_weather_province = BashOperator(
        task_id='dbt_int_weather_province',
        bash_command="""
        cd /opt/dbt && \
        dbt run --select int_weather_province --profiles-dir /opt/dbt/.dbt
    """

    )

    update_fct_weather_province = BashOperator(
        task_id='dbt_fct_weather_province',
        bash_command="""
        cd /opt/dbt && \
        dbt run --select fct_weather_province --profiles-dir /opt/dbt/.dbt
    """
    )

    update_fct_weather_region = BashOperator(
        task_id='dbt_fct_weather_region',
        bash_command="""
        cd /opt/dbt && \
        dbt run --select fct_weather_region --profiles-dir /opt/dbt/.dbt
    """
    )

    predict_weather_province = BashOperator(
        task_id='dbt_predict_weather_province',
        bash_command="""
        cd /opt/dbt && \
        dbt run --select predict_weather_province --profiles-dir /opt/dbt/.dbt
    """
    )

    update_int_weather_province >> update_fct_weather_province >> update_fct_weather_region >> predict_weather_province

dag = dbt_weather_aggregate_prediction()