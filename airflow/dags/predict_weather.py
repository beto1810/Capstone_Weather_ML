from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import joblib
import snowflake.connector
import os
import numpy as np

def predict_weather_7day():
    # --- Load Snowflake credentials from environment or set directly ---
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER', 'YOUR_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD', 'YOUR_PASSWORD')
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT', 'YOUR_ACCOUNT')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE', 'YOUR_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE', 'YOUR_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA_ANALYSIS', 'YOUR_SCHEMA')
    SNOWFLAKE_ROLE = os.getenv('SNOWFLAKE_ROLE')

    # --- Connect to Snowflake and load data ---
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    query = "SELECT * FROM int_lag_weather_province"
    df = pd.read_sql(query, conn)
    conn.close()

    # --- Load regression model and region encoder ---
    reg_model = joblib.load('training_weather_model/multioutput_regressor.pkl')
    region_encoder = joblib.load('training_weather_model/region_label_encoder.pkl')
    clf_model = joblib.load('training_weather_model/weather_condition_model.pkl')
    condition_encoder = joblib.load('training_weather_model/condition_label_encoder.pkl')

    # --- Lowercase all columns for consistent renaming ---
    df.columns = [col.lower() for col in df.columns]

    # --- Rename columns to match model training ---
    column_map = {
        'avgtemp1': 'AVGT_C_LAG_1',
        'avgtemp2': 'AVGT_C_LAG_2',
        'avgtemp3': 'AVGT_C_LAG_3',
        'humidity1': 'AVGHUMIDITY_LAG_1',
        'humidity2': 'AVGHUMIDITY_LAG_2',
        'humidity3': 'AVGHUMIDITY_LAG_3',
        'rainchance1': 'RAIN_CHANCE_LAG_1',
        'rainchance2': 'RAIN_CHANCE_LAG_2',
        'rainchance3': 'RAIN_CHANCE_LAG_3',
        'maxtemp1': 'MAXTEMP_LAG_1',
        'maxtemp2': 'MAXTEMP_LAG_2',
        'maxtemp3': 'MAXTEMP_LAG_3',
        'mintemp1': 'MINTEMP_LAG_1',
        'mintemp2': 'MINTEMP_LAG_2',
        'mintemp3': 'MINTEMP_LAG_3',
        'precip1': 'PRECIP_LAG_1',
        'precip2': 'PRECIP_LAG_2',
        'precip3': 'PRECIP_LAG_3',
        'windkph1': 'MAXWIND_KPH_LAG_1',
        'windkph2': 'MAXWIND_KPH_LAG_2',
        'windkph3': 'MAXWIND_KPH_LAG_3',
        'windmph1': 'MAXWIND_MPH_LAG_1',
        'windmph2': 'MAXWIND_MPH_LAG_2',
        'windmph3': 'MAXWIND_MPH_LAG_3',
    }
    df = df.rename(columns=column_map)

    # --- Encode region ---
    if 'region' in df.columns:
        df['REGION_ENC'] = region_encoder.transform(df['region'])

    # --- Prepare for recursive forecasting ---
    reg_targets = [
        "AVGTEMP_C", "MAXTEMP_C", "MINTEMP_C",
        "TOTALPRECIP_MM", "DAILY_CHANCE_OF_RAIN",
        "AVGHUMIDITY", "MAXWIND_KPH", "MAXWIND_MPH"
    ]
    lag_keys = [
        ('AVGT_C_LAG', 3),
        ('AVGHUMIDITY_LAG', 3),
        ('RAIN_CHANCE_LAG', 3),
        ('MAXTEMP_LAG', 3),
        ('MINTEMP_LAG', 3),
        ('PRECIP_LAG', 3),
        ('MAXWIND_KPH_LAG', 3),
        ('MAXWIND_MPH_LAG', 3)
    ]
    feature_cols = ['REGION_ENC'] + [f'{k}_{i+1}' for k, n in lag_keys for i in range(n)]

    n_days = 7
    all_results = []

    for idx, row in df.iterrows():
        # Initialize lags for all variables
        lags = {}
        for key, n in lag_keys:
            lags[key] = [row[f'{key}_{i+1}'] for i in range(n)]
        region_enc = row['REGION_ENC']
        base_date = pd.to_datetime(row['weather_date'])
        for day in range(1, n_days+1):
            # Build feature vector in the correct order
            features = [region_enc]
            for key, n in lag_keys:
                features += lags[key]
            features = np.array(features).reshape(1, -1)
            pred = reg_model.predict(features)[0]
            # Prepare input for classification
            X_cls = np.concatenate([pred, [region_enc]]).reshape(1, -1)
            y_pred_cls = clf_model.predict(X_cls)
            condition_label = condition_encoder.inverse_transform(y_pred_cls)[0]
            # Store result
            result_row = {
                'province_id': row['province_id'],
                'region': row['region'],
                'date': (base_date + timedelta(days=day)).strftime('%Y-%m-%d'),
                'AVGTEMP_C': pred[0],
                'MAXTEMP_C': pred[1],
                'MINTEMP_C': pred[2],
                'TOTALPRECIP_MM': pred[3],
                'DAILY_CHANCE_OF_RAIN': pred[4],
                'AVGHUMIDITY': pred[5],
                'MAXWIND_KPH': pred[6],
                'MAXWIND_MPH': pred[7],
                'PREDICTED_CONDITION': condition_label
            }
            all_results.append(result_row)
            # Update lags for next day
            lags['AVGT_C_LAG'] = [pred[0]] + lags['AVGT_C_LAG'][:-1]
            lags['MAXTEMP_LAG'] = [pred[1]] + lags['MAXTEMP_LAG'][:-1]
            lags['MINTEMP_LAG'] = [pred[2]] + lags['MINTEMP_LAG'][:-1]
            lags['PRECIP_LAG'] = [pred[3]] + lags['PRECIP_LAG'][:-1]
            lags['RAIN_CHANCE_LAG'] = [pred[4]] + lags['RAIN_CHANCE_LAG'][:-1]
            lags['AVGHUMIDITY_LAG'] = [pred[5]] + lags['AVGHUMIDITY_LAG'][:-1]
            lags['MAXWIND_KPH_LAG'] = [pred[6]] + lags['MAXWIND_KPH_LAG'][:-1]
            lags['MAXWIND_MPH_LAG'] = [pred[7]] + lags['MAXWIND_MPH_LAG'][:-1]

    results_df = pd.DataFrame(all_results)

    # --- Save final results to Snowflake (replace table contents) ---
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        role=SNOWFLAKE_ROLE
    )
    cursor = conn.cursor()

    # Create the table if it does not exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS WEATHER_PREDICTIONS_7DAYS (
            PROVINCE_ID VARCHAR,
            REGION VARCHAR,
            DATE DATE,
            AVGTEMP_C FLOAT,
            MAXTEMP_C FLOAT,
            MINTEMP_C FLOAT,
            TOTALPRECIP_MM FLOAT,
            DAILY_CHANCE_OF_RAIN FLOAT,
            AVGHUMIDITY FLOAT,
            MAXWIND_KPH FLOAT,
            MAXWIND_MPH FLOAT,
            PREDICTED_CONDITION VARCHAR
        )
    """)

    # Truncate the table before inserting new data
    cursor.execute("TRUNCATE TABLE IF EXISTS WEATHER_PREDICTIONS_7DAYS")

    # Prepare insert statement
    insert_sql = """
        INSERT INTO WEATHER_PREDICTIONS_7DAYS (
            PROVINCE_ID, REGION, DATE, AVGTEMP_C, MAXTEMP_C, MINTEMP_C,
            TOTALPRECIP_MM, DAILY_CHANCE_OF_RAIN, AVGHUMIDITY, MAXWIND_KPH, MAXWIND_MPH, PREDICTED_CONDITION
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Convert DataFrame to list of tuples
    data_tuples = [tuple(x) for x in results_df.values]

    # Insert all rows
    cursor.executemany(insert_sql, data_tuples)
    conn.commit()
    cursor.close()
    conn.close()

    print("7-day recursive predictions replaced in Snowflake table WEATHER_PREDICTIONS_7DAYS.")

# --- Airflow DAG definition ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_predict_7day_dag',
    default_args=default_args,
    description='7-day recursive weather prediction and load to Snowflake',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

predict_task = PythonOperator(
    task_id='predict_weather_7day',
    python_callable=predict_weather_7day,
    dag=dag,
)