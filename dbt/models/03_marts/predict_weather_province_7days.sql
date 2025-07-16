{{ config(
    materialized = 'table',
    unique_key = ['province_id', 'predicted_date']
) }}

SELECT
    PROVINCE_ID as province_id,
    REGION as region,
    PREDICTED_DATE as predicted_date,
    AVGTEMP_C as avg_temperature_c,
    MINTEMP_C as min_temperature_c,
    MAXTEMP_C as max_temperature_c,
    DAILY_CHANCE_OF_RAIN as daily_chance_of_rain,
    TOTALPRECIP_MM as total_precip_mm,
    AVGHUMIDITY as avg_humidity,
    MAXWIND_KPH as max_wind_kph,
    MAXWIND_MPH as max_wind_mph,
    PREDICTED_CONDITION as predicted_condition,
    CURRENT_TIMESTAMP() as created_at,
    CURRENT_TIMESTAMP() as updated_at
FROM {{ source('weather_predictions_7days', 'WEATHER_PREDICTIONS_7DAYS') }}
