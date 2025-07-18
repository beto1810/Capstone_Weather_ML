{{ config(
    materialized = 'table',
    unique_key = ['province_id', 'predicted_date']
) }}

SELECT
    PROVINCE_ID,
    REGION,
    PREDICTED_DATE,
    AVGTEMP_C AS AVG_TEMPERATURE_C,
    MINTEMP_C AS MIN_TEMPERATURE_C,
    MAXTEMP_C AS MAX_TEMPERATURE_C,
    DAILY_CHANCE_OF_RAIN,
    TOTALPRECIP_MM AS TOTAL_PRECIP_MM,
    AVGHUMIDITY AS AVG_HUMIDITY,
    MAXWIND_KPH AS MAX_WIND_KPH,
    MAXWIND_MPH AS MAX_WIND_MPH,
    PREDICTED_CONDITION,
    coalesce(lower(PREDICTED_CONDITION) LIKE '%rain%', FALSE) AS IS_RAINING,
    current_timestamp() AS CREATED_AT,
    current_timestamp() AS UPDATED_AT
FROM {{ source('weather_predictions_7days', 'WEATHER_PREDICTIONS_7DAYS') }}
