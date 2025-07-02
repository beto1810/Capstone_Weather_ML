{{ config(materialized='table') }}

WITH base_with_lags AS (
    SELECT * FROM {{ ref('int_lag_weather_province') }}
),

day_1_predictions AS (
    SELECT
        province_id,
        region,
        weather_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            avgtemp1, avgtemp2, avgtemp3,
            maxtemp1, maxtemp2, maxtemp3,
            mintemp1, mintemp2, mintemp3,
            precip1, precip2, precip3,
            rainchance1, rainchance2, rainchance3,
            humidity1, humidity2, humidity3,
            windkph1, windkph2, windkph3,
            windmph1, windmph2, windmph3,
            region
        )) AS forecast_json
    FROM base_with_lags
)

SELECT
    province_id,
    region,
    forecast_json:avgtemp_c::FLOAT AS avgtemp_c,
    forecast_json:maxtemp_c::FLOAT AS maxtemp_c,
    forecast_json:mintemp_c::FLOAT AS mintemp_c,
    forecast_json:totalprecip_mm::FLOAT AS totalprecip_mm,
    forecast_json:daily_chance_of_rain::FLOAT AS daily_chance_of_rain,
    forecast_json:avghumidity::FLOAT AS avghumidity,
    forecast_json:maxwind_kph::FLOAT AS maxwind_kph,
    forecast_json:maxwind_mph::FLOAT AS maxwind_mph,
    DATEADD(DAY, 1, weather_date) AS predicted_date
FROM day_1_predictions

