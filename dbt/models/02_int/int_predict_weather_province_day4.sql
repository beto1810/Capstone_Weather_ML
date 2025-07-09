{{ config(materialized='table') }}

WITH day_1_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
),

day_2_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
),

day_3_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
),

-- Pre-calculate date relationships for more efficient joins
day_1_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, predicted_date) AS day2_date,
        DATEADD(DAY, 2, predicted_date) AS day3_date
    FROM day_1_predictions
),

-- First join: day 1 + day 2 (simplified join with WHERE)
day_1_2_combined AS (
    SELECT
        p1.province_id,
        p1.region,
        p1.predicted_date,
        p1.day2_date,
        p1.day3_date,
        p1.avgtemp_c AS day1_avgtemp,
        p1.maxtemp_c AS day1_maxtemp,
        p1.mintemp_c AS day1_mintemp,
        p1.totalprecip_mm AS day1_precip,
        p1.daily_chance_of_rain AS day1_rain,
        p1.avghumidity AS day1_humidity,
        p1.maxwind_kph AS day1_windkph,
        p1.maxwind_mph AS day1_windmph,
        p2.avgtemp_c AS day2_avgtemp,
        p2.maxtemp_c AS day2_maxtemp,
        p2.mintemp_c AS day2_mintemp,
        p2.totalprecip_mm AS day2_precip,
        p2.daily_chance_of_rain AS day2_rain,
        p2.avghumidity AS day2_humidity,
        p2.maxwind_kph AS day2_windkph,
        p2.maxwind_mph AS day2_windmph
    FROM day_1_with_dates AS p1
    INNER JOIN day_2_predictions AS p2
        ON p1.province_id = p2.province_id
    WHERE p1.day2_date = p2.predicted_date
),

-- Second join: add day 3 (simplified join with WHERE)
day_4_predictions AS (
    SELECT
        p12.province_id,
        p12.region,
        p12.predicted_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 3 predictions (day 1, day 2, day 3)
            p12.day1_avgtemp, p12.day2_avgtemp, p3.avgtemp_c,
            p12.day1_maxtemp, p12.day2_maxtemp, p3.maxtemp_c,
            p12.day1_mintemp, p12.day2_mintemp, p3.mintemp_c,
            p12.day1_precip, p12.day2_precip, p3.totalprecip_mm,
            p12.day1_rain, p12.day2_rain, p3.daily_chance_of_rain,
            p12.day1_humidity, p12.day2_humidity, p3.avghumidity,
            p12.day1_windkph, p12.day2_windkph, p3.maxwind_kph,
            p12.day1_windmph, p12.day2_windmph, p3.maxwind_mph,
            p12.region
        )) AS forecast_json
    FROM day_1_2_combined AS p12
    INNER JOIN day_3_predictions AS p3
        ON p12.province_id = p3.province_id
    WHERE p12.day3_date = p3.predicted_date
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
    KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_CONDITION_VEC(
        avgtemp_c,
        maxtemp_c,
        mintemp_c,
        totalprecip_mm,
        daily_chance_of_rain,
        avghumidity,
        maxwind_kph,
        maxwind_mph,
        region
    ) AS predicted_condition,
    DATEADD(DAY, 3, predicted_date) AS predicted_date
FROM day_4_predictions

