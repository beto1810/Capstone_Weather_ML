{{ config(materialized='table') }}

WITH day_3_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
),

day_4_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
),

day_5_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day5') }}
),

-- Pre-calculate date relationships for more efficient joins
day_3_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, predicted_date) AS day4_date,
        DATEADD(DAY, 2, predicted_date) AS day5_date
    FROM day_3_predictions
),

-- First join: day 3 + day 4 (simplified join with WHERE)
day_3_4_combined AS (
    SELECT
        p3.province_id,
        p3.region,
        p3.predicted_date,
        p3.day4_date,
        p3.day5_date,
        p3.avgtemp_c AS day3_avgtemp,
        p3.maxtemp_c AS day3_maxtemp,
        p3.mintemp_c AS day3_mintemp,
        p3.totalprecip_mm AS day3_precip,
        p3.daily_chance_of_rain AS day3_rain,
        p3.avghumidity AS day3_humidity,
        p3.maxwind_kph AS day3_windkph,
        p3.maxwind_mph AS day3_windmph,
        p4.avgtemp_c AS day4_avgtemp,
        p4.maxtemp_c AS day4_maxtemp,
        p4.mintemp_c AS day4_mintemp,
        p4.totalprecip_mm AS day4_precip,
        p4.daily_chance_of_rain AS day4_rain,
        p4.avghumidity AS day4_humidity,
        p4.maxwind_kph AS day4_windkph,
        p4.maxwind_mph AS day4_windmph
    FROM day_3_with_dates AS p3
    INNER JOIN day_4_predictions AS p4
        ON p3.province_id = p4.province_id
    WHERE p3.day4_date = p4.predicted_date
),

-- Second join: add day 5 (simplified join with WHERE)
day_6_predictions AS (
    SELECT
        p34.province_id,
        p34.region,
        p34.predicted_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 3 predictions (day 3, day 4, day 5)
            p34.day3_avgtemp, p34.day4_avgtemp, p5.avgtemp_c,
            p34.day3_maxtemp, p34.day4_maxtemp, p5.maxtemp_c,
            p34.day3_mintemp, p34.day4_mintemp, p5.mintemp_c,
            p34.day3_precip, p34.day4_precip, p5.totalprecip_mm,
            p34.day3_rain, p34.day4_rain, p5.daily_chance_of_rain,
            p34.day3_humidity, p34.day4_humidity, p5.avghumidity,
            p34.day3_windkph, p34.day4_windkph, p5.maxwind_kph,
            p34.day3_windmph, p34.day4_windmph, p5.maxwind_mph,
            p34.region
        )) AS forecast_json
    FROM day_3_4_combined AS p34
    INNER JOIN day_5_predictions AS p5
        ON p34.province_id = p5.province_id
    WHERE p34.day5_date = p5.predicted_date
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
FROM day_6_predictions