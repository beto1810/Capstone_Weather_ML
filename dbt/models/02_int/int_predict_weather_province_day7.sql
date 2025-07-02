{{ config(materialized='table') }}

WITH day_4_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
),

day_5_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day5') }}
),

day_6_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day6') }}
),

-- Pre-calculate date relationships for more efficient joins
day_4_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, predicted_date) AS day5_date,
        DATEADD(DAY, 2, predicted_date) AS day6_date
    FROM day_4_predictions
),

-- First join: day 4 + day 5 (simplified join with WHERE)
day_4_5_combined AS (
    SELECT
        p4.province_id,
        p4.region,
        p4.predicted_date,
        p4.day5_date,
        p4.day6_date,
        p4.avgtemp_c AS day4_avgtemp,
        p4.maxtemp_c AS day4_maxtemp,
        p4.mintemp_c AS day4_mintemp,
        p4.totalprecip_mm AS day4_precip,
        p4.daily_chance_of_rain AS day4_rain,
        p4.avghumidity AS day4_humidity,
        p4.maxwind_kph AS day4_windkph,
        p4.maxwind_mph AS day4_windmph,
        p5.avgtemp_c AS day5_avgtemp,
        p5.maxtemp_c AS day5_maxtemp,
        p5.mintemp_c AS day5_mintemp,
        p5.totalprecip_mm AS day5_precip,
        p5.daily_chance_of_rain AS day5_rain,
        p5.avghumidity AS day5_humidity,
        p5.maxwind_kph AS day5_windkph,
        p5.maxwind_mph AS day5_windmph
    FROM day_4_with_dates AS p4
    INNER JOIN day_5_predictions AS p5
        ON p4.province_id = p5.province_id
    WHERE p4.day5_date = p5.predicted_date
),

-- Second join: add day 6 (simplified join with WHERE)
day_7_predictions AS (
    SELECT
        p45.province_id,
        p45.region,
        p45.predicted_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 3 predictions (day 4, day 5, day 6)
            p45.day4_avgtemp, p45.day5_avgtemp, p6.avgtemp_c,
            p45.day4_maxtemp, p45.day5_maxtemp, p6.maxtemp_c,
            p45.day4_mintemp, p45.day5_mintemp, p6.mintemp_c,
            p45.day4_precip, p45.day5_precip, p6.totalprecip_mm,
            p45.day4_rain, p45.day5_rain, p6.daily_chance_of_rain,
            p45.day4_humidity, p45.day5_humidity, p6.avghumidity,
            p45.day4_windkph, p45.day5_windkph, p6.maxwind_kph,
            p45.day4_windmph, p45.day5_windmph, p6.maxwind_mph,
            p45.region
        )) AS forecast_json
    FROM day_4_5_combined AS p45
    INNER JOIN day_6_predictions AS p6
        ON p45.province_id = p6.province_id
    WHERE p45.day6_date = p6.predicted_date
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
    DATEADD(DAY, 7, predicted_date) AS predicted_date
FROM day_7_predictions