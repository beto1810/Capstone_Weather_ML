{# {{ config(materialized='table') }}

WITH day_2_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
),

day_3_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
),

day_4_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
),

-- Pre-calculate date relationships for more efficient joins
day_2_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, predicted_date) AS day3_date,
        DATEADD(DAY, 2, predicted_date) AS day4_date
    FROM day_2_predictions
),

-- First join: day 2 + day 3 (simplified join with WHERE)
day_2_3_combined AS (
    SELECT
        p2.province_id,
        p2.region,
        p2.predicted_date,
        p2.day3_date,
        p2.day4_date,
        p2.avgtemp_c AS day2_avgtemp,
        p2.maxtemp_c AS day2_maxtemp,
        p2.mintemp_c AS day2_mintemp,
        p2.totalprecip_mm AS day2_precip,
        p2.daily_chance_of_rain AS day2_rain,
        p2.avghumidity AS day2_humidity,
        p2.maxwind_kph AS day2_windkph,
        p2.maxwind_mph AS day2_windmph,
        p3.avgtemp_c AS day3_avgtemp,
        p3.maxtemp_c AS day3_maxtemp,
        p3.mintemp_c AS day3_mintemp,
        p3.totalprecip_mm AS day3_precip,
        p3.daily_chance_of_rain AS day3_rain,
        p3.avghumidity AS day3_humidity,
        p3.maxwind_kph AS day3_windkph,
        p3.maxwind_mph AS day3_windmph
    FROM day_2_with_dates AS p2
    INNER JOIN day_3_predictions AS p3
        ON p2.province_id = p3.province_id
    WHERE p2.day3_date = p3.predicted_date
),

-- Second join: add day 4 (simplified join with WHERE)
day_5_predictions AS (
    SELECT
        p23.province_id,
        p23.region,
        p23.predicted_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 3 predictions (day 2, day 3, day 4)
            p23.day2_avgtemp, p23.day3_avgtemp, p4.avgtemp_c,
            p23.day2_maxtemp, p23.day3_maxtemp, p4.maxtemp_c,
            p23.day2_mintemp, p23.day3_mintemp, p4.mintemp_c,
            p23.day2_precip, p23.day3_precip, p4.totalprecip_mm,
            p23.day2_rain, p23.day3_rain, p4.daily_chance_of_rain,
            p23.day2_humidity, p23.day3_humidity, p4.avghumidity,
            p23.day2_windkph, p23.day3_windkph, p4.maxwind_kph,
            p23.day2_windmph, p23.day3_windmph, p4.maxwind_mph,
            p23.region
        )) AS forecast_json
    FROM day_2_3_combined AS p23
    INNER JOIN day_4_predictions AS p4
        ON p23.province_id = p4.province_id
    WHERE p23.day4_date = p4.predicted_date
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
FROM day_5_predictions
 #}
