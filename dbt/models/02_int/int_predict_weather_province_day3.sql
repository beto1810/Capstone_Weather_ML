{{ config(materialized='table') }}

WITH base_with_lags AS (
    SELECT * FROM {{ ref('int_lag_weather_province') }}
),

day_1_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
),

day_2_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
),

-- Pre-calculate date relationships for more efficient joins
base_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, weather_date) AS day1_date,
        DATEADD(DAY, 2, weather_date) AS day2_date
    FROM base_with_lags
),

day_3_predictions AS (
    SELECT
        b.province_id,
        b.region,
        b.weather_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 1 actual (lag 3) + 2 predictions (day 1, day 2)
            b.avgtemp3, p1.avgtemp_c, p2.avgtemp_c,
            b.maxtemp3, p1.maxtemp_c, p2.maxtemp_c,
            b.mintemp3, p1.mintemp_c, p2.mintemp_c,
            b.precip3, p1.totalprecip_mm, p2.totalprecip_mm,
            b.rainchance3, p1.daily_chance_of_rain, p2.daily_chance_of_rain,
            b.humidity3, p1.avghumidity, p2.avghumidity,
            b.windkph3, p1.maxwind_kph, p2.maxwind_kph,
            b.windmph3, p1.maxwind_mph, p2.maxwind_mph,
            b.region
        )) AS forecast_json
    FROM base_with_dates AS b
    -- More efficient joins using pre-calculated dates
    INNER JOIN day_1_predictions AS p1
        ON
            b.province_id = p1.province_id
            AND b.day1_date = p1.predicted_date
    INNER JOIN day_2_predictions AS p2
        ON
            b.province_id = p2.province_id
            AND b.day2_date = p2.predicted_date
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
    DATEADD(DAY, 3, weather_date) AS predicted_date
FROM day_3_predictions
