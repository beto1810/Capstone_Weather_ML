{{ config(materialized='table') }}

WITH base_with_lags AS (
    SELECT * FROM {{ ref('int_lag_weather_province') }}
),

day_1_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
),

-- Pre-calculate date relationships for more efficient joins
base_with_dates AS (
    SELECT
        *,
        DATEADD(DAY, 1, weather_date) AS day1_date
    FROM base_with_lags
),

day_2_predictions AS (
    SELECT
        b.province_id,
        b.region,
        b.weather_date,
        PARSE_JSON(KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_ALL_WEATHER_METRICS_VEC(
            -- Use 2 actuals (lag 2, lag 3) + 1 prediction (day 1)
            b.avgtemp2, b.avgtemp3, p1.avgtemp_c,
            b.maxtemp2, b.maxtemp3, p1.maxtemp_c,
            b.mintemp2, b.mintemp3, p1.mintemp_c,
            b.precip2, b.precip3, p1.totalprecip_mm,
            b.rainchance2, b.rainchance3, p1.daily_chance_of_rain,
            b.humidity2, b.humidity3, p1.avghumidity,
            b.windkph2, b.windkph3, p1.maxwind_kph,
            b.windmph2, b.windmph3, p1.maxwind_mph,
            b.region
        )) AS forecast_json
    FROM base_with_dates AS b
    -- More efficient join using pre-calculated date
    INNER JOIN day_1_predictions AS p1
        ON
            b.province_id = p1.province_id
            AND b.day1_date = p1.predicted_date
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
    DATEADD(DAY, 2, weather_date) AS predicted_date
FROM day_2_predictions
