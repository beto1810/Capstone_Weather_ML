-- This model demonstrates true batch processing using the vectorized UDF
-- The PREDICT_CONDITION_VEC function processes the entire dataset as a DataFrame at once


WITH weather_features AS (
    SELECT
        p.province_id,
        p.region,
        p.province_name,
        DATE(w.weather_updated_at) AS weather_date,

        AVG(w.temperature_celsius) AS avg_temperature,
        MAX(w.temperature_celsius) AS max_temperature,
        MIN(w.temperature_celsius) AS min_temperature,

        AVG(w.humidity_percent) AS avg_humidity,

        SUM(w.precipitation_mm) AS sum_precipitation,
        MAX(w.precipitation_mm) AS max_precipitation,
        MIN(w.precipitation_mm) AS min_precipitation,

        MAX(CASE WHEN w.precipitation_mm > 0 THEN 1 ELSE 0 END) AS chance_rain,

        AVG(w.wind_speed_kph) AS avg_wind_kph,
        AVG(w.wind_speed_kph * 0.621371) AS avg_wind_mph-- derived from KPH

    FROM {{ ref('stg_weather_data') }} AS w
    INNER JOIN
        {{ ref('dim_vietnam_provinces') }} AS p
        ON w.province_name = p.province_name
    GROUP BY p.province_id, p.region, p.province_name, weather_date
)

SELECT
    province_id,
    weather_date,
    avg_temperature,
    max_temperature,
    min_temperature,
    sum_precipitation,
    chance_rain,
    avg_humidity,
    avg_wind_kph,
    avg_wind_mph,
    region,

    -- The vectorized function automatically:
    -- 1. Collects all rows from this query
    -- 2. Converts them to a pandas DataFrame
    -- 3. Processes the entire DataFrame in one call
    -- 4. Returns results for all rows

    KAFKA_AIRFLOW_WEATHER.WEATHER_ANALYTICS.PREDICT_CONDITION_VEC(
        avg_temperature,
        max_temperature,
        min_temperature,
        sum_precipitation,
        chance_rain,
        avg_humidity,
        avg_wind_kph,
        avg_wind_mph,
        region
    ) AS predicted_condition
FROM weather_features
ORDER BY province_id, weather_date