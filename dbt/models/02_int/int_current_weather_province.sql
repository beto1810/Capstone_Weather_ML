{{ config(materialized='view') }}


WITH weather_features AS (
    SELECT
        p.province_id,
        p.region,
        p.province_name,
        w.weather_updated_at,
        w.wind_speed_kph,
        w.temperature_celsius,
        w.precipitation_mm,
        w.humidity_percent,
        w.cloud_cover_percent,
        w.uv_index,
        w.is_daytime,
        w.weather_condition,
        ROW_NUMBER() OVER (PARTITION BY p.province_id ORDER BY w.weather_updated_at DESC) AS filter_time
    FROM {{ ref('stg_weather_data') }} AS w
    INNER JOIN
        {{ ref('dim_vietnam_provinces') }} AS p
        ON w.province_name = p.province_name
)

SELECT
    province_id,
    region,
    province_name,
    weather_updated_at,
    wind_speed_kph,
    temperature_celsius,
    precipitation_mm,
    humidity_percent,
    cloud_cover_percent,
    uv_index,
    is_daytime,
    weather_condition,
    CURRENT_TIMESTAMP() AS created_at
FROM weather_features
WHERE filter_time = 1