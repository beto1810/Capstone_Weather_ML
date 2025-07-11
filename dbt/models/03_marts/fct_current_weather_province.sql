{{ config(
    materialized = 'table',
) }}

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

FROM {{ ref('int_current_weather_province') }}
