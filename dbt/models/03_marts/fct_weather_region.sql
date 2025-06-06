{{ config(
    materialized = 'incremental',
    unique_key = ['region', 'weather_date'],
    incremental_strategy = 'merge'
) }}

{% if is_incremental() %}
    WITH latest_date AS (
        SELECT MAX(weather_date) AS max_date
        FROM {{ this }}
    ),
{% else %}
  WITH
{% endif %}

weather_by_region AS (
    SELECT
        p.region,
        CAST(i.weather_date AS DATE) AS weather_date,
        AVG(i.avg_temperature) AS avg_temperature,
        MAX(i.max_temperature) AS max_temperature,
        MIN(i.min_temperature) AS min_temperature,
        AVG(i.avg_humidity) AS avg_humidity,
        SUM(i.sum_precipitation) AS sum_precipitation,
        MAX(i.max_precipitation) AS max_precipitation,
        MIN(i.min_precipitation) AS min_precipitation,
        MAX(CASE WHEN i.sum_precipitation > 0 THEN 1 ELSE 0 END) AS chance_rain,
        AVG(i.avg_wind_kph) AS avg_wind_kph,
        AVG(i.avg_wind_kph * 0.621371) AS avg_wind_mph,
        CURRENT_TIMESTAMP() AS updated_at
    FROM {{ ref('int_weather_province') }} AS i
    INNER JOIN {{ ref('dim_vietnam_provinces') }} AS p
        ON i.province_id = p.province_id
    {% if is_incremental() %}
        INNER JOIN latest_date AS ld ON 1 = 1
        WHERE i.weather_date > ld.max_date
    {% endif %}
    GROUP BY
        p.region,
        CAST(i.weather_date AS DATE)
)

SELECT *
FROM weather_by_region
ORDER BY
    region,
    weather_date
