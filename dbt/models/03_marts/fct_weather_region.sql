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
        p.region AS region,
        CAST(i.weather_date AS DATE) AS weather_date,
        avg(i.avg_temperature) AS avg_temperature,
        max(i.max_temperature) AS max_temperature,
        min(i.min_temperature) AS min_temperature,
        avg(i.avg_humidity) AS avg_humidity,
        sum(i.sum_precipitation) AS sum_precipitation,
        max(i.max_precipitation) AS max_precipitation,
        min(i.min_precipitation) AS min_precipitation,
        max(CASE WHEN i.sum_precipitation > 0 THEN 1 ELSE 0 END) AS chance_rain,
        avg(i.avg_wind_kph) AS avg_wind_kph,
        avg(i.avg_wind_kph * 0.621371) AS avg_wind_mph,
        current_timestamp() AS updated_at
    FROM {{ ref('int_weather_province') }} i
    JOIN {{ ref('dim_vietnam_provinces') }} p
      ON i.province_id = p.province_id
    {% if is_incremental() %}
    JOIN latest_date ld ON 1=1
    WHERE i.weather_date > ld.max_date
    {% endif %}
    GROUP BY
        p.region,
        CAST(i.weather_date AS DATE)
)

SELECT *
FROM weather_by_region
order by 
    region,
    weather_date
