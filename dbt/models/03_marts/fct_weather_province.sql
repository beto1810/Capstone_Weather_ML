{{ config(
    materialized = 'incremental',
    unique_key = 'province_id || weather_date',
    incremental_strategy = 'merge'
) }}

WITH cte AS (
    SELECT 
        i.*,
        p.region
    FROM {{ ref('int_weather_province') }} i
    JOIN {{ ref('dim_vietnam_provinces') }} p
      ON i.province_id = p.province_id
    {% if is_incremental() %}
      WHERE i.weather_date > (SELECT MIN(weather_date) FROM {{ this }})
    {% endif %}
)

SELECT 
    e.*,

    PREDICT_CONDITION(
        e.avg_temperature,
        e.max_temperature,
        e.min_temperature,
        e.sum_precipitation,
        e.chance_rain,
        e.avg_humidity,
        e.avg_wind_kph,
        e.avg_wind_mph,
        e.region
    ) AS condition,

    {% if is_incremental() %}
        {{ this }}.created_at AS created_at,
    {% else %}
        CURRENT_TIMESTAMP() AS created_at,
    {% endif %}

    CURRENT_TIMESTAMP() AS updated_at,

    {% if is_incremental() %}
        CASE 
            WHEN {{ this }}.province_id IS NULL THEN TRUE 
            ELSE FALSE 
        END AS is_new_row
    {% else %}
        TRUE AS is_new_row
    {% endif %}

FROM cte e
{% if is_incremental() %}
LEFT JOIN {{ this }} 
  ON e.province_id = {{ this }}.province_id AND e.weather_date = {{ this }}.weather_date
{% endif %}
