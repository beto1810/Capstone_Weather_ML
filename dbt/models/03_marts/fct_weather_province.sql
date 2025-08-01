{{ config(
    materialized = 'incremental',
    unique_key =  ['province_id', 'weather_date'],
    incremental_strategy = 'merge'
) }}

WITH cte AS (
    SELECT
        i.*,
        p.province_name
    FROM {{ ref('int_weather_province') }} AS i
    INNER JOIN {{ ref('dim_vietnam_provinces') }} AS p
        ON i.province_id = p.province_id
    {% if is_incremental() %}
        WHERE (
            (SELECT MAX(t.weather_date) FROM {{ this }} AS t) IS NULL
            OR i.weather_date > (SELECT MAX(t.weather_date) FROM {{ this }} AS t)
        )
    {% endif %}
)

SELECT
    e.province_id,
    e.province_name,
    e.weather_date,
    e.avg_temperature,
    e.max_temperature,
    e.min_temperature,
    e.sum_precipitation,
    e.chance_rain,
    e.avg_humidity,
    e.avg_wind_kph,
    e.avg_wind_mph,
    e.region,
    e.predicted_condition AS condition,

    CURRENT_TIMESTAMP() AS updated_at,

    {% if is_incremental() %}
        COALESCE(t.province_id IS NULL, FALSE) AS is_new_row
    {% else %}
        TRUE AS is_new_row
    {% endif %}

FROM cte AS e
{% if is_incremental() %}
    LEFT JOIN {{ this }} AS t
        ON e.province_id = t.province_id AND e.weather_date = t.weather_date
{% endif %}
