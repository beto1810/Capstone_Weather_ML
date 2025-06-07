{%- set today = modules.datetime.datetime.now().date() -%}
{%- set seven_days_ago = today - modules.datetime.timedelta(days=7) -%}

WITH last_7_days AS (
    SELECT
        province_id,
        condition,
        avg_temperature,
        sum_precipitation
    FROM {{ ref('fct_weather_province') }}
    WHERE weather_date BETWEEN '{{ seven_days_ago }}' AND '{{ today }}'
),

aggregated AS (
    SELECT
        condition,
        COUNT(DISTINCT province_id) AS num_provinces,
        AVG(avg_temperature) AS avg_temp_last_7_days,
        AVG(sum_precipitation) AS avg_precip_last_7_days
    FROM last_7_days
    GROUP BY condition
)

SELECT *
FROM aggregated