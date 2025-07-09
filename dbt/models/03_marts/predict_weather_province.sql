{{ config(
    materialized='incremental',
    unique_key=['province_id', 'predicted_date'],
    incremental_strategy='merge'
) }}

{% if is_incremental() %}
    {% if adapter.get_relation(this.database, this.schema, this.table) %}
        WITH max_loaded AS (
            SELECT MAX(predicted_date) AS max_date FROM {{ this }}
        ),

        all_predictions AS (
            SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day5') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day6') }}
            UNION ALL
            SELECT * FROM {{ ref('int_predict_weather_province_day7') }}
        )

        SELECT
            *,
            CURRENT_TIMESTAMP() AS loaded_at
        FROM all_predictions
        WHERE predicted_date > COALESCE((SELECT max_date FROM max_loaded), '1900-01-01')
        ORDER BY province_id, predicted_date
    {% else %}
-- First time loading, no incremental filter
all_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day5') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day6') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day7') }}
)
SELECT
    *,
    CURRENT_TIMESTAMP() AS loaded_at
FROM all_predictions
ORDER BY province_id, predicted_date
{% endif %}
{% else %}
-- Combine all 7 days of predictions
WITH all_predictions AS (
    SELECT * FROM {{ ref('int_predict_weather_province_day1') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day2') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day3') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day4') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day5') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day6') }}
    UNION ALL
    SELECT * FROM {{ ref('int_predict_weather_province_day7') }}
)
SELECT
    *,
    CURRENT_TIMESTAMP() AS loaded_at
FROM all_predictions
ORDER BY province_id, predicted_date
{% endif %}
