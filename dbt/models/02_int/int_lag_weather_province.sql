{{ config(materialized='view') }}

WITH latest_data AS (
    SELECT
        fct.province_id,
        fct.weather_date,
        fct.avg_temperature,
        fct.max_temperature,
        fct.min_temperature,
        fct.sum_precipitation,
        fct.chance_rain,
        fct.avg_humidity,
        fct.avg_wind_kph,
        fct.avg_wind_mph,
        dim.region
    FROM {{ ref('int_weather_province') }} AS fct
    INNER JOIN {{ ref('dim_vietnam_provinces') }} AS dim
        ON fct.province_id = dim.province_id
    WHERE fct.weather_date >= DATEADD(DAY, -4, CURRENT_DATE())
),

base AS (
    SELECT
        province_id,
        region,
        weather_date,
        ROW_NUMBER() OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
        ) AS row_num,
        ARRAY_AGG(avg_temperature) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS temp_array,
        ARRAY_AGG(max_temperature) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS max_temp_array,
        ARRAY_AGG(min_temperature) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS min_temp_array,
        ARRAY_AGG(sum_precipitation) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS precip_array,
        ARRAY_AGG(chance_rain) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS rainchance_array,
        ARRAY_AGG(avg_humidity) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS humidity_array,
        ARRAY_AGG(avg_wind_kph) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS windkph_array,
        ARRAY_AGG(avg_wind_mph) OVER (
            PARTITION BY province_id
            ORDER BY weather_date DESC
            ROWS BETWEEN CURRENT ROW AND 3 FOLLOWING
        ) AS windmph_array
    FROM latest_data
)

SELECT
    province_id,
    region,
    weather_date,
    row_num,
    temp_array[0] AS avgtemp1,
    temp_array[1] AS avgtemp2,
    temp_array[2] AS avgtemp3,
    max_temp_array[1] AS maxtemp1,
    max_temp_array[2] AS maxtemp2,
    max_temp_array[3] AS maxtemp3,
    min_temp_array[1] AS mintemp1,
    min_temp_array[2] AS mintemp2,
    min_temp_array[3] AS mintemp3,
    precip_array[1] AS precip1,
    precip_array[2] AS precip2,
    precip_array[3] AS precip3,
    rainchance_array[1] AS rainchance1,
    rainchance_array[2] AS rainchance2,
    rainchance_array[3] AS rainchance3,
    humidity_array[1] AS humidity1,
    humidity_array[2] AS humidity2,
    humidity_array[3] AS humidity3,
    windkph_array[1] AS windkph1,
    windkph_array[2] AS windkph2,
    windkph_array[3] AS windkph3,
    windmph_array[1] AS windmph1,
    windmph_array[2] AS windmph2,
    windmph_array[3] AS windmph3
FROM base
WHERE row_num = 1