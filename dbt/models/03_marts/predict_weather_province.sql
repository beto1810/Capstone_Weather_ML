-- models/fct_forecast_next_day_weather.sql


WITH base AS (
    SELECT
        fct.province_id,
        dim.region,
        fct.weather_date,
         ROW_NUMBER() OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date DESC) AS row_num,

        lag(avg_temperature, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS AVGTEMP1,
        lag(avg_temperature, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS AVGTEMP2,
        lag(avg_temperature, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS AVGTEMP3,

        lag(max_temperature, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MAXTEMP1,
        lag(max_temperature, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MAXTEMP2,
        lag(max_temperature, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MAXTEMP3,

        lag(min_temperature, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MINTEMP1,
        lag(min_temperature, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MINTEMP2,
        lag(min_temperature, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS MINTEMP3,

        lag(sum_precipitation, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS PRECIP1,
        lag(sum_precipitation, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS PRECIP2,
        lag(sum_precipitation, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS PRECIP3,

        lag(chance_rain, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS RAINCHANCE1,
        lag(chance_rain, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS RAINCHANCE2,
        lag(chance_rain, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS RAINCHANCE3,

        lag(avg_humidity, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS HUMIDITY1,
        lag(avg_humidity, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS HUMIDITY2,
        lag(avg_humidity, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS HUMIDITY3,

        lag(avg_wind_kph, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDKPH1,
        lag(avg_wind_kph, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDKPH2,
        lag(avg_wind_kph, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDKPH3,

        lag(avg_wind_mph, 1) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDMPH1,
        lag(avg_wind_mph, 2) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDMPH2,
        lag(avg_wind_mph, 3) OVER (PARTITION BY fct.province_id ORDER BY fct.weather_date) AS WINDMPH3

    FROM {{ ref('fct_weather_province') }} fct
    JOIN {{ ref('dim_vietnam_provinces') }} dim ON fct.province_id = dim.province_id
),
day_1 AS (
    SELECT
        province_id,
        region,
        weather_date,
        PREDICT_ALL_WEATHER_METRICS(
            AVGTEMP1, AVGTEMP2, AVGTEMP3,
            MAXTEMP1, MAXTEMP2, MAXTEMP3,
            MINTEMP1, MINTEMP2, MINTEMP3,
            PRECIP1, PRECIP2, PRECIP3,
            RAINCHANCE1, RAINCHANCE2, RAINCHANCE3,
            HUMIDITY1, HUMIDITY2, HUMIDITY3,
            WINDKPH1, WINDKPH2, WINDKPH3,
            WINDMPH1, WINDMPH2, WINDMPH3,
            region
        ) AS forecast_json
    FROM base
    WHERE row_num = 1
),
predict_day_1 AS (
SELECT
    province_id,
    DATEADD(day, 1, weather_date) AS forecast_date,
    CAST(PARSE_JSON(forecast_json):avgtemp_c AS FLOAT) AS forecast_avg_temperature,
    CAST(PARSE_JSON(forecast_json):maxtemp_c AS FLOAT) AS forecast_max_temperature,
    CAST(PARSE_JSON(forecast_json):mintemp_c AS FLOAT) AS forecast_min_temperature,
    CAST(PARSE_JSON(forecast_json):totalprecip_mm AS FLOAT) AS forecast_precipitation,
    CAST(PARSE_JSON(forecast_json):daily_chance_of_rain AS FLOAT) AS forecast_chance_rain,
    CAST(PARSE_JSON(forecast_json):avghumidity AS FLOAT) AS forecast_humidity,
    CAST(PARSE_JSON(forecast_json):maxwind_kph AS FLOAT) AS forecast_wind_kph,
    CAST(PARSE_JSON(forecast_json):maxwind_mph AS FLOAT) AS forecast_wind_mph,
    PREDICT_CONDITION(
        CAST(PARSE_JSON(forecast_json):avgtemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxtemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):mintemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):totalprecip_mm AS FLOAT),
        CAST(PARSE_JSON(forecast_json):daily_chance_of_rain AS FLOAT),
        CAST(PARSE_JSON(forecast_json):avghumidity AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxwind_kph AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxwind_mph AS FLOAT),
        region
    ) AS forecast_condition,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM day_1
),
day_2 AS (
    SELECT
        predict_day_1.province_id,
        DATEADD(day, 2, weather_date) AS forecast_date,
        PREDICT_ALL_WEATHER_METRICS(
            AVGTEMP2, AVGTEMP3, forecast_avg_temperature,
            MAXTEMP2, MAXTEMP3, forecast_max_temperature,
            MINTEMP2, MINTEMP3, forecast_min_temperature,
            PRECIP2, PRECIP3, forecast_precipitation,
            RAINCHANCE2, RAINCHANCE3, forecast_chance_rain,
            HUMIDITY2, HUMIDITY3, forecast_humidity,
            WINDKPH2, WINDKPH3, forecast_wind_kph,
            WINDMPH2, WINDMPH3, forecast_wind_mph,
            region
        ) AS forecast_json
    FROM base
    join predict_day_1 on base.province_id = predict_day_1.province_id 
    WHERE row_num = 1    
),
predict_day_2 AS (
SELECT
    day_2.province_id,
    forecast_date,
    CAST(PARSE_JSON(forecast_json):avgtemp_c AS FLOAT) AS forecast_avg_temperature,
    CAST(PARSE_JSON(forecast_json):maxtemp_c AS FLOAT) AS forecast_max_temperature,
    CAST(PARSE_JSON(forecast_json):mintemp_c AS FLOAT) AS forecast_min_temperature,
    CAST(PARSE_JSON(forecast_json):totalprecip_mm AS FLOAT) AS forecast_precipitation,
    CAST(PARSE_JSON(forecast_json):daily_chance_of_rain AS FLOAT) AS forecast_chance_rain,
    CAST(PARSE_JSON(forecast_json):avghumidity AS FLOAT) AS forecast_humidity,
    CAST(PARSE_JSON(forecast_json):maxwind_kph AS FLOAT) AS forecast_wind_kph,
    CAST(PARSE_JSON(forecast_json):maxwind_mph AS FLOAT) AS forecast_wind_mph,
    PREDICT_CONDITION(
        CAST(PARSE_JSON(forecast_json):avgtemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxtemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):mintemp_c AS FLOAT),
        CAST(PARSE_JSON(forecast_json):totalprecip_mm AS FLOAT),
        CAST(PARSE_JSON(forecast_json):daily_chance_of_rain AS FLOAT),
        CAST(PARSE_JSON(forecast_json):avghumidity AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxwind_kph AS FLOAT),
        CAST(PARSE_JSON(forecast_json):maxwind_mph AS FLOAT),
        region
    ) AS forecast_condition,
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at
FROM day_2
join base on day_2.province_id = base.province_id
WHERE row_num = 1
)
select * from predict_day_1
UNION ALL
select * from predict_day_2
order by province_id

