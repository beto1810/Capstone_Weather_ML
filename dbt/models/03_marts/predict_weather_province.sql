WITH base AS (
    SELECT
        fct.province_id,
        dim.region,
        fct.weather_date,
        ROW_NUMBER()
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date DESC
            )
            AS row_num,

        LAG(fct.avg_temperature, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS avgtemp1,
        LAG(fct.avg_temperature, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS avgtemp2,
        LAG(fct.avg_temperature, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS avgtemp3,

        LAG(fct.max_temperature, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS maxtemp1,
        LAG(fct.max_temperature, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS maxtemp2,
        LAG(fct.max_temperature, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS maxtemp3,

        LAG(fct.min_temperature, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS mintemp1,
        LAG(fct.min_temperature, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS mintemp2,
        LAG(fct.min_temperature, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS mintemp3,

        LAG(fct.sum_precipitation, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS precip1,
        LAG(fct.sum_precipitation, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS precip2,
        LAG(fct.sum_precipitation, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS precip3,

        LAG(fct.chance_rain, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS rainchance1,
        LAG(fct.chance_rain, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS rainchance2,
        LAG(fct.chance_rain, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS rainchance3,

        LAG(fct.avg_humidity, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS humidity1,
        LAG(fct.avg_humidity, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS humidity2,
        LAG(fct.avg_humidity, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS humidity3,

        LAG(fct.avg_wind_kph, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windkph1,
        LAG(fct.avg_wind_kph, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windkph2,
        LAG(fct.avg_wind_kph, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windkph3,

        LAG(fct.avg_wind_mph, 1)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windmph1,
        LAG(fct.avg_wind_mph, 2)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windmph2,
        LAG(fct.avg_wind_mph, 3)
            OVER (
                PARTITION BY fct.province_id
                ORDER BY fct.weather_date
            )
            AS windmph3

    FROM {{ ref('fct_weather_province') }} AS fct
    INNER JOIN
        {{ ref('dim_vietnam_provinces') }} AS dim
        ON fct.province_id = dim.province_id
),

day_1 AS (
    SELECT
        base.province_id,
        base.region,
        base.weather_date,
        PREDICT_ALL_WEATHER_METRICS(
            base.avgtemp1, base.avgtemp2, base.avgtemp3,
            base.maxtemp1, base.maxtemp2, base.maxtemp3,
            base.mintemp1, base.mintemp2, base.mintemp3,
            base.precip1, base.precip2, base.precip3,
            base.rainchance1, base.rainchance2, base.rainchance3,
            base.humidity1, base.humidity2, base.humidity3,
            base.windkph1, base.windkph2, base.windkph3,
            base.windmph1, base.windmph2, base.windmph3,
            base.region
        ) AS forecast_json
    FROM base
    WHERE base.row_num = 1
),

predict_day_1 AS (
    SELECT
        day_1.province_id,
        DATEADD(DAY, 1, day_1.weather_date) AS forecast_date,
        CAST(PARSE_JSON(day_1.forecast_json):avgtemp_c AS FLOAT)
            AS forecast_avg_temperature,
        CAST(PARSE_JSON(day_1.forecast_json):maxtemp_c AS FLOAT)
            AS forecast_max_temperature,
        CAST(PARSE_JSON(day_1.forecast_json):mintemp_c AS FLOAT)
            AS forecast_min_temperature,
        CAST(PARSE_JSON(day_1.forecast_json):totalprecip_mm AS FLOAT)
            AS forecast_precipitation,
        CAST(PARSE_JSON(day_1.forecast_json):daily_chance_of_rain AS FLOAT)
            AS forecast_chance_rain,
        CAST(PARSE_JSON(day_1.forecast_json):avghumidity AS FLOAT)
            AS forecast_humidity,
        CAST(PARSE_JSON(day_1.forecast_json):maxwind_kph AS FLOAT)
            AS forecast_wind_kph,
        CAST(PARSE_JSON(day_1.forecast_json):maxwind_mph AS FLOAT)
            AS forecast_wind_mph,
        PREDICT_CONDITION(
            CAST(PARSE_JSON(day_1.forecast_json):avgtemp_c AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):maxtemp_c AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):mintemp_c AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):totalprecip_mm AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):daily_chance_of_rain AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):avghumidity AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):maxwind_kph AS FLOAT),
            CAST(PARSE_JSON(day_1.forecast_json):maxwind_mph AS FLOAT),
            day_1.region
        ) AS forecast_condition,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM day_1
),

day_2 AS (
    SELECT
        predict_day_1.province_id,
        DATEADD(DAY, 2, base.weather_date) AS forecast_date,
        PREDICT_ALL_WEATHER_METRICS(
            base.avgtemp2, base.avgtemp3, predict_day_1.forecast_avg_temperature,
            base.maxtemp2, base.maxtemp3, predict_day_1.forecast_max_temperature,
            base.mintemp2, base.mintemp3, predict_day_1.forecast_min_temperature,
            base.precip2, base.precip3, predict_day_1.forecast_precipitation,
            base.rainchance2, base.rainchance3, predict_day_1.forecast_chance_rain,
            base.humidity2, base.humidity3, predict_day_1.forecast_humidity,
            base.windkph2, base.windkph3, predict_day_1.forecast_wind_kph,
            base.windmph2, base.windmph3, predict_day_1.forecast_wind_mph,
            base.region
        ) AS forecast_json
    FROM base
    INNER JOIN predict_day_1 ON base.province_id = predict_day_1.province_id
    WHERE base.row_num = 1
),

predict_day_2 AS (
    SELECT
        day_2.province_id,
        day_2.forecast_date,
        CAST(PARSE_JSON(day_2.forecast_json):avgtemp_c AS FLOAT)
            AS forecast_avg_temperature,
        CAST(PARSE_JSON(day_2.forecast_json):maxtemp_c AS FLOAT)
            AS forecast_max_temperature,
        CAST(PARSE_JSON(day_2.forecast_json):mintemp_c AS FLOAT)
            AS forecast_min_temperature,
        CAST(PARSE_JSON(day_2.forecast_json):totalprecip_mm AS FLOAT)
            AS forecast_precipitation,
        CAST(PARSE_JSON(day_2.forecast_json):daily_chance_of_rain AS FLOAT)
            AS forecast_chance_rain,
        CAST(PARSE_JSON(day_2.forecast_json):avghumidity AS FLOAT)
            AS forecast_humidity,
        CAST(PARSE_JSON(day_2.forecast_json):maxwind_kph AS FLOAT)
            AS forecast_wind_kph,
        CAST(PARSE_JSON(day_2.forecast_json):maxwind_mph AS FLOAT)
            AS forecast_wind_mph,
        PREDICT_CONDITION(
            CAST(PARSE_JSON(day_2.forecast_json):avgtemp_c AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):maxtemp_c AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):mintemp_c AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):totalprecip_mm AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):daily_chance_of_rain AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):avghumidity AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):maxwind_kph AS FLOAT),
            CAST(PARSE_JSON(day_2.forecast_json):maxwind_mph AS FLOAT),
            base.region
        ) AS forecast_condition,
        CURRENT_TIMESTAMP() AS created_at,
        CURRENT_TIMESTAMP() AS updated_at
    FROM day_2
    INNER JOIN base ON day_2.province_id = base.province_id
    WHERE base.row_num = 1
)

SELECT * FROM predict_day_1
UNION ALL
SELECT * FROM predict_day_2
ORDER BY province_id

