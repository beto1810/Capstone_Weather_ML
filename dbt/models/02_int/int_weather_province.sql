SELECT
    p.province_id,
    DATE(w.weather_updated_at) AS weather_date,

    AVG(w.temperature_celsius) AS avg_temperature,
    MAX(w.temperature_celsius) AS max_temperature,
    MIN(w.temperature_celsius) AS min_temperature,

    AVG(w.humidity_percent) AS avg_humidity,

    SUM(w.precipitation_mm) AS sum_precipitation,
    MAX(w.precipitation_mm) AS max_precipitation,
    MIN(w.precipitation_mm) AS min_precipitation,

    MAX(CASE WHEN w.precipitation_mm > 0 THEN 1 ELSE 0 END) AS chance_rain,

    AVG(w.wind_speed_kph) AS avg_wind_kph,
    AVG(w.wind_speed_kph * 0.621371) AS avg_wind_mph-- derived from KPH

FROM {{ ref('stg_weather_data') }} AS w
INNER JOIN
    {{ ref('stg_vietnam_provinces') }} AS p
    ON w.province_name = p.province_name
GROUP BY p.province_id, weather_date
ORDER BY p.province_id
