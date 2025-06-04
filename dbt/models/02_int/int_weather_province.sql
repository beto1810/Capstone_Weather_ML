SELECT 
    stg_vietnam_provinces.province_id AS province_id,
    DATE(weather_updated_at) AS weather_date,

    AVG(temperature_celsius) AS avg_temperature,
    MAX(temperature_celsius) AS max_temperature,
    MIN(temperature_celsius) AS min_temperature,

    AVG(humidity_percent) AS avg_humidity,

    SUM(precipitation_mm) AS sum_precipitation,
    MAX(precipitation_mm) AS max_precipitation,
    MIN(precipitation_mm) AS min_precipitation,

    MAX(CASE WHEN precipitation_mm > 0 THEN 1 ELSE 0 END) AS chance_rain,

    AVG(wind_speed_kph) AS avg_wind_kph,
    AVG(wind_speed_kph * 0.621371) AS avg_wind_mph-- derived from KPH


FROM {{ ref('stg_weather_data') }}
join {{ ref('stg_vietnam_provinces') }} on stg_weather_data.province_name = stg_vietnam_provinces.province_name
GROUP BY province_id, weather_date, region
ORDER BY province_id
