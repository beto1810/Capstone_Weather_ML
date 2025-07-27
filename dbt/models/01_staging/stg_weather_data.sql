with source_cte as (

    select * from {{ source('raw_weather_data', 'RAW_WEATHER_DATA') }}

),

cast_type as (

    select
        cast(province_name as varchar) as province_name,
        cast(last_updated as timestamp) as weather_updated_at,
        cast(temp_c as float) as temperature_celsius,
        cast(temp_f as float) as temperature_fahrenheit,
        cast(is_day as boolean) as is_daytime,
        cast(condition_text as varchar) as weather_condition,
        cast(condition_icon as varchar) as condition_icon_url,
        cast(condition_code as integer) as condition_code,
        cast(wind_mph as float) as wind_speed_mph,
        cast(wind_kph as float) as wind_speed_kph,
        cast(wind_degree as integer) as wind_direction_degrees,
        cast(wind_dir as varchar) as wind_direction,
        cast(pressure_mb as float) as pressure_millibars,
        cast(pressure_in as float) as pressure_inches,
        cast(precip_mm as float) as precipitation_mm,
        cast(precip_in as float) as precipitation_inches,
        cast(humidity as integer) as humidity_percent,
        cast(cloud as integer) as cloud_cover_percent,
        cast(feelslike_c as float) as feels_like_celsius,
        cast(feelslike_f as float) as feels_like_fahrenheit,
        cast(vis_km as float) as visibility_km,
        cast(vis_miles as float) as visibility_miles,
        cast(uv as float) as uv_index,
        cast(gust_mph as float) as wind_gust_mph,
        cast(gust_kph as float) as wind_gust_kph,
        cast(last_updated as timestamp) as record_loaded_at
    from source_cte

)

select * from cast_type
