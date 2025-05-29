
{{ 
    config( materialized='incremental', 
    unique_key=['province_id'], 
    incremental_strategy='merge', 
    merge_update_columns=['province_name','province_name_vn', 'latitude', 'longitude', 'updated_at'] ) }}

with transformed as ( 
    select 
            province_id, 
            province_name,
            province_name_vn, 
            latitude, 
            longitude, 
            current_timestamp() as created_at, 
            current_timestamp() as updated_at
            from {{ ref('stg_vietnam_provinces') }} )

{% if is_incremental() %} 
    -- Only process new or changed records 
    select 
            transformed.province_id, 
            transformed.province_name, 
            transformed.province_name_vn,
            transformed.latitude, 
            transformed.longitude, 
            coalesce(existing.created_at, transformed.created_at) as created_at, 
            transformed.updated_at, 
            from transformed 
            left join {{ this }} as existing on transformed.province_id = existing.province_id 
            where 
                existing.province_id is null -- New records 
                or transformed.province_name != existing.province_name 
                or transformed.latitude != existing.latitude 
                or transformed.longitude != existing.longitude 
{% else %} -- First run - process all records 
    select * from transformed 
{% endif %}