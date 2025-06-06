{{
    config( materialized='incremental',
    unique_key=['district_id'],
    incremental_strategy='merge',
    merge_update_columns=['province_id','district_name', 'district_name_vn', 'latitude', 'longitude', 'updated_at'] ) }}

with transformed as (
    select
        district_id,
        province_id,
        district_name,
        district_name_vn,
        latitude,
        longitude,
        current_timestamp() as created_at,
        current_timestamp() as updated_at
    from {{ ref('stg_vietnam_districts') }}
),

deduplicated as (
    select *
    from (
        select
            *,
            row_number()
                over (
                    partition by district_id
                    order by updated_at desc
                )
                as row_num
        from transformed
    )
    where row_num = 1
)

{% if is_incremental() %}
    -- Only process new or changed records
    select distinct
        deduplicated.district_id,
        deduplicated.province_id,
        deduplicated.district_name,
        deduplicated.district_name_vn,
        deduplicated.latitude,
        deduplicated.longitude,
        deduplicated.updated_at,
        coalesce(existing.created_at, deduplicated.created_at) as created_at
    from deduplicated
    left join
        {{ this }} as existing
        on deduplicated.district_id = existing.district_id
    where
        existing.district_id is null -- New records
        or deduplicated.province_id != existing.province_id
        or deduplicated.district_name != existing.district_name
        or deduplicated.district_name_vn != existing.district_name_vn
        or deduplicated.latitude != existing.latitude
        or deduplicated.longitude != existing.longitude
{% else %}
    select * from deduplicated
{% endif %}