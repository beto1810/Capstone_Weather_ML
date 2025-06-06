{{
    config(
        materialized='incremental',
        unique_key=['district_id'],
        incremental_strategy='merge',
        merge_update_columns=['province_id','district_name', 'district_name_vn', 'latitude', 'longitude', 'updated_at']
    )
}}

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
    select
        district_id,
        province_id,
        district_name,
        district_name_vn,
        latitude,
        longitude,
        created_at,
        updated_at,
        row_number() over (
            partition by district_id
            order by updated_at desc
        ) as row_num
    from transformed
),

final as (
    select
        deduplicated.district_id,
        deduplicated.province_id,
        deduplicated.district_name,
        deduplicated.district_name_vn,
        deduplicated.latitude,
        deduplicated.longitude,
        deduplicated.created_at,
        deduplicated.updated_at
    from deduplicated
    where deduplicated.row_num = 1
)

{% if is_incremental() %}
    select distinct
        final.district_id,
        final.province_id,
        final.district_name,
        final.district_name_vn,
        final.latitude,
        final.longitude,
        final.updated_at,
        coalesce(existing.created_at, final.created_at) as created_at
    from final
    left join
        {{ this }} as existing
        on final.district_id = existing.district_id
    where
        existing.district_id is null -- New records
        or final.province_id != existing.province_id
        or final.district_name != existing.district_name
        or final.district_name_vn != existing.district_name_vn
        or final.latitude != existing.latitude
        or final.longitude != existing.longitude
{% else %}
    select * from final
{% endif %}