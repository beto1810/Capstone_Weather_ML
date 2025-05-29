
select
    cast(DISTRICT_CODE as INT) as district_id,
    cast(PROVINCE_CODE as INT) as province_id,
    cast(EN_NAME as varchar) as district_name,
    cast(VN_NAME as varchar) as district_name_vn,
    cast(latitude as float) as latitude,
    cast(longitude as float) as longitude,
    current_timestamp() as loaded_at
from {{ source('vietnam_geo_data', 'VIETNAM_DISTRICTS') }}
where EN_NAME is not null
    and PROVINCE_CODE is not null
    AND latitude is not null
    AND longitude is not null