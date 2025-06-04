
select
    cast(PROVINCE_CODE as INT) as province_id,
    cast(EN_NAME as varchar) as province_name,
    cast(VN_NAME as varchar) as province_name_vn,
    cast(latitude as float) as latitude,
    cast(longitude as float) as longitude,
    cast(REGION as varchar) as region,
    current_timestamp() as loaded_at
from {{ source('vietnam_geo_data', 'VIETNAM_PROVINCES') }}
where EN_NAME is not null
    and PROVINCE_CODE is not null
