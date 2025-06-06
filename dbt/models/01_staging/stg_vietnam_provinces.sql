select
    cast(PROVINCE_CODE as INT) as PROVINCE_ID,
    cast(EN_NAME as VARCHAR) as PROVINCE_NAME,
    cast(VN_NAME as VARCHAR) as PROVINCE_NAME_VN,
    cast(LATITUDE as FLOAT) as LATITUDE,
    cast(LONGITUDE as FLOAT) as LONGITUDE,
    cast(REGION as VARCHAR) as REGION,
    current_timestamp() as LOADED_AT
from {{ source('vietnam_geo_data', 'VIETNAM_PROVINCES') }}
where
    EN_NAME is not null
    and PROVINCE_CODE is not null
