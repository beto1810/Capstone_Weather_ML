select
    cast(DISTRICT_CODE as INT) as DISTRICT_ID,
    cast(PROVINCE_CODE as INT) as PROVINCE_ID,
    cast(EN_NAME as VARCHAR) as DISTRICT_NAME,
    cast(VN_NAME as VARCHAR) as DISTRICT_NAME_VN,
    cast(LATITUDE as FLOAT) as LATITUDE,
    cast(LONGITUDE as FLOAT) as LONGITUDE,
    current_timestamp() as LOADED_AT
from {{ source('vietnam_geo_data', 'VIETNAM_DISTRICTS') }}
where
    EN_NAME is not null
    and PROVINCE_CODE is not null
    and LATITUDE is not null
    and LONGITUDE is not null