SELECT
    province_name,
    COUNT(*) AS occurrences
FROM {{ ref('stg_vietnam_provinces') }}
GROUP BY province_name
HAVING COUNT(*) > 1
