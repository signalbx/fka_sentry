-- tests if any data came over in the nightly data load
WITH source_data AS (
    SELECT COUNT(*) AS row_count
    FROM {{ source('shiphero_va', 'shipments') }}
)

SELECT row_count
FROM source_data
WHERE row_count = 0