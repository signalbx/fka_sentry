--tests if any of the numerical columns did not come across in the nightly data load
WITH source_data AS (
    SELECT
        SUM(TOTAL_SHIPPING_CHARGED) AS TOTAL_SHIPPING_CHARGED_SUM,
        SUM(LABEL_COST) AS LABEL_COST_SUM
    FROM {{ source('shiphero_va', 'shipments') }} 
)

SELECT TOTAL_SHIPPING_CHARGED_SUM, LABEL_COST_SUM
FROM source_data
WHERE TOTAL_SHIPPING_CHARGED_SUM <= 0 OR LABEL_COST_SUM <= 0