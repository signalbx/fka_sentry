--tests if any of the numerical columns did not come across in the nightly data load
WITH source_data AS (
    SELECT
        SUM(SUBTOTAL) AS subtotal_sum,
        SUM(TOTAL) AS total_sum,
        SUM(SHIPPING) AS shipping_sum,
        SUM(TAX) AS tax_sum
    FROM {{ source('shiphero_va', 'orders') }} 
)

SELECT subtotal_sum, total_sum, shipping_sum, tax_sum
FROM source_data
WHERE subtotal_sum <= 0 OR total_sum <= 0 OR shipping_sum <= 0 OR tax_sum <= 0


