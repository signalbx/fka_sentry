-- tests if there are any sku values that were truncated to scientific notation
SELECT sku
FROM {{ source('shiphero_va', 'orderitems') }}
WHERE REGEXP_LIKE(sku, '^[0-9]+\.?[0-9]*[Ee][+-]?[0-9]+$')