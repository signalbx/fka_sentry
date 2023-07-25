-- tests if there are any null values in the key columns in the products table
SELECT *
FROM {{ source('shiphero_va', 'products') }}
WHERE SKU IS NULL OR _3_PL_CUSTOMER IS NULL