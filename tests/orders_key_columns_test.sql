-- tests if there are any null values in the key values in the orders table
SELECT *
FROM {{ source('shiphero_va', 'orders') }}
WHERE ORDER_DATE IS NULL OR ORDER_NUMBER IS NULL OR _3_PL_CUSTOMER IS NULL
