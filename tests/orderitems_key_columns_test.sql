-- tests if there are any null values in the key columns in the orderitems table
SELECT *
FROM {{ source('shiphero_va', 'orderitems') }}
WHERE UPPER(NAME) not like 'TIP' AND (ORDER_DATE IS NULL OR ORDER_NUMBER IS NULL OR SKU IS NULL)