-- tests if there are any null values in the key columns in the shipments table
SELECT *
FROM {{ source('shiphero_va', 'shipments') }}
WHERE SHIPPING_LABEL_ID IS NULL OR _3_PL_CUSTOMER IS NULL OR ORDER_NUMBER IS NULL OR ORDER_ID IS NULL OR ORDER_DATE IS NULL
