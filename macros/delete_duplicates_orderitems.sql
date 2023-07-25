{% macro delete_duplicates_orderitems(table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT _FILE,
        _LINE,
        _MODIFIED,
        WAREHOUSE,
        COALESCE(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
        STORE,
        COALESCE(SKU, '') AS SKU,
        COALESCE(NAME, '') AS NAME,
        QUANTITY,
        QUANTITY_PENDING_FULFILLMENT,
        QUANTITY_BACKORDERED,
        PRICE,
        SUBTOTAL,
        PENDING_SUBTOTAL,
        PROMOTION_DISCOUNT,
        COALESCE(ORDER_NUMBER, '') AS ORDER_NUMBER,
        COALESCE(LINE_ITEM_STATUS, '') AS LINE_ITEM_STATUS,
        ORDER_STATUS,
        ORDER_SHIPPING_NAME,
        CUSTOMER_EMAIL,
        PROFILE_ID,
        _FIVETRAN_SYNCED,
        ROW_NUMBER() OVER (PARTITION BY formatted_date, ORDER_NUMBER, SKU, NAME, LINE_ITEM_STATUS ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source('shiphero_va', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('shiphero_va', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('shiphero_va', table_name) }} 
        (_FILE,
        _LINE,
        _MODIFIED,
        WAREHOUSE,
        ORDER_DATE,
        STORE,
        SKU,
        NAME,
        QUANTITY,
        QUANTITY_PENDING_FULFILLMENT,
        QUANTITY_BACKORDERED,
        PRICE,
        SUBTOTAL,
        PENDING_SUBTOTAL,
        PROMOTION_DISCOUNT,
        ORDER_NUMBER,
        LINE_ITEM_STATUS,
        ORDER_STATUS,
        ORDER_SHIPPING_NAME,
        CUSTOMER_EMAIL,
        PROFILE_ID,
        _FIVETRAN_SYNCED)

SELECT _FILE,
        _LINE,
        _MODIFIED,
        WAREHOUSE,
        formatted_date,
        STORE,
        SKU,
        NAME,
        QUANTITY,
        QUANTITY_PENDING_FULFILLMENT,
        QUANTITY_BACKORDERED,
        PRICE,
        SUBTOTAL,
        PENDING_SUBTOTAL,
        PROMOTION_DISCOUNT,
        ORDER_NUMBER,
        LINE_ITEM_STATUS,
        ORDER_STATUS,
        ORDER_SHIPPING_NAME,
        CUSTOMER_EMAIL,
        PROFILE_ID,
        _FIVETRAN_SYNCED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}