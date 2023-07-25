{% macro delete_duplicates_shipments(table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT _FILE,
        _LINE,
        _MODIFIED,
        SHIPPING_LABEL_ID,
        PACKER_NAME,
        DISTINCT_ITEMS_SHIPPED,
        QUANTITY_SHIPPED,
        COALESCE(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI AM'),
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD HH12:MI AM'),
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD HH24:MI:SS'),
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD HH12:MI:SS AM'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI:SS'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI:SS AM')) 
        AS formatted_date,
        MERGED,
        CREATED_AT,
        EMAIL,
        SHOP_NAME,
        COMPANY,
        PHONE,
        ORDER_NUMBER,
        PROFILE,
        WAREHOUSE,
        WAREHOUSE_ID,
        ORDER_ID,
        CARRIER,
        SHIPPING_METHOD,
        SHIPPING_NAME,
        TRACKING_NUMBER,
        TO_NAME,
        ADDRESS_1,
        ADDRESS_2,
        CITY,
        ZIP,
        STATE,
        COUNTRY,
        SIZE_LENGTH_X_WIDTH_X_HEIGHT_IN_,
        WEIGHT_LB_,
        WIDTH_IN_,
        LENGTH_IN_,
        BOX_NAME,
        TOTAL_SHIPPING_CHARGED,
        LINE_ITEM_TOTAL,
        SHIPPED_OFF_SHIP_HERO,
        PROMOTION_DISCOUNT,
        DISCOUNTED_LINE_ITEMS_TOTAL,
        DROP_SHIPMENTS,
        LABEL_COST,
        DIFFERENCE,
        TRACKING_URL,
        _3_PL_CUSTOMER,
        ORDER_TAGS,
        _FIVETRAN_SYNCED,
        INSURANCE_AMOUNT,
        HEIGHT_IN_,
        ROW_NUMBER() OVER (PARTITION BY formatted_date, SHIPPING_LABEL_ID, ORDER_NUMBER, ORDER_ID, TRACKING_NUMBER, _3_PL_CUSTOMER ORDER BY _MODIFIED DESC) AS row_number
FROM {{ source('shiphero_va', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('shiphero_va', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('shiphero_va', table_name) }} 
        ( _FILE,
    _LINE,
    _MODIFIED,
    SHIPPING_LABEL_ID,
    PACKER_NAME,
    DISTINCT_ITEMS_SHIPPED,
    QUANTITY_SHIPPED,
    ORDER_DATE,
    MERGED,
    CREATED_AT,
    EMAIL,
    SHOP_NAME,
    COMPANY,
    PHONE,
    ORDER_NUMBER,
    PROFILE,
    WAREHOUSE,
    WAREHOUSE_ID,
    ORDER_ID,
    CARRIER,
    SHIPPING_METHOD,
    SHIPPING_NAME,
    TRACKING_NUMBER,
    TO_NAME,
    ADDRESS_1,
    ADDRESS_2,
    CITY,
    ZIP,
    STATE,
    COUNTRY,
    SIZE_LENGTH_X_WIDTH_X_HEIGHT_IN_,
    WEIGHT_LB_,
    WIDTH_IN_,
    LENGTH_IN_,
    BOX_NAME,
    TOTAL_SHIPPING_CHARGED,
    LINE_ITEM_TOTAL,
    SHIPPED_OFF_SHIP_HERO,
    PROMOTION_DISCOUNT,
    DISCOUNTED_LINE_ITEMS_TOTAL,
    DROP_SHIPMENTS,
    LABEL_COST,
    DIFFERENCE,
    TRACKING_URL,
    _3_PL_CUSTOMER,
    ORDER_TAGS,
    _FIVETRAN_SYNCED,
    INSURANCE_AMOUNT,
    HEIGHT_IN_)

SELECT _FILE,
    _LINE,
    _MODIFIED,
    SHIPPING_LABEL_ID,
    PACKER_NAME,
    DISTINCT_ITEMS_SHIPPED,
    QUANTITY_SHIPPED,
    formatted_date,
    MERGED,
    CREATED_AT,
    EMAIL,
    SHOP_NAME,
    COMPANY,
    PHONE,
    ORDER_NUMBER,
    PROFILE,
    WAREHOUSE,
    WAREHOUSE_ID,
    ORDER_ID,
    CARRIER,
    SHIPPING_METHOD,
    SHIPPING_NAME,
    TRACKING_NUMBER,
    TO_NAME,
    ADDRESS_1,
    ADDRESS_2,
    CITY,
    ZIP,
    STATE,
    COUNTRY,
    SIZE_LENGTH_X_WIDTH_X_HEIGHT_IN_,
    WEIGHT_LB_,
    WIDTH_IN_,
    LENGTH_IN_,
    BOX_NAME,
    TOTAL_SHIPPING_CHARGED,
    LINE_ITEM_TOTAL,
    SHIPPED_OFF_SHIP_HERO,
    PROMOTION_DISCOUNT,
    DISCOUNTED_LINE_ITEMS_TOTAL,
    DROP_SHIPMENTS,
    LABEL_COST,
    DIFFERENCE,
    TRACKING_URL,
    _3_PL_CUSTOMER,
    ORDER_TAGS,
    _FIVETRAN_SYNCED,
    INSURANCE_AMOUNT,
    HEIGHT_IN_
FROM {{ temp_table_name }}
WHERE row_number = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}
