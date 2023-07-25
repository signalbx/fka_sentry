{% macro delete_duplicates_orders(table_name, temp_table_name) %}



-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT _FILE,
        _LINE,
        _MODIFIED,
        COALESCE(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date,
        COALESCE(_3_PL_CUSTOMER, '') AS _3_PL_CUSTOMER,
        COALESCE(ORDER_NUMBER, '') AS ORDER_NUMBER,
        STATUS,
        WAREHOUSES,
        PROFILE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        ADDRESS,
        CITY,
        STATE,
        ZIP,
        COUNTRY,
        SUBTOTAL,
        DISCOUNT,
        SHIPPING,
        TAX,
        TOTAL,
        READY_TO_SHIP,
        SHIPPING_NAME,
        CARRIER,
        METHOD,
        EXPECTED_WEIGHT_LB_,
        STORE,
        ON_HOLD,
        PICK_PRIORITY,
        LABELS,
        TOTAL_QUANTITY_BACKORDERED,
        ORDER_DATE_UTC,
        _FIVETRAN_SYNCED,
        ROW_NUMBER() OVER (PARTITION BY formatted_date, ORDER_NUMBER, _3_PL_CUSTOMER ORDER BY _MODIFIED DESC, STORE ASC) AS row_num
FROM {{ source('shiphero_va', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('shiphero_va', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('shiphero_va', table_name) }} 
        (_FILE,
        _LINE,
        _MODIFIED,
        ORDER_DATE,
        _3_PL_CUSTOMER,
        ORDER_NUMBER,
        STATUS,
        WAREHOUSES,
        PROFILE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        ADDRESS,
        CITY,
        STATE,
        ZIP,
        COUNTRY,
        SUBTOTAL,
        DISCOUNT,
        SHIPPING,
        TAX,
        TOTAL,
        READY_TO_SHIP,
        SHIPPING_NAME,
        CARRIER,
        METHOD,
        EXPECTED_WEIGHT_LB_,
        STORE,
        ON_HOLD,
        PICK_PRIORITY,
        LABELS,
        TOTAL_QUANTITY_BACKORDERED,
        ORDER_DATE_UTC,
        _FIVETRAN_SYNCED)

SELECT _FILE,
        _LINE,
        _MODIFIED,
        formatted_date,
        _3_PL_CUSTOMER,
        ORDER_NUMBER,
        STATUS,
        WAREHOUSES,
        PROFILE,
        FIRST_NAME,
        LAST_NAME,
        EMAIL,
        ADDRESS,
        CITY,
        STATE,
        ZIP,
        COUNTRY,
        SUBTOTAL,
        DISCOUNT,
        SHIPPING,
        TAX,
        TOTAL,
        READY_TO_SHIP,
        SHIPPING_NAME,
        CARRIER,
        METHOD,
        EXPECTED_WEIGHT_LB_,
        STORE,
        ON_HOLD,
        PICK_PRIORITY,
        LABELS,
        TOTAL_QUANTITY_BACKORDERED,
        ORDER_DATE_UTC,
        _FIVETRAN_SYNCED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};


{% endmacro %}











