{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ORDER_DATE', 'ORDER_NUMBER', '_3_PL_CUSTOMER'],
    pre_hook=[
        "DELETE FROM {{ source('shiphero_va', 'orders') }} WHERE ORDER_DATE IS NULL;",
        "DELETE FROM {{ source('shiphero_va', 'orders_dbt') }} WHERE ORDER_DATE IS NULL;",
        "{{ delete_duplicates_orders('orders', 'temp_orders_table') }}",
        "{{ delete_duplicates_orders('orders_dbt', 'temp_orders_dbt_table') }}",
    ]

) }}


WITH stage AS (
    SELECT
        _FILE,
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
        _FIVETRAN_SYNCED,
        REQUIRED_SHIP_DATE,
        HOLD_UNTIL_DATE,
        PRIORITY,
        USER_LOCK
    FROM {{ source('shiphero_va', 'orders') }}
    WHERE formatted_date is not null
),

prod AS (
    SELECT
        _FILE,
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
        _FIVETRAN_SYNCED,
        HOLD_UNTIL_DATE,
        PRIORITY,
        USER_LOCK
    FROM {{ source('shiphero_va', 'orders_dbt') }}
    WHERE formatted_date is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.formatted_date = prod.formatted_date
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
),

updates AS (
    SELECT
        matched_records._FILE,
        matched_records._LINE,
        matched_records._MODIFIED,
        matched_records.formatted_date as ORDER_DATE,
        matched_records._3_PL_CUSTOMER,
        matched_records.ORDER_NUMBER,
        matched_records.STATUS,
        matched_records.WAREHOUSES,
        matched_records.PROFILE,
        matched_records.FIRST_NAME,
        matched_records.LAST_NAME,
        matched_records.EMAIL,
        matched_records.ADDRESS,
        matched_records.CITY,
        matched_records.STATE,
        matched_records.ZIP,
        matched_records.COUNTRY,
        matched_records.SUBTOTAL,
        matched_records.DISCOUNT,
        matched_records.SHIPPING,
        matched_records.TAX,
        matched_records.TOTAL,
        matched_records.READY_TO_SHIP,
        matched_records.SHIPPING_NAME,
        matched_records.CARRIER,
        matched_records.METHOD,
        matched_records.EXPECTED_WEIGHT_LB_,
        matched_records.STORE,
        matched_records.ON_HOLD,
        matched_records.PICK_PRIORITY,
        matched_records.LABELS,
        matched_records.TOTAL_QUANTITY_BACKORDERED,
        matched_records.ORDER_DATE_UTC,
        matched_records._FIVETRAN_SYNCED,
        matched_records.REQUIRED_SHIP_DATE,
        matched_records.HOLD_UNTIL_DATE,
        matched_records.PRIORITY,
        matched_records.USER_LOCK
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.formatted_date = prod.formatted_date
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
    WHERE prod.formatted_date IS NULL and prod.ORDER_NUMBER IS NULL and prod._3_PL_CUSTOMER IS NULL
),

inserts AS (
    SELECT
        non_matched_records._FILE,
        non_matched_records._LINE,
        non_matched_records._MODIFIED,
        non_matched_records.formatted_date as ORDER_DATE,
        non_matched_records._3_PL_CUSTOMER,
        non_matched_records.ORDER_NUMBER,
        non_matched_records.STATUS,
        non_matched_records.WAREHOUSES,
        non_matched_records.PROFILE,
        non_matched_records.FIRST_NAME,
        non_matched_records.LAST_NAME,
        non_matched_records.EMAIL,
        non_matched_records.ADDRESS,
        non_matched_records.CITY,
        non_matched_records.STATE,
        non_matched_records.ZIP,
        non_matched_records.COUNTRY,
        non_matched_records.SUBTOTAL,
        non_matched_records.DISCOUNT,
        non_matched_records.SHIPPING,
        non_matched_records.TAX,
        non_matched_records.TOTAL,
        non_matched_records.READY_TO_SHIP,
        non_matched_records.SHIPPING_NAME,
        non_matched_records.CARRIER,
        non_matched_records.METHOD,
        non_matched_records.EXPECTED_WEIGHT_LB_,
        non_matched_records.STORE,
        non_matched_records.ON_HOLD,
        non_matched_records.PICK_PRIORITY,
        non_matched_records.LABELS,
        non_matched_records.TOTAL_QUANTITY_BACKORDERED,
        non_matched_records.ORDER_DATE_UTC,
        non_matched_records._FIVETRAN_SYNCED,
        non_matched_records.REQUIRED_SHIP_DATE,
        non_matched_records.HOLD_UNTIL_DATE,
        non_matched_records.PRIORITY,
        non_matched_records.USER_LOCK
    FROM non_matched_records)



SELECT
    _FILE,
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
    _FIVETRAN_SYNCED,
    REQUIRED_SHIP_DATE,
    HOLD_UNTIL_DATE,
    PRIORITY,
    USER_LOCK
FROM updates

UNION

SELECT
    _FILE,
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
    _FIVETRAN_SYNCED,
    REQUIRED_SHIP_DATE,
    HOLD_UNTIL_DATE,
    PRIORITY,
    USER_LOCK
FROM inserts