{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['ORDER_DATE', 'ORDER_NUMBER', 'SKU', 'NAME', 'LINE_ITEM_STATUS'],
    pre_hook=[
        "DELETE FROM {{ source('shiphero_va', 'orderitems') }} WHERE ORDER_DATE IS NULL;",
        "DELETE FROM {{ source('shiphero_va', 'orderitems_dbt') }} WHERE ORDER_DATE IS NULL;",
        "{{ delete_duplicates_orderitems('orderitems', 'temp_orderitems_table') }}",
        "{{ delete_duplicates_orderitems('orderitems_dbt', 'temp_orderitems_dbt_table') }}",
    ]

) }}


WITH stage AS (
    SELECT
        _FILE,
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
    FROM {{ source('shiphero_va', 'orderitems') }}
    WHERE formatted_date is not null
),

prod AS (
    SELECT
        _FILE,
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
    FROM {{ source('shiphero_va', 'orderitems_dbt') }}
    WHERE formatted_date is not null
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.formatted_date = prod.formatted_date
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage.SKU = prod.SKU
        AND stage.LINE_ITEM_STATUS = prod.LINE_ITEM_STATUS
),

updates AS (
    SELECT
        matched_records._FILE,
        matched_records._LINE,
        matched_records._MODIFIED,
        matched_records.WAREHOUSE,
        matched_records.formatted_date as ORDER_DATE,
        matched_records.STORE,
        matched_records.SKU,
        matched_records.NAME,
        matched_records.QUANTITY,
        matched_records.QUANTITY_PENDING_FULFILLMENT,
        matched_records.QUANTITY_BACKORDERED,
        matched_records.PRICE,
        matched_records.SUBTOTAL,
        matched_records.PENDING_SUBTOTAL,
        matched_records.PROMOTION_DISCOUNT,
        matched_records.ORDER_NUMBER,
        matched_records.LINE_ITEM_STATUS,
        matched_records.ORDER_STATUS,
        matched_records.ORDER_SHIPPING_NAME,
        matched_records.CUSTOMER_EMAIL,
        matched_records.PROFILE_ID,
        matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.formatted_date = prod.formatted_date
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage.SKU = prod.SKU
        AND stage.LINE_ITEM_STATUS = prod.LINE_ITEM_STATUS
    WHERE prod.formatted_date IS NULL and prod.ORDER_NUMBER IS NULL and prod.SKU IS NULL
),

inserts AS (
    SELECT
        non_matched_records._FILE,
        non_matched_records._LINE,
        non_matched_records._MODIFIED,
        non_matched_records.WAREHOUSE,
        non_matched_records.formatted_date as ORDER_DATE,
        non_matched_records.STORE,
        non_matched_records.SKU,
        non_matched_records.NAME,
        non_matched_records.QUANTITY,
        non_matched_records.QUANTITY_PENDING_FULFILLMENT,
        non_matched_records.QUANTITY_BACKORDERED,
        non_matched_records.PRICE,
        non_matched_records.SUBTOTAL,
        non_matched_records.PENDING_SUBTOTAL,
        non_matched_records.PROMOTION_DISCOUNT,
        non_matched_records.ORDER_NUMBER,
        non_matched_records.LINE_ITEM_STATUS,
        non_matched_records.ORDER_STATUS,
        non_matched_records.ORDER_SHIPPING_NAME,
        non_matched_records.CUSTOMER_EMAIL,
        non_matched_records.PROFILE_ID,
        non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)



SELECT
    _FILE,
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
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
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
    _FIVETRAN_SYNCED
FROM inserts
