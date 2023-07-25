{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['SHIPPING_LABEL_ID', '_3_PL_CUSTOMER', 'ORDER_NUMBER', 'ORDER_ID', 'ORDER_DATE'],
    pre_hook=[
        "{{ delete_duplicates_shipments('shipments', 'temp_shipments_table') }}",
        "{{ delete_duplicates_shipments('shipments_dbt', 'temp_shipments_dbt_table') }}",
    ]

) }}


WITH stage AS (
    SELECT
        _FILE,
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
        HEIGHT_IN_
    FROM {{ source('shiphero_va', 'shipments') }}
),

prod AS (
    SELECT
        _FILE,
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
        HEIGHT_IN_
    FROM {{ source('shiphero_va', 'shipments_dbt') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.SHIPPING_LABEL_ID = prod.SHIPPING_LABEL_ID
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage.ORDER_ID = prod.ORDER_ID
        AND stage.ORDER_DATE = prod.ORDER_DATE
),

updates AS (
    SELECT
        matched_records._FILE,
        matched_records._LINE,
        matched_records._MODIFIED,
        matched_records.SHIPPING_LABEL_ID,
        matched_records.PACKER_NAME,
        matched_records.DISTINCT_ITEMS_SHIPPED,
        matched_records.QUANTITY_SHIPPED,
        matched_records.ORDER_DATE,
        matched_records.MERGED,
        matched_records.CREATED_AT,
        matched_records.EMAIL,
        matched_records.SHOP_NAME,
        matched_records.COMPANY,
        matched_records.PHONE,
        matched_records.ORDER_NUMBER,
        matched_records.PROFILE,
        matched_records.WAREHOUSE,
        matched_records.WAREHOUSE_ID,
        matched_records.ORDER_ID,
        matched_records.CARRIER,
        matched_records.SHIPPING_METHOD,
        matched_records.SHIPPING_NAME,
        matched_records.TRACKING_NUMBER,
        matched_records.TO_NAME,
        matched_records.ADDRESS_1,
        matched_records.ADDRESS_2,
        matched_records.CITY,
        matched_records.ZIP,
        matched_records.STATE,
        matched_records.COUNTRY,
        matched_records.SIZE_LENGTH_X_WIDTH_X_HEIGHT_IN_,
        matched_records.WEIGHT_LB_,
        matched_records.WIDTH_IN_,
        matched_records.LENGTH_IN_,
        matched_records.BOX_NAME,
        matched_records.TOTAL_SHIPPING_CHARGED,
        matched_records.LINE_ITEM_TOTAL,
        matched_records.SHIPPED_OFF_SHIP_HERO,
        matched_records.PROMOTION_DISCOUNT,
        matched_records.DISCOUNTED_LINE_ITEMS_TOTAL,
        matched_records.DROP_SHIPMENTS,
        matched_records.LABEL_COST,
        matched_records.DIFFERENCE,
        matched_records.TRACKING_URL,
        matched_records._3_PL_CUSTOMER,
        matched_records.ORDER_TAGS,
        matched_records._FIVETRAN_SYNCED,
        matched_records.INSURANCE_AMOUNT,
        matched_records.HEIGHT_IN_
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.SHIPPING_LABEL_ID = prod.SHIPPING_LABEL_ID
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
        AND stage.ORDER_NUMBER = prod.ORDER_NUMBER
        AND stage.ORDER_ID = prod.ORDER_ID
        AND stage.ORDER_DATE = prod.ORDER_DATE
    WHERE prod.SHIPPING_LABEL_ID IS NULL and prod._3_PL_CUSTOMER IS NULL and 
            prod.ORDER_NUMBER IS NULL and prod.ORDER_ID IS NULL and prod.ORDER_DATE IS NULL
),

inserts AS (
    SELECT
        non_matched_records._FILE,
        non_matched_records._LINE,
        non_matched_records._MODIFIED,
        non_matched_records.SHIPPING_LABEL_ID,
        non_matched_records.PACKER_NAME,
        non_matched_records.DISTINCT_ITEMS_SHIPPED,
        non_matched_records.QUANTITY_SHIPPED,
        non_matched_records.ORDER_DATE,
        non_matched_records.MERGED,
        non_matched_records.CREATED_AT,
        non_matched_records.EMAIL,
        non_matched_records.SHOP_NAME,
        non_matched_records.COMPANY,
        non_matched_records.PHONE,
        non_matched_records.ORDER_NUMBER,
        non_matched_records.PROFILE,
        non_matched_records.WAREHOUSE,
        non_matched_records.WAREHOUSE_ID,
        non_matched_records.ORDER_ID,
        non_matched_records.CARRIER,
        non_matched_records.SHIPPING_METHOD,
        non_matched_records.SHIPPING_NAME,
        non_matched_records.TRACKING_NUMBER,
        non_matched_records.TO_NAME,
        non_matched_records.ADDRESS_1,
        non_matched_records.ADDRESS_2,
        non_matched_records.CITY,
        non_matched_records.ZIP,
        non_matched_records.STATE,
        non_matched_records.COUNTRY,
        non_matched_records.SIZE_LENGTH_X_WIDTH_X_HEIGHT_IN_,
        non_matched_records.WEIGHT_LB_,
        non_matched_records.WIDTH_IN_,
        non_matched_records.LENGTH_IN_,
        non_matched_records.BOX_NAME,
        non_matched_records.TOTAL_SHIPPING_CHARGED,
        non_matched_records.LINE_ITEM_TOTAL,
        non_matched_records.SHIPPED_OFF_SHIP_HERO,
        non_matched_records.PROMOTION_DISCOUNT,
        non_matched_records.DISCOUNTED_LINE_ITEMS_TOTAL,
        non_matched_records.DROP_SHIPMENTS,
        non_matched_records.LABEL_COST,
        non_matched_records.DIFFERENCE,
        non_matched_records.TRACKING_URL,
        non_matched_records._3_PL_CUSTOMER,
        non_matched_records.ORDER_TAGS,
        non_matched_records._FIVETRAN_SYNCED,
        non_matched_records.INSURANCE_AMOUNT,
        non_matched_records.HEIGHT_IN_
    FROM non_matched_records)



SELECT
    _FILE,
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
    HEIGHT_IN_
FROM updates

UNION

SELECT
    _FILE,
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
    HEIGHT_IN_
FROM inserts