{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['SKU', '_3_PL_CUSTOMER'],
    pre_hook=[
        "{{ delete_duplicates_products('products', 'temp_products_table') }}",
        "{{ delete_duplicates_products('products_dbt', 'temp_products_dbt_table') }}",
    ]

) }}


WITH stage AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        NAME,
        SKU,
        ON_HAND,
        _3_PL_CUSTOMER,
        AVAILABLE,
        ALLOCATED,
        BACKORDER,
        RESERVED,
        ON_ORDER,
        SELL_AHEAD,
        PENDING_RETURN,
        NON_SELLABLE_TOTAL,
        SOLD_IN_LAST_30_DAYS,
        VENDORS,
        BARCODE,
        WAREHOUSE,
        WEIGHT_LB_,
        NO_AIR,
        BUILD_KIT,
        FINAL_SALE,
        REORDER_LEVEL,
        REORDER_AMOUNT,
        PRICE,
        VALUE,
        VIRTUAL,
        COUNTRY_OF_MANUFACTURE,
        TAGS,
        TARIFF_CODE,
        NEEDS_SERIAL_NUMBER,
        ACTIVE,
        _FIVETRAN_SYNCED
    FROM {{ source('shiphero_va', 'products') }}
),

prod AS (
    SELECT
        _FILE,
        _LINE,
        _MODIFIED,
        NAME,
        SKU,
        ON_HAND,
        _3_PL_CUSTOMER,
        AVAILABLE,
        ALLOCATED,
        BACKORDER,
        RESERVED,
        ON_ORDER,
        SELL_AHEAD,
        PENDING_RETURN,
        NON_SELLABLE_TOTAL,
        SOLD_IN_LAST_30_DAYS,
        VENDORS,
        BARCODE,
        WAREHOUSE,
        WEIGHT_LB_,
        NO_AIR,
        BUILD_KIT,
        FINAL_SALE,
        REORDER_LEVEL,
        REORDER_AMOUNT,
        PRICE,
        VALUE,
        VIRTUAL,
        COUNTRY_OF_MANUFACTURE,
        TAGS,
        TARIFF_CODE,
        NEEDS_SERIAL_NUMBER,
        ACTIVE,
        _FIVETRAN_SYNCED
    FROM {{ source('shiphero_va', 'products_dbt') }}
),

matched_records AS (
    SELECT
        stage.*
    FROM stage
    JOIN prod
        ON stage.SKU = prod.SKU
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
),

updates AS (
    SELECT
        matched_records._FILE,
        matched_records._LINE,
        matched_records._MODIFIED,
        matched_records.NAME,
        matched_records.SKU,
        matched_records.ON_HAND,
        matched_records._3_PL_CUSTOMER,
        matched_records.AVAILABLE,
        matched_records.ALLOCATED,
        matched_records.BACKORDER,
        matched_records.RESERVED,
        matched_records.ON_ORDER,
        matched_records.SELL_AHEAD,
        matched_records.PENDING_RETURN,
        matched_records.NON_SELLABLE_TOTAL,
        matched_records.SOLD_IN_LAST_30_DAYS,
        matched_records.VENDORS,
        matched_records.BARCODE,
        matched_records.WAREHOUSE,
        matched_records.WEIGHT_LB_,
        matched_records.NO_AIR,
        matched_records.BUILD_KIT,
        matched_records.FINAL_SALE,
        matched_records.REORDER_LEVEL,
        matched_records.REORDER_AMOUNT,
        matched_records.PRICE,
        matched_records.VALUE,
        matched_records.VIRTUAL,
        matched_records.COUNTRY_OF_MANUFACTURE,
        matched_records.TAGS,
        matched_records.TARIFF_CODE,
        matched_records.NEEDS_SERIAL_NUMBER,
        matched_records.ACTIVE,
        matched_records._FIVETRAN_SYNCED
    FROM matched_records),

non_matched_records AS (
    SELECT
        stage.*
    FROM stage
    LEFT JOIN prod
        ON stage.SKU = prod.SKU
        AND stage._3_PL_CUSTOMER = prod._3_PL_CUSTOMER
    WHERE prod.SKU IS NULL and prod._3_PL_CUSTOMER IS NULL
),

inserts AS (
    SELECT
        non_matched_records._FILE,
        non_matched_records._LINE,
        non_matched_records._MODIFIED,
        non_matched_records.NAME,
        non_matched_records.SKU,
        non_matched_records.ON_HAND,
        non_matched_records._3_PL_CUSTOMER,
        non_matched_records.AVAILABLE,
        non_matched_records.ALLOCATED,
        non_matched_records.BACKORDER,
        non_matched_records.RESERVED,
        non_matched_records.ON_ORDER,
        non_matched_records.SELL_AHEAD,
        non_matched_records.PENDING_RETURN,
        non_matched_records.NON_SELLABLE_TOTAL,
        non_matched_records.SOLD_IN_LAST_30_DAYS,
        non_matched_records.VENDORS,
        non_matched_records.BARCODE,
        non_matched_records.WAREHOUSE,
        non_matched_records.WEIGHT_LB_,
        non_matched_records.NO_AIR,
        non_matched_records.BUILD_KIT,
        non_matched_records.FINAL_SALE,
        non_matched_records.REORDER_LEVEL,
        non_matched_records.REORDER_AMOUNT,
        non_matched_records.PRICE,
        non_matched_records.VALUE,
        non_matched_records.VIRTUAL,
        non_matched_records.COUNTRY_OF_MANUFACTURE,
        non_matched_records.TAGS,
        non_matched_records.TARIFF_CODE,
        non_matched_records.NEEDS_SERIAL_NUMBER,
        non_matched_records.ACTIVE,
        non_matched_records._FIVETRAN_SYNCED
    FROM non_matched_records)



SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    NAME,
    SKU,
    ON_HAND,
    _3_PL_CUSTOMER,
    AVAILABLE,
    ALLOCATED,
    BACKORDER,
    RESERVED,
    ON_ORDER,
    SELL_AHEAD,
    PENDING_RETURN,
    NON_SELLABLE_TOTAL,
    SOLD_IN_LAST_30_DAYS,
    VENDORS,
    BARCODE,
    WAREHOUSE,
    WEIGHT_LB_,
    NO_AIR,
    BUILD_KIT,
    FINAL_SALE,
    REORDER_LEVEL,
    REORDER_AMOUNT,
    PRICE,
    VALUE,
    VIRTUAL,
    COUNTRY_OF_MANUFACTURE,
    TAGS,
    TARIFF_CODE,
    NEEDS_SERIAL_NUMBER,
    ACTIVE,
    _FIVETRAN_SYNCED
FROM updates

UNION

SELECT
    _FILE,
    _LINE,
    _MODIFIED,
    NAME,
    SKU,
    ON_HAND,
    _3_PL_CUSTOMER,
    AVAILABLE,
    ALLOCATED,
    BACKORDER,
    RESERVED,
    ON_ORDER,
    SELL_AHEAD,
    PENDING_RETURN,
    NON_SELLABLE_TOTAL,
    SOLD_IN_LAST_30_DAYS,
    VENDORS,
    BARCODE,
    WAREHOUSE,
    WEIGHT_LB_,
    NO_AIR,
    BUILD_KIT,
    FINAL_SALE,
    REORDER_LEVEL,
    REORDER_AMOUNT,
    PRICE,
    VALUE,
    VIRTUAL,
    COUNTRY_OF_MANUFACTURE,
    TAGS,
    TARIFF_CODE,
    NEEDS_SERIAL_NUMBER,
    ACTIVE,
    _FIVETRAN_SYNCED
FROM inserts
