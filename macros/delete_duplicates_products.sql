{% macro delete_duplicates_products(table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
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
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY SKU, _3_PL_CUSTOMER ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source('shiphero_va', table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source('shiphero_va', table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source('shiphero_va', table_name) }} 
        (_FILE,
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
        _FIVETRAN_SYNCED)

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
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}