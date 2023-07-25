-- analyzes the average number of rows that a production table has per date for the last 30 days and fail if the stage table has less than 20% of that average
WITH prod_data AS (
    SELECT COALESCE(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS ORDER_DT, COUNT(*) as row_count
    FROM {{ source('shiphero_va', 'shipments_prod') }}
    WHERE ORDER_DT >= DATEADD(day, -30, CURRENT_DATE())
    GROUP BY ORDER_DT
),
stage_data AS (
    SELECT COUNT(*) as row_count
    FROM {{ source('shiphero_va', 'shipments') }}
    WHERE COALESCE(TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(ORDER_DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(ORDER_DATE, 'MM/DD/YYYY HH12:MI AM')) >= DATEADD(day, -30, CURRENT_DATE())
),
avg_prod_data AS (
    SELECT SUM(row_count) / COUNT(DISTINCT ORDER_DT) as avg_row_count
    FROM prod_data
),

comparison AS (
    SELECT row_count < 0.2 * avg_row_count AS condition_met
    FROM stage_data, avg_prod_data
)

SELECT 1
FROM comparison
WHERE condition_met = TRUE