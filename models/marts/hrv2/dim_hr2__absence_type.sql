{{ config(materialized='table') }}

WITH absence_types AS (
    SELECT 
        type_absence AS absence_code,
        type_absence_fr AS absence_description,
        CASE 
            WHEN type_absence IN ('A1', 'A2') THEN 'Work Accident'
            WHEN type_absence IN ('P1', 'P2', 'P3') THEN 'Accident' 
            WHEN type_absence IN ('Z1', 'Z2', 'Z3') THEN 'Illness'
            ELSE 'Other'
        END AS absence_category,
        CASE 
            WHEN type_absence IN ('A1', 'P1', 'Z1') THEN '1st month'
            WHEN type_absence IN ('P2', 'Z2') THEN '2nd-12th month'
            WHEN type_absence IN ('P3', 'Z3') THEN 'After 1st year'
            WHEN type_absence IN ('A2') THEN 'After 1st month'
            ELSE 'Other'
        END AS absence_period_type,
        CASE 
            WHEN type_absence IN ('A1', 'A2') THEN 1  -- Work accidents have highest priority for analysis
            WHEN type_absence IN ('P1', 'P2', 'P3') THEN 2  -- Personal accidents second
            WHEN type_absence IN ('Z1', 'Z2', 'Z3') THEN 3  -- Illness third
            ELSE 4
        END AS absence_sort_order
    FROM {{ ref('Absence_Type') }}
)

SELECT 
    absence_code,
    absence_description,
    absence_category,
    absence_period_type,
    absence_sort_order,
    -- Add derived flags for easier filtering
    CASE WHEN absence_category = 'Work Accident' THEN 1 ELSE 0 END AS is_work_accident,
    CASE WHEN absence_category = 'Accident' THEN 1 ELSE 0 END AS is_personal_accident,
    CASE WHEN absence_category = 'Illness' THEN 1 ELSE 0 END AS is_illness,
    CASE WHEN absence_period_type = '1st month' THEN 1 ELSE 0 END AS is_first_month
FROM absence_types