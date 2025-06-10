{{ config(materialized='table') }}

WITH absence_unpivoted AS (
    -- Unpivot absence quantities
    SELECT 
        fdcp,
        firm_id,
        department_id,
        category_id,
        person_id,
        dateperiod AS absence_date,
        year,
        quarter,
        month,
        qty_days_worked,
        qty_working_days,
        
        'A1' AS absence_type,
        qty_a1_days AS absence_days,
        freq_a1_days AS absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_a1_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'A2', qty_a2_days, freq_a2_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_a2_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'P1', qty_p1_days, freq_p1_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_p1_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'P2', qty_p2_days, freq_p2_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_p2_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'P3', qty_p3_days, freq_p3_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_p3_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'Z1', qty_z1_days, freq_z1_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_z1_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'Z2', qty_z2_days, freq_z2_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_z2_days > 0
    
    UNION ALL
    
    SELECT fdcp, firm_id, department_id, category_id, person_id, dateperiod, year, quarter, month,
           qty_days_worked, qty_working_days, 'Z3', qty_z3_days, freq_z3_days
    FROM {{ ref('stg_hr__absences') }}
    WHERE qty_z3_days > 0
),

final AS (
    SELECT
        a.*,
        -- Calculate absence rate
        CASE WHEN a.qty_working_days > 0 
             THEN ROUND((a.absence_days / a.qty_working_days) * 100, 2)
             ELSE 0 
        END AS absence_rate_percent,
        
        -- Join employee dimensions
        e.gender,
        e.age_group,
        e.seniority_group,
        e.region,
        
        -- Join work plan for NACE
        w.nace_code,
        w.nace_description,
        
        -- Date calculations
        DATE_TRUNC('month', a.absence_date) AS absence_month,
        CONCAT(a.year, '-', LPAD(a.month, 2, '0')) AS year_month
        
    FROM absence_unpivoted a
    LEFT JOIN {{ ref('dim_hr2__employee') }} e ON a.fdcp = e.fdcp
    LEFT JOIN {{ ref('stg_hr__work_plan') }} w ON a.fdcp = w.fdcp_identifier
        AND a.absence_date BETWEEN w.valid_from_date AND w.valid_to_date
)

SELECT * FROM final