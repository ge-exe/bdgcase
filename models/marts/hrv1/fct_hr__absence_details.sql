{{ config(materialized='table') }}

-- Unpivot absence data for detailed analysis by absence type
WITH absence_unpivoted AS (
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        
        -- Work Accidents
        'A1' as absence_type_code,
        'Work Accident - 1st month' as absence_type_description,
        'Work Accident' as absence_category,
        COALESCE(qty_a1_days, 0) as absence_days,
        COALESCE(freq_a1_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'A2' as absence_type_code,
        'Work Accident - After 1st month' as absence_type_description,
        'Work Accident' as absence_category,
        COALESCE(qty_a2_days, 0) as absence_days,
        COALESCE(freq_a2_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    -- Regular Accidents
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'P1' as absence_type_code,
        'Accident - 1st month' as absence_type_description,
        'Accident' as absence_category,
        COALESCE(qty_p1_days, 0) as absence_days,
        COALESCE(freq_p1_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'P2' as absence_type_code,
        'Accident - 2nd-12th month' as absence_type_description,
        'Accident' as absence_category,
        COALESCE(qty_p2_days, 0) as absence_days,
        COALESCE(freq_p2_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'P3' as absence_type_code,
        'Accident - After 1 year' as absence_type_description,
        'Accident' as absence_category,
        COALESCE(qty_p3_days, 0) as absence_days,
        COALESCE(freq_p3_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    -- Illness
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'Z1' as absence_type_code,
        'Illness - 1st month' as absence_type_description,
        'Illness' as absence_category,
        COALESCE(qty_z1_days, 0) as absence_days,
        COALESCE(freq_z1_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'Z2' as absence_type_code,
        'Illness - 2nd-12th month' as absence_type_description,
        'Illness' as absence_category,
        COALESCE(qty_z2_days, 0) as absence_days,
        COALESCE(freq_z2_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
    
    UNION ALL
    
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        'Z3' as absence_type_code,
        'Illness - After 1 year' as absence_type_description,
        'Illness' as absence_category,
        COALESCE(qty_z3_days, 0) as absence_days,
        COALESCE(freq_z3_days, 0) as absence_frequency
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
),

enriched_absences AS (
    SELECT 
        absence_up.*,
        
        -- Add work days context from main absence table
        stgabsences.qty_days_worked,
        stgabsences.qty_working_days,
        
        -- Calculate absence rate for this specific type
        CASE 
            WHEN stgabsences.qty_working_days > 0 THEN 
                absence_up.absence_days * 100.0 / stgabsences.qty_working_days
            ELSE 0 
        END AS absence_type_rate_percentage,
        
        -- Severity classification
        CASE 
            WHEN absence_up.absence_days = 0 THEN 'No Absence'
            WHEN absence_up.absence_days <= 2 THEN 'Low'
            WHEN absence_up.absence_days <= 5 THEN 'Medium'
            WHEN absence_up.absence_days <= 10 THEN 'High'
            ELSE 'Very High'
        END AS absence_severity,
        
        -- Duration classification based on type
        CASE 
            WHEN absence_up.absence_type_code IN ('A1', 'P1', 'Z1') THEN 'Short-term'
            WHEN absence_up.absence_type_code IN ('A2', 'P2', 'Z2') THEN 'Medium-term'
            WHEN absence_up.absence_type_code IN ('P3', 'Z3') THEN 'Long-term'
        END AS absence_duration_category
        
    FROM absence_unpivoted absence_up
    LEFT JOIN {{ ref('stg_hr__absences') }} stgabsences
        ON absence_up.employee_key = stgabsences.fdcp 
        AND absence_up.period_date = stgabsences.dateperiod
    WHERE absence_up.period_date >= '2016-06-01' 
      AND absence_up.period_date <= '2018-06-01'
)

SELECT 
    -- Composite key
    {{ dbt_utils.generate_surrogate_key(['period_date', 'employee_key', 'absence_type_code']) }} AS absence_fact_key,
    
    -- Foreign keys
    period_date as date_key,
    employee_key,
    
    -- Absence type details
    absence_type_code,
    absence_type_description,
    absence_category,
    absence_duration_category,
    
    -- Metrics
    absence_days,
    absence_frequency,
    absence_type_rate_percentage,
    absence_severity,
    
    -- Context
    qty_days_worked,
    qty_working_days,
    
    -- Flags for easier analysis
    CASE WHEN absence_days > 0 THEN 1 ELSE 0 END as has_absence,
    CASE WHEN absence_frequency > 0 THEN 1 ELSE 0 END as has_absence_frequency,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at

FROM enriched_absences
WHERE absence_days > 0 OR absence_frequency > 0  -- Only include records with actual absences