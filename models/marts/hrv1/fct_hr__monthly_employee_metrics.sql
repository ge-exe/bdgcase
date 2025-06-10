{{ config(materialized='table') }}

WITH monthly_spine AS (
    -- Generate all month/employee combinations for employees active in each month
    SELECT DISTINCT
        dimdate.date_key,
        dimemployee.employee_key,
        dimemployee.firm_id,
        dimemployee.department_id,
        dimemployee.category_id,
        dimemployee.person_id,
        dimemployee.contract_start_date,
        dimemployee.contract_end_date
    FROM {{ ref('dim_hr__date') }} dimdate
    CROSS JOIN {{ ref('dim_hr__employee') }} dimemployee
    WHERE dimdate.date_key >= dimemployee.contract_start_date 
      AND (dimemployee.contract_end_date IS NULL OR dimdate.date_key <= dimemployee.contract_end_date)
      AND dimdate.date_key >= '2016-06-01'  -- 24 months back from June 2018
      AND dimdate.date_key <= '2018-06-01'
),

salary_data AS (
    -- Monthly salary information
    SELECT 
        fdcp as employee_key,
        period_date,
        gross_salary,
        net_salary,
        gross_salary_108,
        salary_category
    FROM {{ ref('stg_hr__salary_statement') }}
    WHERE period_date IS NOT NULL
),

absence_data AS (
    -- Monthly absence aggregations
    SELECT 
        fdcp as employee_key,
        dateperiod as period_date,
        
        -- Total absence days by category
        COALESCE(qty_a1_days, 0) + COALESCE(qty_a2_days, 0) AS total_work_accident_days,
        COALESCE(qty_p1_days, 0) + COALESCE(qty_p2_days, 0) + COALESCE(qty_p3_days, 0) AS total_accident_days,
        COALESCE(qty_z1_days, 0) + COALESCE(qty_z2_days, 0) + COALESCE(qty_z3_days, 0) AS total_illness_days,
        
        -- Total absence frequencies by category
        COALESCE(freq_a1_days, 0) + COALESCE(freq_a2_days, 0) AS total_work_accident_freq,
        COALESCE(freq_p1_days, 0) + COALESCE(freq_p2_days, 0) + COALESCE(freq_p3_days, 0) AS total_accident_freq,
        COALESCE(freq_z1_days, 0) + COALESCE(freq_z2_days, 0) + COALESCE(freq_z3_days, 0) AS total_illness_freq,
        
        -- Individual absence types for detailed analysis
        qty_a1_days, qty_a2_days,
        qty_p1_days, qty_p2_days, qty_p3_days,
        qty_z1_days, qty_z2_days, qty_z3_days,
        
        freq_a1_days, freq_a2_days,
        freq_p1_days, freq_p2_days, freq_p3_days,
        freq_z1_days, freq_z2_days, freq_z3_days,
        
        -- Work days
        qty_days_worked,
        qty_working_days
        
    FROM {{ ref('stg_hr__absences') }}
    WHERE dateperiod IS NOT NULL
),

-- Employee lifecycle events
lifecycle_events AS (
    SELECT 
        employee_key,
        contract_start_date as event_date,
        'HIRE' as event_type
    FROM {{ ref('dim_hr__employee') }}
    
    UNION ALL
    
    SELECT 
        employee_key,
        contract_end_date as event_date,
        'EXIT' as event_type
    FROM {{ ref('dim_hr__employee') }}
    WHERE contract_end_date IS NOT NULL
),

monthly_events AS (
    SELECT 
        DATE_TRUNC('MONTH', event_date) as month_date,
        employee_key,
        event_type,
        ROW_NUMBER() OVER (PARTITION BY employee_key, DATE_TRUNC('MONTH', event_date), event_type ORDER BY event_date) as rn
    FROM lifecycle_events
    WHERE event_date >= '2016-06-01' AND event_date <= '2018-06-01'
),

fact_base AS (
    SELECT 
        mspine.date_key,
        mspine.employee_key,
        
        -- Salary metrics
        COALESCE(sdata.gross_salary, 0) AS gross_salary,
        COALESCE(sdata.net_salary, 0) AS net_salary,
        COALESCE(sdata.gross_salary_108, 0) AS gross_salary_108,
        sdata.salary_category,
        
        -- Absence metrics
        COALESCE(absencedata.total_work_accident_days, 0) AS total_work_accident_days,
        COALESCE(absencedata.total_accident_days, 0) AS total_accident_days,
        COALESCE(absencedata.total_illness_days, 0) AS total_illness_days,
        
        COALESCE(absencedata.total_work_accident_freq, 0) AS total_work_accident_freq,
        COALESCE(absencedata.total_accident_freq, 0) AS total_accident_freq,
        COALESCE(absencedata.total_illness_freq, 0) AS total_illness_freq,
        
        -- Detailed absence types
        COALESCE(absencedata.qty_a1_days, 0) AS qty_a1_days,
        COALESCE(absencedata.qty_a2_days, 0) AS qty_a2_days,
        COALESCE(absencedata.qty_p1_days, 0) AS qty_p1_days,
        COALESCE(absencedata.qty_p2_days, 0) AS qty_p2_days,
        COALESCE(absencedata.qty_p3_days, 0) AS qty_p3_days,
        COALESCE(absencedata.qty_z1_days, 0) AS qty_z1_days,
        COALESCE(absencedata.qty_z2_days, 0) AS qty_z2_days,
        COALESCE(absencedata.qty_z3_days, 0) AS qty_z3_days,
        
        -- Work metrics
        COALESCE(absencedata.qty_days_worked, 0) AS qty_days_worked,
        COALESCE(absencedata.qty_working_days, 0) AS qty_working_days,
        
        -- Employee lifecycle flags
        CASE WHEN me_hire.employee_key IS NOT NULL THEN 1 ELSE 0 END AS is_new_hire,
        CASE WHEN me_exit.employee_key IS NOT NULL THEN 1 ELSE 0 END AS is_exit,
        
        -- Headcount flag (1 for each active employee-month)
        1 AS headcount,
        
        -- Total absence days
        COALESCE(absencedata.total_work_accident_days, 0) + 
        COALESCE(absencedata.total_accident_days, 0) + 
        COALESCE(absencedata.total_illness_days, 0) AS total_absence_days,
        
        -- Absence rate calculation
        CASE 
            WHEN COALESCE(absencedata.qty_working_days, 0) > 0 THEN 
                (COALESCE(absencedata.total_work_accident_days, 0) + 
                 COALESCE(absencedata.total_accident_days, 0) + 
                 COALESCE(absencedata.total_illness_days, 0)) * 100.0 / absencedata.qty_working_days
            ELSE 0 
        END AS absence_rate_percentage
        
    FROM monthly_spine mspine
    LEFT JOIN salary_data sdata 
        ON mspine.employee_key = sdata.employee_key 
        AND mspine.date_key = sdata.period_date
    LEFT JOIN absence_data absencedata 
        ON mspine.employee_key = absencedata.employee_key 
        AND mspine.date_key = absencedata.period_date
    LEFT JOIN monthly_events me_hire 
        ON mspine.employee_key = me_hire.employee_key 
        AND mspine.date_key = me_hire.month_date 
        AND me_hire.event_type = 'HIRE'
        AND me_hire.rn = 1
    LEFT JOIN monthly_events me_exit 
        ON mspine.employee_key = me_exit.employee_key 
        AND mspine.date_key = me_exit.month_date 
        AND me_exit.event_type = 'EXIT'
        AND me_exit.rn = 1
)

SELECT 
    -- Composite key
    {{ dbt_utils.generate_surrogate_key(['date_key', 'employee_key']) }} AS fact_key,
    
    -- Foreign keys
    date_key,
    employee_key,
    
    -- Metrics
    gross_salary,
    net_salary,
    gross_salary_108,
    salary_category,
    
    -- Absence metrics
    total_work_accident_days,
    total_accident_days,
    total_illness_days,
    total_absence_days,
    absence_rate_percentage,
    
    total_work_accident_freq,
    total_accident_freq,
    total_illness_freq,
    
    -- Detailed absence types
    qty_a1_days, qty_a2_days,
    qty_p1_days, qty_p2_days, qty_p3_days,
    qty_z1_days, qty_z2_days, qty_z3_days,
    
    -- Work metrics
    qty_days_worked,
    qty_working_days,
    
    -- Employee lifecycle
    is_new_hire,
    is_exit,
    headcount,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at

FROM fact_base