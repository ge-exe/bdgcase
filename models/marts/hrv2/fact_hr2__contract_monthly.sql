{{ config(materialized='table') }}

WITH date_spine AS (
    SELECT DISTINCT
        date_id,
        DATE_TRUNC('month', date_day) AS month_date,
        month_id,
        year,
        month,
        quarter
    FROM {{ ref('dim_hr2__date') }}
    WHERE date_day >= '2016-06-01'
      AND date_day <= CURRENT_DATE()
      AND date_day = month_start  -- Only get first day of each month
),

employee_months AS (
    SELECT
        -- Add the missing date columns here
        d.date_id,
        d.month_id,
        d.month_date,
        d.year,
        d.month,
        d.quarter,
        
        -- Employee information
        e.fdcp,
        e.firm_id,
        e.department_id,
        e.category_id,
        e.person_id,
       
        -- Contract is active if month_date is between start and end (or no end)
        CASE WHEN d.month_date >= DATE_TRUNC('month', e.contract_start_date)
             AND (e.contract_end_date IS NULL OR d.month_date <= DATE_TRUNC('month', e.contract_end_date))
             THEN 1 ELSE 0
        END AS is_active,
       
        -- New hire indicator (contract starts this month)
        CASE WHEN d.month_date = DATE_TRUNC('month', e.contract_start_date)
             THEN 1 ELSE 0
        END AS is_new_hire,
       
        -- Exit indicator (contract ends this month)
        CASE WHEN e.contract_end_date IS NOT NULL
             AND d.month_date = DATE_TRUNC('month', e.contract_end_date)
             THEN 1 ELSE 0
        END AS is_exit,
       
        e.contract_start_date,
        e.contract_end_date,
        e.gender,
        e.age_group,
        e.seniority_group,
        e.region
       
    FROM {{ ref('dim_hr2__employee') }} e
    CROSS JOIN date_spine d
),

salary_aggregation AS (
    SELECT
        fdcp,
        DATE_TRUNC('month', period_date) AS month_date,
        AVG(gross_salary) AS avg_gross_salary,
        AVG(net_salary) AS avg_net_salary,
        COUNT(*) AS salary_records
    FROM {{ ref('stg_hr__salary_statement') }}
    WHERE period_date IS NOT NULL
    GROUP BY fdcp, DATE_TRUNC('month', period_date)
),

final AS (
    SELECT
        em.date_id,
        em.month_id,
        em.month_date,
        em.year,
        em.month,
        em.quarter,
        em.fdcp,
        em.firm_id,
        em.department_id,
        em.category_id,
        em.person_id,
       
        -- Metrics
        em.is_active AS headcount,
        em.is_new_hire,
        em.is_exit,
       
        -- Salary information
        COALESCE(s.avg_gross_salary, 0) AS monthly_gross_salary,
        COALESCE(s.avg_net_salary, 0) AS monthly_net_salary,
       
        -- Dimensions for easy joining
        em.gender,
        em.age_group,
        em.seniority_group,
        em.region,
       
        -- Calculate age and seniority at this specific month
        DATEDIFF('year', em.contract_start_date, em.month_date) AS seniority_at_month,
       
        -- Additional flags
        CASE WHEN em.month_date = DATE_TRUNC('month', CURRENT_DATE()) THEN 1 ELSE 0 END AS is_current_month,
        CASE WHEN em.month_date = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE())) THEN 1 ELSE 0 END AS is_last_month
       
    FROM employee_months em
    LEFT JOIN salary_aggregation s
        ON em.fdcp = s.fdcp
        AND em.month_date = s.month_date
    WHERE em.month_date >= '2016-06-01'
)

SELECT * FROM final