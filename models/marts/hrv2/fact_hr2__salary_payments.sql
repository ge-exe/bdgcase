{{ config(materialized='table') }}

SELECT
    s.fdcp,
    s.firm_id,
    s.department_id,
    s.category_id,
    s.person_id,
    s.period_date,
    s.salary_year,
    s.salary_month,
    s.salary_quarter,
    
    -- Salary metrics
    s.gross_salary,
    s.net_salary,
    s.gross_salary_108,
    s.net_to_gross_percentage,
    s.gross_108_difference,
    s.gross_108_percentage_increase,
    s.salary_category,
    
    -- Employee dimensions
    e.gender,
    e.age_group,
    e.seniority_group,
    e.region,
    e.contract_type,
    
    -- Date calculations
    DATE_TRUNC('month', s.period_date) AS salary_month_date,
    CONCAT(s.salary_year, '-', LPAD(s.salary_month, 2, '0')) AS year_month
    
FROM {{ ref('stg_hr__salary_statement') }} s
LEFT JOIN {{ ref('dim_hr2__employee') }} e ON s.fdcp = e.fdcp