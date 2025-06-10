{{ config(materialized='view') }}

SELECT
    month_date,
    year,
    month,
    quarter,
    
    -- Headcount metrics
    SUM(headcount) AS total_headcount,
    SUM(is_new_hire) AS new_hires,
    SUM(is_exit) AS exits,
    SUM(is_new_hire) - SUM(is_exit) AS net_movement,
    
    -- Salary costs
    SUM(CASE WHEN headcount = 1 THEN monthly_gross_salary ELSE 0 END) AS total_gross_salary_cost,
    SUM(CASE WHEN headcount = 1 THEN monthly_net_salary ELSE 0 END) AS total_net_salary_cost,
    
    -- By dimensions
    region,
    gender,
    age_group,
    seniority_group
    
FROM {{ ref('fact_hr2__contract_monthly') }}
GROUP BY 
    month_date, year, month, quarter,
    region, gender, age_group, seniority_group