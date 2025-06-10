{{ config(materialized='table') }}

-- Comprehensive summary mart for PowerBI dashboards
-- Pre-aggregated metrics for better performance in PowerBI

WITH monthly_summary AS (
    SELECT 
        -- Date dimensions
        dimdate.date_key,
        dimdate.year,
        dimdate.month_number,
        dimdate.month_name,
        dimdate.year_month,
        dimdate.quarter,
        dimdate.previous_month,
        dimdate.same_month_previous_year,
        
        -- Employee dimensions
        dimemployee.region,
        dimemployee.age_group,
        dimemployee.gender,
        dimemployee.seniority_group,
        dimemployee.contract_type,
        dimemployee.nace_code,
        dimemployee.nace_description,
        dimemployee.work_intensity_category,
        
        -- Core metrics
        COUNT(*) as total_headcount,
        SUM(factemetrics.is_new_hire) as new_hires,
        SUM(factemetrics.is_exit) as exits,
        
        -- Financial metrics
        SUM(factemetrics.gross_salary) as total_gross_salary,
        SUM(factemetrics.net_salary) as total_net_salary,
        AVG(factemetrics.gross_salary) as avg_gross_salary,
        MEDIAN(factemetrics.gross_salary) as median_gross_salary,
        
        -- Absence metrics
        SUM(factemetrics.total_absence_days) as total_absence_days,
        AVG(factemetrics.absence_rate_percentage) as avg_absence_rate,
        SUM(factemetrics.total_work_accident_days) as total_work_accident_days,
        SUM(factemetrics.total_accident_days) as total_accident_days,
        SUM(factemetrics.total_illness_days) as total_illness_days,
        
        -- Work metrics
        SUM(factemetrics.qty_days_worked) as total_days_worked,
        SUM(factemetrics.qty_working_days) as total_working_days,
        
        -- Counts by salary category
        COUNT(CASE WHEN factemetrics.salary_category = 'High' THEN 1 END) as high_salary_count,
        COUNT(CASE WHEN factemetrics.salary_category = 'Medium-High' THEN 1 END) as medium_high_salary_count,
        COUNT(CASE WHEN factemetrics.salary_category = 'Medium' THEN 1 END) as medium_salary_count,
        COUNT(CASE WHEN factemetrics.salary_category = 'Low-Medium' THEN 1 END) as low_medium_salary_count,
        COUNT(CASE WHEN factemetrics.salary_category = 'Low' THEN 1 END) as low_salary_count,
        
        -- Absence severity counts
        COUNT(CASE WHEN factemetrics.total_absence_days = 0 THEN 1 END) as no_absence_count,
        COUNT(CASE WHEN factemetrics.total_absence_days BETWEEN 1 AND 2 THEN 1 END) as low_absence_count,
        COUNT(CASE WHEN factemetrics.total_absence_days BETWEEN 3 AND 5 THEN 1 END) as medium_absence_count,
        COUNT(CASE WHEN factemetrics.total_absence_days > 5 THEN 1 END) as high_absence_count
        
    FROM {{ ref('fct_hr__monthly_employee_metrics') }} factemetrics
    JOIN {{ ref('dim_hr__employee') }} dimemployee ON factemetrics.employee_key = dimemployee.employee_key
    JOIN {{ ref('dim_hr__date') }} dimdate ON factemetrics.date_key = dimdate.date_key
    GROUP BY 
        dimdate.date_key,
        dimdate.year,
        dimdate.month_number,
        dimdate.month_name,
        dimdate.year_month,
        dimdate.quarter,
        dimdate.previous_month,
        dimdate.same_month_previous_year,
        dimemployee.region,
        dimemployee.age_group,
        dimemployee.gender,
        dimemployee.seniority_group,
        dimemployee.contract_type,
        dimemployee.nace_code,
        dimemployee.nace_description,
        dimemployee.work_intensity_category
),

previous_year_comparison AS (
    SELECT 
        -- Date and dimensional attributes
        current_summary.date_key,
        current_summary.year,
        current_summary.month_number,
        current_summary.month_name,
        current_summary.year_month,
        current_summary.quarter,
        current_summary.region,
        current_summary.age_group,
        current_summary.gender,
        current_summary.seniority_group,
        current_summary.contract_type,
        current_summary.nace_code,
        current_summary.work_intensity_category,
        
        -- Current period metrics
        current_summary.total_headcount,
        current_summary.new_hires,
        current_summary.exits,
        current_summary.total_gross_salary,
        current_summary.avg_gross_salary,
        current_summary.total_absence_days,
        current_summary.avg_absence_rate,
        
        -- Previous year metrics
        previous_summary.total_headcount as py_total_headcount,
        previous_summary.new_hires as py_new_hires,
        previous_summary.exits as py_exits,
        previous_summary.total_gross_salary as py_total_gross_salary,
        previous_summary.avg_gross_salary as py_avg_gross_salary,
        previous_summary.total_absence_days as py_total_absence_days,
        previous_summary.avg_absence_rate as py_avg_absence_rate,
        
        -- Year-over-year calculations
        current_summary.total_headcount - COALESCE(previous_summary.total_headcount, 0) as yoy_headcount_change,
        CASE 
            WHEN COALESCE(previous_summary.total_headcount, 0) > 0 THEN
                ((current_summary.total_headcount - previous_summary.total_headcount) * 100.0 / previous_summary.total_headcount)
            ELSE NULL
        END as yoy_headcount_change_pct,
        
        CASE 
            WHEN COALESCE(previous_summary.avg_gross_salary, 0) > 0 THEN
                ((current_summary.avg_gross_salary - previous_summary.avg_gross_salary) * 100.0 / previous_summary.avg_gross_salary)
            ELSE NULL
        END as yoy_salary_change_pct,
        
        CASE 
            WHEN COALESCE(previous_summary.avg_absence_rate, 0) > 0 THEN
                current_summary.avg_absence_rate - previous_summary.avg_absence_rate
            ELSE NULL
        END as yoy_absence_rate_change
        
    FROM monthly_summary current_summary
    LEFT JOIN monthly_summary previous_summary
        ON current_summary.same_month_previous_year = previous_summary.date_key
        AND current_summary.region = previous_summary.region
        AND current_summary.age_group = previous_summary.age_group
        AND current_summary.gender = previous_summary.gender
        AND current_summary.seniority_group = previous_summary.seniority_group
        AND current_summary.contract_type = previous_summary.contract_type
        AND current_summary.nace_code = previous_summary.nace_code
        AND current_summary.work_intensity_category = previous_summary.work_intensity_category
)

SELECT 
    -- Primary dimensions
    date_key,
    year,
    month_number,
    month_name,
    year_month,
    quarter,
    region,
    age_group,
    gender,
    seniority_group,
    contract_type,
    nace_code,
    work_intensity_category,
    
    -- Current period metrics
    total_headcount,
    new_hires,
    exits,
    total_gross_salary,
    avg_gross_salary,
    total_absence_days,
    avg_absence_rate,
    
    -- Previous year comparison
    py_total_headcount,
    py_avg_gross_salary,
    py_avg_absence_rate,
    yoy_headcount_change,
    yoy_headcount_change_pct,
    yoy_salary_change_pct,
    yoy_absence_rate_change,
    
    -- Net change (hires - exits)
    new_hires - exits as net_change,
    
    -- Turnover rate (exits / average headcount * 100)
    CASE 
        WHEN total_headcount > 0 THEN
            (exits * 100.0 / total_headcount)
        ELSE 0
    END as monthly_turnover_rate,
    
    -- Hire rate (new hires / average headcount * 100)
    CASE 
        WHEN total_headcount > 0 THEN
            (new_hires * 100.0 / total_headcount)
        ELSE 0
    END as monthly_hire_rate,
    
    -- Absence intensity
    CASE 
        WHEN total_headcount > 0 THEN
            total_absence_days / total_headcount
        ELSE 0
    END as absence_days_per_employee,
    
    -- Metadata
    CURRENT_TIMESTAMP() as created_at

FROM previous_year_comparison