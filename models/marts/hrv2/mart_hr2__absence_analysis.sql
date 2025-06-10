{{ config(materialized='view') }}

SELECT
    absence_month,
    year,
    quarter,
    month,
    
    -- Absence metrics
    absence_type,
    COUNT(DISTINCT fdcp) AS employees_with_absences,
    SUM(absence_days) AS total_absence_days,
    SUM(absence_frequency) AS total_absence_incidents,
    AVG(absence_rate_percent) AS avg_absence_rate,
    
    -- By dimensions
    region,
    gender,
    age_group,
    seniority_group,
    nace_code,
    nace_description
    
FROM {{ ref('fact_hr2__absences') }}
GROUP BY 
    absence_month, year, quarter, month, absence_type,
    region, gender, age_group, seniority_group, nace_code, nace_description