{{ config(materialized='table') }}

WITH employee_base AS (
    SELECT
        fdcp,
        firm_id,
        department_id,
        category_id,
        person_id,
        contract_start_date,
        contract_end_date,
        company_start_date,
        birth_date,
        gender,
        nationality,
        contract_type,
        contract_zip_code,
        current_age,
        seniority_years,
        contract_termination_reason
    FROM {{ ref('stg_hr__contract_basis') }}
),

employee_with_region AS (
    SELECT
        e.*,
        p.region_code,
        p.region,
        -- Age groups for analysis
        CASE 
            WHEN current_age < 20 THEN '<20'
            WHEN current_age < 25 THEN '20-25'
            WHEN current_age < 30 THEN '25-30'
            WHEN current_age < 40 THEN '30-40'
            WHEN current_age < 50 THEN '40-50'
            ELSE '50+' 
        END AS age_group,
        
        -- Seniority groups
        CASE 
            WHEN seniority_years < 1 THEN '<1 year'
            WHEN seniority_years < 3 THEN '1-3 years'
            WHEN seniority_years < 5 THEN '3-5 years'
            WHEN seniority_years < 10 THEN '5-10 years'
            ELSE '10+ years' 
        END AS seniority_group,
        
        -- Contract status
        CASE 
            WHEN contract_end_date IS NULL THEN 'Permanent'
            WHEN contract_end_date >= CURRENT_DATE() THEN 'Active Fixed-term'
            ELSE 'Expired'
        END AS contract_status
        
    FROM employee_base e
    LEFT JOIN {{ ref('stg_hr__postcodes') }} p 
        ON e.contract_zip_code = p.postcode
)

SELECT * FROM employee_with_region