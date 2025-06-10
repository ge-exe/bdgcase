{{ config(materialized='table') }}

WITH employee_base AS (
    SELECT 
        contractbasis.fdcp,
        contractbasis.firm_id,
        contractbasis.department_id,
        contractbasis.category_id,
        contractbasis.person_id,
        
        -- Employee demographics
        contractbasis.gender,
        contractbasis.nationality,
        contractbasis.birth_date,
        contractbasis.current_age,
        
        -- Contract information
        contractbasis.contract_start_date,
        contractbasis.contract_end_date,
        contractbasis.company_start_date,
        contractbasis.seniority_years,
        contractbasis.contract_type,
        contractbasis.contract_termination_reason,
        
        -- Location
        contractbasis.contract_zip_code,
        stgpostcodes.region_code,
        stgpostcodes.region,
        
        -- Work plan (get most recent active plan)
        stgworkplan.working_days_per_week,
        stgworkplan.nace_code,
        stgworkplan.nace_description,
        stgworkplan.work_intensity_category,
        
        -- Derived age groups for analysis
        CASE 
            WHEN contractbasis.current_age < 20 THEN '<20'
            WHEN contractbasis.current_age < 25 THEN '20-25'
            WHEN contractbasis.current_age < 30 THEN '25-30'
            WHEN contractbasis.current_age < 40 THEN '30-40'
            WHEN contractbasis.current_age < 50 THEN '40-50'
            ELSE '50+'
        END AS age_group,
        
        -- Seniority groups
        CASE 
            WHEN contractbasis.seniority_years < 1 THEN '<1 year'
            WHEN contractbasis.seniority_years < 2 THEN '1-2 years'
            WHEN contractbasis.seniority_years < 5 THEN '2-5 years'
            WHEN contractbasis.seniority_years < 10 THEN '5-10 years'
            ELSE '10+ years'
        END AS seniority_group,
        
        -- Contract status
        CASE 
            WHEN contractbasis.contract_end_date IS NULL THEN 'Permanent'
            WHEN contractbasis.contract_end_date > CURRENT_DATE() THEN 'Active Fixed-Term'
            ELSE 'Expired'
        END AS contract_status
        
    FROM {{ ref('stg_hr__contract_basis') }} contractbasis
    LEFT JOIN {{ ref('stg_hr__postcodes') }} stgpostcodes 
        ON contractbasis.contract_zip_code = stgpostcodes.postcode
    LEFT JOIN (
        -- Get most recent work plan for each employee
        SELECT 
            fdcp_identifier,
            working_days_per_week,
            nace_code,
            nace_description,
            work_intensity_category,
            ROW_NUMBER() OVER (PARTITION BY fdcp_identifier ORDER BY valid_from_date DESC) as rn
        FROM {{ ref('stg_hr__work_plan') }}
        WHERE work_plan_status = 'Active'
    ) stgworkplan ON contractbasis.fdcp = stgworkplan.fdcp_identifier AND stgworkplan.rn = 1
)

SELECT 
    -- Primary key
    fdcp AS employee_key,
    
    -- Identifiers
    firm_id,
    department_id,
    category_id,
    person_id,
    
    -- Demographics
    gender,
    nationality,
    birth_date,
    current_age,
    age_group,
    
    -- Contract details
    contract_start_date,
    contract_end_date,
    company_start_date,
    seniority_years,
    seniority_group,
    contract_type,
    contract_termination_reason,
    contract_status,
    
    -- Location
    contract_zip_code,
    region_code,
    region,
    
    -- Work details
    working_days_per_week,
    nace_code,
    nace_description,
    work_intensity_category,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at,
    CURRENT_TIMESTAMP() AS updated_at

FROM employee_base