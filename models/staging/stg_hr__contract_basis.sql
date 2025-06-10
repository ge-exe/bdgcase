{{ config(materialized='view') }}


WITH source AS (
    SELECT * FROM {{ source('raw', 'contract_basis') }}
),

cleaned AS (
    SELECT
        -- Construct FDCP key
        firm_id::int || '|' ||
        LPAD(department_id::STRING, 2, '0') || '|' ||
        category_id::STRING || '|' ||
        person_id::int AS fdcp,
   
        firm_id::int as firm_id,
        department_id,
        category_id,
        person_id::int as person_id,

        contract_zip_code,
        
        -- Date conversions later we will format this in powerbi to the right format
        TRY_TO_DATE(contract_start_date, 'DD/MM/YYYY') AS contract_start_date,
        TRY_TO_DATE(contract_end_date, 'DD/MM/YYYY') AS contract_end_date,
        TRY_TO_DATE(company_start_date, 'DD/MM/YYYY') AS company_start_date,
        TRY_TO_DATE(birth_date, 'DD/MM/YYYY') AS birth_date,
        
        -- Clean categorical fields
        UPPER(TRIM(gender)) AS gender,
        UPPER(TRIM(nationality)) AS nationality,
        TRIM(contract_type) AS contract_type,
        TRIM(contract_termination_reason) AS contract_termination_reason,
        
        
        
        -- Calculate derived fields
        DATEDIFF('YEAR', TRY_TO_DATE(birth_date, 'DD/MM/YYYY'), CURRENT_DATE()) AS current_age,
        DATEDIFF('YEAR', TRY_TO_DATE(company_start_date, 'DD/MM/YYYY'), CURRENT_DATE()) AS seniority_years
        
    FROM source
)

SELECT * FROM cleaned