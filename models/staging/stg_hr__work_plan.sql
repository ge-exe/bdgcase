{{ config(materialized='view') }}

with

source as (

    select * from {{ source('raw', 'work_plan') }}

),

cleaned as (

    select
        -- Original FDCP field
        fdcp as fdcp_original,
        
        -- Destructure FDCP into individual components and remove leading zeros
        split_part(fdcp, '|', 1)::int as firm_identifier,
        split_part(fdcp, '|', 2)::string as department_identifier,
        split_part(fdcp, '|', 3)::string as category_identifier,
        split_part(fdcp, '|', 4)::int as person_identifier,
        
        -- Date conversions
        try_to_date(valid_from, 'DD/MM/YYYY') as valid_from_date,
        try_to_date(valid_to, 'DD/MM/YYYY') as valid_to_date,
        
        -- Work plan details (handle European decimal format)
        try_to_decimal(replace(working_days_per_week, ',', '.'), 10, 2) as working_days_per_week,
        nace_code,
        trim(nace_description) as nace_description
        
        
    from source
    where fdcp is not null

),

final as (

    select
        -- Reconstruct clean FDCP without leading zeros (matching contract_basis format)
        firm_identifier::string || '|' ||
        lpad(department_identifier::string, 2, '0') || '|' ||
        category_identifier::string || '|' ||
        person_identifier::string as fdcp_identifier,
        
        -- Individual ID components
        firm_identifier as firm_id,
        department_identifier as department_id,
        category_identifier as category_id,
        person_identifier as person_id,
        
        -- Date fields
        valid_from_date,
        valid_to_date,
        
        -- Work plan details
        working_days_per_week,
        nace_code,
        SUBSTRING(nace_description, 8, 150) as nace_description,
        
        -- Calculated fields
        datediff('day', valid_from_date, valid_to_date) as validity_period_days,
        
        case
            when valid_to_date >= current_date() then 'Active'
            else 'Expired'
        end as work_plan_status,
        
        -- Work intensity classification
        case
            when working_days_per_week >= 5 then 'Full Time'
            when working_days_per_week >= 3 then 'Part Time'
            when working_days_per_week > 0 then 'Minimal'
            else 'Not Specified'
        end as work_intensity_category
        
    from cleaned

)

select * from final