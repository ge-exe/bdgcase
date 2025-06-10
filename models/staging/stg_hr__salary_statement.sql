{{ config(materialized='view') }}

with
source as (
    select * from {{ source('raw', 'salary_statement') }}
),

-- First, identify and remove exact duplicates
deduplicated_source as (
    select 
        *,
        row_number() over (
            partition by 
                fdcp, 
                gross_salary, 
                net_salary, 
                gross_salary_108, 
                period 
            order by 
                -- Prefer records with more complete data
                case when gross_salary is not null and trim(gross_salary) != '' then 1 else 2 end,
                case when net_salary is not null and trim(net_salary) != '' then 1 else 2 end,
                case when gross_salary_108 is not null and trim(gross_salary_108) != '' then 1 else 2 end
        ) as row_rank
    from source
    where fdcp is not null
      and period is not null
),

-- Keep only the first occurrence of each duplicate group
unique_source as (
    select * 
    from deduplicated_source 
    where row_rank = 1
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
        try_to_date(period, 'DD/MM/YYYY') as period_date,
        
        
        -- Salary fields (handle European decimal format)
        try_to_decimal(replace(gross_salary, ',', '.'), 15, 2) as gross_salary_raw,
        try_to_decimal(replace(net_salary, ',', '.'), 15, 2) as net_salary_raw,
        try_to_decimal(replace(gross_salary_108, ',', '.'), 15, 2) as gross_salary_108_raw
        
    from unique_source
    where try_to_date(period, 'DD/MM/YYYY') is not null
),

final as (
    select
        -- Reconstruct clean FDCP without leading zeros (matching contract_basis format)
        firm_identifier::string || '|' ||
        lpad(department_identifier::string, 2, '0') || '|' ||
        category_identifier::string || '|' ||
        person_identifier::string as fdcp,
        
        -- Individual ID components
        firm_identifier as firm_id,
        department_identifier as department_id,
        category_identifier as category_id,
        person_identifier as person_id,
        
        -- Date fields
        period_date,
    
        
        -- Salary amounts
        gross_salary_raw as gross_salary,
        net_salary_raw as net_salary,
        gross_salary_108_raw as gross_salary_108,
        
        -- Calculated fields
        case
            when gross_salary_raw > 0 then
                (net_salary_raw / gross_salary_raw) * 100
            else null
        end as net_to_gross_percentage,
        
        case
            when gross_salary_raw > 0 then
                gross_salary_108_raw - gross_salary_raw
            else null
        end as gross_108_difference,
        
        case
            when gross_salary_raw > 0 then
                ((gross_salary_108_raw / gross_salary_raw) - 1) * 100
            else null
        end as gross_108_percentage_increase,
        
        -- Salary categorization
        case
            when gross_salary_raw >= 5000 then 'High'
            when gross_salary_raw >= 3000 then 'Medium-High'
            when gross_salary_raw >= 2000 then 'Medium'
            when gross_salary_raw >= 1000 then 'Low-Medium'
            when gross_salary_raw > 0 then 'Low'
            else 'No Salary'
        end as salary_category,
        
        -- Extract year and month for easier analysis
        year(period_date) as salary_year,
        month(period_date) as salary_month,
        quarter(period_date) as salary_quarter
        
    from cleaned
)

select * from final