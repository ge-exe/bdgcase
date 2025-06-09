{{ config(materialized='view') }}

SELECT
    -- Construct FDCP
    LPAD(firm_id::STRING, 10, '0') || '|' ||
    LPAD(department_id::STRING, 2, '0') || '|' ||
    category_id::STRING || '|' ||
    LPAD(person_id::STRING, 10, '0') AS fdcp,
   
    firm_id,
    department_id,
    category_id,
    person_id,
    year,
    quarter,
    month,
    
    -- Period is already a DATE type, no conversion needed
    period AS period_date,
    DATE_TRUNC('MONTH', period) AS period_month,
   
    -- Absence quantities (only relevant types)
    qty_a1_days,
    qty_a2_days,
    qty_p1_days,
    qty_p2_days,
    qty_p3_days,
    qty_z1_days,
    qty_z2_days,
    qty_z3_days,
   
    -- Absence frequencies (only relevant types)
    freq_a1_days,
    freq_a2_days,
    freq_p1_days,
    freq_p2_days,
    freq_p3_days,
    freq_z1_days,
    freq_z2_days,
    freq_z3_daqs, -- Note: this has a typo in the source table
   
    -- Convert string fields to proper numeric types
    TRY_TO_NUMBER(qty_days_worked) AS qty_days_worked,
    TRY_TO_NUMBER(qty_working_days) AS qty_working_days
   
FROM {{ source('raw', 'absences') }}
WHERE period IS NOT NULL