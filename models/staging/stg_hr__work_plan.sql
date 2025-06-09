{{ config(materialized='view') }}

SELECT
    fdcp,
    TRY_TO_DATE(valid_from, 'DD/MM/YYYY') AS valid_from,
    TRY_TO_DATE(valid_to, 'DD/MM/YYYY') AS valid_to,
    working_days_per_week,
    nace_code,
    TRIM(nace_description) AS nace_description
FROM {{ source('raw', 'work_plan') }}