{{ config(materialized='view') }}

SELECT
    fdcp,
    TRY_TO_DATE(period, 'DD/MM/YYYY') AS period_date,
    DATE_TRUNC('MONTH', TRY_TO_DATE(period, 'DD/MM/YYYY')) AS period_month,
    gross_salary,
    TRY_TO_DECIMAL(REPLACE(net_salary, ',', '.'), 10, 2) AS net_salary,
    TRY_TO_DECIMAL(REPLACE(gross_salary_108, ',', '.'), 10, 2) AS gross_salary_108
FROM {{ source('raw', 'salary_statement') }}
WHERE TRY_TO_DATE(period, 'DD/MM/YYYY') IS NOT NULL