{{ config(materialized='view') }}

SELECT * FROM {{ source('raw', 'contract_basis') }}