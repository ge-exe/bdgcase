{{ config(materialized='table') }}

SELECT
    region_code,
    region,
    postcode
FROM {{ ref('stg_hr__postcodes') }}