{{ config(materialized='view') }}

SELECT
    postcode,
    region_code,
    region
FROM {{ source('raw', 'postcodes') }}