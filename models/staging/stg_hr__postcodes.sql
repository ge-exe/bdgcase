{{ config(materialized='view') }}

SELECT
    postcode,
    region_code,
    CASE 
        WHEN region = 'Région Flamande' THEN 'Flamande'
        WHEN region = 'Région de Bruxelles-Capitale' THEN 'Bruxelles-Capitale'
        WHEN region = 'Région Wallonne' THEN 'Wallonne'
        ELSE region  -- Fallback for any unexpected values
    END AS region
FROM {{ source('raw', 'postcodes') }}