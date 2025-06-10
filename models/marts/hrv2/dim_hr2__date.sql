{{ config(materialized='table') }}

WITH date_spine AS (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2016-06-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    -- Surrogate key: YYYYMMDD format as integer
    CAST(REPLACE(CAST(date_day AS STRING), '-', '') AS INTEGER) AS date_id,
    
    -- Natural key
    date_day,
    
    -- Date parts
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(DAY FROM date_day) AS day_of_month,
    DAYOFWEEK(date_day) AS day_of_week,
    DAYNAME(date_day) AS day_name,
    MONTHNAME(date_day) AS month_name,
    
    -- Month and year groupings
    DATE_TRUNC('month', date_day) AS month_start,
    LAST_DAY(date_day) AS month_end,
    CAST(REPLACE(CAST(DATE_TRUNC('month', date_day) AS STRING), '-', '') AS INTEGER) AS month_id,
    
    -- Seasons
    CASE WHEN EXTRACT(MONTH FROM date_day) IN (12,1,2) THEN 'Winter'
         WHEN EXTRACT(MONTH FROM date_day) IN (3,4,5) THEN 'Spring'
         WHEN EXTRACT(MONTH FROM date_day) IN (6,7,8) THEN 'Summer'
         ELSE 'Autumn' END AS season,
    
    -- Year groupings
    DATE_TRUNC('year', date_day) AS year_start,
    CONCAT(EXTRACT(YEAR FROM date_day), '-', LPAD(EXTRACT(MONTH FROM date_day), 2, '0')) AS year_month,
    
    -- Useful flags
    CASE WHEN DAYOFWEEK(date_day) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN date_day = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,
    CASE WHEN DATE_TRUNC('month', date_day) = DATE_TRUNC('month', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_month
    
FROM date_spine