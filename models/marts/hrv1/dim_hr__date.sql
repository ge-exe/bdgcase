{{ config(materialized='table') }}

-- Generate date dimension for 36 months (24 months back from June 2018 + buffer)
WITH date_spine AS (
    SELECT 
        DATEADD(MONTH, SEQ4(), '2016-06-01'::DATE) AS month_date
    FROM TABLE(GENERATOR(ROWCOUNT => 120))
),

date_details AS (
    SELECT 
        month_date,
        YEAR(month_date) AS year,
        MONTH(month_date) AS month_number,
        MONTHNAME(month_date) AS month_name,
        QUARTER(month_date) AS quarter,
        DAYOFYEAR(month_date) AS day_of_year,
        WEEKOFYEAR(month_date) AS week_of_year,
        
        -- Formatted dates for display
        TO_CHAR(month_date, 'YYYY-MM') AS year_month,
        TO_CHAR(month_date, 'YYYY-QQ') AS year_quarter,
        TO_CHAR(month_date, 'MMMM YYYY') AS month_year_name,
        
        -- Previous periods for comparisons
        DATEADD(MONTH, -1, month_date) AS previous_month,
        DATEADD(MONTH, -12, month_date) AS same_month_previous_year,
        
        -- Month boundaries
        DATE_TRUNC('MONTH', month_date) AS month_start_date,
        LAST_DAY(month_date) AS month_end_date,
        
        -- Fiscal periods (assuming calendar year = fiscal year)
        CASE 
            WHEN MONTH(month_date) <= 6 THEN 'H1'
            ELSE 'H2'
        END AS fiscal_half,
        
        -- Relative periods from June 2018 (our reference point)
        DATEDIFF(MONTH, month_date, '2018-06-01') AS months_from_reference,
        
        -- Period classifications
        CASE 
            WHEN month_date <= '2018-06-01' THEN 'Historical'
            WHEN month_date = '2018-06-01' THEN 'Current'
            ELSE 'Future'
        END AS period_type
        
    FROM date_spine
)

SELECT 
    -- Primary key
    month_date AS date_key,
    
    -- Date components
    year,
    month_number,
    month_name,
    quarter,
    day_of_year,
    week_of_year,
    
    -- Display formats
    year_month,
    year_quarter,
    month_year_name,
    
    -- Navigation dates
    previous_month,
    same_month_previous_year,
    
    -- Month boundaries
    month_start_date,
    month_end_date,
    
    -- Fiscal periods
    fiscal_half,
    
    -- Analysis helpers
    months_from_reference,
    period_type,
    
    -- Flags for easier filtering
    CASE WHEN year = YEAR(CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_year,
    CASE WHEN month_date = DATE_TRUNC('MONTH', CURRENT_DATE()) THEN TRUE ELSE FALSE END AS is_current_month,
    
    -- Metadata
    CURRENT_TIMESTAMP() AS created_at

FROM date_details
ORDER BY month_date