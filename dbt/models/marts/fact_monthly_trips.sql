{{ config(materialized='table') }}

SELECT 
    DATE_TRUNC(start_time, MONTH) as trip_month,
    start_station_id,  -- Use the ID to join to your new Dim table
    COUNT(*) as total_trips,
    ROUND(AVG(duration_minutes), 2) as avg_duration
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2