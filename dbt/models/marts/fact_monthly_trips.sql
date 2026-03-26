{{ config(materialized='table') }}

SELECT
    DATE_TRUNC(start_time, MONTH) as month_date,
    start_station_id as station_key,
    COUNT(*) as total_trips,
    ROUND(AVG(duration_seconds) / 60, 2) as avg_duration_mins,
    ROUND(SUM(duration_seconds) / 3600,0) as total_hours_used
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2