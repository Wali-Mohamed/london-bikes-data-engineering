{{ config(materialized='table') }}

SELECT 
    DISTINCT(start_station_id) as station_key,
    start_station_name,
    -- In a real project, you'd join this to a CSV with coordinates/boroughs
    COUNT(*) as lifetime_trips_originated 
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2