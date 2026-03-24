{{ config(materialized='view') }}
SELECT 
    start_time,
    start_station_id,
    start_station_name,
    duration_seconds,
    -- Transformation: Convert seconds to minutes for easier analysis
    ROUND(duration_seconds / 60, 2) as duration_minutes
FROM `santander-bikes-pipeline.london_bikes_dw.trips_partitioned`
WHERE start_time IS NOT NULL 
  AND start_station_id IS NOT NULL