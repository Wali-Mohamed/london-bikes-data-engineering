-- models/marts/rpt_monthly_station_performance.sql

{{ config(materialized='table') }}

WITH monthly_trips AS (
    SELECT * FROM {{ ref('fact_monthly_trips') }}
),

stations AS (
    SELECT * FROM {{ ref('dim_stations') }}
)

SELECT
    t.month_date,
    t.station_key,
    s.start_station_name,
    s.station_area,
    t.total_trips,
    t.avg_duration_mins,
    -- This helps you see if a station is overperforming its usual lifetime average
    s.lifetime_trips_to_date
FROM monthly_trips t
LEFT JOIN stations s 
    ON t.station_key = s.station_key
