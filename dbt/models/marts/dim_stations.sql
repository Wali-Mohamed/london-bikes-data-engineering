{{ config(materialized='table') }}

SELECT 
    start_station_id AS station_key,
    start_station_name,
    -- Extract the area after the comma
    CASE 
        WHEN CONTAINS_SUBSTR(start_station_name, ',') 
        THEN TRIM(SPLIT(start_station_name, ',')[SAFE_OFFSET(1)])
        ELSE 'Unknown'
    END AS station_area,
    COUNT(*) AS lifetime_trips_to_date 
FROM {{ ref('stg_trips') }}
GROUP BY 1, 2, 3