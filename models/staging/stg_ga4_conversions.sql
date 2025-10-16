{{ config(materialized='view') }}

-- Purpose: Extract all purchase (conversion) events from GA4 data

SELECT
  user_pseudo_id,
  ga_session_id,
  event_ts AS conversion_ts,
  COALESCE(event_value, 0) AS conversion_value
FROM {{ ref('stg_ga4_events') }}
WHERE event_name = 'purchase'
