{{ config(materialized='view') }}

-- Purpose: Create session-level data by grouping events for each user/session pair

WITH sessions AS (
  SELECT
    user_pseudo_id,
    ga_session_id,
    MIN(event_ts) AS session_start_ts,
    MAX(event_ts) AS session_end_ts,
    COUNT(*) AS total_events,
    COUNTIF(event_name = 'page_view') AS pageviews,
    COUNTIF(event_name = 'purchase') AS purchases
  FROM {{ ref('stg_ga4_events') }}
  WHERE ga_session_id IS NOT NULL
  GROUP BY 1, 2
)

SELECT * FROM sessions
