	{{ config(materialized='view') }}

-- Purpose: Identify the first event in each session for each user
-- This gives us the starting point ("touchpoint") for attribution analysis

WITH session_first AS (
  SELECT
    e.user_pseudo_id,
    e.ga_session_id,
    ARRAY_AGG(e ORDER BY e.event_ts ASC LIMIT 1)[OFFSET(0)] AS first_event
  FROM {{ ref('stg_ga4_events') }} e
  WHERE e.ga_session_id IS NOT NULL
  GROUP BY 1, 2
)

SELECT
  first_event.user_pseudo_id,
  first_event.ga_session_id,
  first_event.event_ts AS touchpoint_ts,
  COALESCE(NULLIF(first_event.source, ''), '(direct)') AS source,
  COALESCE(NULLIF(first_event.medium, ''), '(none)') AS medium,
  COALESCE(NULLIF(first_event.campaign, ''), '(not set)') AS campaign
FROM session_first
