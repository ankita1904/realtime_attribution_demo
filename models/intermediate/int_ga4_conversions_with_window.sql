{{ config(materialized='table') }}

-- Purpose: Join conversions with eligible touchpoints within 30-day lookback window

WITH conversions AS (
  SELECT *
  FROM {{ ref('stg_ga4_conversions') }}
),

touchpoints AS (
  SELECT *
  FROM {{ ref('stg_ga4_touchpoints') }}
),

eligible AS (
  SELECT
    c.user_pseudo_id,
    c.ga_session_id AS conversion_session_id,
    c.conversion_ts,
    c.conversion_value,
    tp.ga_session_id AS touch_session_id,
    tp.touchpoint_ts,
    tp.source,
    tp.medium,
    tp.campaign
  FROM conversions c
  JOIN touchpoints tp
    ON c.user_pseudo_id = tp.user_pseudo_id
   AND tp.touchpoint_ts BETWEEN TIMESTAMP_SUB(c.conversion_ts, INTERVAL 30 DAY)
                           AND c.conversion_ts
)

SELECT * FROM eligible
