{{ config(materialized='table') }}

-- Purpose:
-- Assign each conversion to the first eligible touchpoint (First Click Attribution)

WITH ranked AS (
  SELECT
    user_pseudo_id,
    conversion_ts,
    conversion_value,
    source,
    medium,
    campaign,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, conversion_ts
      ORDER BY touchpoint_ts ASC, DATE(touchpoint_ts) ASC
    ) AS rn
  FROM {{ ref('int_ga4_conversions_with_window') }}
)

SELECT
  user_pseudo_id,
  conversion_ts,
  conversion_value,
  source,
  medium,
  campaign,
  'First Click' AS attribution_model
FROM ranked
WHERE rn = 1
