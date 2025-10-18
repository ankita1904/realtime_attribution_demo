{{ config(materialized='table') }}

-- Purpose:
-- Assign each conversion to the most recent (last) eligible touchpoint.
-- Preference: pick the latest non-direct channel when available.

WITH base AS (
  SELECT *
  FROM {{ ref('int_ga4_conversions_with_window') }}
),

ranked AS (
  SELECT
    user_pseudo_id,
    conversion_ts,
    conversion_value,
    source,
    medium,
    campaign,
    ROW_NUMBER() OVER (
      PARTITION BY user_pseudo_id, conversion_ts
      ORDER BY (LOWER(source) = '(direct)') ASC,  -- prefer non-direct first
               touchpoint_ts DESC
    ) AS rn
  FROM base
)

SELECT
  user_pseudo_id,
  conversion_ts,
  conversion_value,
  source,
  medium,
  campaign,
  'Last Click' AS attribution_model
FROM ranked
WHERE rn = 1

