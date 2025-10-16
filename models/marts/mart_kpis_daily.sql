{{ config(materialized='table') }}

-- Purpose:
-- Combine First-Click and Last-Click attribution marts
-- to produce daily KPIs (conversions & revenue per channel/model)

WITH first_click AS (
  SELECT
    DATE(conversion_ts) AS dt,
    COALESCE(source,'(direct)') AS source,
    COALESCE(medium,'(none)')  AS medium,
    COALESCE(campaign,'(not set)') AS campaign,
    'First Click' AS attribution_model,
    COUNT(*) AS conversions,
    SUM(COALESCE(conversion_value,0)) AS revenue
  FROM {{ ref('mart_attribution_first_click') }}
  GROUP BY 1,2,3,4,5
),

last_click AS (
  SELECT
    DATE(conversion_ts) AS dt,
    COALESCE(source,'(direct)') AS source,
    COALESCE(medium,'(none)')  AS medium,
    COALESCE(campaign,'(not set)') AS campaign,
    'Last Click' AS attribution_model,
    COUNT(*) AS conversions,
    SUM(COALESCE(conversion_value,0)) AS revenue
  FROM {{ ref('mart_attribution_last_click') }}
  GROUP BY 1,2,3,4,5
)

SELECT * FROM first_click
UNION ALL
SELECT * FROM last_click
