{{ config(materialized='table') }}

-- Purpose:
-- Apply marketing channel classification rules to each touchpoint
-- This model standardizes the traffic_source fields into human-readable channel groups

WITH tp AS (
  SELECT *,
    CASE
      WHEN LOWER(medium) IN ('cpc','ppc','paidsearch') THEN 'Paid Search'
      WHEN LOWER(medium) IN ('display','cpm') THEN 'Display'
      WHEN LOWER(source) LIKE '%facebook%' OR LOWER(source) LIKE '%instagram%' THEN 'Paid Social'
      WHEN LOWER(medium) IN ('email') THEN 'Email'
      WHEN LOWER(medium) IN ('affiliate') THEN 'Affiliates'
      WHEN LOWER(medium) IN ('referral') THEN 'Referral'
      WHEN LOWER(medium) IN ('organic') THEN 'Organic Search'
      WHEN LOWER(source) = '(direct)' OR LOWER(medium) IN ('(none)','none') THEN 'Direct'
      ELSE 'Other'
    END AS channel
  FROM {{ ref('stg_ga4_touchpoints') }}
)

SELECT * FROM tp
