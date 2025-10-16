{{ config(materialized='view') }}

-- Directly reading GA4 events from the public dataset (no intermediate copy)

SELECT
  event_date,
  TIMESTAMP_MICROS(event_timestamp) AS event_ts,
  user_pseudo_id,
  event_name,
  platform,
  geo.country,
  traffic_source.source,
  traffic_source.medium,
  traffic_source.name AS campaign,

  -- Extract GA session ID
  (
    SELECT CAST(ep.value.int_value AS INT64)
    FROM UNNEST(event_params) ep
    WHERE ep.key = 'ga_session_id'
  ) AS ga_session_id,

  -- Extract event value (for purchases, etc.)
  (
    SELECT CAST(ep.value.double_value AS FLOAT64)
    FROM UNNEST(event_params) ep
    WHERE ep.key = 'value'
  ) AS event_value

FROM {{ source('ga4_public', 'events_*') }}
-- WHERE _TABLE_SUFFIX BETWEEN '20210101' AND '20210131'
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20210331'


