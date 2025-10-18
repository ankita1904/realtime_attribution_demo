# 🧭 Real-Time Attribution Dashboard (BigQuery + dbt + Looker Studio)

## 📌 Project Overview

This project demonstrates a **real-time marketing attribution pipeline** using the **Google Analytics 4 (GA4) public dataset**, **dbt**, **BigQuery**, and **Looker Studio**.  
It implements **First-Click** and **Last-Click Attribution** models with a **30-day lookback window**, a **streaming demo**, and a **real-time dashboard**.

---

## 🧩 Architecture

**Data Source**

- Google Analytics 4 public dataset  
  `bigquery-public-data.ga4_obfuscated_sample_ecommerce`

- BigQuery Project: `realtime-attribution-demo`
- Dataset: `demo_attribution`

**Transformation**

- dbt(Data Build Tool) models
  - Staging: `stg_ga4_events`, `stg_ga4_sessions`, `stg_ga4_touchpoints`, `stg_ga4_conversions`
  - Intermediate: `int_ga4_conversions_with_window`, `int_ga4_touchpoints_with_rules`
  - Marts: `mart_attribution_first_click`, `mart_attribution_last_click`, `mart_kpis_daily`

**Streaming Layer**

- Python script `stream_demo.py`
- Streaming Demo:
This project includes a lightweight Python script (stream_demo.py) that generates 10–20 synthetic GA4-style events and loads them into the BigQuery table demo_attribution.stream_events.
Because of BigQuery Sandbox disables streaming inserts, the demo uses the free Load Job API (load_table_from_json) to batch-load events instead of the Streaming API.
Each record has a unique event_id used as the insertId and as the unique_key in dbt, ensuring deduplication and idempotent materialization when the job reruns.
Events appear in BigQuery within 10–20 seconds.

**Visualization**

- Looker Studio (Free Google tool)
  - KPI cards for conversions and revenue
  - Time-series comparison (First vs Last Click)
  - source breakdown
  - Filters and optional live streaming panel

---

## 🧱 Architecture Diagram (Text Layout)

+-------------------------------------------+
| Google Analytics 4 Public Dataset |
| (bigquery-public-data.ga4*obfuscated_sample_ecommerce) |
+-------------------------------------------+
|
v
+-------------------------------------------+
| BigQuery |
| Dataset: demo_attribution |
| - stg_ga4*_ (staging) |
| - int*ga4*_ (intermediate) |
| - mart\_\* (marts) |
+-------------------------------------------+
^ |
| v
dbt transformations Python stream_demo.py
(SQL + models)
|
v
+-------------------------------------------+
| Looker Studio Dashboard |
| (KPI, Trends, source) |
+-------------------------------------------+
