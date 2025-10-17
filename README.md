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
- Streams real-time events into BigQuery table `stream_events`

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
