# 🏨 Hotel Analytics Platform

**End-to-End Analytics Engineering Project (Postgres → Snowflake → dbt → Power BI → Airflow)**

## Overview

This project is an end-to-end analytics platform designed to model how production-grade analytics systems are built in real companies.

It simulates a hotel management domain and focuses on:

* Reliable data ingestion
* Analytics-ready dimensional modeling
* Metric-driven reporting
* Orchestrated, repeatable pipelines

The goal is not just dashboards, but **trustworthy data foundations** that analytics teams can confidently build on.

---

## Architecture

**OLTP → OLAP → Analytics → BI**

```
PostgreSQL (OLTP)
   ↓
Snowflake (RAW → STAGING → DATAMART)
   ↓
dbt (models, tests, transformations)
   ↓
Power BI (business-facing dashboards)
   ↓
Airflow + Docker (orchestration & scheduling)
```

---

## Key Features

* **Synthetic OLTP Data Generation**

  * Realistic hotel data (bookings, rooms, hotels, pricing, dates)
  * Supports multiple booking statuses (confirmed, cancelled, no-show)
  * Designed to expose real analytics edge cases

* **Incremental Data Ingestion**

  * PostgreSQL → Snowflake pipeline
  * Handles growing datasets without full reloads

* **Analytics-Ready Data Modeling (dbt)**

  * Clear separation of layers:

    * `RAW`: landed source data
    * `STAGING`: cleaned, typed, standardized models
    * `DATAMART`: fact & dimension tables for analytics
  * Star-schema style modeling aligned with analytics engineering best practices

* **Business-Focused Metrics**

  * Occupancy rate
  * Booking trends
  * Revenue metrics
  * Cancellation & no-show analysis
  * Daily and monthly KPI views

* **BI Layer**

  * Power BI dashboards built directly on curated datamart views
  * Designed for stakeholder-ready consumption

* **Orchestration**

  * End-to-end pipeline scheduled and run using **Airflow**
  * Fully containerized using **Docker Compose**

---

## Tech Stack

* **Databases**: PostgreSQL, Snowflake
* **Transformation**: dbt
* **Orchestration**: Apache Airflow
* **Containerization**: Docker & Docker Compose
* **BI / Visualization**: Power BI
* **Languages**: SQL, Python

---

## Data Model (High Level)

* **Fact Tables**

  * `fct_booking`
  * `fct_booking_room`
* **Dimension Tables**

  * `dim_hotel`
  * `dim_room`
  * `dim_date`
* **Analytics Views**

  * Daily KPI views
  * Booking status summaries
  * Revenue & occupancy metrics

Models are designed to support:

* Time-series analysis
* Trend comparisons
* Drill-downs by hotel, room type, and date

---

## Why This Project

This project was built to mirror real analytics engineering work:

* Translating messy source data into analytics-ready datasets
* Designing schemas that scale with new questions
* Enabling BI tools without logic duplication
* Prioritizing clarity, reliability, and reproducibility

It reflects how analytics platforms are built at data-driven companies—not just how dashboards are created.

---

## How to Run (High Level)

1. Spin up PostgreSQL and Airflow using Docker Compose
2. Generate and load synthetic OLTP data
3. Ingest data into Snowflake
4. Run dbt models to build the analytics layer
5. Refresh Power BI dashboards
6. Schedule the full pipeline via Airflow

> Detailed setup steps are documented in the `/docs` folder.

---

## Future Improvements

* Data quality checks and anomaly detection
* Slowly changing dimensions (SCDs)
* Metrics layer abstraction
* CI/CD for dbt models
* Dashboard versioning & testing

