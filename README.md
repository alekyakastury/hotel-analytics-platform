# Hotel Analytics Platform

A production-style data engineering project that models real hotel operations in an **OLTP PostgreSQL system** and prepares the foundation for **analytics modeling (Snowflake + dbt)**.

This repo emphasizes **data modeling, integrity, and reproducibility** (not just queries): the same concerns you face in production systems.

---

## Tech Stack

**Current**
- PostgreSQL (OLTP system of record)
- Docker + Docker Compose
- SQL (transactional DDL + rerunnable seed)
- pgAdmin (UI inspection)

**Planned**
- Snowflake (OLAP warehouse)
- dbt (transformations + marts)
- Airflow (optional orchestration)

---

## Project Goals

- Model a **normalized OLTP schema** for hotel operations (bookings, stays, inventory, pricing, billing, payments)
- Make the system **reproducible end-to-end** using Docker + SQL
- Seed with **realistic transactional data** suitable for analytics validation
- Build toward analytics-ready marts (facts/dims) using Snowflake + dbt

---

## Why This OLTP Schema Is Realistic

The schema intentionally separates concepts that are commonly conflated in toy projects:

- **Booking vs Stay**: planned intent vs actual execution (check-in/out, no-shows)
- **Room vs Room Night**: static inventory vs day-level occupancy (prevents wrong occupancy metrics)
- **Invoice vs Payment vs Refund**: financial truth with auditability (prevents revenue double-counting)
- **Discounts/Promotions/Taxes**: modeled explicitly for accurate net revenue analytics

This structure mirrors real systems and avoids analytics pitfalls such as double-counting revenue, incorrect occupancy rates, and mixing “booked” with “stayed”.

---

## Architecture Overview

This project was designed to mirror a real-world analytics engineering pipeline, with a strong emphasis on correctness, reproducibility, and scalability, rather than tooling for its own sake.

![Data Flow Diagram](docs\data_flow_diagram.png)

### 1. OLTP (PostgreSQL) → Analytics (Snowflake) Separation

#### Decision:
Use PostgreSQL as the transactional (OLTP) system and Snowflake as the analytical warehouse.

#### Rationale:

OLTP systems are optimized for writes and point lookups, not analytical scans.

Snowflake provides elastic compute, separation of storage and compute, and is well-suited for large analytical queries and BI workloads.

This separation reflects real production architectures used at companies like Netflix, Airbnb, and Stripe.

#### Tradeoff:
This introduces data movement and eventual consistency, which is acceptable for analytics use cases.

### 2. File-Based Ingestion via CSV (Postgres → CSV → Snowflake RAW)

#### Decision:
Ingest data from Postgres into Snowflake using partitioned CSV (flat file) exports rather than direct database connectors or CDC tools.

#### Rationale:

CSV-based ingestion is deterministic, easy to debug, and cost-controlled.

Partitioned files (e.g., monthly booking exports) enable parallel loading and incremental processing.

This approach makes the pipeline reproducible locally without requiring managed services.

Snowflake’s COPY INTO is highly optimized for bulk file ingestion.

#### Tradeoff:
This is batch-oriented and not real-time. The architecture is intentionally designed for analytics latency, not operational immediacy.

### 3. RAW → STAGING → DATAMART Layering in Snowflake

#### Decision:
Adopt a three-layer modeling approach inside Snowflake:

RAW: landed source data with minimal transformation

STAGING: cleaned, typed, standardized models

DATAMART: business-ready facts and dimensions

#### Rationale:

Preserves source fidelity while enabling clean transformations.

Makes debugging and backfills straightforward.

Aligns with analytics engineering best practices and dbt conventions.

Allows downstream models to be rebuilt without re-ingesting source data.

#### Tradeoff:
More models and schemas increase upfront complexity, but significantly improve long-term maintainability and trust.

### 4. dbt for Transformations, Testing, and Documentation

#### Decision:
Use dbt to manage all transformations from RAW → STAGING → DATAMART, including KPI views.

#### Rationale:

dbt enforces modular, SQL-first transformations executed directly in Snowflake.

Built-in tests (not_null, unique, relationships) act as data quality guardrails.

dbt documentation and lineage graphs make the data model self-explanatory.

Incremental models allow scaling without full recomputation.

#### Tradeoff:
dbt requires upfront model structuring, but significantly reduces long-term analytical debt.

### 5. Airflow as the Orchestration Layer

#### Decision:
Use Apache Airflow to orchestrate the pipeline end-to-end.

#### Rationale:

Airflow manages when and in what order tasks run.

It encodes dependencies between export, load, and transformation steps.

Built-in retries, scheduling, and failure visibility make the pipeline production-like.

Airflow treats each step as an independent, observable unit of work.

#### Clarification:
Airflow does not perform transformations or store data. It orchestrates tasks that do.

### 6. Docker for Execution Environment Isolation

#### Decision:
Run each pipeline task inside Docker containers.

#### Rationale:

Docker guarantees consistent runtime environments across machines.

Each task (Postgres export, Snowflake load, dbt run) has its own dependency set.

Containers make the pipeline portable and reproducible.

#### Key Distinction:

Docker defines how tasks run.

Airflow defines when tasks run and in what order.

This separation mirrors production-grade data platforms.

### 7. Analytics-Centric Data Modeling (Room-Night Grain)

#### Decision:
Model core analytics facts at the room-night grain.

#### Rationale:

Metrics like Occupancy, ADR, and RevPAR require room-night alignment.

Avoids incorrect aggregations that occur when using booking-level facts.

Enables consistent daily, weekly, and monthly KPIs.

#### Tradeoff:
Room-night expansion increases row counts, but ensures metric correctness — a deliberate and necessary choice.

### 8. BI Layer Built on Semantic Views

#### Decision:
Expose dashboards only through curated KPI views built on top of datamarts.

#### Rationale:

Centralizes metric definitions.

Prevents metric drift across dashboards.

Allows BI tools to remain thin consumers of trusted data.

#### Summary

This architecture prioritizes:

Correct analytical grain

Reproducibility

Clear separation of concerns

Scalability through design, not brute-force volume

The result is a pipeline that can scale conceptually to large data volumes while remaining understandable, testable, and maintainable.

#### OLTP Data Model (PostgreSQL)

The following ERD represents the **normalized OLTP schema** used as the system of record.
It models real hotel operations including bookings, stays, inventory, billing, and payments.

![Hotel OLTP ERD](docs/hotel_oltp_full_erd.jpeg)

[Open full-resolution ERD](docs/hotel_oltp_full_erd.jpeg)


> The schema separates booking intent from stay execution, static room inventory from
> room-night occupancy, and invoices from payments to support accurate analytics downstream.

#### Analytics Model (Coming Next)

A dimensional analytics model (facts & dimensions) will be built in Snowflake using dbt
on top of this OLTP foundation.
