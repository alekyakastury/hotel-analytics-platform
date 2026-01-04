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

**Current stage (OLTP)**
