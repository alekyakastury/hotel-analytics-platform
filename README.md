# Hotel Analytics Platform

A production-style **end-to-end data engineering project** that models real hotel operations using a modern analytics stack.

This project demonstrates how an OLTP system (PostgreSQL) can be designed, seeded with realistic transactional data, and later transformed into analytics-ready datasets for reporting and decision-making.

---

## 🎯 Project Goals

- Design a **normalized OLTP schema** that reflects real hotel operations:
  bookings, stays, room inventory, pricing, billing, and payments
- Ensure the system is **reproducible end-to-end** using Docker and SQL
- Seed the database with **realistic transactional data** suitable for analytics
- Lay a clean foundation for downstream **analytics engineering (dbt, Snowflake)**

This project intentionally mirrors how data is modeled and bootstrapped in real production environments.

---

## 🧱 Architecture Overview

**Current stage (OLTP):**

### Why This OLTP Schema Is Realistic

The schema intentionally separates:
- **Bookings vs. stays** (planned vs. actual execution)
- **Room inventory vs. room-night occupancy**
- **Invoices vs. payments vs. refunds**

These distinctions mirror real production systems and prevent common analytics pitfalls such as:
- double-counting revenue
- incorrect occupancy calculations
- mixing booking intent with realized stays

## Analytics Use Cases Enabled

This OLTP model enables downstream analytics such as:
- Daily occupancy rate by hotel and room type
- Revenue, ADR, and RevPAR trends
- Cancellation and no-show impact analysis
- Promotion effectiveness and discount leakage
- Customer lifetime value and repeat behavior

These metrics will be modeled in Snowflake using dbt in subsequent stages.

**Tech Stack**
- PostgreSQL (OLTP)
- Docker & Docker Compose
- SQL (DDL + transactional seeding)
- pgAdmin (UI inspection)

*Planned:* Snowflake, dbt, Airflow
