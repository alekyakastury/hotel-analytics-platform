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
