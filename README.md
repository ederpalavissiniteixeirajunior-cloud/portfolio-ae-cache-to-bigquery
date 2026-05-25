# 🚀 Legacy-to-Cloud Modern Data Stack Platform

[![dbt Core Version](https://img.shields.io/badge/dbt--core-v1.10%2B-orange?logo=dbt)](https://github.com/dbt-labs/dbt-core)
[![Data Warehouse](https://img.shields.io/badge/Data%20Warehouse-Google%20BigQuery-blue?logo=google-cloud)](https://cloud.google.com/bigquery)
[![Front-End UI](https://img.shields.io/badge/Presentation-Evidence.dev-brightgreen)](https://evidence.dev/)
[![Infrastructure Cost](https://img.shields.io/badge/Infrastructure%20Cost-$0%20(Net%20Zero)-success)](#)

An enterprise-grade Analytics Engineering platform that migrates transactional data from an on-premises legacy system (**InterSystems Caché**) into a secure, cloud-based Modern Data Stack (**Google BigQuery**) at **absolute zero infrastructure cost**.

This project serves as a showcase of senior-level proficiency in **Analytics as Code**, **Edge Data Governance**, **Advanced Dimensional Modeling (SCD Type 2)**, and automated **CI/CD Quality Gates**.

---

## 📌 The Architecture & Data Flow

The platform transforms highly coupled, non-historical legacy relational schemas into an optimized cloud star schema using an event-driven Medallion pipeline.

[On-Prem Legacy]       [Edge Ingestion Perim.]         [Cloud Data Warehouse]       [Analytics as Code UI]
+--------------------+   +-------------------+       +---------------------------+   +--------------------+
| InterSystems Caché | ─>| Python 3.9+ (VPS) | ──────>| Google BigQuery (Sandbox) | ─>|  Evidence.dev      |
| (Transactional OS) |   | + Faker Anonymous |       |  - Bronze (Append-Only)   |   |  (Jamstack Layout) |
+--------------------+   +-------------------+       |  - Silver (SCD Type 2)    |   |  - DuckDB WASM     |
│                  |  - Gold (Star Schema)     |   +--------------------+
│                  +---------------------------+             ▲
▼                                                            │
[Webhook Dispatch] ───────────────────────────────────────────────────┘
(Triggers GitHub Actions CD Workflow on successful load)