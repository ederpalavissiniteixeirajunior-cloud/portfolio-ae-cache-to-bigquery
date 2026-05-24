---
title: Data Pipeline & Architecture
sidebar_position: 2
---

### 🔄 End-to-End Data Journey Breakdown

The platform's architecture is divided into a three-stage operational workflow, ensuring decoupling, edge governance, and seamless data transformation.

<Grid cols={3}>

> ### 1. Edge & Ingestion Pipeline
> **Environment:** On-Premises Ubuntu VPS  
> **Engine:** Python 3.9 Lambda-Style Script  
> 
> * **Extraction:** Executes robust local SQL queries to fetch modified records from the InterSystems Cache operational instance.
> * **Edge Governance:** Enforces GDPR/LGPD compliance by utilizing an in-memory anonymization layer with Faker to mask sensitive customer fields before cloud ingestion.

> ### 2. Cloud Data Architecture
> **Environment:** Google Cloud Platform Sandbox  
> **Engine:** BigQuery Serverless Warehouse  
> 
> * **Bronze Layer:** Append-only logging capturing raw historical data mutations.
> * **Silver Layer:** Hard schema enforcement, data type conformation, and full dbt SCD Type 2 snapshots to prevent dimensional data drift.
> * **Gold Layer:** Kimball Star Schema design pattern containing late-binding surrogate keys for historical referential integrity.

> ### 3. Universal Exposition Layer
> **Environment:** Static GitHub Pages CDN  
> **Engine:** Evidence.dev Framework + DuckDB WASM  
> 
> * **Jamstack Aggregation:** Pre-compiles analytical layers during CI/CD execution into high-speed static content assets.
> * **In-Browser Compute:** Utilizes embedded client-side DuckDB execution engines to handle micro-queries and re-render visual components dynamically with zero cloud infrastructure costs.

</Grid>

---

## 🛡️ Edge Governance: PII Masking & Compliance

To enforce compliance with strict global standards (**GDPR / LGPD**), sensitive customer information is never transmitted raw to the cloud. The python ingestion engine running on the Ubuntu VPS intercept mutations and pseudonymizes data in-memory before standard chunk streams are uploaded.

<LastRefreshed/>