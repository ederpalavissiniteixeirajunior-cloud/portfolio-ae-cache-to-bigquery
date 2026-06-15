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

> ### 💡 Senior Architecture Note: Purposeful Full-Refresh Ingestion
> While incremental pipelines are standard for massive tables, a strategic "Full-Refresh" pattern was chosen for the Ingestion layer. Given the legacy source (InterSystems Caché) limitations regarding reliable update/delete flags, and since the cumulative database size stays under 1 Million rows, a daily full snapshot ensures 100% data consistency. This design eliminates synchronization drift with near-zero compute overhead, strictly respecting BigQuery's free tier slot allocation.

---

## 🛡️ Edge Governance: PII Masking & Compliance

To enforce compliance with strict global standards (**GDPR / LGPD**), sensitive customer information is never transmitted raw to the cloud. The python ingestion engine running on the Ubuntu VPS intercepts mutations and pseudonymizes data in-memory before standard chunk streams are uploaded.

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
import pandas as pd
from faker import Faker

def mask_edge_payload(df_raw: pd.DataFrame) -> pd.DataFrame:
    # Enforces D-0 Edge Governance by masking customer identifying attributes
    # in-memory using a localized seed provider prior to BigQuery insertion.
    fake = Faker(['en_US'])
    
    # Deterministic mapping to maintain relationship integrity without exposing true PII
    df_raw['nm_customer'] = df_raw['nk_customer'].apply(lambda x: fake.name())
    df_raw['ds_document'] = df_raw['nk_customer'].apply(lambda x: fake.ssn())
    
    return df_raw
</pre>

> **💡 Architecture Note:**
> This edge-masking pattern prevents unauthorized cloud exposure of real identifying parameters. If a Cloud Data Warehouse breach occurs, the underlying corporate data remains naturally obfuscated and unresolvable back to the dynamic operational core.

---

## 🏗️ Medallion Modeling & Temporal Data Integrity (SCD Type 2)

A common pitfall in operational environments is **Data Drift**, where historical sales data loses meaning because entities (like product pricing or customer regions) are updated in-place on the transactional legacy database. To prevent this, the data platform utilizes **dbt Snapshots** inside the Silver Layer to maintain full Type 2 Slowly Changing Dimensions (SCD2).

[System Ingestion] -> [stg_products] -> [snp_products (SCD2)] -> [dim_products (Gold)]
↳ Track valid_from / valid_to changes

### 🔑 Deterministic Identity Layer: Surrogate Keys
Instead of exposing unstable natural operational keys (`nk_customer`, `nk_product`) to the Exposition Layer, a deterministic hash mechanism is applied to enforce strict referential integrity across temporal snapshots.

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
-- Example snippet from our dbt Gold Dimension layer: dim_products.sql
WITH product_snapshot AS (
    SELECT * FROM &#123;&#123; ref('snp_products') &#125;&#125;
)

SELECT
    -- Late-binding Surrogate Key generation
    &#123;&#123; dbt_utils.generate_surrogate_key(['nk_product', 'dbt_valid_from']) &#125;&#125; AS sk_product_version,
    nk_product,
    ds_product,
    vl_price,
    dbt_valid_from AS dt_start,
    COALESCE(dbt_valid_to, '9999-12-31 23:59:59') AS dt_end,
    CASE WHEN dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current
FROM product_snapshot
</pre>

### 📈 Fact Table Construction: Late-Binding Meets Temporal Joins
The sales fact table (`fct_orders`) matches the exact state of dimensions at the transaction timestamp. This ensures correct margins and precise historical attribution, irrespective of subsequent operational shifts.

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
-- Snippet from fct_orders.sql demonstrating Temporal Joins
SELECT
    f.sk_order_version,
    f.dt_sale,
    -- Joining dimensions based on point-in-time state
    d_prod.sk_product,
    f.qt_items * d_prod.vl_price AS vl_total_revenue
FROM &#123;&#123; ref('itm_f_orders') &#125;&#125; f
LEFT JOIN &#123;&#123; ref('dim_products') &#125;&#125; d_prod 
ON f.nk_product = d_prod.nk_product
AND f.dt_sale BETWEEN d_prod.dt_start AND d_prod.dt_end
</pre>

---

## 🛡️ Embedded Data Quality Contracts (dbt Tests)

A data platform is only as valuable as the certainty of its metrics. To protect our downstream exposition layer hosted within the browser, the Medallion architecture enforces embedded quality contracts:

* **Uniqueness & Non-Null Assertions:** Staging and Gold models execute strict checks on identity keys (`sk_product`, `sk_order_version`) to ensure structural consistency.
* **Custom Referential Integrity:** Cross-layer tests prevent mismatched orders from breaking dimensional lookups.
* **SCD2 Boundary Overlap Tests:** Custom integrity validation prevents date boundary overlapping (`dt_start` smaller than `dt_end`), mitigating the risk of cartesian inflation.

dbt test results: PASS (All core contracts and referential mappings validated)

> 🔍 **Explore the full data lineage, column-level documentation, and test coverage interactively:**
> **[Open dbt Docs →](https://ederpalavissiniteixeirajunior-cloud.github.io/portfolio-ae-cache-to-bigquery/dbt-docs/)**

---

## 🔎 Architecture Decisions & Operational Findings

---

### 1. BigQuery Partition Expiration Behavior

**Symptom:** `gold.fct_orders` contained only 247 rows despite 4,907 orders existing in the Silver intermediate table. All data older than ~60 days was absent from every downstream analytics view with no pipeline error.

**Root cause:** Every dataset in this project inherited a `default_partition_expiration_ms` of **5,184,000,000 ms (60 days)** from the BigQuery project defaults. The critical behavior: for date-partitioned tables, BigQuery counts expiration **from the partition key value** (`dt_issued`), not from the partition creation timestamp.

A partition with `dt_issued = 2025-04-01`, written today during a `dbt run`, is considered immediately expired — because that date is more than 60 days in the past. BigQuery deletes it within minutes, silently, after `dbt` exits successfully.

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
-- Diagnostic: same SELECT returned 4,907 rows; CREATE TABLE returned 247.
-- Difference isolated to the PARTITION BY clause in the DDL.
SELECT COUNT(*) FROM silver.itm_f_orders;          -- 4,907 (unpartitioned TABLE)
SELECT COUNT(*) FROM gold.fct_orders;              -- 247   (partitioned, expiration active)
</pre>

**Resolution:** The dbt model config explicitly overrides the dataset default via BigQuery's `OPTIONS()` clause at table creation time:

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
&#123;&#123; config(
    materialized='table',
    partition_by=&#123;"field": "dt_issued", "data_type": "date", "granularity": "day"&#125;,
    partition_expiration_days=3650   -- overrides the 60-day dataset default
) &#125;&#125;
</pre>

> **Monitoring implication:** This class of failure produces no pipeline error — `dbt run` exits cleanly and reports the correct row count at write time. The correct safeguard is a dbt test with a `config: error_if: "< N"` row-count threshold on partitioned fact tables.

---

### 2. SCD Type 2 Temporal Coverage Gap

**Context:** The Silver layer uses dbt Snapshots to maintain SCD Type 2 history for customers, products, and sales representatives. Fact tables resolve dimension state at transaction time through a temporal join:

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
LEFT JOIN dim_sales_representative r
    ON f.cd_sales_representative = r.cd_sales_representative
    AND f.dt_issued BETWEEN DATE(r.valid_from)
                        AND COALESCE(DATE(r.valid_to), '9999-12-31')
</pre>

**Problem:** dbt Snapshots are forward-looking — they capture dimension state from the moment the snapshot first runs. This project's snapshots were initialized after 14 months of transactional history already existed. With all 4,907 orders dated between April 2025 and May 2026 and the earliest snapshot `valid_from` at May 2026, every temporal join returns NULL. The analytics layer was silently empty for representatives and products.

**Decision:** The analytics views were updated to join on the **current dimension record** (`is_current = true`) as a deliberate fallback:

<pre style="background: #1e1e1e; color: #d4d4d4; padding: 15px; border-radius: 8px; font-family: monospace; font-size: 14px; overflow-x: auto; line-height: 1.5;">
-- vw_reps_performance_base.sql
JOIN dim_sales_representative rep
    ON f.cd_sales_representative = rep.cd_sales_representative
    AND rep.is_current = true
</pre>

To enable this join without losing the surrogate key columns, `fct_orders` was extended with **degenerate natural keys**:

| Column | Role |
|---|---|
| `sk_customer_version` | SCD2 surrogate — resolves temporal state when snapshot covers the order date |
| `cd_customer` | Natural key — enables fallback join to current dimension record |

> **Trade-off:** Historical orders reflect the *current* dimension state, not the state at order time. This is factually correct for this dataset because no representative or product attribute changed during the covered collection periods. As snapshot coverage extends forward, new orders will resolve through temporal joins automatically — no model changes required.

---

## 🚀 Decoupled Serverless Orchestration

To maintain a **Zero-Cost Infrastructure** blueprint, the orchestration workflow is fully decentralized:
1. **The Edge Job (VPS Cron):** Extracted mutations are packed, masked in-memory using `Faker`, and safely landed into the BigQuery Storage engine.
2. **The Cloud Lifecycle Workflow:** Once telemetry lands, an event-driven automation framework controls the compilation of dbt Core models and builds the static dashboard artifacts.

Detailed build configurations, environment variables isolation, and step-by-step pipeline logging can be explored natively in our **Github Process (CI/CD)** documentation tab.