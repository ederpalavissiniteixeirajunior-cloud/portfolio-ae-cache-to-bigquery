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
    &#123;&#123; dbt_utils.generate_surrogate_key(['nk_product', 'dbt_valid_from']) &#125;&#125; AS sk_product,
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
    f.id_order,
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

* **Uniqueness & Non-Null Assertions:** Staging and Gold models execute strict checks on identity keys (`sk_product`, `id_order`) to ensure structural consistency.
* **Custom Referential Integrity:** Cross-layer tests prevent mismatched orders from breaking dimensional lookups.
* **SCD2 Boundary Overlap Tests:** Custom integrity validation prevents date boundary overlapping (`dt_start` smaller than `dt_end`), mitigating the risk of cartesian inflation.

dbt test results: PASS (All core contracts and referential mappings validated)

---

## 🚀 Decoupled Serverless Orchestration

To maintain a **Zero-Cost Infrastructure** blueprint, the orchestration workflow is fully decentralized:
1. **The Edge Job (VPS Cron):** Extracted mutations are packed, masked in-memory using `Faker`, and safely landed into the BigQuery Storage engine.
2. **The Cloud Lifecycle Workflow:** Once telemetry lands, an event-driven automation framework controls the compilation of dbt Core models and builds the static dashboard artifacts.

Detailed build configurations, environment variables isolation, and step-by-step pipeline logging can be explored natively in our **Github Process (CI/CD)** documentation tab.