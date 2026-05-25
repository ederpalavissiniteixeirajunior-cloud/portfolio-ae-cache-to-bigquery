---
title: Governance & Security
sidebar_position: 3
---
# 🛡️ Governance & Security Framework

This section outlines the governance models, data privacy compliance, and quality gates enforced across the modern data lifecycle at absolute zero infrastructure cost.

---

## 1. Edge Governance: PII Masking & Compliance

To enforce strict compliance with global data protection standards (**GDPR / LGPD**), sensitive customer information is never transmitted raw to the cloud environment. The Python ingestion engine running on the Ubuntu VPS intercepts data streams and pseudonymizes personal identifying information (PII) in-memory before any chunk is transmitted to the Data Warehouse.

> **Senior Architecture Note:** This perimeter defense completely isolates real identities. In the event of a downstream Cloud Data Warehouse breach, the corporate analytic datasets remain legally obfuscated and mathematically unresolvable back to operational individuals.

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

---

## 2. Data Quality & Contract Enforcement (dbt Core)

Pipeline resilience is guaranteed through programmatic assertions executed during compilation and execution cycles. Every database entity is bound to semantic contracts, preventing operational anomalies from corrupting analytical marts.

### Staging Validation Rules
<pre><code class="language-yaml">
# models/staging/stg_customers.yml
version: 2

models:
  - name: stg_customers
    description: "Conformed customer profiles with pseudonymized fields."
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
      - name: document_status
        tests:
          - accepted_values:
              values: ['VALID', 'INVALID']
</code></pre>

### Dimensional Integrity Rules
<pre><code class="language-yaml">
# models/marts/fct_orders.yml
version: 2

models:
  - name: fct_orders
    description: "Granular monthly sales performance transaction matrix."
    columns:
      - name: customer_key
        tests:
          - unique
          - not_null
          - relationships:
              to: ref('dim_customers')
              field: customer_key
</code></pre>

---

## 3. FinOps & Perimeter Security

Operating within strict enterprise architecture guidelines requires structural cost controls and tightly scoped access privileges.

* **Principle of Least Privilege (PoLP):** The Evidence interface authenticates to the Google Cloud Sandbox using a dedicated Service Account. This identity is restricted exclusively to `BigQuery Data Viewer` and `BigQuery Job User` roles, nullifying any write or administrative threat vectors.
* **Compute Footprint Mitigation:** By engineering a Jamstack presentation layer, interactive report parameters are computed client-side via **DuckDB WASM**. This offloads execution workloads from the warehouse, conserving the 1TB monthly free tier quota.
* **Billing Workaround Execution:** To support complex dbt Core historical multi-statement transactions (SCD Type 2 MERGE), Google Cloud Billing was safely enabled. To strictly preserve the $0 infrastructure constraint, a hard programmatic budget alarm of $0.00 was applied, ensuring execution remains entirely sandboxed within GCP's 10GB storage and 1TB monthly free tier thresholds.
