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

<pre><code class="language-python">
# pipeline/ingestion/masking_engine.py
from faker import Faker

fake = Faker('pt_BR')

def mask_edge_payload(raw_row):
    # Ephemeral in-memory data transformation before cloud delivery
    return &#123;
        "id_cliente": raw_row["id_cliente"],
        "nome_cliente": fake.name(),
        "documento": fake.cpf(),
        "data_cadastro": raw_row["data_cadastro"]
    &#125;
</code></pre>

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
# models/marts/fct_sales_monthly.yml
version: 2

models:
  - name: fct_sales_monthly
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
* **Billing Workaround Execution:** Google Cloud Billing was safely enabled to unlock native Data Manipulation Language (**DML MERGE**) capabilities required for dbt Core historical snapshots (**SCD Type 2**), while ensuring real-world compute remains at a true net-zero expense.
