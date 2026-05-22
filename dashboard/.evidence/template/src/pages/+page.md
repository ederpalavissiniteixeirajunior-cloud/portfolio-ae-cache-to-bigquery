---
title: Project Overview
sidebar_label: Project Overview
---

# Modernizing Legacy Data: Zero-Cost End-to-End Analytics

Welcome. This project demonstrates a production-grade **Modern Data Stack** built to bridge the gap between a 20-year-old legacy system (**InterSystems Caché**) and modern executive decision-making.

## 🎯 The Business Challenge
In many industries, legacy systems store goldmines of data but lack the flexibility for modern analytics. The main issues addressed here were:
- **Data Drift:** Historical reports changed when master data (like prices) were updated.
- **Security:** Sensitive customer data needed to be anonymized before hitting the cloud.
- **Cost:** Implementing a high-end solution with **$0 infrastructure overhead**.

## 🛠️ The Architecture
> This pipeline is fully automated and follows the **Medallion Architecture**.

- **Ingestion:** Python scripts running on a VPS (Ubuntu) with PII masking.
- **Warehouse:** Google BigQuery (Sandbox environment).
- **Transformation:** dbt Core (v1.10+) implementing **SCD Type 2**.
- **Delivery:** Evidence.dev hosted on GitHub Pages.

