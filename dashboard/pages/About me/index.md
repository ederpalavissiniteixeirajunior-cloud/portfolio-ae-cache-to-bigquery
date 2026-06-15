---
title: About Me
sidebar_position: 5
---

# Eder Palavissini Jr.
**Senior Analytics Engineer & BI Manager**

I'm a data professional with 5+ years building end-to-end data platforms — from legacy ERP ingestion through dimensional modeling to self-service BI. I work at the intersection of engineering and business: I design the architecture, write the transformation logic, and stay in the room when the numbers need explaining to executives.

I'm open to senior Analytics Engineering and Data Engineering roles in remote-first international teams.

---

## Current Roles

**Senior Analytics Engineer — HUBXP** *(2026 – present)*
Analytics Engineering in the fintech space. dbt, BigQuery, data modeling, pipeline governance.

**Analytics Lead / BI Manager — KE Showroom & Licensing** *(Aug 2024 – present, on-demand)*
Led the BI strategy for a Brazilian fashion wholesale company. Delivered a 60% revenue increase through predictive sales models and improved inventory visibility. Automated ETL and reporting workflows, achieving a 75% reduction in reporting time.

---

## Previous Experience

**Senior Planning Analyst — Vivensis** *(Oct 2023 – Aug 2024)*
Data platform work on Databricks (Apache Spark). ETL pipeline development, dimensional modeling, and planning analytics for an e-commerce operation.

**Control Desk Lead — Webhelp** *(Jul 2021 – Oct 2023)*
Operational intelligence and reporting for a large CX operation. Identified process bottlenecks via data analysis, contributing to a 63% reduction in unresolved customer cases.

---

## Technical Stack

| Domain | Tools |
|---|---|
| **Transformation** | dbt Core, SQL (BigQuery, SQL Server, PostgreSQL) |
| **Data Engineering** | Python (Pandas), Apache Spark (Databricks), ETL/ELT pipelines |
| **Warehouses** | BigQuery, Databricks, SQL Server, MongoDB |
| **BI & Visualization** | Power BI (Advanced DAX), Evidence.dev |
| **DevOps & Governance** | GitHub Actions (CI/CD), dbt tests, LGPD/GDPR compliance |
| **Language** | Portuguese (native), English (advanced) |

---

## About This Portfolio

This project was built to demonstrate Modern Data Stack proficiency in a fully automated, zero-cost environment — not to simulate production, but to run one.

The data is extracted daily from a live on-premises ERP (InterSystems Caché) via a Python script running on a VPS. PII fields are anonymized in memory before leaving the server. dbt handles all transformations, testing, and documentation. Evidence.dev renders the dashboard as static files via GitHub Actions, served on GitHub Pages.

Designing this system surfaced real infrastructure constraints — including a BigQuery partition expiration behavior that silently deleted historical data after every dbt run without any pipeline error. Diagnosing and resolving that issue is documented in the Data Pipeline & Architecture page (sidebar).

---

## Let's Connect

<Link 
    url="https://www.linkedin.com/in/eder-palavissini/?locale=en-US"
    label="LinkedIn"
    newTab=true
/>
|
<Link 
    url="https://github.com/ederpalavissiniteixeirajunior-cloud"
    label="GitHub"
    newTab=true
/>
|
<Link 
    url="mailto:ederpalavissiniteixeirajunior@gmail.com"
    label="Email"
    newTab=true
/>
