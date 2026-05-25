---
title: GitHub Process (CI/CD)
sidebar_position: 4
---

# 🚀 GitHub Process & CI/CD Pipelines

This section details how the data platform achieves absolute automation and deployment stability under an **Analytics as Code** model. Every code change triggers automated compliance and regression gates before updating the production environment.

---

## 1. Automated Continuous Integration (dbt CI Gate)

To prevent breaking changes, data drift, or structural syntax issues from entering the production Data Warehouse, a strict Continuous Integration (CI) flow is enforced via **GitHub Actions**. 

When an engineer opens a Pull Request (PR) against the `main` branch, an ephemeral runner isolates the branch environment, executes linting checks, validates SQL compilation, and runs the entire suite of schema assertions.

<pre><code class="language-yaml">
# .github/workflows/dbt_ci_validation.yml
name: dbt_ci_validation

on:
  pull_request:
    branches: [ main ]
    paths:
      - 'dbt_project/**'

jobs:
  validate_models:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code Repository
        uses: actions/checkout@v4

      - name: Set up Python Environment
        uses: actions/setup-python@v5
        with:
          python-version: '3.10'

      - name: Install Dependencies & dbt Core
        run: |
          pip install --upgrade pip
          pip install dbt-bigquery

      - name: Enforce Data Pipeline Quality Gates
        env:
          GCP_SA_KEY: $&#123;&#123; secrets.GCP_SA_KEY &#125;&#125;
        run: |
          cd dbt_project
          dbt deps
          dbt compile --profiles-dir .
          dbt test --profiles-dir .
</code></pre>

---

## 2. Continuous Deployment (Evidence Jamstack Delivery)

Once a Pull Request passes all testing criteria and is merged into `main`, a separate Continuous Deployment (CD) pipeline triggers automatically. 

Because Evidence.dev functions as a static site generator (SSG), the runner triggers a production build, authenticates into Google Cloud to fetch the latest aggregate views from the BigQuery Analytics layer, processes the dynamic markdown pages into an optimized Jamstack distribution, and pushes it directly to **GitHub Pages**.

<pre><code class="language-yaml">
# .github/workflows/evidence_gh_pages_deploy.yml
name: Deploy Dashboard to GitHub Pages

on:
  push:
    branches: [ main ]
  schedule:
    - cron: '0 8 * * *' # Automated daily refresh aligned with the VPS ingestion window

jobs:
  build_and_deploy:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout Code Repository
        uses: actions/checkout@v4

      - name: Install Node.js Runtime
        uses: actions/setup-node@v4
        with:
          node-version: 18

      - name: Install Application Packages
        run: npm install

      - name: Compile Jamstack Site Assets
        env:
          GCP_SERVICE_ACCOUNT: $&#123;&#123; secrets.GCP_SA_KEY &#125;&#125;
        run: npm run build

      - name: Publish Build Distribution to GitHub Pages
        uses: JamesIves/github-pages-deploy-action@v4
        with:
          folder: build
          branch: gh-pages
</code></pre>

---

## 3. Production Security & Secret Isolation

Enterprise architecture demands strict containment of operational tokens and environment configurations. The platform achieves zero configuration leaks through cryptographic key isolation:

* **Repository Secrets Management:** The Google Cloud Service Account json key (`GCP_SA_KEY`) is stored within encrypted GitHub Repository Secrets. It is never exposed in cleartext within code files or configuration structures.
* **Separation of Duties:** The Python ingestion engine on the VPS manages its local InterSystems Caché network credentials independently from the GitHub Actions environment. GitHub never holds the transactional source database passwords, and the VPS never holds the Evidence deployment tokens, eliminating single-point-of-failure vectors across the infrastructure.