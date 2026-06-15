---
title: GitHub Process (CI/CD)
sidebar_position: 4
---

# 🚀 GitHub Process & CI/CD Pipeline

This section details how the data platform achieves full automation under an **Analytics as Code** model. Every commit to `main` — and every daily cron trigger — runs a sequential three-job pipeline that validates data quality before publishing the dashboard.

---

## Pipeline Overview

```text
┌─────────────────────────────────────────────────────────────────┐
│  Triggers: push → main  │  workflow_dispatch  │  cron 07:00 BRT │
└──────────────┬──────────────────────────────────────────────────┘
               │
       ┌───────▼────────┐
       │   Job 1: dbt   │  deps → seed → snapshot → run → test → docs
       └───────┬────────┘
               │ (only if dbt passes)
       ┌───────▼────────┐
       │  Job 2: build  │  Evidence sources → static build → embed dbt docs
       └───────┬────────┘
               │ (only if build passes)
       ┌───────▼────────┐
       │ Job 3: deploy  │  GitHub Pages → Slack notification
       └────────────────┘
```

---

## Job 1 — dbt Quality Gate

The first job is the critical quality gate. It runs the full dbt pipeline against the production BigQuery environment and blocks deployment if any test fails.

<pre><code class="language-yaml">
jobs:
  dbt:
    runs-on: ubuntu-latest
    env:
      DBT_BRAND_SCOPE: $&#123;&#123; secrets.DBT_BRAND_SCOPE &#125;&#125;
    steps:
      - uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: '3.11'
          cache: 'pip'

      - name: Install dbt
        run: pip install dbt-core dbt-bigquery

      - name: Create dbt profiles.yml
        run: |
          mkdir -p ~/.dbt
          cat &lt;&lt;EOF > ~/.dbt/profiles.yml
          portfolio_ae:
            target: prod
            outputs:
              prod:
                type: bigquery
                method: service-account
                project: portfolio-ae-vendas
                dataset: raw
                keyfile: /tmp/gcp-key.json
                location: southamerica-east1
          EOF

      - name: dbt deps
        run: dbt deps

      - name: dbt seed
        run: dbt seed

      - name: dbt snapshot       # SCD Type 2 — captures dimension changes
        run: dbt snapshot

      - name: dbt run            # builds Bronze → Silver → Gold → Analytics
        run: dbt run

      - name: dbt test           # 99 tests: unique, not_null, relationships
        run: dbt test

      - name: dbt docs generate  # builds catalog.json + manifest.json
        run: dbt docs generate

      - name: Notify Slack on failure
        if: failure()
        # sends alert to #data-alerts with link to failed run
</code></pre>

> **Design decision:** `DBT_BRAND_SCOPE` is set at the **job level** (not step level) because dbt parses all model files during compilation — including seeds — which means the env var must be available before any dbt command runs.

---

## Job 2 — Evidence.dev Build

Runs only after Job 1 passes. Fetches aggregated views from BigQuery's Analytics layer, compiles the static site, and embeds the dbt docs at `/dbt-docs/`.

<pre><code class="language-yaml">
  build:
    needs: dbt   # blocked until Job 1 succeeds
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - name: Setup Node.js
        uses: actions/setup-node@v4
        with:
          node-version: 20

      - name: Run Evidence Sources   # pulls data from BigQuery Analytics layer
        run: npm run sources
        env:
          GOOGLE_APPLICATION_CREDENTIALS: ./gcp-key.json

      - name: Build Static Site
        run: npm run build
        env:
          EVIDENCE_BASE_PATH: /portfolio-ae-cache-to-bigquery

      - name: Download dbt docs artifact
        uses: actions/download-artifact@v4
        with:
          name: dbt-docs

      - name: Embed dbt docs into Evidence build
        run: |
          mkdir -p ./dashboard/build/dbt-docs
          cp index.html manifest.json catalog.json ./dashboard/build/dbt-docs/

      - name: Upload Pages artifact
        uses: actions/upload-pages-artifact@v3
        with:
          path: ./dashboard/build
</code></pre>

---

## Job 3 — GitHub Pages Deploy

Publishes the static artifact to GitHub Pages and sends a Slack notification on success.

<pre><code class="language-yaml">
  deploy:
    needs: build   # blocked until Job 2 succeeds
    environment:
      name: github-pages
      url: $&#123;&#123; steps.deployment.outputs.page_url &#125;&#125;
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@v4

      - name: Notify Slack on success
        if: success()
        # sends confirmation to #data-alerts with live dashboard URL
</code></pre>

---

## Secret Isolation

All credentials are stored as encrypted GitHub Repository Secrets and never appear in code:

| Secret | Purpose |
|---|---|
| `GCP_SERVICE_ACCOUNT_KEY` | BigQuery authentication (dbt + Evidence) |
| `DBT_BRAND_SCOPE` | Product brand filter injected at runtime |
| `SLACK_WEBHOOK_URL` | Pipeline notification endpoint |

The VPS credentials (InterSystems Caché DSN, local `.env`) are managed independently and never transmitted to GitHub — ensuring complete separation of concerns between the ingestion layer and the CI/CD environment.
