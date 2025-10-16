# DTC Commerce Analytics Platform

Company-grade, end-to-end data engineering project (AWS + Redshift + Databricks + dbt + Airflow + Looker).

## Quickstart
1) Create a Python venv and install deps: `pip install -r requirements.txt`
2) Generate synthetic data: `python scripts/generate_sample_data.py --days 90`
3) (Optional) Sync to S3: `python scripts/load_to_s3.py --bucket ca-raw`
4) Configure dbt profile for Redshift, then: `dbt build`
5) Start Airflow locally and trigger DAGs in `airflow/dags`
6) Connect Looker to your warehouse and import `looker/model.lkml`

## Architecture
- Raw → Bronze (staging) → Silver (business ready) → Gold (marts)
- Orchestrated by Airflow, validated with dbt tests, monitored via CloudWatch.
- Reproducible infra with Terraform. CI runs dbt checks on each PR.

Generated: 2025-10-16T05:34:00.624977Z
