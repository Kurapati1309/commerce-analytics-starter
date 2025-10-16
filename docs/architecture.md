# Architecture

- Ingestion: CSV/Parquet to S3 (raw) + stream (optional Kinesis)
- Storage: S3 (Bronze/Silver), Redshift (Gold)
- Transform: dbt (Bronze→Silver→Gold)
- Orchestration: Airflow (daily + hourly snapshot)
- Observability: dbt tests + logs
- BI: Looker dashboards (Exec, Funnel, ROAS, LTV, Inventory)
