# Runbook

## Incidents
1. Check Airflow task logs.
2. Review dbt test failures (`target/run_results.json`).
3. If a raw file is malformed, fix and re-run backfill.

## Backfill
- Use Airflow `dag_dbt_transform` with date params or `dbt run -s +gold` with `--vars` window.

## Rollback
- Re-run last green state: `dbt run --state` or revert PR and retrigger CI.
