# Ashraya Kadel Module 2 ETL

Retail data warehouse coursework in three parts:

- `Task2.1/` — Snowflake schema: DDL, date seed (2026-01-01 to 2035-12-31), ERD, design notes.
- `Task 2.2/` — Architecture notes: medallion flow, quality gates, retention and reload policy.
- `Task 2.3/` — Working pipeline: Airflow 3 on Docker, loads five daily CSVs from `data/` into Snowflake through bronze → silver → gold, with a batch log and reconciliation checks.

## How to run

Everything lives in `Task 2.3/`. Docker Compose runs Airflow 3.3.2 with CeleryExecutor, Redis, and Postgres.

```bash
cd "Task 2.3"
docker compose up -d
```

Wait a minute or two, then open http://localhost:8080 (login `airflow` / `airflow` unless you changed it in `.env`).

You need a `.env` file next to `docker-compose.yaml` with at least `FERNET_KEY` and the JWT secret. On Linux also set `AIRFLOW_UID` to your user id, or the init container will warn and files will end up owned by root.

Before the pipeline can talk to Snowflake, create a connection with id `snowflake_conn` in the Airflow UI (Admin → Connections) or via the CLI. The DAG expects warehouse `MODULE2` and database `MODULE2_ETL`; the control DDL creates the schema and batch log on first run.

The DAG is `my_etl_dag`, scheduled `@daily`, with `catchup=False`. Each run:

1. Sits on a sensor until all five files for that date exist in `data/` (naming: `pos_sales_20261114.csv`, `store_master_20261114.csv`, etc.).
2. Runs the DDL scripts, then loads bronze, silver, and gold in order.
3. Writes per-task status to `MODULE2_ETL.CONTROL.C_BATCH_LOG` and runs `validate_recon.sql` at the end.

To run the DAG tests locally (they only check DAG structure, no Snowflake needed):

```bash
cd "Task 2.3"
pytest tests/ -v
```

## How to backfill

Since `catchup=False`, missed days do not replay on their own. Backfill explicitly:

```bash
docker compose exec airflow-scheduler \
    airflow dags backfill my_etl_dag -s 2026-11-14 -e 2026-11-18
```

or trigger a single date from the UI with a logical date.

Things to know before you do:

- The data folder only contains files for 2026-11-14 through 2026-11-18. For any other date the file sensor will sit and time out. Drop the five CSVs for the missing date into `data/` first.
- `max_active_runs=1`, so a range backfill processes one date at a time. Five dates is fine; more will take a while.
- The SQL scripts use `{{ ds_nodash }}` in file names and `{{ ds }}` as batch id, so each backfilled run is self-contained and re-running a date just reloads that date's batch.
- After a backfill, check reconciliation numbers in `evidence/` for what a clean run looks like: silver sales amount = gold `FCT_SALES` amount.
- If a source file itself was wrong, do not overwrite it. Per the reload policy in `Task 2.2/retention_reload_policy.md`, archive the corrected file as a new version and process it as a new batch linked by `PARENT_BATCH_ID`.

## Known limitations

- Local development only. The compose file ships default passwords (`airflow`/`airflow` for Postgres and the admin user) and no TLS. Do not deploy it as is.
- Snowflake credentials live in the Airflow connection and `.env`, not in the SQL. If the connection id changes, update `SNOWFLAKE_CONN_ID` in `config/config.py`.
- `PUT file:///opt/airflow/data/...` paths in the bronze load scripts are hardcoded to the container mount. If you move the data directory, both the compose volume and the scripts need updating.
- Data is a five-day sample. Nothing here has been tested against realistic volumes; the weekly sales fact and reconciliation queries assume small data.
- The DAG tests cover import, task graph, and cycles only. There are no tests for the SQL scripts themselves.
- `dim_customer_load.sql` and the customer dimension exist but the daily source files carry no customer data; the customer dimension stays empty on this dataset.
- Bronze and silver retention is defined as 30 days in the policy document, but nothing in the pipeline enforces it yet. Cleanup is manual.
