USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA control;

INSERT INTO control.C_PARAMS
    (RUN_ID, DAG_ID, BATCH_ID, DS, DS_NODASH, LOGICAL_DATE, TRIGGERED_BY, STARTED_AT)
VALUES (
    '{{ run_id }}',
    '{{ dag.dag_id }}',
    DATE '{{ ds }}',
    '{{ ds }}',
    '{{ ds_nodash }}',
    '{{ logical_date }}'::TIMESTAMP_TZ,
    '{{ dag_run.run_type }}',     -- 'scheduled' | 'manual' | 'backfill' | 'dataset_triggered'
    CURRENT_TIMESTAMP()
);