USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA control;

-- Gold dims + facts (12 targets)
-- Append one load event per target for this (batch, run).
-- NOT EXISTS makes a task retry/clear safe (Snowflake does not enforce the UNIQUE).

INSERT INTO control.C_LOAD_DATES (TARGET_TABLE, LAYER, BATCH_ID, RUN_ID)
SELECT s.TARGET_TABLE, s.LAYER, DATE '{{ ds }}', '{{ run_id }}'
FROM (VALUES
    ('gold', 'DIM_COMPANY'),
    ('gold', 'DIM_STATE'),
    ('gold', 'DIM_CUSTOMER'),
    ('gold', 'DIM_DEPARTMENT'),
    ('gold', 'DIM_DISTRICT'),
    ('gold', 'DIM_CLASS'),
    ('gold', 'DIM_LOCATION'),
    ('gold', 'DIM_ITEM'),
    ('gold', 'FCT_SALES'),
    ('gold', 'FCT_PRICE'),
    ('gold', 'FCT_INVENTORY'),
    ('gold', 'FCT_SALES_PER_WEEK')
) AS s(LAYER, TARGET_TABLE)
WHERE NOT EXISTS (
    SELECT 1
    FROM control.C_LOAD_DATES t
    WHERE t.TARGET_TABLE = s.TARGET_TABLE
      AND t.BATCH_ID     = DATE '{{ ds }}'
      AND t.RUN_ID       = '{{ run_id }}'
);