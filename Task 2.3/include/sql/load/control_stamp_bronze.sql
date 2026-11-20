USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA control;

-- Bronze targets
-- Append one load event per target for this (batch, run).
-- NOT EXISTS makes a task retry/clear safe (Snowflake does not enforce the UNIQUE).

INSERT INTO control.C_LOAD_DATES (TARGET_TABLE, LAYER, BATCH_ID, RUN_ID)
SELECT s.TARGET_TABLE, s.LAYER, DATE '{{ ds }}', '{{ run_id }}'
FROM (VALUES
    ('bronze', 'STG_STORE_MASTER'),
    ('bronze', 'STG_ITEM_MASTER'),
    ('bronze', 'STG_INVENTORY_MASTER'),
    ('bronze', 'STG_POS_SALES'),
    ('bronze', 'STG_PRICE_MASTER')
) AS s(LAYER, TARGET_TABLE)
WHERE NOT EXISTS (
    SELECT 1
    FROM control.C_LOAD_DATES t
    WHERE t.TARGET_TABLE = s.TARGET_TABLE
      AND t.BATCH_ID     = DATE '{{ ds }}'
      AND t.RUN_ID       = '{{ run_id }}'
);