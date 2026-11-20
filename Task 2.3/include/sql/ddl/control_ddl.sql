-- ======================================================================
-- CONTROL SCHEMA DDL
-- Idempotent. Recreates C_LOAD_DATES (append-only history) and
-- C_BATCH_LOG (with script metadata) if dropped.
-- C_PARAMS and C_BATCH_SCRIPTS unchanged from previous version.
-- ======================================================================

CREATE DATABASE IF NOT EXISTS MODULE2_ETL;
CREATE SCHEMA IF NOT EXISTS MODULE2_ETL.CONTROL;

-- ----------------------------------------------------------------------
-- C_PARAMS — one row per DAG run (unchanged)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MODULE2_ETL.CONTROL.C_PARAMS (
    RUN_ID        VARCHAR       NOT NULL,
    DAG_ID        VARCHAR       NOT NULL,
    BATCH_ID      DATE          NOT NULL,
    DS            VARCHAR       NOT NULL,
    DS_NODASH     VARCHAR       NOT NULL,
    LOGICAL_DATE  TIMESTAMP_TZ,
    TRIGGERED_BY  VARCHAR,
    STARTED_AT    TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_C_PARAMS PRIMARY KEY (RUN_ID)
);

-- ----------------------------------------------------------------------
-- C_BATCH_SCRIPTS — static catalog (unchanged)
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MODULE2_ETL.CONTROL.C_BATCH_SCRIPTS (
    SCRIPT_ID     NUMBER AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
    LAYER         VARCHAR NOT NULL,
    PHASE         NUMBER  NOT NULL,
    SCRIPT_NAME   VARCHAR NOT NULL,
    TARGET_TABLE  VARCHAR NOT NULL,
    IS_ACTIVE     BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT PK_C_BATCH_SCRIPTS PRIMARY KEY (SCRIPT_ID),
    CONSTRAINT UQ_C_BATCH_SCRIPTS_NAME UNIQUE (SCRIPT_NAME)
);

MERGE INTO MODULE2_ETL.CONTROL.C_BATCH_SCRIPTS t
USING (
    SELECT * FROM VALUES
        ('bronze', 1, 'bronze_load_store_master.sql',     'STG_STORE_MASTER',     TRUE),
        ('bronze', 1, 'bronze_load_item_master.sql',      'STG_ITEM_MASTER',      TRUE),
        ('bronze', 1, 'bronze_load_inventory_master.sql', 'STG_INVENTORY_MASTER', TRUE),
        ('bronze', 1, 'bronze_load_pos_sales.sql',        'STG_POS_SALES',        TRUE),
        ('bronze', 1, 'bronze_load_price_master.sql',     'STG_PRICE_MASTER',     TRUE),
        ('silver', 1, 'silver_load_store_master.sql',     'TMP_STORE_MASTER',     TRUE),
        ('silver', 1, 'silver_load_item_master.sql',      'TMP_ITEM_MASTER',      TRUE),
        ('silver', 2, 'silver_load_inventory_master.sql', 'TMP_INVENTORY_MASTER', TRUE),
        ('silver', 2, 'silver_load_pos_sales.sql',        'TMP_POS_SALES',        TRUE),
        ('silver', 2, 'silver_load_price_master.sql',     'TMP_PRICE_MASTER',     TRUE),
        ('gold',   1, 'dim_company_load.sql',             'DIM_COMPANY',          TRUE),
        ('gold',   1, 'dim_state_load.sql',               'DIM_STATE',            TRUE),
        ('gold',   1, 'dim_customer_load.sql',            'DIM_CUSTOMER',         TRUE),
        ('gold',   2, 'dim_department_load.sql',          'DIM_DEPARTMENT',       TRUE),
        ('gold',   2, 'dim_district_load.sql',            'DIM_DISTRICT',         TRUE),
        ('gold',   3, 'dim_class_load.sql',               'DIM_CLASS',            TRUE),
        ('gold',   3, 'dim_location_load.sql',            'DIM_LOCATION',         TRUE),
        ('gold',   4, 'dim_item_load.sql',                'DIM_ITEM',             TRUE),
        ('gold',   5, 'fct_sales_load.sql',               'FCT_SALES',            TRUE),
        ('gold',   5, 'fct_price_load.sql',               'FCT_PRICE',            TRUE),
        ('gold',   5, 'fct_inventory_load.sql',           'FCT_INVENTORY',        TRUE),
        ('gold',   6, 'fct_sales_per_week_load.sql',      'FCT_SALES_PER_WEEK',   TRUE)
) AS s(LAYER, PHASE, SCRIPT_NAME, TARGET_TABLE, IS_ACTIVE)
ON t.SCRIPT_NAME = s.SCRIPT_NAME
WHEN NOT MATCHED THEN INSERT (LAYER, PHASE, SCRIPT_NAME, TARGET_TABLE, IS_ACTIVE)
VALUES (s.LAYER, s.PHASE, s.SCRIPT_NAME, s.TARGET_TABLE, s.IS_ACTIVE);

-- ----------------------------------------------------------------------
-- C_BATCH_LOG — append-only per-task execution log (ENRICHED)
-- New: SCRIPT_NAME, LAYER, PHASE columns
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MODULE2_ETL.CONTROL.C_BATCH_LOG (
    LOG_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
    RUN_ID          VARCHAR       NOT NULL,
    BATCH_ID        DATE          NOT NULL,
    TASK_ID         VARCHAR       NOT NULL,
    SCRIPT_NAME     VARCHAR,                -- NEW: resolved from catalog
    LAYER           VARCHAR,                -- NEW
    PHASE           NUMBER,                 -- NEW
    STATUS          VARCHAR       NOT NULL,
    ELAPSED_MS      NUMBER,
    ERROR_MESSAGE   VARCHAR,
    STARTED_AT      TIMESTAMP_NTZ,
    FINISHED_AT     TIMESTAMP_NTZ,
    CONSTRAINT PK_C_BATCH_LOG PRIMARY KEY (LOG_ID)
);

-- ----------------------------------------------------------------------
-- C_LOAD_DATES — append-only history of (target, batch) load events
-- Replaces the old watermark semantic.
-- ----------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS MODULE2_ETL.CONTROL.C_LOAD_DATES (
    LOAD_ID        NUMBER AUTOINCREMENT START 1 INCREMENT 1 NOT NULL,
    TARGET_TABLE   VARCHAR       NOT NULL,
    LAYER          VARCHAR       NOT NULL,
    BATCH_ID       DATE          NOT NULL,   -- which ds was loaded
    LOADED_AT      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP(),  -- when it actually loaded
    RUN_ID         VARCHAR       NOT NULL,
    CONSTRAINT PK_C_LOAD_DATES PRIMARY KEY (LOAD_ID),
    CONSTRAINT UQ_C_LOAD_DATES UNIQUE (TARGET_TABLE, BATCH_ID, RUN_ID)
);