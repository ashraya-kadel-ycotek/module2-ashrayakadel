USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
CREATE SCHEMA IF NOT EXISTS bronze;
USE SCHEMA bronze;

-- ============================================================
-- BRONZE STAGING TABLES
-- Purpose : land EVERY source row, no casting, no dropping.
-- Rule    : all business columns VARCHAR. Casting happens in Silver,
--           so cast failures become rejectable rows (AC #3) and
--           recon ties out (AC #5).
-- Audit   : SRC_FILE_NM, SRC_ROW_NUM (line within file), BATCH_ID.
-- Grain   : one row per source CSV line, per batch.
-- ============================================================

CREATE TABLE IF NOT EXISTS BRONZE.STG_STORE_MASTER (
    LOCATION_ID     VARCHAR(20),
    LOCATION_NAME   VARCHAR(255),
    ADDRESS_LINE    VARCHAR(500),
    DISTRICT_ID     VARCHAR(20),
    DISTRICT_NAME   VARCHAR(100),
    DISTRICT_CODE   VARCHAR(10),
    STATE_ID        VARCHAR(20),
    STATE_NAME      VARCHAR(100),
    STATE_CODE      VARCHAR(10),

    SRC_FILE_NM     VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM     NUMBER(38,0)    NOT NULL,
    BATCH_ID        DATE            NOT NULL
)
COMMENT = 'Bronze staging: raw store master (all VARCHAR, no casting)';

CREATE TABLE IF NOT EXISTS BRONZE.STG_ITEM_MASTER (
    ITEM_ID                 VARCHAR(20),
    ITEM_NAME               VARCHAR(255),
    COLOR                   VARCHAR(50),
    SIZE                    VARCHAR(50),
    CLASS_ID                VARCHAR(20),
    CLASS_NAME              VARCHAR(100),
    CLASS_DESCRIPTION       VARCHAR(500),
    DEPARTMENT_ID           VARCHAR(20),
    DEPARTMENT_NAME         VARCHAR(100),
    DEPARTMENT_MANAGER_NAME VARCHAR(255),
    COMPANY_ID              VARCHAR(20),
    COMPANY_NAME            VARCHAR(255),
    COMPANY_TYPE            VARCHAR(50),

    SRC_FILE_NM             VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM             NUMBER(38,0)    NOT NULL,
    BATCH_ID                DATE            NOT NULL
)
COMMENT = 'Bronze staging: raw item master (all VARCHAR, no casting)';

CREATE TABLE IF NOT EXISTS BRONZE.STG_INVENTORY_MASTER (
    INVENTORY_DT    VARCHAR(20),
    ITEM_ID         VARCHAR(20),
    LOCATION_ID     VARCHAR(20),
    STOCK_ON_HAND   VARCHAR(50),
    IN_TRANSIT      VARCHAR(50),

    SRC_FILE_NM     VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM     NUMBER(38,0)    NOT NULL,
    BATCH_ID        DATE            NOT NULL
)
COMMENT = 'Bronze staging: raw inventory snapshot (all VARCHAR, no casting)';

CREATE TABLE IF NOT EXISTS BRONZE.STG_POS_SALES (
    TRANSACTION_ID              VARCHAR(20),
    TRANSACTION_LINE_NUMBER     VARCHAR(20),
    SALES_DT                    VARCHAR(20),
    ITEM_ID                     VARCHAR(20),
    LOCATION_ID                 VARCHAR(20),
    CUSTOMER_ID                 VARCHAR(20),
    CUSTOMER_NAME               VARCHAR(255),
    CUSTOMER_EMAIL              VARCHAR(255),
    CUSTOMER_PHONE              VARCHAR(30),
    SLS_QTY                     VARCHAR(50),
    SLS_AMT                     VARCHAR(50),

    SRC_FILE_NM                 VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM                 NUMBER(38,0)    NOT NULL,
    BATCH_ID                    DATE            NOT NULL
)
COMMENT = 'Bronze staging: raw POS sales lines (all VARCHAR, no casting)';

CREATE TABLE IF NOT EXISTS BRONZE.STG_PRICE_MASTER (
    PRICE_DT        VARCHAR(20),
    ITEM_ID         VARCHAR(20),
    LOCATION_ID     VARCHAR(20),
    UNIT_PRICE_AMT  VARCHAR(50),
    EFF_FROM_DATE   VARCHAR(20),
    EFF_TO_DATE     VARCHAR(20),

    SRC_FILE_NM     VARCHAR(255)    NOT NULL,
    SRC_ROW_NUM     NUMBER(38,0)    NOT NULL,
    BATCH_ID        DATE            NOT NULL
)
COMMENT = 'Bronze staging: raw price master (all VARCHAR, no casting)';