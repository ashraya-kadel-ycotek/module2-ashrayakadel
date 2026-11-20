USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA bronze;

-- ============================================================
-- 1. CREATE A TEMPORARY STAGE FOR THE DAILY FILES
-- ============================================================
CREATE OR REPLACE TEMPORARY STAGE BRONZE.TMP_DAILY_STAGE
    FILE_FORMAT = (
        TYPE = CSV
        FIELD_DELIMITER = ','
        SKIP_HEADER = 1
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('', 'NULL')
        TRIM_SPACE = TRUE
    );


PUT file:///opt/airflow/data/store_master_{{ ds_nodash }}.csv     @BRONZE.TMP_DAILY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file:///opt/airflow/data/item_master_{{ ds_nodash }}.csv      @BRONZE.TMP_DAILY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file:///opt/airflow/data/inventory_master_{{ ds_nodash }}.csv @BRONZE.TMP_DAILY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file:///opt/airflow/data/pos_sales_{{ ds_nodash }}.csv        @BRONZE.TMP_DAILY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file:///opt/airflow/data/price_master_{{ ds_nodash }}.csv     @BRONZE.TMP_DAILY_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
-- ============================================================
-- 2. UPLOAD FILES (paths templated by Airflow; no creds here)
-- ============================================================
-- ============================================================
-- 3. COPY INTO STAGING TABLES WITH AUDIT COLUMNS
--    - All business columns are VARCHAR (no casting, no drops)
--    - Idempotent: DELETE existing batch, then COPY (AC1)
--    - ON_ERROR = 'CONTINUE' so bad rows pass through to Silver
--      for quarantining (AC3)
--    - Wrapped in BEGIN/COMMIT per table
-- ============================================================

-- ------------------------------------------------------------
-- STG_STORE_MASTER
-- ------------------------------------------------------------
BEGIN;

DELETE FROM BRONZE.STG_STORE_MASTER
WHERE BATCH_ID = '{{ ds }}'::DATE;

COPY INTO BRONZE.STG_STORE_MASTER (
    LOCATION_ID, LOCATION_NAME, ADDRESS_LINE, DISTRICT_ID, DISTRICT_NAME,
    DISTRICT_CODE, STATE_ID, STATE_NAME, STATE_CODE,
    SRC_FILE_NM, SRC_ROW_NUM, BATCH_ID
)
FROM (
    SELECT
        t.$1, t.$2, t.$3, t.$4, t.$5,
        t.$6, t.$7, t.$8, t.$9,
        METADATA$FILENAME::VARCHAR(255),
        METADATA$FILE_ROW_NUMBER,
        '{{ ds }}'::DATE
    FROM @BRONZE.TMP_DAILY_STAGE/store_master_{{ ds_nodash }}.csv.gz t
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COMMIT;

-- ------------------------------------------------------------
-- STG_ITEM_MASTER
-- ------------------------------------------------------------
BEGIN;

DELETE FROM BRONZE.STG_ITEM_MASTER
WHERE BATCH_ID = '{{ ds }}'::DATE;

COPY INTO BRONZE.STG_ITEM_MASTER (
    ITEM_ID, ITEM_NAME, COLOR, SIZE, CLASS_ID, CLASS_NAME, CLASS_DESCRIPTION,
    DEPARTMENT_ID, DEPARTMENT_NAME, DEPARTMENT_MANAGER_NAME, COMPANY_ID, COMPANY_NAME, COMPANY_TYPE,
    SRC_FILE_NM, SRC_ROW_NUM, BATCH_ID
)
FROM (
    SELECT
        t.$1, t.$2, t.$3, t.$4, t.$5, t.$6, t.$7,
        t.$8, t.$9, t.$10, t.$11, t.$12, t.$13,
        METADATA$FILENAME::VARCHAR(255),
        METADATA$FILE_ROW_NUMBER,
        '{{ ds }}'::DATE
    FROM @BRONZE.TMP_DAILY_STAGE/item_master_{{ ds_nodash }}.csv.gz t
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COMMIT;

-- ------------------------------------------------------------
-- STG_INVENTORY_MASTER
-- ------------------------------------------------------------
BEGIN;

DELETE FROM BRONZE.STG_INVENTORY_MASTER
WHERE BATCH_ID = '{{ ds }}'::DATE;

COPY INTO BRONZE.STG_INVENTORY_MASTER (
    INVENTORY_DT, ITEM_ID, LOCATION_ID, STOCK_ON_HAND, IN_TRANSIT,
    SRC_FILE_NM, SRC_ROW_NUM, BATCH_ID
)
FROM (
    SELECT
        t.$1, t.$2, t.$3, t.$4, t.$5,
        METADATA$FILENAME::VARCHAR(255),
        METADATA$FILE_ROW_NUMBER,
        '{{ ds }}'::DATE
    FROM @BRONZE.TMP_DAILY_STAGE/inventory_master_{{ ds_nodash }}.csv.gz t
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COMMIT;

-- ------------------------------------------------------------
-- STG_POS_SALES
-- ------------------------------------------------------------
BEGIN;

DELETE FROM BRONZE.STG_POS_SALES
WHERE BATCH_ID = '{{ ds }}'::DATE;

COPY INTO BRONZE.STG_POS_SALES (
    TRANSACTION_ID, TRANSACTION_LINE_NUMBER, SALES_DT, ITEM_ID, LOCATION_ID,
    CUSTOMER_ID, CUSTOMER_NAME, CUSTOMER_EMAIL, CUSTOMER_PHONE, SLS_QTY, SLS_AMT,
    SRC_FILE_NM, SRC_ROW_NUM, BATCH_ID
)
FROM (
    SELECT
        t.$1, t.$2, t.$3, t.$4, t.$5,
        t.$6, t.$7, t.$8, t.$9, t.$10, t.$11,
        METADATA$FILENAME::VARCHAR(255),
        METADATA$FILE_ROW_NUMBER,
        '{{ ds }}'::DATE
    FROM @BRONZE.TMP_DAILY_STAGE/pos_sales_{{ ds_nodash }}.csv.gz t
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COMMIT;

-- ------------------------------------------------------------
-- STG_PRICE_MASTER
-- ------------------------------------------------------------
BEGIN;

DELETE FROM BRONZE.STG_PRICE_MASTER
WHERE BATCH_ID = '{{ ds }}'::DATE;

COPY INTO BRONZE.STG_PRICE_MASTER (
    PRICE_DT, ITEM_ID, LOCATION_ID, UNIT_PRICE_AMT, EFF_FROM_DATE, EFF_TO_DATE,
    SRC_FILE_NM, SRC_ROW_NUM, BATCH_ID
)
FROM (
    SELECT
        t.$1, t.$2, t.$3, t.$4, t.$5, t.$6,
        METADATA$FILENAME::VARCHAR(255),
        METADATA$FILE_ROW_NUMBER,
        '{{ ds }}'::DATE
    FROM @BRONZE.TMP_DAILY_STAGE/price_master_{{ ds_nodash }}.csv.gz t
)
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"')
ON_ERROR = 'CONTINUE';

COMMIT;