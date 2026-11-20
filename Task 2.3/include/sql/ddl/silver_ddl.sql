-- ============================================================
-- SILVER LAYER DDL  (with _ROW_HASH change-detection column)
-- Database : MODULE2_ETL
-- Schemas  : silver, control
-- ============================================================

USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS control;

-- ============================================================
-- CONTROL.C_REJ_TABLE
-- Rejected rows from all Silver TMP loads.
-- DETAILS holds the full source row (OBJECT_CONSTRUCT(*)).
-- ============================================================
CREATE TABLE IF NOT EXISTS control.C_REJ_TABLE (
    REJ_ID          NUMBER(38,0)    NOT NULL AUTOINCREMENT,
    SRC_FILE_NM     VARCHAR(255)    NOT NULL,
    BATCH_ID        DATE            NOT NULL,
    TARGET_TABLE    VARCHAR(200)    NOT NULL,
    FAILURE_TS      TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    REASON_CODE     VARCHAR(50)     NOT NULL,
    REASON          VARCHAR(1000)   NOT NULL,
    DETAILS         VARIANT         NOT NULL,

    CONSTRAINT PK_C_REJ_TABLE
        PRIMARY KEY (REJ_ID),
    CONSTRAINT CK_C_REJ_REASON_CODE
        CHECK (REASON_CODE IN (
            'DUP_BUSINESS_KEY',
            'NEGATIVE_AMOUNT',
            'NULL_REQUIRED_FIELD',
            'REFERENTIAL_INTEGRITY_FAILURE',
            'TYPE_CAST_FAILURE',
            'OVERLAPPING_PRICE_RANGE'
        ))
)
COMMENT = 'Control: rejected rows from Silver layer loads';

-- ============================================================
-- CONTROL.C_BATCH_RECON
-- Per-batch reconciliation counts.
-- Invariant: BRONZE_CNT = TMP_CNT + REJ_CNT  (VARIANCE = 0)
-- ============================================================
CREATE TABLE IF NOT EXISTS control.C_BATCH_RECON (
    BATCH_ID        DATE            NOT NULL,
    SRC_FILE_NM     VARCHAR(255)    NOT NULL,
    TARGET_TABLE    VARCHAR(200)    NOT NULL,
    BRONZE_CNT      NUMBER(38,0)    NOT NULL,
    TMP_CNT         NUMBER(38,0)    NOT NULL,
    REJ_CNT         NUMBER(38,0)    NOT NULL,
    VARIANCE        NUMBER(38,0)    NOT NULL,
    STATUS          VARCHAR(20)     NOT NULL,
    RECON_TS        TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),

    CONSTRAINT PK_C_BATCH_RECON
        PRIMARY KEY (BATCH_ID, SRC_FILE_NM, TARGET_TABLE),
    CONSTRAINT CK_C_BATCH_RECON_STATUS
        CHECK (STATUS IN ('PASSED','FAILED')),
    CONSTRAINT CK_C_BATCH_RECON_VARIANCE
        CHECK (VARIANCE = 0 OR STATUS = 'FAILED')
)
COMMENT = 'Control: per-batch reconciliation between Bronze, TMP and REJ';

-- ============================================================
-- SILVER.TMP_STORE_MASTER
-- Grain: 1 row per LOCATION_ID (business key)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.TMP_STORE_MASTER (
    -- business columns
    LOCATION_ID         VARCHAR(20)     NOT NULL,
    LOCATION_NAME       VARCHAR(255)    NOT NULL,
    ADDRESS_LINE        VARCHAR(500)    NOT NULL,
    DISTRICT_ID         VARCHAR(20)     NOT NULL,
    DISTRICT_NAME       VARCHAR(100)    NOT NULL,
    DISTRICT_CODE       VARCHAR(10)     NOT NULL,
    STATE_ID            VARCHAR(20)     NOT NULL,
    STATE_NAME          VARCHAR(100)    NOT NULL,
    STATE_CODE          VARCHAR(10)     NOT NULL,

    -- silver audit columns
    _SRC_FILE_NM        VARCHAR(255)    NOT NULL,
    _BATCH_ID           DATE            NOT NULL,
    _LOAD_TS            TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH           VARCHAR(32)     NOT NULL,

    CONSTRAINT PK_TMP_STORE_MASTER
        PRIMARY KEY (LOCATION_ID)
)
COMMENT = 'Silver staging: validated store master';

-- ============================================================
-- SILVER.TMP_ITEM_MASTER
-- Grain: 1 row per (ITEM_ID, CLASS_ID, DEPARTMENT_ID)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.TMP_ITEM_MASTER (
    -- business columns
    ITEM_ID                 VARCHAR(20)     NOT NULL,
    ITEM_NAME               VARCHAR(255)    NOT NULL,
    COLOR                   VARCHAR(50)     NOT NULL,
    SIZE                    VARCHAR(50)     NOT NULL,
    CLASS_ID                VARCHAR(20)     NOT NULL,
    CLASS_NAME              VARCHAR(100)    NOT NULL,
    CLASS_DESCRIPTION       VARCHAR(500)    NOT NULL,
    DEPARTMENT_ID           VARCHAR(20)     NOT NULL,
    DEPARTMENT_NAME         VARCHAR(100)    NOT NULL,
    DEPARTMENT_MANAGER_NAME VARCHAR(255)    NOT NULL,
    COMPANY_ID              VARCHAR(20)     NOT NULL,
    COMPANY_NAME            VARCHAR(255)    NOT NULL,
    COMPANY_TYPE            VARCHAR(50)     NOT NULL,

    -- silver audit columns
    _SRC_FILE_NM            VARCHAR(255)    NOT NULL,
    _BATCH_ID               DATE            NOT NULL,
    _LOAD_TS                TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH               VARCHAR(32)     NOT NULL,

    CONSTRAINT PK_TMP_ITEM_MASTER
        PRIMARY KEY (ITEM_ID, CLASS_ID, DEPARTMENT_ID)
)
COMMENT = 'Silver staging: validated item master';

-- ============================================================
-- SILVER.TMP_INVENTORY_MASTER
-- Grain: 1 row per (INVENTORY_DT, ITEM_ID, LOCATION_ID)
-- (Historical daily snapshots preserved.)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.TMP_INVENTORY_MASTER (
    -- business columns
    INVENTORY_DT        DATE            NOT NULL,
    ITEM_ID             VARCHAR(20)     NOT NULL,
    LOCATION_ID         VARCHAR(20)     NOT NULL,
    STOCK_ON_HAND       NUMBER(38,0)    NOT NULL,
    IN_TRANSIT          NUMBER(38,0)    NOT NULL,

    -- silver audit columns
    _SRC_FILE_NM        VARCHAR(255)    NOT NULL,
    _BATCH_ID           DATE            NOT NULL,
    _LOAD_TS            TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH           VARCHAR(32)     NOT NULL,

    CONSTRAINT PK_TMP_INVENTORY_MASTER
        PRIMARY KEY (INVENTORY_DT, ITEM_ID, LOCATION_ID),
    CONSTRAINT CK_TMP_INV_STOCK_ON_HAND
        CHECK (STOCK_ON_HAND >= 0),
    CONSTRAINT CK_TMP_INV_IN_TRANSIT
        CHECK (IN_TRANSIT >= 0)
)
COMMENT = 'Silver staging: validated inventory snapshot';

-- ============================================================
-- SILVER.TMP_POS_SALES
-- Grain: 1 row per (TRANSACTION_ID, TRANSACTION_LINE_NUMBER)
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.TMP_POS_SALES (
    -- business columns
    TRANSACTION_ID              VARCHAR(20)     NOT NULL,
    TRANSACTION_LINE_NUMBER     NUMBER(38,0)    NOT NULL,
    SALES_DT                    DATE            NOT NULL,
    ITEM_ID                     VARCHAR(20)     NOT NULL,
    LOCATION_ID                 VARCHAR(20)     NOT NULL,
    CUSTOMER_ID                 VARCHAR(20)     NOT NULL,
    CUSTOMER_NAME               VARCHAR(255)    NOT NULL,
    CUSTOMER_EMAIL              VARCHAR(255)    NOT NULL,
    CUSTOMER_PHONE              VARCHAR(30)     NOT NULL,
    SLS_QTY                     NUMBER(38,0)    NOT NULL,
    SLS_AMT                     NUMBER(18,2)    NOT NULL,

    -- silver audit columns
    _SRC_FILE_NM                VARCHAR(255)    NOT NULL,
    _BATCH_ID                   DATE            NOT NULL,
    _LOAD_TS                    TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH                   VARCHAR(32)     NOT NULL,

    CONSTRAINT PK_TMP_POS_SALES
        PRIMARY KEY (TRANSACTION_ID, TRANSACTION_LINE_NUMBER),
    CONSTRAINT CK_TMP_POS_SLS_QTY
        CHECK (SLS_QTY >= 0),
    CONSTRAINT CK_TMP_POS_SLS_AMT
        CHECK (SLS_AMT >= 0)
)
COMMENT = 'Silver staging: validated POS sales lines';

-- ============================================================
-- SILVER.TMP_PRICE_MASTER
-- Grain: 1 row per (ITEM_ID, LOCATION_ID, EFF_FROM_DATE)
-- Versioned (SCD2 source for Gold).
-- ============================================================
CREATE TABLE IF NOT EXISTS silver.TMP_PRICE_MASTER (
    -- business columns
    PRICE_DT            DATE            NOT NULL,
    ITEM_ID             VARCHAR(20)     NOT NULL,
    LOCATION_ID         VARCHAR(20)     NOT NULL,
    UNIT_PRICE_AMT      NUMBER(18,2)    NOT NULL,
    EFF_FROM_DATE       DATE            NOT NULL,
    EFF_TO_DATE         DATE            NOT NULL,

    -- silver audit columns
    _SRC_FILE_NM        VARCHAR(255)    NOT NULL,
    _BATCH_ID           DATE            NOT NULL,
    _LOAD_TS            TIMESTAMP_NTZ(9) NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    _ROW_HASH           VARCHAR(32)     NOT NULL,

    CONSTRAINT PK_TMP_PRICE_MASTER
        PRIMARY KEY (ITEM_ID, LOCATION_ID, EFF_FROM_DATE),
    CONSTRAINT CK_TMP_PRICE_UNIT_PRICE_AMT
        CHECK (UNIT_PRICE_AMT >= 0),
    CONSTRAINT CK_TMP_PRICE_EFF_DATES
        CHECK (EFF_TO_DATE >= EFF_FROM_DATE)
)
COMMENT = 'Silver staging: validated price master (SCD2 source for Gold)';