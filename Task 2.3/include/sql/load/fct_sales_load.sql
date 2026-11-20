USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: FCT_SALES
-- Source : silver.TMP_POS_SALES
-- Grain  : (TRANSACTION_ID, TRANSACTION_LINE_NUMBER)  -- per POS line
-- FKs    : DAY_KY, ITEM_KY, LOCATION_KY, CUSTOMER_KY
--          all resolved as-of SALES_DT (SCD2 range join for item/loc/cust)
-- Rejects: rows whose dim FKs don't resolve -> control.C_REJ_TABLE
--          SRC_ROW_NUM = NULL (silver doesn't carry it);
--          _ROW_HASH preserved in DETAILS:SRC_ROW_HASH for traceability.
-- MERGE  : idempotent on (TRANSACTION_ID, TRANSACTION_LINE_NUMBER)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Build staged fact rows with resolved dim FKs + RI flag.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_FCT_SALES_STG AS
SELECT
    src.TRANSACTION_ID,
    src.TRANSACTION_LINE_NUMBER,
    src.SALES_DT,
    src.ITEM_ID,
    src.LOCATION_ID,
    src.CUSTOMER_ID,
    src.SLS_QTY,
    src.SLS_AMT,
    src._SRC_FILE_NM            AS SRC_FILE_NM,
    src._ROW_HASH               AS SRC_ROW_HASH,
    CAST(NULL AS NUMBER)        AS SRC_ROW_NUM,

    d.DAY_KY,
    i.ITEM_KY,
    l.LOCATION_KY,
    c.CUSTOMER_KY,

    (d.DAY_KY      IS NULL
  OR i.ITEM_KY     IS NULL
  OR l.LOCATION_KY IS NULL
  OR c.CUSTOMER_KY IS NULL) AS RI_FAIL_FLG

FROM silver.TMP_POS_SALES src

LEFT JOIN gold.DIM_DAY d
       ON d.CALENDAR_DT = src.SALES_DT

LEFT JOIN gold.DIM_ITEM i
       ON i.ITEM_ID = src.ITEM_ID
      AND src.SALES_DT BETWEEN i.EFF_FROM_DATE AND i.EFF_TO_DATE

LEFT JOIN gold.DIM_LOCATION l
       ON l.LOCATION_ID = src.LOCATION_ID
      AND src.SALES_DT BETWEEN l.EFF_FROM_DATE AND l.EFF_TO_DATE

LEFT JOIN gold.DIM_CUSTOMER c
       ON c.CUSTOMER_ID = src.CUSTOMER_ID
      AND src.SALES_DT BETWEEN c.EFF_FROM_DATE AND c.EFF_TO_DATE

WHERE src._BATCH_ID = DATE '{{ ds }}';

-- ----------------------------------------------------------------------------
-- 2. Write join-miss rows to the reject table.
-- ----------------------------------------------------------------------------
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT
    s.SRC_FILE_NM,
    DATE '{{ ds }}',
    'FCT_SALES',
    'REFERENTIAL_INTEGRITY_FAILURE',
    'DAY_KY / ITEM_KY / LOCATION_KY / CUSTOMER_KY could not be resolved as-of SALES_DT',
    OBJECT_CONSTRUCT(
        'TRANSACTION_ID',          s.TRANSACTION_ID,
        'TRANSACTION_LINE_NUMBER', s.TRANSACTION_LINE_NUMBER,
        'SALES_DT',                s.SALES_DT,
        'ITEM_ID',                 s.ITEM_ID,
        'LOCATION_ID',             s.LOCATION_ID,
        'CUSTOMER_ID',             s.CUSTOMER_ID,
        'SLS_QTY',                 s.SLS_QTY,
        'SLS_AMT',                 s.SLS_AMT,
        'DAY_KY',                  s.DAY_KY,
        'ITEM_KY',                 s.ITEM_KY,
        'LOCATION_KY',             s.LOCATION_KY,
        'CUSTOMER_KY',             s.CUSTOMER_KY,
        'SRC_FILE_NM',             s.SRC_FILE_NM,
        'SRC_ROW_HASH',            s.SRC_ROW_HASH
    )
FROM TMP_FCT_SALES_STG s
WHERE s.RI_FAIL_FLG = TRUE;

-- ----------------------------------------------------------------------------
-- 3. MERGE valid rows into FCT_SALES — idempotent on the grain.
-- ----------------------------------------------------------------------------
MERGE INTO gold.FCT_SALES t
USING (
    SELECT
        TRANSACTION_ID, TRANSACTION_LINE_NUMBER,
        DAY_KY, ITEM_KY, LOCATION_KY, CUSTOMER_KY,
        SLS_QTY, SLS_AMT
    FROM TMP_FCT_SALES_STG
    WHERE RI_FAIL_FLG = FALSE
) s
ON  t.TRANSACTION_ID          = s.TRANSACTION_ID
AND t.TRANSACTION_LINE_NUMBER = s.TRANSACTION_LINE_NUMBER

WHEN MATCHED AND (
        t.DAY_KY      <> s.DAY_KY
     OR t.ITEM_KY     <> s.ITEM_KY
     OR t.LOCATION_KY <> s.LOCATION_KY
     OR t.CUSTOMER_KY <> s.CUSTOMER_KY
     OR COALESCE(t.SLS_QTY, -1) <> COALESCE(s.SLS_QTY, -1)
     OR COALESCE(t.SLS_AMT, -1) <> COALESCE(s.SLS_AMT, -1)
) THEN UPDATE SET
    DAY_KY      = s.DAY_KY,
    ITEM_KY     = s.ITEM_KY,
    LOCATION_KY = s.LOCATION_KY,
    CUSTOMER_KY = s.CUSTOMER_KY,
    SLS_QTY     = s.SLS_QTY,
    SLS_AMT     = s.SLS_AMT,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}',
    SRC_SYS_CD  = 'TMP_POS_SALES'

WHEN NOT MATCHED THEN INSERT (
    TRANSACTION_ID, TRANSACTION_LINE_NUMBER,
    DAY_KY, ITEM_KY, LOCATION_KY, CUSTOMER_KY,
    SLS_QTY, SLS_AMT,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID
)
VALUES (
    s.TRANSACTION_ID, s.TRANSACTION_LINE_NUMBER,
    s.DAY_KY, s.ITEM_KY, s.LOCATION_KY, s.CUSTOMER_KY,
    s.SLS_QTY, s.SLS_AMT,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'TMP_POS_SALES', DATE '{{ ds }}'
);

-- ----------------------------------------------------------------------------
-- 4. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_FCT_SALES_STG;