USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: FCT_PRICE
-- Source : silver.TMP_PRICE_MASTER
-- Grain  : (DAY_KY, ITEM_KY, LOCATION_KY)
-- FKs    : DAY_KY, ITEM_KY, LOCATION_KY — all resolved as-of PRICE_DT
-- EFF_*  : PRICE VALIDITY copied from silver (Meaning A).
--          Each silver price version becomes its own fact row keyed by DAY_KY;
--          versions coexist permanently — no SCD2 on the fact.
-- Rejects: rows whose dim FKs don't resolve -> control.C_REJ_TABLE
--          SRC_ROW_NUM = NULL (silver doesn't carry it);
--          _ROW_HASH preserved in DETAILS:SRC_ROW_HASH for traceability.
-- MERGE  : idempotent on (DAY_KY, ITEM_KY, LOCATION_KY)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Build staged fact rows with resolved dim FKs + RI flag.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_FCT_PRICE_STG AS
SELECT
    src.PRICE_DT,
    src.ITEM_ID,
    src.LOCATION_ID,
    src.UNIT_PRICE_AMT,
    src.EFF_FROM_DATE           AS SRC_EFF_FROM_DATE,
    src.EFF_TO_DATE             AS SRC_EFF_TO_DATE,
    src._SRC_FILE_NM            AS SRC_FILE_NM,
    src._ROW_HASH               AS SRC_ROW_HASH,
    CAST(NULL AS NUMBER)        AS SRC_ROW_NUM,

    d.DAY_KY,
    i.ITEM_KY,
    l.LOCATION_KY,

    (d.DAY_KY IS NULL OR i.ITEM_KY IS NULL OR l.LOCATION_KY IS NULL) AS RI_FAIL_FLG

FROM silver.TMP_PRICE_MASTER src

LEFT JOIN gold.DIM_DAY d
       ON d.CALENDAR_DT = src.PRICE_DT

LEFT JOIN gold.DIM_ITEM i
       ON i.ITEM_ID = src.ITEM_ID
      AND src.PRICE_DT BETWEEN i.EFF_FROM_DATE AND i.EFF_TO_DATE

LEFT JOIN gold.DIM_LOCATION l
       ON l.LOCATION_ID = src.LOCATION_ID
      AND src.PRICE_DT BETWEEN l.EFF_FROM_DATE AND l.EFF_TO_DATE

WHERE src._BATCH_ID = DATE '{{ ds }}';

-- ----------------------------------------------------------------------------
-- 2. Write join-miss rows to the reject table.
-- ----------------------------------------------------------------------------
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT
    s.SRC_FILE_NM,
    DATE '{{ ds }}',
    'FCT_PRICE',
    'REFERENTIAL_INTEGRITY_FAILURE',
    'DAY_KY / ITEM_KY / LOCATION_KY could not be resolved as-of PRICE_DT',
    OBJECT_CONSTRUCT(
        'PRICE_DT',       s.PRICE_DT,
        'ITEM_ID',        s.ITEM_ID,
        'LOCATION_ID',    s.LOCATION_ID,
        'UNIT_PRICE_AMT', s.UNIT_PRICE_AMT,
        'EFF_FROM_DATE',  s.SRC_EFF_FROM_DATE,
        'EFF_TO_DATE',    s.SRC_EFF_TO_DATE,
        'DAY_KY',         s.DAY_KY,
        'ITEM_KY',        s.ITEM_KY,
        'LOCATION_KY',    s.LOCATION_KY,
        'SRC_FILE_NM',    s.SRC_FILE_NM,
        'SRC_ROW_HASH',   s.SRC_ROW_HASH
    )
FROM TMP_FCT_PRICE_STG s
WHERE s.RI_FAIL_FLG = TRUE;

-- ----------------------------------------------------------------------------
-- 3. MERGE valid rows into FCT_PRICE — idempotent on the grain.
-- ----------------------------------------------------------------------------
MERGE INTO gold.FCT_PRICE t
USING (
    SELECT
        DAY_KY, ITEM_KY, LOCATION_KY,
        UNIT_PRICE_AMT,
        SRC_EFF_FROM_DATE, SRC_EFF_TO_DATE
    FROM TMP_FCT_PRICE_STG
    WHERE RI_FAIL_FLG = FALSE
) s
ON  t.DAY_KY      = s.DAY_KY
AND t.ITEM_KY     = s.ITEM_KY
AND t.LOCATION_KY = s.LOCATION_KY

WHEN MATCHED AND (
        COALESCE(t.UNIT_PRICE_AMT, -1) <> COALESCE(s.UNIT_PRICE_AMT, -1)
     OR COALESCE(t.EFF_FROM_DATE, DATE '1900-01-01') <> COALESCE(s.SRC_EFF_FROM_DATE, DATE '1900-01-01')
     OR COALESCE(t.EFF_TO_DATE,   DATE '9999-12-31') <> COALESCE(s.SRC_EFF_TO_DATE,   DATE '9999-12-31')
) THEN UPDATE SET
    UNIT_PRICE_AMT = s.UNIT_PRICE_AMT,
    EFF_FROM_DATE  = s.SRC_EFF_FROM_DATE,
    EFF_TO_DATE    = s.SRC_EFF_TO_DATE,
    REC_UPDT_TS    = CURRENT_TIMESTAMP(),
    BATCH_ID       = DATE '{{ ds }}',
    SRC_SYS_CD     = 'TMP_PRICE_MASTER'

WHEN NOT MATCHED THEN INSERT (
    DAY_KY, ITEM_KY, LOCATION_KY,
    UNIT_PRICE_AMT,
    EFF_FROM_DATE, EFF_TO_DATE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID
)
VALUES (
    s.DAY_KY, s.ITEM_KY, s.LOCATION_KY,
    s.UNIT_PRICE_AMT,
    s.SRC_EFF_FROM_DATE, s.SRC_EFF_TO_DATE,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'TMP_PRICE_MASTER', DATE '{{ ds }}'
);

-- ----------------------------------------------------------------------------
-- 4. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_FCT_PRICE_STG;