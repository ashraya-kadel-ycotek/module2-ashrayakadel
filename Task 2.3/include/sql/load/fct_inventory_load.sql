USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: FCT_INVENTORY
-- Source : silver.TMP_INVENTORY_MASTER
-- Grain  : (DAY_KY, ITEM_KY, LOCATION_KY)
-- FKs    : DAY_KY, ITEM_KY, LOCATION_KY — all resolved as-of INVENTORY_DT
-- EFF_*  : from DIM_ITEM (silver has no EFF_* cols)
-- Rejects: rows whose dim FKs don't resolve -> control.C_REJ_TABLE
--          SRC_ROW_NUM = NULL (silver doesn't carry it);
--          _ROW_HASH is preserved in DETAILS:SRC_ROW_HASH for traceability.
-- MERGE  : idempotent on (DAY_KY, ITEM_KY, LOCATION_KY)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Build staged fact rows with resolved dim FKs + RI flag.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_FCT_INVENTORY_STG AS
SELECT
    src.INVENTORY_DT,
    src.ITEM_ID,
    src.LOCATION_ID,
    src.STOCK_ON_HAND,
    src.IN_TRANSIT,
    i.EFF_FROM_DATE             AS SRC_EFF_FROM_DATE,
    i.EFF_TO_DATE               AS SRC_EFF_TO_DATE,
    src._SRC_FILE_NM            AS SRC_FILE_NM,
    src._ROW_HASH               AS SRC_ROW_HASH,
    CAST(NULL AS NUMBER)        AS SRC_ROW_NUM,

    d.DAY_KY,
    i.ITEM_KY,
    l.LOCATION_KY,

    (d.DAY_KY IS NULL OR i.ITEM_KY IS NULL OR l.LOCATION_KY IS NULL) AS RI_FAIL_FLG

FROM silver.TMP_INVENTORY_MASTER src

LEFT JOIN gold.DIM_DAY d
       ON d.CALENDAR_DT = src.INVENTORY_DT

LEFT JOIN gold.DIM_ITEM i
       ON i.ITEM_ID = src.ITEM_ID
      AND src.INVENTORY_DT BETWEEN i.EFF_FROM_DATE AND i.EFF_TO_DATE

LEFT JOIN gold.DIM_LOCATION l
       ON l.LOCATION_ID = src.LOCATION_ID
      AND src.INVENTORY_DT BETWEEN l.EFF_FROM_DATE AND l.EFF_TO_DATE

WHERE src._BATCH_ID = DATE '{{ ds }}';

-- ----------------------------------------------------------------------------
-- 2. Write join-miss rows to the reject table.
-- ----------------------------------------------------------------------------
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT
    s.SRC_FILE_NM,
    DATE '{{ ds }}',
    'FCT_INVENTORY',
    'REFERENTIAL_INTEGRITY_FAILURE',
    'DAY_KY / ITEM_KY / LOCATION_KY could not be resolved as-of INVENTORY_DT',
    OBJECT_CONSTRUCT(
        'INVENTORY_DT',   s.INVENTORY_DT,
        'ITEM_ID',        s.ITEM_ID,
        'LOCATION_ID',    s.LOCATION_ID,
        'STOCK_ON_HAND',  s.STOCK_ON_HAND,
        'IN_TRANSIT',     s.IN_TRANSIT,
        'DAY_KY',         s.DAY_KY,
        'ITEM_KY',        s.ITEM_KY,
        'LOCATION_KY',    s.LOCATION_KY,
        'SRC_FILE_NM',    s.SRC_FILE_NM,
        'SRC_ROW_HASH',   s.SRC_ROW_HASH
    )
FROM TMP_FCT_INVENTORY_STG s
WHERE s.RI_FAIL_FLG = TRUE;

-- ----------------------------------------------------------------------------
-- 3. MERGE valid rows into FCT_INVENTORY — idempotent on the grain.
-- ----------------------------------------------------------------------------
MERGE INTO gold.FCT_INVENTORY t
USING (
    SELECT
        DAY_KY, ITEM_KY, LOCATION_KY,
        STOCK_ON_HAND, IN_TRANSIT,
        SRC_EFF_FROM_DATE, SRC_EFF_TO_DATE
    FROM TMP_FCT_INVENTORY_STG
    WHERE RI_FAIL_FLG = FALSE
) s
ON  t.DAY_KY      = s.DAY_KY
AND t.ITEM_KY     = s.ITEM_KY
AND t.LOCATION_KY = s.LOCATION_KY

WHEN MATCHED AND (
        COALESCE(t.STOCK_ON_HAND, -1) <> COALESCE(s.STOCK_ON_HAND, -1)
     OR COALESCE(t.IN_TRANSIT,    -1) <> COALESCE(s.IN_TRANSIT,    -1)
     OR COALESCE(t.EFF_FROM_DATE, DATE '1900-01-01') <> COALESCE(s.SRC_EFF_FROM_DATE, DATE '1900-01-01')
     OR COALESCE(t.EFF_TO_DATE,   DATE '9999-12-31') <> COALESCE(s.SRC_EFF_TO_DATE,   DATE '9999-12-31')
) THEN UPDATE SET
    STOCK_ON_HAND = s.STOCK_ON_HAND,
    IN_TRANSIT    = s.IN_TRANSIT,
    EFF_FROM_DATE = s.SRC_EFF_FROM_DATE,
    EFF_TO_DATE   = s.SRC_EFF_TO_DATE,
    REC_UPDT_TS   = CURRENT_TIMESTAMP(),
    BATCH_ID      = DATE '{{ ds }}',
    SRC_SYS_CD    = 'TMP_INVENTORY_MASTER'

WHEN NOT MATCHED THEN INSERT (
    DAY_KY, ITEM_KY, LOCATION_KY,
    STOCK_ON_HAND, IN_TRANSIT,
    EFF_FROM_DATE, EFF_TO_DATE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID
)
VALUES (
    s.DAY_KY, s.ITEM_KY, s.LOCATION_KY,
    s.STOCK_ON_HAND, s.IN_TRANSIT,
    s.SRC_EFF_FROM_DATE, s.SRC_EFF_TO_DATE,
    CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'TMP_INVENTORY_MASTER', DATE '{{ ds }}'
);

-- ----------------------------------------------------------------------------
-- 4. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_FCT_INVENTORY_STG;