USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_ITEM
-- Source : silver.TMP_ITEM_MASTER
-- Parent : DIM_CLASS (CLASS_KY via CLASS_ID, CURR_FLG = TRUE)
-- SCD    : ITEM_NAME, COLOR, SIZE -> SCD2 (all attrs are SCD2 per ERD)
-- Dedup  : one row per ITEM_ID from today's batch
-- KY     : ITEM_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source + resolved parent CLASS_KY.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_ITEM_SRC AS
WITH ranked AS (
    SELECT
        s.ITEM_ID,
        s.ITEM_NAME,
        s.COLOR,
        s.SIZE,
        s.CLASS_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.ITEM_ID
            ORDER BY s._LOAD_TS DESC, s.ITEM_ID DESC
        ) AS RN
    FROM silver.TMP_ITEM_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.ITEM_ID IS NOT NULL
)
SELECT
    r.ITEM_ID,
    r.ITEM_NAME,
    r.COLOR,
    r.SIZE,
    c.CLASS_KY
FROM ranked r
JOIN gold.DIM_CLASS c
  ON c.CLASS_ID = r.CLASS_ID
 AND c.CURR_FLG = TRUE
WHERE r.RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose ITEM_NAME / COLOR / SIZE
--    (or parent CLASS_KY) changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_ITEM tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_ITEM_SRC src
WHERE tgt.ITEM_ID = src.ITEM_ID
  AND tgt.CURR_FLG = TRUE
  AND (
        COALESCE(tgt.ITEM_NAME, '') <> COALESCE(src.ITEM_NAME, '')
     OR COALESCE(tgt.COLOR,     '') <> COALESCE(src.COLOR,     '')
     OR COALESCE(tgt.SIZE,      '') <> COALESCE(src.SIZE,      '')
     OR tgt.CLASS_KY <> src.CLASS_KY
  );

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for items whose attributes
--     (or parent class) changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_ITEM (
    ITEM_ID,
    ITEM_NAME, COLOR, SIZE, CLASS_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.ITEM_ID,
    src.ITEM_NAME,
    src.COLOR,
    src.SIZE,
    src.CLASS_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_ITEM_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_ITEM tgt
    WHERE tgt.ITEM_ID = src.ITEM_ID
      AND tgt.CURR_FLG = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_ITEM tgt
    WHERE tgt.ITEM_ID = src.ITEM_ID
      AND tgt.CURR_FLG = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New items — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_ITEM (
    ITEM_ID,
    ITEM_NAME, COLOR, SIZE, CLASS_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.ITEM_ID,
    src.ITEM_NAME,
    src.COLOR,
    src.SIZE,
    src.CLASS_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_ITEM_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_ITEM tgt
    WHERE tgt.ITEM_ID = src.ITEM_ID
);

-- ----------------------------------------------------------------------------
-- 4. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_ITEM_SRC;