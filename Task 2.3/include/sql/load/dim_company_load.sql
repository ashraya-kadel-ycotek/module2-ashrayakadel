USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_COMPANY
-- Source : silver.TMP_ITEM_MASTER
-- SCD    : COMPANY_NAME -> SCD2 (close + open new row on change)
--          COMPANY_TYPE -> SCD1 (overwrite in place)
-- Dedup  : one row per COMPANY_ID from today's batch
-- KY     : COMPANY_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source — exactly one row per COMPANY_ID.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_COMPANY_SRC AS
WITH ranked AS (
    SELECT
        s.COMPANY_ID,
        s.COMPANY_NAME,
        s.COMPANY_TYPE,
        ROW_NUMBER() OVER (
            PARTITION BY s.COMPANY_ID
            ORDER BY s._LOAD_TS DESC, s.ITEM_ID DESC
        ) AS RN
    FROM silver.TMP_ITEM_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.COMPANY_ID IS NOT NULL
)
SELECT COMPANY_ID, COMPANY_NAME, COMPANY_TYPE
FROM ranked
WHERE RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose COMPANY_NAME is changing.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_COMPANY tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_COMPANY_SRC src
WHERE tgt.COMPANY_ID = src.COMPANY_ID
  AND tgt.CURR_FLG   = TRUE
  AND COALESCE(tgt.COMPANY_NAME, '') <> COALESCE(src.COMPANY_NAME, '');

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for companies whose name changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_COMPANY (
    COMPANY_ID,
    COMPANY_NAME, COMPANY_TYPE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.COMPANY_ID,
    src.COMPANY_NAME,
    src.COMPANY_TYPE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_COMPANY_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_COMPANY tgt
    WHERE tgt.COMPANY_ID = src.COMPANY_ID
      AND tgt.CURR_FLG   = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID   = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_COMPANY tgt
    WHERE tgt.COMPANY_ID = src.COMPANY_ID
      AND tgt.CURR_FLG   = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New companies — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_COMPANY (
    COMPANY_ID,
    COMPANY_NAME, COMPANY_TYPE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.COMPANY_ID,
    src.COMPANY_NAME,
    src.COMPANY_TYPE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_COMPANY_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_COMPANY tgt
    WHERE tgt.COMPANY_ID = src.COMPANY_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same COMPANY_NAME, but COMPANY_TYPE changed. Overwrite.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_COMPANY tgt
SET
    COMPANY_TYPE = src.COMPANY_TYPE,
    REC_UPDT_TS  = CURRENT_TIMESTAMP(),
    BATCH_ID     = DATE '{{ ds }}'
FROM TMP_DIM_COMPANY_SRC src
WHERE tgt.COMPANY_ID  = src.COMPANY_ID
  AND tgt.CURR_FLG    = TRUE
  AND COALESCE(tgt.COMPANY_NAME, '') = COALESCE(src.COMPANY_NAME, '')
  AND COALESCE(tgt.COMPANY_TYPE, '') <> COALESCE(src.COMPANY_TYPE, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_COMPANY_SRC;