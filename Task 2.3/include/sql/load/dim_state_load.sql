USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_STATE
-- Source : silver.TMP_STORE_MASTER
-- SCD    : STATE_CODE -> SCD2 (close + open new row on change)
--          STATE_NAME -> SCD1 (overwrite in place)
-- Dedup  : one row per STATE_ID from today's batch
-- KY     : STATE_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source — exactly one row per STATE_ID.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_STATE_SRC AS
WITH ranked AS (
    SELECT
        s.STATE_ID,
        s.STATE_NAME,
        s.STATE_CODE,
        ROW_NUMBER() OVER (
            PARTITION BY s.STATE_ID
            ORDER BY s._LOAD_TS DESC, s.LOCATION_ID DESC
        ) AS RN
    FROM silver.TMP_STORE_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.STATE_ID IS NOT NULL
)
SELECT STATE_ID, STATE_NAME, STATE_CODE
FROM ranked
WHERE RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose STATE_CODE is changing.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_STATE tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_STATE_SRC src
WHERE tgt.STATE_ID = src.STATE_ID
  AND tgt.CURR_FLG = TRUE
  AND COALESCE(tgt.STATE_CODE, '') <> COALESCE(src.STATE_CODE, '');

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for states whose code changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_STATE (
    STATE_ID,
    STATE_NAME, STATE_CODE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.STATE_ID,
    src.STATE_NAME,
    src.STATE_CODE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_STATE_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_STATE tgt
    WHERE tgt.STATE_ID = src.STATE_ID
      AND tgt.CURR_FLG = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_STATE tgt
    WHERE tgt.STATE_ID = src.STATE_ID
      AND tgt.CURR_FLG = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New states — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_STATE (
    STATE_ID,
    STATE_NAME, STATE_CODE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.STATE_ID,
    src.STATE_NAME,
    src.STATE_CODE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_STATE_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_STATE tgt
    WHERE tgt.STATE_ID = src.STATE_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same STATE_CODE, but STATE_NAME changed. Overwrite.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_STATE tgt
SET
    STATE_NAME  = src.STATE_NAME,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_STATE_SRC src
WHERE tgt.STATE_ID  = src.STATE_ID
  AND tgt.CURR_FLG  = TRUE
  AND COALESCE(tgt.STATE_CODE, '') = COALESCE(src.STATE_CODE, '')
  AND COALESCE(tgt.STATE_NAME, '') <> COALESCE(src.STATE_NAME, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_STATE_SRC;