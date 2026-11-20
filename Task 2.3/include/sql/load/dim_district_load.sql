USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_DISTRICT
-- Source : silver.TMP_STORE_MASTER
-- Parent : DIM_STATE (STATE_KY via STATE_ID, CURR_FLG = TRUE)
-- SCD    : DISTRICT_CODE -> SCD2
--          DISTRICT_NAME -> SCD1
-- Dedup  : one row per DISTRICT_ID from today's batch
-- KY     : DISTRICT_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source + resolved parent STATE_KY.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_DISTRICT_SRC AS
WITH ranked AS (
    SELECT
        s.DISTRICT_ID,
        s.DISTRICT_NAME,
        s.DISTRICT_CODE,
        s.STATE_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.DISTRICT_ID
            ORDER BY s._LOAD_TS DESC, s.LOCATION_ID DESC
        ) AS RN
    FROM silver.TMP_STORE_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.DISTRICT_ID IS NOT NULL
)
SELECT
    r.DISTRICT_ID,
    r.DISTRICT_NAME,
    r.DISTRICT_CODE,
    st.STATE_KY
FROM ranked r
JOIN gold.DIM_STATE st
  ON st.STATE_ID = r.STATE_ID
 AND st.CURR_FLG = TRUE
WHERE r.RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose DISTRICT_CODE is changing.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_DISTRICT tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_DISTRICT_SRC src
WHERE tgt.DISTRICT_ID = src.DISTRICT_ID
  AND tgt.CURR_FLG    = TRUE
  AND (
        COALESCE(tgt.DISTRICT_CODE, '') <> COALESCE(src.DISTRICT_CODE, '')
     OR tgt.STATE_KY <> src.STATE_KY
  );

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for districts whose code
--     (or parent state) changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_DISTRICT (
    DISTRICT_ID,
    DISTRICT_NAME, DISTRICT_CODE, STATE_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.DISTRICT_ID,
    src.DISTRICT_NAME,
    src.DISTRICT_CODE,
    src.STATE_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_DISTRICT_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_DISTRICT tgt
    WHERE tgt.DISTRICT_ID = src.DISTRICT_ID
      AND tgt.CURR_FLG    = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID    = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_DISTRICT tgt
    WHERE tgt.DISTRICT_ID = src.DISTRICT_ID
      AND tgt.CURR_FLG    = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New districts — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_DISTRICT (
    DISTRICT_ID,
    DISTRICT_NAME, DISTRICT_CODE, STATE_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.DISTRICT_ID,
    src.DISTRICT_NAME,
    src.DISTRICT_CODE,
    src.STATE_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_DISTRICT_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_DISTRICT tgt
    WHERE tgt.DISTRICT_ID = src.DISTRICT_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same DISTRICT_CODE (and same parent), but NAME changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_DISTRICT tgt
SET
    DISTRICT_NAME = src.DISTRICT_NAME,
    REC_UPDT_TS   = CURRENT_TIMESTAMP(),
    BATCH_ID      = DATE '{{ ds }}'
FROM TMP_DIM_DISTRICT_SRC src
WHERE tgt.DISTRICT_ID = src.DISTRICT_ID
  AND tgt.CURR_FLG    = TRUE
  AND COALESCE(tgt.DISTRICT_CODE, '') = COALESCE(src.DISTRICT_CODE, '')
  AND tgt.STATE_KY = src.STATE_KY
  AND COALESCE(tgt.DISTRICT_NAME, '') <> COALESCE(src.DISTRICT_NAME, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_DISTRICT_SRC;