USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_LOCATION
-- Source : silver.TMP_STORE_MASTER
-- Parent : DIM_DISTRICT (DISTRICT_KY via DISTRICT_ID, CURR_FLG = TRUE)
-- SCD    : LOCATION_NAME -> SCD2
--          ADDRESS_LINE  -> SCD1
-- Dedup  : one row per LOCATION_ID from today's batch
-- KY     : LOCATION_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source + resolved parent DISTRICT_KY.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_LOCATION_SRC AS
WITH ranked AS (
    SELECT
        s.LOCATION_ID,
        s.LOCATION_NAME,
        s.ADDRESS_LINE,
        s.DISTRICT_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.LOCATION_ID
            ORDER BY s._LOAD_TS DESC, s.LOCATION_ID DESC
        ) AS RN
    FROM silver.TMP_STORE_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.LOCATION_ID IS NOT NULL
)
SELECT
    r.LOCATION_ID,
    r.LOCATION_NAME,
    r.ADDRESS_LINE,
    d.DISTRICT_KY
FROM ranked r
JOIN gold.DIM_DISTRICT d
  ON d.DISTRICT_ID = r.DISTRICT_ID
 AND d.CURR_FLG    = TRUE
WHERE r.RN = 1
  -- Stale-batch guard: skip any location whose target history already
  -- contains a version effective AFTER this batch date.
  AND NOT EXISTS (
        SELECT 1
        FROM gold.DIM_LOCATION t
        WHERE t.LOCATION_ID    = r.LOCATION_ID
          AND t.EFF_FROM_DATE  > DATE '{{ ds }}'
  );

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose LOCATION_NAME (or parent) changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_LOCATION tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_LOCATION_SRC src
WHERE tgt.LOCATION_ID = src.LOCATION_ID
  AND tgt.CURR_FLG    = TRUE
  AND (
        COALESCE(tgt.LOCATION_NAME, '') <> COALESCE(src.LOCATION_NAME, '')
     OR tgt.DISTRICT_KY <> src.DISTRICT_KY
  );

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for locations whose name (or parent)
--     changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_LOCATION (
    LOCATION_ID,
    LOCATION_NAME, ADDRESS_LINE, DISTRICT_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.LOCATION_ID,
    src.LOCATION_NAME,
    src.ADDRESS_LINE,
    src.DISTRICT_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_LOCATION_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_LOCATION tgt
    WHERE tgt.LOCATION_ID = src.LOCATION_ID
      AND tgt.CURR_FLG    = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID    = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_LOCATION tgt
    WHERE tgt.LOCATION_ID = src.LOCATION_ID
      AND tgt.CURR_FLG    = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New locations — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_LOCATION (
    LOCATION_ID,
    LOCATION_NAME, ADDRESS_LINE, DISTRICT_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.LOCATION_ID,
    src.LOCATION_NAME,
    src.ADDRESS_LINE,
    src.DISTRICT_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_STORE_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_LOCATION_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_LOCATION tgt
    WHERE tgt.LOCATION_ID = src.LOCATION_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same name (and same parent), but ADDRESS_LINE changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_LOCATION tgt
SET
    ADDRESS_LINE = src.ADDRESS_LINE,
    REC_UPDT_TS  = CURRENT_TIMESTAMP(),
    BATCH_ID     = DATE '{{ ds }}'
FROM TMP_DIM_LOCATION_SRC src
WHERE tgt.LOCATION_ID = src.LOCATION_ID
  AND tgt.CURR_FLG    = TRUE
  AND COALESCE(tgt.LOCATION_NAME, '') = COALESCE(src.LOCATION_NAME, '')
  AND tgt.DISTRICT_KY = src.DISTRICT_KY
  AND COALESCE(tgt.ADDRESS_LINE, '') <> COALESCE(src.ADDRESS_LINE, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_LOCATION_SRC;