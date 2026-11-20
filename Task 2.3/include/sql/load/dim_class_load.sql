USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_CLASS
-- Source : silver.TMP_ITEM_MASTER
-- Parent : DIM_DEPARTMENT (DEPARTMENT_KY via DEPARTMENT_ID, CURR_FLG = TRUE)
-- SCD    : CLASS_NAME        -> SCD2
--          CLASS_DESCRIPTION -> SCD1
-- Dedup  : one row per CLASS_ID from today's batch
-- KY     : CLASS_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source + resolved parent DEPARTMENT_KY.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_CLASS_SRC AS
WITH ranked AS (
    SELECT
        s.CLASS_ID,
        s.CLASS_NAME,
        s.CLASS_DESCRIPTION,
        s.DEPARTMENT_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.CLASS_ID
            ORDER BY s._LOAD_TS DESC, s.ITEM_ID DESC
        ) AS RN
    FROM silver.TMP_ITEM_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.CLASS_ID IS NOT NULL
)
SELECT
    r.CLASS_ID,
    r.CLASS_NAME,
    r.CLASS_DESCRIPTION,
    d.DEPARTMENT_KY
FROM ranked r
JOIN gold.DIM_DEPARTMENT d
  ON d.DEPARTMENT_ID = r.DEPARTMENT_ID
 AND d.CURR_FLG       = TRUE
WHERE r.RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose CLASS_NAME (or parent) changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_CLASS tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_CLASS_SRC src
WHERE tgt.CLASS_ID = src.CLASS_ID
  AND tgt.CURR_FLG = TRUE
  AND (
        COALESCE(tgt.CLASS_NAME, '') <> COALESCE(src.CLASS_NAME, '')
     OR tgt.DEPARTMENT_KY <> src.DEPARTMENT_KY
  );

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for classes whose name (or parent)
--     changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_CLASS (
    CLASS_ID,
    CLASS_NAME, CLASS_DESCRIPTION, DEPARTMENT_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.CLASS_ID,
    src.CLASS_NAME,
    src.CLASS_DESCRIPTION,
    src.DEPARTMENT_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_CLASS_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_CLASS tgt
    WHERE tgt.CLASS_ID = src.CLASS_ID
      AND tgt.CURR_FLG = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_CLASS tgt
    WHERE tgt.CLASS_ID = src.CLASS_ID
      AND tgt.CURR_FLG = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New classes — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_CLASS (
    CLASS_ID,
    CLASS_NAME, CLASS_DESCRIPTION, DEPARTMENT_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.CLASS_ID,
    src.CLASS_NAME,
    src.CLASS_DESCRIPTION,
    src.DEPARTMENT_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_CLASS_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_CLASS tgt
    WHERE tgt.CLASS_ID = src.CLASS_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same name (and same parent), but DESCRIPTION changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_CLASS tgt
SET
    CLASS_DESCRIPTION = src.CLASS_DESCRIPTION,
    REC_UPDT_TS       = CURRENT_TIMESTAMP(),
    BATCH_ID          = DATE '{{ ds }}'
FROM TMP_DIM_CLASS_SRC src
WHERE tgt.CLASS_ID = src.CLASS_ID
  AND tgt.CURR_FLG = TRUE
  AND COALESCE(tgt.CLASS_NAME, '') = COALESCE(src.CLASS_NAME, '')
  AND tgt.DEPARTMENT_KY = src.DEPARTMENT_KY
  AND COALESCE(tgt.CLASS_DESCRIPTION, '') <> COALESCE(src.CLASS_DESCRIPTION, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_CLASS_SRC;