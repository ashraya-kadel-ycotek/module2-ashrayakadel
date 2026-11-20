USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_DEPARTMENT
-- Source : silver.TMP_ITEM_MASTER
-- Parent : DIM_COMPANY (COMPANY_KY via COMPANY_ID, CURR_FLG = TRUE)
-- SCD    : DEPARTMENT_NAME         -> SCD2
--          DEPARTMENT_MANAGER_NAME -> SCD1
-- Dedup  : one row per DEPARTMENT_ID from today's batch
-- KY     : DEPARTMENT_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source + resolved parent COMPANY_KY.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_DEPARTMENT_SRC AS
WITH ranked AS (
    SELECT
        s.DEPARTMENT_ID,
        s.DEPARTMENT_NAME,
        s.DEPARTMENT_MANAGER_NAME,
        s.COMPANY_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.DEPARTMENT_ID
            ORDER BY s._LOAD_TS DESC, s.ITEM_ID DESC
        ) AS RN
    FROM silver.TMP_ITEM_MASTER s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.DEPARTMENT_ID IS NOT NULL
)
SELECT
    r.DEPARTMENT_ID,
    r.DEPARTMENT_NAME,
    r.DEPARTMENT_MANAGER_NAME,
    c.COMPANY_KY
FROM ranked r
JOIN gold.DIM_COMPANY c
  ON c.COMPANY_ID = r.COMPANY_ID
 AND c.CURR_FLG   = TRUE
WHERE r.RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose DEPARTMENT_NAME is changing.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_DEPARTMENT tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_DEPARTMENT_SRC src
WHERE tgt.DEPARTMENT_ID = src.DEPARTMENT_ID
  AND tgt.CURR_FLG      = TRUE
  AND (
        COALESCE(tgt.DEPARTMENT_NAME, '') <> COALESCE(src.DEPARTMENT_NAME, '')
     OR tgt.COMPANY_KY <> src.COMPANY_KY
  );

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for departments whose name
--     (or parent company) changed.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_DEPARTMENT (
    DEPARTMENT_ID,
    DEPARTMENT_NAME, DEPARTMENT_MANAGER_NAME, COMPANY_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.DEPARTMENT_ID,
    src.DEPARTMENT_NAME,
    src.DEPARTMENT_MANAGER_NAME,
    src.COMPANY_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_DEPARTMENT_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_DEPARTMENT tgt
    WHERE tgt.DEPARTMENT_ID = src.DEPARTMENT_ID
      AND tgt.CURR_FLG      = FALSE
      AND tgt.EFF_TO_DATE   = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID      = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_DEPARTMENT tgt
    WHERE tgt.DEPARTMENT_ID = src.DEPARTMENT_ID
      AND tgt.CURR_FLG      = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New departments — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_DEPARTMENT (
    DEPARTMENT_ID,
    DEPARTMENT_NAME, DEPARTMENT_MANAGER_NAME, COMPANY_KY,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.DEPARTMENT_ID,
    src.DEPARTMENT_NAME,
    src.DEPARTMENT_MANAGER_NAME,
    src.COMPANY_KY,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_ITEM_MASTER',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_DEPARTMENT_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_DEPARTMENT tgt
    WHERE tgt.DEPARTMENT_ID = src.DEPARTMENT_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same name (and same parent), but MANAGER changed.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_DEPARTMENT tgt
SET
    DEPARTMENT_MANAGER_NAME = src.DEPARTMENT_MANAGER_NAME,
    REC_UPDT_TS             = CURRENT_TIMESTAMP(),
    BATCH_ID                = DATE '{{ ds }}'
FROM TMP_DIM_DEPARTMENT_SRC src
WHERE tgt.DEPARTMENT_ID = src.DEPARTMENT_ID
  AND tgt.CURR_FLG      = TRUE
  AND COALESCE(tgt.DEPARTMENT_NAME, '') = COALESCE(src.DEPARTMENT_NAME, '')
  AND tgt.COMPANY_KY = src.COMPANY_KY
  AND COALESCE(tgt.DEPARTMENT_MANAGER_NAME, '') <> COALESCE(src.DEPARTMENT_MANAGER_NAME, '');

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_DEPARTMENT_SRC;