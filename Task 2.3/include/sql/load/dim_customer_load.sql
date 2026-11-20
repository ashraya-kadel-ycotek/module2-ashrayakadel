USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: DIM_CUSTOMER
-- Source : silver.TMP_POS_SALES
-- SCD    : CUSTOMER_NAME / CUSTOMER_EMAIL  -> SCD1 (overwrite in place)
--          CUSTOMER_PHONE                  -> SCD2 (close old row, open new row)
-- Dedup  : one row per CUSTOMER_ID from today's batch
--          ORDER BY SALES_DT DESC, TRANSACTION_ID DESC
-- KY     : CUSTOMER_KY is AUTOINCREMENT — never supplied in INSERT
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Deduplicated today's source — exactly one row per CUSTOMER_ID.
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_DIM_CUSTOMER_SRC AS
WITH ranked AS (
    SELECT
        s.CUSTOMER_ID,
        s.CUSTOMER_NAME,
        s.CUSTOMER_EMAIL,
        s.CUSTOMER_PHONE,
        s.SALES_DT,
        s.TRANSACTION_ID,
        ROW_NUMBER() OVER (
            PARTITION BY s.CUSTOMER_ID
            ORDER BY s.SALES_DT DESC,
                     s.TRANSACTION_ID DESC
        ) AS RN
    FROM silver.TMP_POS_SALES s
    WHERE s._BATCH_ID = DATE '{{ ds }}'
      AND s.CUSTOMER_ID IS NOT NULL
)
SELECT
    CUSTOMER_ID,
    CUSTOMER_NAME,
    CUSTOMER_EMAIL,
    CUSTOMER_PHONE
FROM ranked
WHERE RN = 1;

-- ----------------------------------------------------------------------------
-- 2. SCD2 close-out — close current rows whose phone is changing.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_CUSTOMER tgt
SET
    EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}'),
    CURR_FLG    = FALSE,
    REC_UPDT_TS = CURRENT_TIMESTAMP(),
    BATCH_ID    = DATE '{{ ds }}'
FROM TMP_DIM_CUSTOMER_SRC src
WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
  AND tgt.CURR_FLG    = TRUE
  AND COALESCE(tgt.CUSTOMER_PHONE, '') <> COALESCE(src.CUSTOMER_PHONE, '');

-- ----------------------------------------------------------------------------
-- 3a. SCD2 open — insert a NEW history row for customers whose phone changed.
--     (the old row was just closed in step 2)
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME, CUSTOMER_EMAIL, CUSTOMER_PHONE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.CUSTOMER_ID,
    src.CUSTOMER_NAME,
    src.CUSTOMER_EMAIL,
    src.CUSTOMER_PHONE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_POS_SALES',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_CUSTOMER_SRC src
WHERE EXISTS (
    SELECT 1
    FROM gold.DIM_CUSTOMER tgt
    WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
      AND tgt.CURR_FLG    = FALSE
      AND tgt.EFF_TO_DATE = DATEADD('day', -1, DATE '{{ ds }}')
      AND tgt.BATCH_ID    = DATE '{{ ds }}'
)
AND NOT EXISTS (
    SELECT 1
    FROM gold.DIM_CUSTOMER tgt
    WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
      AND tgt.CURR_FLG    = TRUE
);

-- ----------------------------------------------------------------------------
-- 3b. New customers — never seen before. Insert as current row.
-- ----------------------------------------------------------------------------
INSERT INTO gold.DIM_CUSTOMER (
    CUSTOMER_ID,
    CUSTOMER_NAME, CUSTOMER_EMAIL, CUSTOMER_PHONE,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID,
    EFF_FROM_DATE, EFF_TO_DATE, CURR_FLG
)
SELECT
    src.CUSTOMER_ID,
    src.CUSTOMER_NAME,
    src.CUSTOMER_EMAIL,
    src.CUSTOMER_PHONE,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'TMP_POS_SALES',
    DATE '{{ ds }}',
    DATE '{{ ds }}',
    DATE '9999-12-31',
    TRUE
FROM TMP_DIM_CUSTOMER_SRC src
WHERE NOT EXISTS (
    SELECT 1
    FROM gold.DIM_CUSTOMER tgt
    WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
);

-- ----------------------------------------------------------------------------
-- 4. SCD1 update — same phone, but NAME or EMAIL changed. Overwrite in place.
-- ----------------------------------------------------------------------------
UPDATE gold.DIM_CUSTOMER tgt
SET
    CUSTOMER_NAME  = src.CUSTOMER_NAME,
    CUSTOMER_EMAIL = src.CUSTOMER_EMAIL,
    REC_UPDT_TS    = CURRENT_TIMESTAMP(),
    BATCH_ID       = DATE '{{ ds }}'
FROM TMP_DIM_CUSTOMER_SRC src
WHERE tgt.CUSTOMER_ID = src.CUSTOMER_ID
  AND tgt.CURR_FLG    = TRUE
  AND COALESCE(tgt.CUSTOMER_PHONE, '') = COALESCE(src.CUSTOMER_PHONE, '')
  AND (
        COALESCE(tgt.CUSTOMER_NAME,  '') <> COALESCE(src.CUSTOMER_NAME,  '')
     OR COALESCE(tgt.CUSTOMER_EMAIL, '') <> COALESCE(src.CUSTOMER_EMAIL, '')
  );

-- ----------------------------------------------------------------------------
-- 5. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_DIM_CUSTOMER_SRC;