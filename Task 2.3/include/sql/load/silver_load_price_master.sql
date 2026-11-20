USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;

-- ============================================================================
-- 5. TMP_PRICE_MASTER
-- Grain: 1 row per (ITEM_ID, LOCATION_ID, EFF_FROM_DATE)
-- ============================================================================

-- 5.1 Type-cast failures
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'TYPE_CAST_FAILURE',
       'PRICE_DT / UNIT_PRICE_AMT / EFF_FROM_DATE / EFF_TO_DATE failed to cast',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
WHERE s.BATCH_ID = DATE '{{ ds }}'
  AND (
       TRY_TO_DATE(s.PRICE_DT, 'YYYY-MM-DD') IS NULL
    OR TRY_TO_NUMBER(s.UNIT_PRICE_AMT) IS NULL
    OR TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD') IS NULL
    OR TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD') IS NULL
  );

-- 5.2 NULL required fields
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'NULL_REQUIRED_FIELD',
       'Required price master field is NULL',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
WHERE s.BATCH_ID = DATE '{{ ds }}'
  AND (
       s.PRICE_DT IS NULL
    OR s.ITEM_ID IS NULL
    OR s.LOCATION_ID IS NULL
    OR s.UNIT_PRICE_AMT IS NULL
    OR s.EFF_FROM_DATE IS NULL
    OR s.EFF_TO_DATE IS NULL
  );

-- 5.3 Negative price
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'NEGATIVE_AMOUNT',
       'UNIT_PRICE_AMT is negative',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
WHERE s.BATCH_ID = DATE '{{ ds }}'
  AND TRY_TO_NUMBER(s.UNIT_PRICE_AMT) < 0;

-- 5.4 Invalid effective date range
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'TYPE_CAST_FAILURE',
       'EFF_TO_DATE is earlier than EFF_FROM_DATE',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
WHERE s.BATCH_ID = DATE '{{ ds }}'
  AND TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD') IS NOT NULL
  AND TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD') IS NOT NULL
  AND TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD')
      < TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD');

-- 5.5 Duplicate business key
-- Business key: ITEM_ID, LOCATION_ID, EFF_FROM_DATE
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'DUP_BUSINESS_KEY',
       '(ITEM_ID, LOCATION_ID, EFF_FROM_DATE) duplicated within batch',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
JOIN (
    SELECT
        ITEM_ID,
        LOCATION_ID,
        EFF_FROM_DATE
    FROM bronze.STG_PRICE_MASTER
    WHERE BATCH_ID = DATE '{{ ds }}'
    GROUP BY ITEM_ID, LOCATION_ID, EFF_FROM_DATE
    HAVING COUNT(*) > 1
) d
    ON d.ITEM_ID = s.ITEM_ID
   AND d.LOCATION_ID = s.LOCATION_ID
   AND d.EFF_FROM_DATE = s.EFF_FROM_DATE
WHERE s.BATCH_ID = DATE '{{ ds }}';

-- 5.6 Referential integrity
INSERT INTO control.C_REJ_TABLE
    (SRC_FILE_NM, BATCH_ID, TARGET_TABLE, REASON_CODE, REASON, DETAILS)
SELECT s.SRC_FILE_NM, DATE '{{ ds }}', 'TMP_PRICE_MASTER',
       'REFERENTIAL_INTEGRITY_FAILURE',
       'ITEM_ID or LOCATION_ID missing in Silver master tables',
       OBJECT_CONSTRUCT(s.*)
FROM bronze.STG_PRICE_MASTER s
WHERE s.BATCH_ID = DATE '{{ ds }}'
  AND (
      NOT EXISTS (
          SELECT 1
          FROM silver.TMP_ITEM_MASTER i
          WHERE i.ITEM_ID = s.ITEM_ID
      )
      OR NOT EXISTS (
          SELECT 1
          FROM silver.TMP_STORE_MASTER st
          WHERE st.LOCATION_ID = s.LOCATION_ID
      )
  );

-- 5.7 MERGE valid records
MERGE INTO silver.TMP_PRICE_MASTER t
USING (
    SELECT
        s.PRICE_DT::DATE AS PRICE_DT,
        s.ITEM_ID,
        s.LOCATION_ID,
        TRY_TO_NUMBER(s.UNIT_PRICE_AMT)::NUMBER(18,2) AS UNIT_PRICE_AMT,
        TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD') AS EFF_FROM_DATE,
        TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD') AS EFF_TO_DATE,
        s.SRC_FILE_NM,

        MD5(
            COALESCE(s.PRICE_DT, '') || '|' ||
            COALESCE(s.ITEM_ID, '') || '|' ||
            COALESCE(s.LOCATION_ID, '') || '|' ||
            COALESCE(s.UNIT_PRICE_AMT, '') || '|' ||
            COALESCE(s.EFF_FROM_DATE, '') || '|' ||
            COALESCE(s.EFF_TO_DATE, '')
        ) AS ROW_HASH

    FROM bronze.STG_PRICE_MASTER s

    WHERE s.BATCH_ID = DATE '{{ ds }}'

      -- Required fields
      AND s.PRICE_DT IS NOT NULL
      AND s.ITEM_ID IS NOT NULL
      AND s.LOCATION_ID IS NOT NULL
      AND s.UNIT_PRICE_AMT IS NOT NULL
      AND s.EFF_FROM_DATE IS NOT NULL
      AND s.EFF_TO_DATE IS NOT NULL

      -- Valid data types
      AND TRY_TO_DATE(s.PRICE_DT, 'YYYY-MM-DD') IS NOT NULL
      AND TRY_TO_NUMBER(s.UNIT_PRICE_AMT) IS NOT NULL
      AND TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD') IS NOT NULL
      AND TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD') IS NOT NULL

      -- Valid price and dates
      AND TRY_TO_NUMBER(s.UNIT_PRICE_AMT) >= 0
      AND TRY_TO_DATE(s.EFF_TO_DATE, 'YYYY-MM-DD')
          >= TRY_TO_DATE(s.EFF_FROM_DATE, 'YYYY-MM-DD')

      -- Valid references
      AND EXISTS (
          SELECT 1
          FROM silver.TMP_ITEM_MASTER i
          WHERE i.ITEM_ID = s.ITEM_ID
      )
      AND EXISTS (
          SELECT 1
          FROM silver.TMP_STORE_MASTER st
          WHERE st.LOCATION_ID = s.LOCATION_ID
      )

      -- Exclude duplicate business keys
      AND NOT EXISTS (
          SELECT 1
          FROM bronze.STG_PRICE_MASTER d
          WHERE d.BATCH_ID = DATE '{{ ds }}'
            AND d.ITEM_ID = s.ITEM_ID
            AND d.LOCATION_ID = s.LOCATION_ID
            AND d.EFF_FROM_DATE = s.EFF_FROM_DATE
        GROUP BY d.ITEM_ID, d.LOCATION_ID, d.EFF_FROM_DATE
        HAVING COUNT(*) > 1
      )
) s

ON  t.ITEM_ID = s.ITEM_ID
AND t.LOCATION_ID = s.LOCATION_ID
AND t.EFF_FROM_DATE = s.EFF_FROM_DATE

WHEN MATCHED AND t._ROW_HASH <> s.ROW_HASH THEN
UPDATE SET
    PRICE_DT = s.PRICE_DT,
    UNIT_PRICE_AMT = s.UNIT_PRICE_AMT,
    EFF_TO_DATE = s.EFF_TO_DATE,
    _SRC_FILE_NM = s.SRC_FILE_NM,
    _BATCH_ID = DATE '{{ ds }}',
    _LOAD_TS = CURRENT_TIMESTAMP(),
    _ROW_HASH = s.ROW_HASH

WHEN NOT MATCHED THEN
INSERT (
    PRICE_DT,
    ITEM_ID,
    LOCATION_ID,
    UNIT_PRICE_AMT,
    EFF_FROM_DATE,
    EFF_TO_DATE,
    _SRC_FILE_NM,
    _BATCH_ID,
    _LOAD_TS,
    _ROW_HASH
)
VALUES (
    s.PRICE_DT,
    s.ITEM_ID,
    s.LOCATION_ID,
    s.UNIT_PRICE_AMT,
    s.EFF_FROM_DATE,
    s.EFF_TO_DATE,
    s.SRC_FILE_NM,
    DATE '{{ ds }}',
    CURRENT_TIMESTAMP(),
    s.ROW_HASH
);

-- 5.8 Batch reconciliation
INSERT INTO control.C_BATCH_RECON
    (BATCH_ID, SRC_FILE_NM, TARGET_TABLE,
     BRONZE_CNT, TMP_CNT, REJ_CNT, VARIANCE, STATUS)

WITH b AS (
    SELECT COUNT(*) c
    FROM bronze.STG_PRICE_MASTER
    WHERE BATCH_ID = DATE '{{ ds }}'
),
r AS (
    SELECT COUNT(*) c
    FROM control.C_REJ_TABLE
    WHERE BATCH_ID = DATE '{{ ds }}'
      AND TARGET_TABLE = 'TMP_PRICE_MASTER'
),
t AS (
    SELECT COUNT(*) c
    FROM bronze.STG_PRICE_MASTER s
    WHERE s.BATCH_ID = DATE '{{ ds }}'
      AND NOT EXISTS (
          SELECT 1
          FROM control.C_REJ_TABLE r
          WHERE r.BATCH_ID = DATE '{{ ds }}'
            AND r.TARGET_TABLE = 'TMP_PRICE_MASTER'
            AND r.DETAILS:SRC_FILE_NM::VARCHAR = s.SRC_FILE_NM
            AND r.DETAILS:SRC_ROW_NUM::NUMBER = s.SRC_ROW_NUM
      )
)
SELECT
    DATE '{{ ds }}',
    COALESCE(
        (SELECT MAX(SRC_FILE_NM)
         FROM bronze.STG_PRICE_MASTER
         WHERE BATCH_ID = DATE '{{ ds }}'),
        'price_master'
    ),
    'TMP_PRICE_MASTER',
    b.c,
    t.c,
    r.c,
    b.c - (t.c + r.c),
    CASE
        WHEN b.c = (t.c + r.c) THEN 'PASSED'
        ELSE 'FAILED'
    END
FROM b, r, t;