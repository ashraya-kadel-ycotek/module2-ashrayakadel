
-- ============================================================================
-- validate_recon.sql
-- Test 1: Net sales reconciliation
--   Source A : MODULE2_ETL.GOLD.FCT_SALES        SUM(SLS_AMT)  [ct_sales]
--   Source B : MODULE2_ETL.SILVER.TMP_POS_SALES  SUM(SLS_AMT)  [pos_sales]
-- Both filtered to :batch_id.
-- Writes one row to MODULE2_ETL.CONTROL.C_BATCH_RECON.
-- Fails the DAG (1/0) if the sums do NOT match.
-- Idempotent on rerun: MERGE on (BATCH_ID, SRC_FILE_NM, TARGET_TABLE).
-- Self-contained: adds the value-recon columns to C_BATCH_RECON if missing.
-- ============================================================================

USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;

-- --------------------------------------------------------------------------
-- 0) Idempotent schema extension for value recon.
-- --------------------------------------------------------------------------

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS CHECK_NAME VARCHAR(200);

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS SOURCE_A_TABLE VARCHAR(200);

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS SOURCE_A_VAL NUMBER(38,6);

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS SOURCE_B_TABLE VARCHAR(200);

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS SOURCE_B_VAL NUMBER(38,6);

ALTER TABLE MODULE2_ETL.CONTROL.C_BATCH_RECON
ADD COLUMN IF NOT EXISTS DIFF_VAL NUMBER(38,6);

-- --------------------------------------------------------------------------
-- 1) Compute both sums and MERGE the result into C_BATCH_RECON
-- --------------------------------------------------------------------------

MERGE INTO MODULE2_ETL.CONTROL.C_BATCH_RECON t
USING (
    WITH a AS (
        SELECT COALESCE(SUM(SLS_AMT), 0) AS SLS_AMT
        FROM MODULE2_ETL.GOLD.FCT_SALES
        WHERE BATCH_ID = %(batch_id)s
    ),
    b AS (
        SELECT COALESCE(SUM(SLS_AMT), 0) AS SLS_AMT
        FROM MODULE2_ETL.SILVER.TMP_POS_SALES
        WHERE _BATCH_ID = %(batch_id)s
    )
    SELECT
        %(batch_id)s AS BATCH_ID,
        'RECON:NET_SLS_FCT_VS_POS' AS SRC_FILE_NM,
        'FCT_SALES / TMP_POS_SALES' AS TARGET_TABLE,
        0 AS BRONZE_CNT,
        0 AS TMP_CNT,
        0 AS REJ_CNT,
        a.SLS_AMT - b.SLS_AMT AS VARIANCE,
        IFF(a.SLS_AMT = b.SLS_AMT, 'PASSED', 'FAILED') AS STATUS,
        'NET_SLS_FCT_VS_POS' AS CHECK_NAME,
        'GOLD.FCT_SALES' AS SOURCE_A_TABLE,
        a.SLS_AMT AS SOURCE_A_VAL,
        'SILVER.TMP_POS_SALES' AS SOURCE_B_TABLE,
        b.SLS_AMT AS SOURCE_B_VAL,
        a.SLS_AMT - b.SLS_AMT AS DIFF_VAL
    FROM a
    CROSS JOIN b
) s
ON  t.BATCH_ID = s.BATCH_ID
AND t.SRC_FILE_NM = s.SRC_FILE_NM
AND t.TARGET_TABLE = s.TARGET_TABLE

WHEN MATCHED THEN UPDATE SET
    VARIANCE = s.VARIANCE,
    STATUS = s.STATUS,
    CHECK_NAME = s.CHECK_NAME,
    SOURCE_A_TABLE = s.SOURCE_A_TABLE,
    SOURCE_A_VAL = s.SOURCE_A_VAL,
    SOURCE_B_TABLE = s.SOURCE_B_TABLE,
    SOURCE_B_VAL = s.SOURCE_B_VAL,
    DIFF_VAL = s.DIFF_VAL,
    RECON_TS = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT
    (
        BATCH_ID,
        SRC_FILE_NM,
        TARGET_TABLE,
        BRONZE_CNT,
        TMP_CNT,
        REJ_CNT,
        VARIANCE,
        STATUS,
        CHECK_NAME,
        SOURCE_A_TABLE,
        SOURCE_A_VAL,
        SOURCE_B_TABLE,
        SOURCE_B_VAL,
        DIFF_VAL
    )
VALUES
    (
        s.BATCH_ID,
        s.SRC_FILE_NM,
        s.TARGET_TABLE,
        s.BRONZE_CNT,
        s.TMP_CNT,
        s.REJ_CNT,
        s.VARIANCE,
        s.STATUS,
        s.CHECK_NAME,
        s.SOURCE_A_TABLE,
        s.SOURCE_A_VAL,
        s.SOURCE_B_TABLE,
        s.SOURCE_B_VAL,
        s.DIFF_VAL
    );

-- --------------------------------------------------------------------------
-- 2) Fail the DAG if this batch's net-sales recon is not PASSED.
--    Division by zero raises an error only when a FAILED row exists.
-- --------------------------------------------------------------------------

SELECT 1 / 0
FROM MODULE2_ETL.CONTROL.C_BATCH_RECON
WHERE BATCH_ID = %(batch_id)s
  AND SRC_FILE_NM = 'RECON:NET_SLS_FCT_VS_POS'
  AND TARGET_TABLE = 'FCT_SALES / TMP_POS_SALES'
  AND STATUS = 'FAILED';
