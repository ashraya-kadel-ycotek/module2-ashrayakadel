USE WAREHOUSE MODULE2;
USE DATABASE MODULE2_ETL;
USE SCHEMA gold;

-- ============================================================================
-- GOLD LOAD: FCT_SALES_PER_WEEK
-- Source : gold.FCT_SALES (aggregated) -> roll up to DIM_WEEK
-- Grain  : (WEEK_KY, ITEM_KY, LOCATION_KY)
-- Pattern: SCOPED REBUILD
--          1. Identify weeks touched by today's FCT_SALES batch
--          2. DELETE those weeks from FCT_SALES_PER_WEEK
--          3. Re-INSERT from FCT_SALES grouped by the aggregate grain,
--             scoped to the affected weeks
-- Why scoped:
--          FCT_SALES rows for the same week can arrive across multiple
--          daily batches; a full truncate would rewrite the entire table
--          on every run. Scoping to affected weeks keeps the rebuild
--          correct AND scales over time.
-- No SCD2 on facts: each week/item/location is one aggregate row.
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. Determine the set of WEEK_KY affected by today's FCT_SALES batch.
--    (any week that has at least one FCT_SALES row in this batch)
-- ----------------------------------------------------------------------------
CREATE OR REPLACE TEMPORARY TABLE TMP_FCT_WEEK_KEYS AS
SELECT DISTINCT d.WEEK_KY
FROM gold.FCT_SALES f
JOIN gold.DIM_DAY d
  ON d.DAY_KY = f.DAY_KY
WHERE f.BATCH_ID = DATE '{{ ds }}';

-- ----------------------------------------------------------------------------
-- 2. Delete the affected weeks from the aggregate (scoped rebuild).
-- ----------------------------------------------------------------------------
DELETE FROM gold.FCT_SALES_PER_WEEK
WHERE WEEK_KY IN (SELECT WEEK_KY FROM TMP_FCT_WEEK_KEYS);

-- ----------------------------------------------------------------------------
-- 3. Re-INSERT the aggregate for the affected weeks, from FCT_SALES.
--    Grain: (WEEK_KY, ITEM_KY, LOCATION_KY)
-- ----------------------------------------------------------------------------
INSERT INTO gold.FCT_SALES_PER_WEEK (
    WEEK_KY, ITEM_KY, LOCATION_KY,
    SLS_QTY, SLS_AMT,
    REC_INS_TS, REC_UPDT_TS, SRC_SYS_CD, BATCH_ID
)
SELECT
    d.WEEK_KY,
    f.ITEM_KY,
    f.LOCATION_KY,
    SUM(f.SLS_QTY)  AS SLS_QTY,
    SUM(f.SLS_AMT)  AS SLS_AMT,
    CURRENT_TIMESTAMP(),
    CURRENT_TIMESTAMP(),
    'DERIVED',
    DATE '{{ ds }}'
FROM gold.FCT_SALES f
JOIN gold.DIM_DAY d
  ON d.DAY_KY = f.DAY_KY
WHERE d.WEEK_KY IN (SELECT WEEK_KY FROM TMP_FCT_WEEK_KEYS)
GROUP BY d.WEEK_KY, f.ITEM_KY, f.LOCATION_KY;

-- ----------------------------------------------------------------------------
-- 4. Cleanup
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS TMP_FCT_WEEK_KEYS;