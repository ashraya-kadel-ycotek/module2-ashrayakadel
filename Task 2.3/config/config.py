import os

# ============================================================
# ROOT + LAYER DIRECTORIES
# ============================================================
PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow")
DATA_DIR     = os.path.join(PROJECT_ROOT, "data")

SQL_ROOT     = os.path.join(PROJECT_ROOT, "include", "sql")
LOAD_DIR     = os.path.join(SQL_ROOT, "load")
DDL_DIR      = os.path.join(SQL_ROOT, "ddl")
VALIDATE_DIR = SQL_ROOT                      # validate_recon.sql lives here

# Legacy alias — keep so existing code doesn't break
SQL_DIR = LOAD_DIR

# ============================================================
# SNOWFLAKE
# ============================================================
SNOWFLAKE_CONN_ID   = "snowflake_conn"
CONTROL_DB          = "MODULE2_ETL"
CONTROL_SCHEMA      = "CONTROL"
BATCH_LOG_TABLE     = f"{CONTROL_DB}.{CONTROL_SCHEMA}.C_BATCH_LOG"

# ============================================================
# FILE CHECK
# ============================================================
EXPECTED_PREFIXES = [
    "inventory_master",
    "item_master",
    "pos_sales",
    "price_master",
    "store_master",
]

# ============================================================
# DAG SETTINGS
# ============================================================
DAG_ID              = "my_etl_dag"
DAG_SCHEDULE        = "@daily"
DAG_START_DATE      = (2026, 11, 14)         # parsed in DAG
DAG_MAX_ACTIVE_RUNS = 1
DAG_TAGS = ["sensor", "file", "snowflake", "bronze", "silver", "gold", "control"]

# ============================================================
# SCRIPT CATALOG (filenames only — dirs resolved by helpers)
# ============================================================
# DDL
DDL_SCRIPTS = {
    "control": "control_ddl.sql",
    "bronze":  "bronze_ddl.sql",
    "silver":  "silver_ddl.sql",
    "gold":    "gold_ddl.sql",
}

# CONTROL (non-DDL helpers)
CONTROL_SCRIPTS = {
    "init_params":  "control_init_params.sql",
    "stamp_bronze": "control_stamp_bronze.sql",
    "stamp_silver": "control_stamp_silver.sql",
    "stamp_gold":   "control_stamp_gold.sql",
}

# SILVER
SILVER_RESET  = "silver_reset.sql"
SILVER_PHASE1 = [
    "silver_load_store_master.sql",
    "silver_load_item_master.sql",
]
SILVER_PHASE2 = [
    "silver_load_inventory_master.sql",
    "silver_load_pos_sales.sql",
    "silver_load_price_master.sql",
]

# GOLD — dimensions
GOLD_DIM_PHASE1 = [
    "dim_company_load.sql",
    "dim_state_load.sql",
    "dim_customer_load.sql",
]
GOLD_DIM_PHASE2 = [
    "dim_department_load.sql",
    "dim_district_load.sql",
]
GOLD_DIM_PHASE3 = [
    "dim_class_load.sql",
    "dim_location_load.sql",
]
GOLD_DIM_PHASE4 = ["dim_item_load.sql"]

# GOLD — facts
GOLD_FCT_PHASE1 = [
    "fct_sales_load.sql",
    "fct_price_load.sql",
    "fct_inventory_load.sql",
]
GOLD_FCT_PHASE2 = "fct_sales_per_week_load.sql"

# RECONCILIATION
VALIDATE_RECON = "validate_recon.sql"

# ============================================================
# BRONZE (derived — don't duplicate)
# ============================================================
BRONZE_LOAD_SCRIPTS = [f"bronze_load_{p}.sql" for p in EXPECTED_PREFIXES]

# ============================================================
# PATH RESOLVERS
# ============================================================
def ddl(name: str) -> str:
    return os.path.join(DDL_DIR, name)

def load(name: str) -> str:
    return os.path.join(LOAD_DIR, name)

def validate(name: str) -> str:
    return os.path.join(VALIDATE_DIR, name)