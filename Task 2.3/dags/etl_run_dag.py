import os
import sys
import pendulum

sys.path.insert(0, os.environ.get("PROJECT_ROOT", "/opt/airflow"))

from config.config import (
    DATA_DIR,
    EXPECTED_PREFIXES,
    SNOWFLAKE_CONN_ID,
    BATCH_LOG_TABLE,
    DAG_ID,
    DAG_SCHEDULE,
    DAG_START_DATE,
    DAG_MAX_ACTIVE_RUNS,
    DAG_TAGS,
    DDL_SCRIPTS,
    CONTROL_SCRIPTS,
    BRONZE_LOAD_SCRIPTS,
    SILVER_RESET,
    SILVER_PHASE1,
    SILVER_PHASE2,
    GOLD_DIM_PHASE1,
    GOLD_DIM_PHASE2,
    GOLD_DIM_PHASE3,
    GOLD_DIM_PHASE4,
    GOLD_FCT_PHASE1,
    GOLD_FCT_PHASE2,
    VALIDATE_RECON,
    ddl,
    load,
    validate,
)

from airflow.sdk import DAG, TaskGroup, task
try:
    from airflow.providers.standard.sensors.python import PythonSensor
except ImportError:
    from airflow.sensors.python import PythonSensor
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

DAG_TZ = pendulum.timezone("UTC")


def _layer_for(task_id: str) -> str:
    if task_id.startswith(("load_bronze_data", "run_bronze_ddl", "stamp_bronze")):
        return "bronze"
    if task_id.startswith(("silver_", "run_silver_ddl", "stamp_silver")):
        return "silver"
    if task_id.startswith(("gold_", "run_gold_ddl", "stamp_gold")):
        return "gold"
    return "control"


def _log_to_batch_log(context, status: str, error_msg: str | None = None):
    ti = context["ti"]
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    safe_err = ""
    if error_msg:
        safe_err = str(error_msg)[:4000].replace("'", "''").replace("\n", " ")
    hook.run(sql=f"""
        INSERT INTO {BATCH_LOG_TABLE}
            (RUN_ID, BATCH_ID, LAYER, SCRIPT_NAME, TASK_ID, STATUS,
             ELAPSED_MS, ERROR_MESSAGE, STARTED_AT, FINISHED_AT)
        SELECT
            '{context['run_id']}',
            DATE '{context['ds']}',
            '{_layer_for(ti.task_id)}',
            NULL,
            '{ti.task_id}',
            '{status}',
            DATEDIFF('millisecond', TO_TIMESTAMP_TZ('{ti.start_date}'), CURRENT_TIMESTAMP()),
            NULLIF('{safe_err}', ''),
            TO_TIMESTAMP_TZ('{ti.start_date}'),
            CURRENT_TIMESTAMP()
    """)


def log_success(context):
    _log_to_batch_log(context, "SUCCESS")


def log_failure(context):
    _log_to_batch_log(context, "FAILED", context.get("exception", "unknown error"))


CALLBACKS = {"on_success_callback": log_success, "on_failure_callback": log_failure}


def check_five_files(templates_dict=None, **context):
    ds_nodash = templates_dict["ds_nodash"]
    found, missing, empty = set(), set(), []
    for prefix in EXPECTED_PREFIXES:
        path = os.path.join(DATA_DIR, f"{prefix}_{ds_nodash}.csv")
        if not os.path.exists(path):
            missing.add(prefix)
        elif os.path.getsize(path) == 0:
            empty.append(path)
        else:
            found.add(prefix)
    print(f"ds={ds_nodash} found={sorted(found)} missing={sorted(missing)} empty={empty}")
    return not missing and not empty


with DAG(
    dag_id=DAG_ID,
    start_date=pendulum.datetime(*DAG_START_DATE, tz=DAG_TZ),
    schedule=DAG_SCHEDULE,
    catchup=False,
    max_active_runs=DAG_MAX_ACTIVE_RUNS,
    tags=DAG_TAGS,
    template_searchpath=[ddl(""), load(""), validate("")],
) as dag:

    @task
    def start_task():
        print("This is the test start.")

    @task
    def test_snowflake_connection():
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        print("Successfully connected to Snowflake!")
        conn.close()

    @task
    def end_task():
        print(
            "Pipeline complete: control + params, bronze, silver, "
            "gold dims, gold facts, gold agg, watermarks stamped, "
            "net-sales recon PASSED."
        )

    # NOTE: prefix_group_id=False keeps every task_id exactly as before, so
    # _layer_for(), the C_BATCH_LOG rows and your task history keep working.

    # ============ CONTROL ============
    with TaskGroup(group_id="control", prefix_group_id=False) as control_grp:
        run_control_ddl = SQLExecuteQueryOperator(
            task_id="run_control_ddl",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=DDL_SCRIPTS["control"],
            split_statements=True,
            autocommit=True,
        )

        init_params = SQLExecuteQueryOperator(
            task_id="init_params",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=CONTROL_SCRIPTS["init_params"],
            split_statements=True,
            autocommit=True,
        )

        run_control_ddl >> init_params

    # ============ CHECKS (connection + file gate) ============
    with TaskGroup(group_id="checks", prefix_group_id=False) as checks_grp:
        snowflake_test = test_snowflake_connection()

        check_complete = PythonSensor(
            task_id="check_five_files",
            python_callable=check_five_files,
            templates_dict={"ds_nodash": "{{ ds_nodash }}"},
            poke_interval=60,
            timeout=3600,
            mode="reschedule",
        )

        snowflake_test >> check_complete

    # ============ BRONZE ============
    with TaskGroup(group_id="bronze", prefix_group_id=False) as bronze_grp:
        run_bronze_ddl = SQLExecuteQueryOperator(
            task_id="run_bronze_ddl",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=DDL_SCRIPTS["bronze"],
            split_statements=True,
            autocommit=True,
        )

        load_bronze = SQLExecuteQueryOperator.partial(
            task_id="load_bronze_data",
            conn_id=SNOWFLAKE_CONN_ID,
            split_statements=True,
            autocommit=False,
            **CALLBACKS,
        ).expand(sql=BRONZE_LOAD_SCRIPTS)

        stamp_bronze = SQLExecuteQueryOperator(
            task_id="stamp_bronze",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=CONTROL_SCRIPTS["stamp_bronze"],
            split_statements=True,
            autocommit=True,
        )

        run_bronze_ddl >> load_bronze >> stamp_bronze

    # ============ SILVER ============
    with TaskGroup(group_id="silver", prefix_group_id=False) as silver_grp:
        run_silver_ddl = SQLExecuteQueryOperator(
            task_id="run_silver_ddl",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=DDL_SCRIPTS["silver"],
            split_statements=True,
            autocommit=True,
        )

        silver_reset = SQLExecuteQueryOperator(
            task_id="silver_reset",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=SILVER_RESET,
            split_statements=True,
            autocommit=False,
        )

        silver_phase1 = SQLExecuteQueryOperator.partial(
            task_id="silver_load_phase1",
            conn_id=SNOWFLAKE_CONN_ID,
            split_statements=True,
            autocommit=False,
            **CALLBACKS,
        ).expand(sql=SILVER_PHASE1)

        silver_phase2 = SQLExecuteQueryOperator.partial(
            task_id="silver_load_phase2",
            conn_id=SNOWFLAKE_CONN_ID,
            split_statements=True,
            autocommit=False,
            **CALLBACKS,
        ).expand(sql=SILVER_PHASE2)

        stamp_silver = SQLExecuteQueryOperator(
            task_id="stamp_silver",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=CONTROL_SCRIPTS["stamp_silver"],
            split_statements=True,
            autocommit=True,
        )

        run_silver_ddl >> silver_reset >> silver_phase1 >> silver_phase2 >> stamp_silver

    # ============ GOLD ============
    with TaskGroup(group_id="gold", prefix_group_id=False) as gold_grp:
        run_gold_ddl = SQLExecuteQueryOperator(
            task_id="run_gold_ddl",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=DDL_SCRIPTS["gold"],
            split_statements=True,
            autocommit=True,
        )

        with TaskGroup(group_id="gold_dims", prefix_group_id=False) as gold_dims_grp:
            gold_dim_phase1 = SQLExecuteQueryOperator.partial(
                task_id="gold_load_dim_phase1",
                conn_id=SNOWFLAKE_CONN_ID,
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            ).expand(sql=GOLD_DIM_PHASE1)

            gold_dim_phase2 = SQLExecuteQueryOperator.partial(
                task_id="gold_load_dim_phase2",
                conn_id=SNOWFLAKE_CONN_ID,
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            ).expand(sql=GOLD_DIM_PHASE2)

            gold_dim_phase3 = SQLExecuteQueryOperator.partial(
                task_id="gold_load_dim_phase3",
                conn_id=SNOWFLAKE_CONN_ID,
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            ).expand(sql=GOLD_DIM_PHASE3)

            gold_dim_phase4 = SQLExecuteQueryOperator(
                task_id="gold_load_dim_item",
                conn_id=SNOWFLAKE_CONN_ID,
                sql=GOLD_DIM_PHASE4[0],
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            )

            gold_dim_phase1 >> gold_dim_phase2 >> gold_dim_phase3 >> gold_dim_phase4

        with TaskGroup(group_id="gold_facts", prefix_group_id=False) as gold_facts_grp:
            gold_fct_phase1 = SQLExecuteQueryOperator.partial(
                task_id="gold_load_fct_phase1",
                conn_id=SNOWFLAKE_CONN_ID,
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            ).expand(sql=GOLD_FCT_PHASE1)

            gold_fct_phase2 = SQLExecuteQueryOperator(
                task_id="gold_load_fct_sales_per_week",
                conn_id=SNOWFLAKE_CONN_ID,
                sql=GOLD_FCT_PHASE2,
                split_statements=True,
                autocommit=False,
                **CALLBACKS,
            )

            gold_fct_phase1 >> gold_fct_phase2

        stamp_gold = SQLExecuteQueryOperator(
            task_id="stamp_gold",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=CONTROL_SCRIPTS["stamp_gold"],
            split_statements=True,
            autocommit=True,
        )

        run_gold_ddl >> gold_dims_grp >> gold_facts_grp >> stamp_gold

    # ============ RECONCILIATION ============
    with TaskGroup(group_id="recon", prefix_group_id=False) as recon_grp:
        validate_recon = SQLExecuteQueryOperator(
            task_id="validate_recon_net_sales",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=VALIDATE_RECON,
            parameters={"batch_id": "{{ ds }}"},
            split_statements=True,
            autocommit=True,
            **CALLBACKS,
        )

    # ============ FLOW ============
    start = start_task()
    end = end_task()

    (
        start
        >> control_grp
        >> checks_grp
        >> bronze_grp
        >> silver_grp
        >> gold_grp
        >> recon_grp
        >> end
    )