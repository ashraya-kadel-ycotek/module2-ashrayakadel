import importlib.util
import os
import sys
from pathlib import Path

import pytest

# Make sure the project root is importable so `from config.config import ...` works
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", Path(__file__).resolve().parents[1]))
sys.path.insert(0, str(PROJECT_ROOT))

DAG_FILE = PROJECT_ROOT / "dags" / "etl_run_dag.py"


def _load_dag_module():
    """Import dags/etl_run_dag.py as a module and return it."""
    if not DAG_FILE.exists():
        raise FileNotFoundError(f"DAG file not found: {DAG_FILE}")

    spec = importlib.util.spec_from_file_location("etl_run_dag", DAG_FILE)
    module = importlib.util.module_from_spec(spec)
    sys.modules["etl_run_dag"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def dag_module():
    """Load the DAG module once per test session."""
    return _load_dag_module()


@pytest.fixture(scope="session")
def dag(dag_module):
    """Return the single DAG object defined in the module."""
    # If you ever add more DAGs, use dag_module.dag.dag_id or pick by id.
    return dag_module.dag