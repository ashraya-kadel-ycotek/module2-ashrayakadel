from airflow import DAG


def test_dag_file_imports(dag_module):
    """The DAG file must import without raising."""
    assert dag_module is not None


def test_dag_object_exists(dag):
    """The module must expose a DAG object."""
    assert isinstance(dag, DAG)


def test_dag_id(dag):
    """dag_id must match the expected value (guards against accidental rename)."""
    assert dag.dag_id == "my_etl_dag"


def test_dag_has_tasks(dag):
    """A DAG with zero tasks is almost certainly a build failure."""
    assert len(dag.tasks) > 0


def test_all_task_ids_unique(dag):
    """Airflow requires unique task_ids within a DAG."""
    task_ids = [t.task_id for t in dag.tasks]
    assert len(task_ids) == len(set(task_ids)), f"Duplicate task_ids: {task_ids}"