# DAG tests

Run from the project root:

    pytest tests/ -v

What's covered:
- test_dag_parses.py    -> DAG file imports, DAG object exists, dag_id, tasks exist, unique task_ids
- test_dag_no_cycles.py -> no cycles, topological sort works, all tasks reachable, no self-loops

