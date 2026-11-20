import pytest


def test_no_cycles(dag):
    """Airflow's canonical cycle check — raises if the graph has a cycle."""
    assert dag.test_cycle() is None


def test_topological_sort_succeeds(dag):
    """topological_sort() raises AirflowDagCycleException on a cyclic graph."""
    ordered = dag.topological_sort()
    assert len(ordered) == len(dag.tasks)


def test_every_task_reachable_from_root(dag):
    """Every task must be reachable from at least one root (no orphans)."""
    roots = dag.roots
    assert len(roots) >= 1, "DAG has no root tasks"

    reachable = set()
    for root in roots:
        reachable.update(dag.get_task(t.task_id) for t in root.get_flat_relative_ids(upstream=False) or [])
        reachable.add(root)
        for downstream in dag.partial_subset(
            task_ids_or_regex=None,
            include_upstream=False,
            include_downstream=True,
            include_direct_upstream=False,
        ).tasks:
            reachable.add(downstream)

    # Simpler & more robust: walk from each root via get_direct_relative_ids
    reachable = set()
    stack = list(roots)
    while stack:
        t = stack.pop()
        if t.task_id in reachable:
            continue
        reachable.add(t.task_id)
        for child_id in t.get_direct_relative_ids(upstream=False):
            stack.append(dag.get_task(child_id))

    all_ids = {t.task_id for t in dag.tasks}
    assert reachable == all_ids, f"Orphan tasks: {all_ids - reachable}"


def test_no_task_has_self_loop(dag):
    """A task must not depend on itself."""
    for t in dag.tasks:
        assert t.task_id not in t.upstream_task_ids, f"{t.task_id} depends on itself (upstream)"
        assert t.task_id not in t.downstream_task_ids, f"{t.task_id} depends on itself (downstream)"