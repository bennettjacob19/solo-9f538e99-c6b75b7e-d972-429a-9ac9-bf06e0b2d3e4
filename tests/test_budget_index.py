"""Budget accounting semantics, exercised against the rehearsal API.

The rehearsal side keeps PodDisruptionBudget numbers in an incremental index.
These tests pin both the arithmetic and the cost shape:

* eviction granted  -> healthy drops, matching stays;
* pod actually exits -> matching drops (and desired follows for maxUnavailable);
* budget change interleaved with eviction -> the decision uses the threshold
  and counts in force when the operation was serialised;
* rejected eviction -> no pod state change and no quota consumed;
* per-budget-operation work does not grow with the total pod count.
"""

from __future__ import annotations

from app.models import PodState
from simulator.main import STATE, SimPDB


def _budget(sim, namespace="shop", name="web-pdb"):
    state = sim.get("/api/sim/state").json()
    return next(b for b in state["budgets"] if b["namespace"] == namespace and b["name"] == name)


def _pod(sim, namespace, name):
    state = sim.get("/api/sim/state").json()
    return next(p for p in state["pods"] if p["namespace"] == namespace and p["name"] == name)


def _evict(sim, namespace, name, uid=None):
    body = {"deleteOptions": {"preconditions": {"uid": uid}}} if uid else {}
    return sim.post(
        f"/api/v1/namespaces/{namespace}/pods/{name}/eviction", json=body
    )


def test_rehearsal_page_exposes_live_budget_numbers(sim):
    budget = _budget(sim)
    # Five web pods, all healthy, minAvailable=3 -> two disruptions allowed.
    assert budget["expectedPods"] == 5
    assert budget["currentHealthy"] == 5
    assert budget["desiredHealthy"] == 3
    assert budget["disruptionsAllowed"] == 2

    page = sim.get("/").text
    for label in ("匹配数", "健康数", "期望健康数", "允许驱逐数", "预算索引自检"):
        assert label in page


def test_threshold_switch_recomputes_numbers_without_pod_walk(sim):
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 4})
    budget = _budget(sim)
    assert budget["desiredHealthy"] == 4
    assert budget["disruptionsAllowed"] == 1

    sim.post("/api/sim/budgets/shop/web-pdb", json={"maxUnavailable": 2})
    budget = _budget(sim)
    assert budget["desiredHealthy"] == 3  # 5 matching - 2
    assert budget["disruptionsAllowed"] == 2

    assert STATE.metrics["budget_pod_scans"] == 0


def test_grant_updates_healthy_and_exit_updates_matching(sim):
    uid = _pod(sim, "shop", "web-0")["uid"]
    response = _evict(sim, "shop", "web-0", uid)
    assert response.status_code == 201

    budget = _budget(sim)
    # Granted eviction: the pod is still counted as matching but no longer
    # healthy.
    assert budget["expectedPods"] == 5
    assert budget["currentHealthy"] == 4
    assert _pod(sim, "shop", "web-0")["terminating"] is True

    sim.post("/api/sim/pods/shop/web-0/exit")
    budget = _budget(sim)
    assert budget["expectedPods"] == 4
    assert budget["currentHealthy"] == 4
    assert budget["disruptionsAllowed"] == 1


def test_max_unavailable_desired_follows_matching_count(sim):
    sim.post("/api/sim/budgets/shop/web-pdb", json={"maxUnavailable": 1})
    budget = _budget(sim)
    assert (budget["expectedPods"], budget["desiredHealthy"], budget["disruptionsAllowed"]) == (
        5,
        4,
        1,
    )

    uid = _pod(sim, "shop", "web-0")["uid"]
    assert _evict(sim, "shop", "web-0", uid).status_code == 201
    budget = _budget(sim)
    # Matching count unchanged while terminating -> desired unchanged.
    assert budget["expectedPods"] == 5
    assert budget["desiredHealthy"] == 4
    assert budget["currentHealthy"] == 4
    assert budget["disruptionsAllowed"] == 0

    sim.post("/api/sim/pods/shop/web-0/exit")
    budget = _budget(sim)
    # Matching dropped by one; with maxUnavailable the desired follows.
    assert budget["expectedPods"] == 4
    assert budget["desiredHealthy"] == 3
    assert budget["currentHealthy"] == 4
    assert budget["disruptionsAllowed"] == 1


def test_rejected_eviction_changes_nothing(sim):
    # minAvailable=5: no disruption is allowed.
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 5})
    before = _budget(sim)
    response = _evict(sim, "shop", "web-0")

    assert response.status_code == 429
    after = _budget(sim)
    assert after == before
    pod = _pod(sim, "shop", "web-0")
    assert pod["terminating"] is False
    assert pod["healthy"] is True


def test_threshold_change_between_evictions_applies_to_next_one(sim):
    # Grant one with minAvailable=4 (one disruption allowed).
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 4})
    web0 = _pod(sim, "shop", "web-0")
    assert _evict(sim, "shop", "web-0", web0["uid"]).status_code == 201
    assert _budget(sim)["disruptionsAllowed"] == 0

    # A second eviction is refused; the grant did consume the quota.
    web1 = _pod(sim, "shop", "web-1")
    assert _evict(sim, "shop", "web-1", web1["uid"]).status_code == 429
    assert _pod(sim, "shop", "web-1")["terminating"] is False

    # Relax the threshold; the next eviction is judged against the new one.
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 3})
    assert _budget(sim)["disruptionsAllowed"] == 1
    assert _evict(sim, "shop", "web-1", web1["uid"]).status_code == 201
    assert _pod(sim, "shop", "web-1")["terminating"] is True


def test_eviction_uses_uid_precondition_and_rejection_keeps_counts(sim):
    old_uid = _pod(sim, "shop", "web-0")["uid"]
    sim.post("/api/sim/pods/shop/web-0/replace", json={"node": "node-a"})
    budget_before = _budget(sim)

    response = _evict(sim, "shop", "web-0", old_uid)
    assert response.status_code == 409
    assert _budget(sim) == budget_before
    assert _pod(sim, "shop", "web-0")["terminating"] is False


def test_budget_operations_do_not_scan_more_as_pods_grow(sim):
    """Per-budget-operation traversal count stays flat as pods are added.

    ``budget_pod_scans`` counts only deliberate full walks (the reconcile
    self-check).  PDB reads, threshold changes, grants and exits must not add
    any, while ``budget_state_reads`` / ``budget_index_updates`` grow by a
    bounded amount per operation.
    """
    small = sim.post("/api/sim/budget-index/benchmark", json={"pods": 10}).json()
    large = sim.post("/api/sim/budget-index/benchmark", json={"pods": 2000}).json()

    assert large["pods"] > small["pods"]
    assert small["pod_scans_added"] == 0
    assert large["pod_scans_added"] == 0
    # A benchmark performs exactly 100 indexed reads regardless of pod count.
    assert small["budget_reads"] == large["budget_reads"] == 100

    # The same holds while doing real work on the shared budget.
    before = STATE.metrics.copy()
    sim.post("/api/sim/budgets/shop/web-pdb", json={"maxUnavailable": 1})
    _budget(sim)
    uid = _pod(sim, "shop", "web-0")["uid"]
    _evict(sim, "shop", "web-0", uid)
    sim.post("/api/sim/pods/shop/web-0/exit")
    _budget(sim)

    assert STATE.metrics["budget_pod_scans"] == before["budget_pod_scans"]
    # Index updates are per affected pod/budget, not per cluster pod.
    assert STATE.metrics["budget_index_updates"] - before["budget_index_updates"] <= 6


def test_index_cross_check_matches_reference_after_many_operations(sim):
    sim.post("/api/sim/budgets/shop/web-pdb", json={"maxUnavailable": 1})
    for name in ("web-0", "web-1"):
        uid = _pod(sim, "shop", name)["uid"]
        _evict(sim, "shop", name, uid)
    sim.post("/api/sim/pods/exit-terminating")
    sim.post("/api/sim/pods/shop/web-2/replace", json={"node": "node-b"})
    sim.post(
        "/api/sim/pods",
        json={"namespace": "shop", "name": "web-new", "node": "node-a", "labels": {"app": "web"}},
    )
    sim.post("/api/sim/pods/shop/web-3/ready", json={"ready": False})
    sim.post("/api/sim/budgets/shop/web-pdb", json={"minAvailable": 2})

    result = sim.post("/api/sim/budget-index/reconcile")
    assert result.status_code == 200
    assert result.json()["ok"] is True

    # The index equals the reference computation over the live pod list.
    for key, pdb in STATE.pdbs.items():
        reference = pdb.budget(list(STATE.pods.values()))
        assert STATE.budget_status(pdb) == reference, key


def test_drain_path_budget_work_is_flat_in_cluster_pod_count(manager, sim):
    """Driving a drain does the same budget work with 0 or 2000 extra pods."""
    def measure() -> tuple[int, int]:
        before = STATE.metrics.copy()
        round_ = manager.start("node-a")
        manager.run(round_.round_id)
        sim.post("/api/sim/pods/exit-terminating")
        manager.recheck(round_.round_id)
        after = STATE.metrics
        return (
            after["budget_index_updates"] - before["budget_index_updates"],
            after["budget_pod_scans"] - before["budget_pod_scans"],
        )

    small_updates, small_scans = measure()

    STATE.reset()
    sim.post("/api/sim/budget-index/benchmark", json={"pods": 2000})
    large_updates, large_scans = measure()

    assert large_scans == small_scans == 0
    # Per-eviction index maintenance is bounded by the budgets that cover the
    # evicted pod; unrelated pods in the cluster add no work.
    assert large_updates == small_updates


def test_budget_accounting_matches_drain_status(manager, sim):
    """The drain page's per-row budget summary reflects the same numbers."""
    sim.post("/api/sim/budgets/shop/web-pdb", json={"maxUnavailable": 1})
    round_ = manager.start("node-a")
    manager.run(round_.round_id)

    web_rows = [p for p in round_.evictable if p.name.startswith("web")]
    for row in web_rows:
        budget = next(b for b in row.budgets if b.name == "web-pdb")
        live = _budget(sim)
        assert budget.expected == live["expectedPods"]
        assert budget.healthy == live["currentHealthy"]
        assert budget.desired == live["desiredHealthy"]
        assert budget.allowed == live["disruptionsAllowed"]

    assert any(p.state is PodState.TERMINATING for p in web_rows)
    assert any(p.state is PodState.BLOCKED for p in web_rows)
