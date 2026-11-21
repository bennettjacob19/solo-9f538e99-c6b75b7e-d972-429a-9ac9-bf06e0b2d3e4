"""A small, stateful Kubernetes API look-alike for rehearsing node drains.

It is not a Kubernetes distribution.  It implements only the handful of
endpoints the drain UI uses, but it implements their *semantics* faithfully
where the exercise depends on them:

* ``PATCH /api/v1/nodes/{name}`` for cordoning (merge patch).
* ``GET /api/v1/pods?fieldSelector=spec.nodeName=...`` for listing.
* ``POST /api/v1/namespaces/{ns}/pods/{name}/eviction`` implementing the
  ``policy/v1`` Eviction subresource, including the UID precondition.
* ``GET /apis/policy/v1/poddisruptionbudgets`` with live budget accounting.

Eviction decisions are made under one lock against freshly computed budget
status, so two nodes drained concurrently cannot jointly exceed a shared
PodDisruptionBudget.

The ``/api/sim`` endpoints and the page at ``/`` let an operator change a
budget, replace a pod under the same name (new UID), make a replacement ready,
or let an evicted pod finish terminating.
"""

from __future__ import annotations

import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response
from fastapi.templating import Jinja2Templates

BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

NODE_NAMES = ["node-a", "node-b", "node-c"]


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _new_uid() -> str:
    return uuid.uuid4().hex


@dataclass
class SimPod:
    name: str
    namespace: str
    node: str
    uid: str = field(default_factory=_new_uid)
    labels: dict[str, str] = field(default_factory=dict)
    owner_kind: str | None = None
    static: bool = False
    ready: bool = True
    phase: str = "Running"
    terminating: bool = False
    deletion_timestamp: str | None = None
    resource_version: int = 1

    @property
    def key(self) -> tuple[str, str]:
        return (self.namespace, self.name)

    @property
    def healthy(self) -> bool:
        return self.phase == "Running" and self.ready and not self.terminating


@dataclass
class SimPDB:
    name: str
    namespace: str
    selector: dict[str, str]
    min_available: int | None = None
    max_unavailable: int | None = None
    generation: int = 1
    resource_version: int = 1

    def budget_from_counts(self, expected: int, healthy: int) -> dict[str, int]:
        """Budget status from the indexed counts, without touching pods.

        ``expected`` is the number of matching pod objects; a pod that is
        terminating still matches until it actually exits.  ``healthy`` drops
        as soon as a pod stops being ready, including the moment an eviction is
        granted.  For ``maxUnavailable`` the desired count is derived from the
        current match count, so it follows pods leaving the index.
        """
        if self.min_available is not None:
            desired = self.min_available
        else:
            desired = max(0, expected - (self.max_unavailable or 0))
        return {
            "disruptionsAllowed": max(0, healthy - desired),
            "currentHealthy": healthy,
            "desiredHealthy": desired,
            "expectedPods": expected,
        }

    def budget(self, pods: list[SimPod]) -> dict[str, int]:
        """Reference implementation over an explicit pod list.

        Normal request paths never call this: they read the incremental index
        maintained by :class:`ClusterState`, which keeps per-operation cost
        independent of the total pod count.
        """
        matching = [
            pod
            for pod in pods
            if pod.namespace == self.namespace
            and all(pod.labels.get(k) == v for k, v in self.selector.items())
        ]
        return self.budget_from_counts(
            len(matching), sum(1 for pod in matching if pod.healthy)
        )


@dataclass
class SimNode:
    name: str
    unschedulable: bool = False
    ready: bool = True
    uid: str = field(default_factory=_new_uid)
    resource_version: int = 1


class ClusterState:
    """Cluster state plus incremental PodDisruptionBudget accounting.

    Budget numbers are maintained as an index while pods change, not by
    re-scanning every pod:

    * ``_pdb_members[pod_key]`` records which budgets match that pod.
    * ``_pdb_counts[budget_key]`` is ``[expected, healthy]``.
    * ``_pdb_selectors[(namespace, label, value)]`` finds candidate budgets
      from a pod's labels without walking the pod list.

    An eviction request therefore costs O(labels + matching budgets), a
    budget read costs O(1), and letting a pod exit costs O(budgets matching
    that pod) — none of them grows with the total number of pods.
    """

    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.nodes: dict[str, SimNode] = {}
        self.pods: dict[tuple[str, str], SimPod] = {}
        self.pdbs: dict[tuple[str, str], SimPDB] = {}
        self.events: list[dict[str, Any]] = []
        # Budget index; rebuilt by reset() and maintained by the mutators.
        self._pdb_selectors: dict[tuple[str, str, str], set[tuple[str, str]]] = {}
        self._empty_selector_pdbs: dict[str, set[tuple[str, str]]] = {}
        self._pdb_members: dict[tuple[str, str], set[tuple[str, str]]] = {}
        self._pdb_counts: dict[tuple[str, str], list[int]] = {}
        # Observability for the rehearsal page and the scaling self-test.
        # ``budget_pod_scans`` counts full pod-list walks taken on a budget
        # path; it must stay 0 while serving requests if the index is used,
        # while ``budget_state_reads`` grows once per budget number read.
        self.metrics: dict[str, int] = {
            "budget_pod_scans": 0,
            "budget_state_reads": 0,
            "budget_index_updates": 0,
            "budget_index_builds": 0,
        }
        self.reset()

    # -- seed --------------------------------------------------------------
    def reset(self) -> None:
        with self.lock:
            self.nodes = {name: SimNode(name=name) for name in NODE_NAMES}
            self.pods = {}
            self.pdbs = {}
            self.events = []
            self._reset_budget_index()

            def add(
                namespace: str,
                name: str,
                node: str,
                labels: dict[str, str],
                owner_kind: str | None = None,
                static: bool = False,
            ) -> None:
                pod = SimPod(
                    namespace=namespace,
                    name=name,
                    node=node,
                    labels=labels,
                    owner_kind=owner_kind,
                    static=static,
                )
                self.pods[pod.key] = pod

            # A ReplicaSet-like workload spread over all three nodes.
            for index, node in enumerate(["node-a", "node-a", "node-b", "node-b", "node-c"]):
                add("shop", f"web-{index}", node, {"app": "web"}, owner_kind="ReplicaSet")
            # A two-replica cache, one pod on each of the two target nodes.
            add("shop", "cache-0", "node-a", {"app": "cache"}, owner_kind="StatefulSet")
            add("shop", "cache-1", "node-b", {"app": "cache"}, owner_kind="StatefulSet")
            # DaemonSets mirror nodes and must never be evicted.
            for suffix, node in zip("abc", ["node-a", "node-b", "node-c"]):
                add(
                    "kube-system",
                    f"log-agent-{suffix}",
                    node,
                    {"app": "log-agent"},
                    owner_kind="DaemonSet",
                )
            # A static pod owned by the kubelet.
            add("kube-system", "kube-vip-a", "node-a", {"app": "kube-vip"}, static=True)

            self.pdbs[("shop", "web-pdb")] = SimPDB(
                name="web-pdb", namespace="shop", selector={"app": "web"}, min_available=3
            )
            self.pdbs[("shop", "cache-pdb")] = SimPDB(
                name="cache-pdb", namespace="shop", selector={"app": "cache"}, min_available=1
            )
            # One index build at seed time; later operations only adjust it.
            self._rebuild_budget_index()
            self._event("reset", "演练环境已恢复到初始状态")

    # -- budget index ------------------------------------------------------
    def _reset_budget_index(self) -> None:
        self._pdb_selectors = {}
        self._empty_selector_pdbs = {}
        self._pdb_members = {}
        self._pdb_counts = {}
        for metric in (
            "budget_pod_scans",
            "budget_state_reads",
            "budget_index_updates",
        ):
            self.metrics[metric] = 0
        self.metrics["budget_index_builds"] = 0

    def _register_pdb(self, pdb: SimPDB) -> None:
        key = (pdb.namespace, pdb.name)
        self._pdb_counts[key] = [0, 0]
        if not pdb.selector:
            self._empty_selector_pdbs.setdefault(pdb.namespace, set()).add(key)
            return
        for label, value in pdb.selector.items():
            self._pdb_selectors.setdefault(
                (pdb.namespace, label, value), set()
            ).add(key)

    def _candidate_pdbs(
        self, namespace: str, labels: dict[str, str]
    ) -> set[tuple[str, str]]:
        """Budgets that could match, from the label index only."""
        candidates = set(self._empty_selector_pdbs.get(namespace, ()))
        selectors = self._pdb_selectors
        for label, value in labels.items():
            candidates.update(selectors.get((namespace, label, value), ()))
        return candidates

    def _matching_budgets(
        self, namespace: str, labels: dict[str, str]
    ) -> set[tuple[str, str]]:
        matched: set[tuple[str, str]] = set()
        for key in self._candidate_pdbs(namespace, labels):
            pdb = self.pdbs.get(key)
            if pdb is None:
                continue
            if all(labels.get(k) == v for k, v in pdb.selector.items()):
                matched.add(key)
        return matched

    def _attach_pod(self, pod: SimPod) -> None:
        """Add one pod to the budget index.  Cost: labels + matching budgets."""
        self._detach_pod(pod)
        members = self._matching_budgets(pod.namespace, pod.labels)
        self._pdb_members[pod.key] = members
        healthy = 1 if pod.healthy else 0
        for key in members:
            counts = self._pdb_counts.setdefault(key, [0, 0])
            counts[0] += 1
            counts[1] += healthy
        self.metrics["budget_index_updates"] += 1

    def _detach_pod(self, pod: SimPod) -> None:
        """Remove one pod from the budget index.

        Matching count drops here — this is the "pod actually exited" edge,
        not the moment eviction was granted.
        """
        members = self._pdb_members.pop(pod.key, None)
        if not members:
            return
        healthy = 1 if pod.healthy else 0
        for key in members:
            counts = self._pdb_counts.get(key)
            if counts is None:
                continue
            counts[0] -= 1
            counts[1] -= healthy
        self.metrics["budget_index_updates"] += 1

    def _adjust_pod_health(self, pod: SimPod, delta: int) -> None:
        """Apply one healthy-count transition to every budget covering the pod."""
        for key in self._pdb_members.get(pod.key, ()):
            counts = self._pdb_counts.get(key)
            if counts is not None:
                counts[1] += delta
        self.metrics["budget_index_updates"] += 1

    def _refresh_pod_health(self, pod: SimPod, was_healthy: bool) -> None:
        """Adjust healthy counts after a readiness change."""
        if was_healthy == pod.healthy:
            return
        self._adjust_pod_health(pod, 1 if pod.healthy else -1)

    def _rebuild_budget_index(self) -> None:
        """Seed the index.  Only run on reset, never on a request path."""
        self._pdb_selectors = {}
        self._empty_selector_pdbs = {}
        self._pdb_members = {}
        self._pdb_counts = {}
        for pdb in self.pdbs.values():
            self._register_pdb(pdb)
        for pod in self.pods.values():
            members = self._matching_budgets(pod.namespace, pod.labels)
            self._pdb_members[pod.key] = members
            healthy = 1 if pod.healthy else 0
            for key in members:
                counts = self._pdb_counts.setdefault(key, [0, 0])
                counts[0] += 1
                counts[1] += healthy
        self.metrics["budget_index_builds"] += 1

    def budget_status(self, pdb: SimPDB) -> dict[str, int]:
        """Current budget numbers from the index.  Cost: O(1) in pod count."""
        counts = self._pdb_counts.get((pdb.namespace, pdb.name), [0, 0])
        self.metrics["budget_state_reads"] += 1
        return pdb.budget_from_counts(counts[0], counts[1])

    def reconcile_budget_index(self) -> None:
        """Full pod walk that cross-checks the index (tests and drills only).

        Its cost grows with the pod count by design, so it is never called on
        a normal request path.  ``budget_pod_scans`` records how many such
        walks happened.
        """
        self.metrics["budget_pod_scans"] += 1
        expected: dict[tuple[str, str], list[int]] = {
            key: [0, 0] for key in self.pdbs
        }
        for pod in self.pods.values():
            for key in self._matching_budgets(pod.namespace, pod.labels):
                counts = expected.setdefault(key, [0, 0])
                counts[0] += 1
                counts[1] += 1 if pod.healthy else 0
        for key, counts in expected.items():
            if self._pdb_counts.get(key) != counts:
                raise RuntimeError(
                    f"预算索引不一致 {key}: 索引 {self._pdb_counts.get(key)}，"
                    f"全量重算 {counts}"
                )
        if set(self._pdb_members) != set(self.pods):
            missing = set(self.pods) - set(self._pdb_members)
            extra = set(self._pdb_members) - set(self.pods)
            raise RuntimeError(f"预算成员索引不一致：缺少 {missing}，多余 {extra}")

    def _event(self, kind: str, message: str, **extra: Any) -> None:
        self.events.insert(
            0,
            {"at": _now_iso(), "kind": kind, "message": message, **extra},
        )
        del self.events[200:]

    # -- serialisation -----------------------------------------------------
    def node_json(self, node: SimNode) -> dict[str, Any]:
        return {
            "apiVersion": "v1",
            "kind": "Node",
            "metadata": {
                "name": node.name,
                "uid": node.uid,
                "resourceVersion": str(node.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
                "labels": {"kubernetes.io/hostname": node.name},
            },
            "spec": {"unschedulable": node.unschedulable},
            "status": {
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if node.ready else "False",
                        "reason": "KubeletReady" if node.ready else "KubeletNotReady",
                    }
                ],
                "capacity": {"cpu": "8", "memory": "32Gi", "pods": "110"},
            },
        }

    def pod_json(self, pod: SimPod) -> dict[str, Any]:
        metadata: dict[str, Any] = {
            "name": pod.name,
            "namespace": pod.namespace,
            "uid": pod.uid,
            "resourceVersion": str(pod.resource_version),
            "creationTimestamp": "2026-01-01T00:00:00Z",
            "labels": dict(pod.labels),
        }
        if pod.terminating and pod.deletion_timestamp:
            metadata["deletionTimestamp"] = pod.deletion_timestamp
            metadata["deletionGracePeriodSeconds"] = 30
        if pod.owner_kind:
            metadata["ownerReferences"] = [
                {
                    "apiVersion": "apps/v1",
                    "kind": pod.owner_kind,
                    "name": f"{pod.name.rsplit('-', 1)[0]}-owner",
                    "uid": _new_uid(),
                    "controller": True,
                }
            ]
        if pod.static:
            metadata["annotations"] = {
                "kubernetes.io/config.mirror": _new_uid(),
                "kubernetes.io/config.source": "file",
            }
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": metadata,
            "spec": {
                "nodeName": pod.node,
                "containers": [{"name": "app", "image": "registry.local/app:1.0"}],
            },
            "status": {
                "phase": pod.phase,
                "conditions": [
                    {
                        "type": "Ready",
                        "status": "True" if pod.healthy else "False",
                    }
                ],
                "containerStatuses": [
                    {
                        "name": "app",
                        "ready": pod.healthy,
                        "restartCount": 0,
                        "image": "registry.local/app:1.0",
                        "imageID": "registry.local/app@sha256:"
                        + ("0" * 64),
                        "containerID": f"containerd://{pod.uid[:16]}",
                        "started": True if pod.healthy else False,
                        "state": {"running": {}} if pod.healthy else {"waiting": {"reason": "ContainerCreating"}},
                    }
                ],
            },
        }

    def pdb_json(self, pdb: SimPDB) -> dict[str, Any]:
        status = self.budget_status(pdb)
        spec: dict[str, Any] = {"selector": {"matchLabels": dict(pdb.selector)}}
        if pdb.min_available is not None:
            spec["minAvailable"] = pdb.min_available
        else:
            spec["maxUnavailable"] = pdb.max_unavailable or 0
        return {
            "apiVersion": "policy/v1",
            "kind": "PodDisruptionBudget",
            "metadata": {
                "name": pdb.name,
                "namespace": pdb.namespace,
                "uid": f"pdb-{pdb.namespace}-{pdb.name}",
                "generation": pdb.generation,
                "resourceVersion": str(pdb.resource_version),
                "creationTimestamp": "2026-01-01T00:00:00Z",
            },
            "spec": spec,
            "status": {
                "observedGeneration": pdb.generation,
                **status,
                "disruptionsAllowed": status["disruptionsAllowed"],
            },
        }


STATE = ClusterState()
app = FastAPI(title="drain rehearsal simulator", docs_url=None, redoc_url=None)


# -- helpers ---------------------------------------------------------------
def status_response(code: int, reason: str, message: str) -> JSONResponse:
    kind = "Status"
    return JSONResponse(
        status_code=code,
        content={
            "apiVersion": "v1",
            "kind": kind,
            "status": "Failure" if code >= 400 else "Success",
            "message": message,
            "reason": reason,
            "code": code,
        },
        media_type="application/json",
    )


def _find_pod(namespace: str, name: str) -> SimPod | None:
    return STATE.pods.get((namespace, name))


def _field_selector_matches(pod: SimPod, selector: str) -> bool:
    if not selector:
        return True
    for term in selector.split(","):
        if "=" not in term:
            continue
        key, _, value = term.partition("=")
        key = key.strip()
        value = value.lstrip("=")
        if key == "spec.nodeName" and pod.node != value:
            return False
        if key == "metadata.namespace" and pod.namespace != value:
            return False
        if key == "metadata.name" and pod.name != value:
            return False
        if key == "status.phase" and pod.phase != value:
            return False
    return True


def _eviction_precondition_uid(body: dict[str, Any]) -> str | None:
    options = body.get("deleteOptions") or {}
    preconditions = options.get("preconditions") or {}
    return preconditions.get("uid")


# -- Kubernetes API surface ------------------------------------------------
@app.get("/api/v1/nodes")
def list_nodes() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.node_json(node) for node in STATE.nodes.values()]
    return {
        "apiVersion": "v1",
        "kind": "NodeList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/api/v1/nodes/{name}")
def read_node(name: str) -> Response:
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        return JSONResponse(STATE.node_json(node))


@app.patch("/api/v1/nodes/{name}")
async def patch_node(name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        node = STATE.nodes.get(name)
        if node is None:
            return status_response(404, "NotFound", f'nodes "{name}" not found')
        if isinstance(body, dict):
            desired = body.get("spec", {}).get("unschedulable")
            if desired is not None:
                node.unschedulable = bool(desired)
                node.resource_version += 1
                STATE._event(
                    "cordon" if node.unschedulable else "uncordon",
                    f"节点 {name} 已{'封锁' if node.unschedulable else '解除封锁'}",
                )
        return JSONResponse(STATE.node_json(node))


@app.get("/api/v1/pods")
def list_pods(fieldSelector: str | None = None, labelSelector: str | None = None) -> dict[str, Any]:
    with STATE.lock:
        pods = [
            STATE.pod_json(pod)
            for pod in STATE.pods.values()
            if _field_selector_matches(pod, fieldSelector or "")
        ]
    if labelSelector:
        # The drain UI does not use this, but accept the usual k=v form.
        required = {}
        for term in labelSelector.split(","):
            if "=" in term:
                key, _, value = term.partition("=")
                required[key.strip()] = value.strip()
        pods = [
            pod
            for pod in pods
            if all(pod["metadata"].get("labels", {}).get(k) == v for k, v in required.items())
        ]
    return {
        "apiVersion": "v1",
        "kind": "PodList",
        "metadata": {"resourceVersion": "1"},
        "items": pods,
    }


@app.get("/api/v1/namespaces/{namespace}/pods/{name}")
def read_pod(namespace: str, name: str) -> Response:
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        return JSONResponse(STATE.pod_json(pod))


@app.get("/api/v1/pods/{name}")
def read_pod_any_namespace(name: str) -> Response:
    with STATE.lock:
        matches = [pod for pod in STATE.pods.values() if pod.name == name]
        if not matches:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        return JSONResponse(STATE.pod_json(matches[0]))


@app.post("/api/v1/namespaces/{namespace}/pods/{name}/eviction")
async def evict_pod(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    precondition_uid = _eviction_precondition_uid(body)
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(
                404, "NotFound", f'pods "{name}" not found in namespace "{namespace}"'
            )
        if precondition_uid and precondition_uid != pod.uid:
            STATE._event(
                "eviction-rejected",
                f"{namespace}/{name} 的 UID 前置条件不匹配（请求 {precondition_uid[:8]}，当前 {pod.uid[:8]}）",
            )
            return status_response(
                409,
                "Conflict",
                f'Eviction of pod "{name}" rejected: UID precondition failed',
            )
        if pod.static:
            return status_response(
                500, "InternalError", f'cannot evict static pod "{namespace}/{name}"'
            )
        if pod.terminating:
            STATE._event("eviction-idempotent", f"{namespace}/{name} 已在退出中")
            return JSONResponse(
                {"apiVersion": "policy/v1", "kind": "Eviction", "metadata": {"name": name}},
                status_code=201,
            )
        matching_pdbs = [
            STATE.pdbs[key]
            for key in STATE._matching_budgets(pod.namespace, pod.labels)
        ]
        # Recompute from the index, then apply this one candidate: a pod is
        # healthy when counted, so one disruption is needed.  A rejected
        # eviction touches neither the index nor the pod object.
        for pdb in matching_pdbs:
            status = STATE.budget_status(pdb)
            if status["disruptionsAllowed"] <= 0:
                STATE._event(
                    "eviction-blocked",
                    f"{namespace}/{name} 受 {pdb.namespace}/{pdb.name} 阻挡"
                    f"（健康 {status['currentHealthy']}，"
                    f"期望 {status['desiredHealthy']}）",
                )
                return status_response(
                    429,
                    "TooManyRequests",
                    "Cannot evict pod as it would violate the pod's disruption budget.",
                )
        was_healthy = pod.healthy
        pod.terminating = True
        pod.deletion_timestamp = _now_iso()
        pod.resource_version += 1
        # The granted eviction makes the pod unhealthy immediately (healthy
        # count drops now); matching count only drops when it exits.  Only a
        # healthy -> unhealthy transition consumes budget, so an already
        # unhealthy pod does not decrement the healthy count twice.
        if was_healthy and not pod.healthy:
            STATE._adjust_pod_health(pod, -1)
        STATE._event("eviction", f"{namespace}/{name} 获准驱逐")
        return JSONResponse(
            {
                "apiVersion": "policy/v1",
                "kind": "Eviction",
                "metadata": {"name": name, "namespace": namespace},
            },
            status_code=201,
        )


@app.get("/apis/policy/v1/poddisruptionbudgets")
def list_pdbs() -> dict[str, Any]:
    with STATE.lock:
        items = [STATE.pdb_json(pdb) for pdb in STATE.pdbs.values()]
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudgetList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets")
def list_pdbs_in_namespace(namespace: str) -> dict[str, Any]:
    with STATE.lock:
        items = [
            STATE.pdb_json(pdb) for pdb in STATE.pdbs.values() if pdb.namespace == namespace
        ]
    return {
        "apiVersion": "policy/v1",
        "kind": "PodDisruptionBudgetList",
        "metadata": {"resourceVersion": "1"},
        "items": items,
    }


@app.get("/apis/policy/v1/namespaces/{namespace}/poddisruptionbudgets/{name}")
def read_pdb(namespace: str, name: str) -> Response:
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        return JSONResponse(STATE.pdb_json(pdb))


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"status": "ok"}


# -- rehearsal controls ----------------------------------------------------
@app.get("/api/sim/state")
def sim_state() -> dict[str, Any]:
    with STATE.lock:
        pods = list(STATE.pods.values())
        return {
            "nodes": [
                {
                    "name": node.name,
                    "unschedulable": node.unschedulable,
                    "ready": node.ready,
                }
                for node in sorted(STATE.nodes.values(), key=lambda n: n.name)
            ],
            "pods": [
                {
                    "namespace": pod.namespace,
                    "name": pod.name,
                    "uid": pod.uid,
                    "node": pod.node,
                    "labels": pod.labels,
                    "owner_kind": pod.owner_kind,
                    "static": pod.static,
                    "ready": pod.ready,
                    "phase": pod.phase,
                    "terminating": pod.terminating,
                    "healthy": pod.healthy,
                }
                for pod in sorted(pods, key=lambda p: (p.namespace, p.name))
            ],
            "budgets": [
                {
                    "namespace": pdb.namespace,
                    "name": pdb.name,
                    "selector": pdb.selector,
                    "minAvailable": pdb.min_available,
                    "maxUnavailable": pdb.max_unavailable,
                    "generation": pdb.generation,
                    **STATE.budget_status(pdb),
                }
                for pdb in sorted(STATE.pdbs.values(), key=lambda p: (p.namespace, p.name))
            ],
            "metrics": dict(STATE.metrics),
            "events": STATE.events[:40],
        }


@app.post("/api/sim/budget-index/reconcile")
def reconcile_budget_index() -> Response:
    """Full walk over all pods, used only to cross-check the index."""
    with STATE.lock:
        try:
            STATE.reconcile_budget_index()
        except RuntimeError as exc:
            return status_response(500, "InternalError", str(exc))
        return JSONResponse(
            {
                "ok": True,
                "pods": len(STATE.pods),
                "budgets": [STATE.budget_status(pdb) | {"name": pdb.name} for pdb in STATE.pdbs.values()],
                "metrics": dict(STATE.metrics),
            }
        )


@app.post("/api/sim/budget-index/benchmark")
async def benchmark_budget_index(request: Request) -> Response:
    """Add background pods, then measure one budget read + eviction cycle.

    The point of the exercise: per-operation cost must not grow with the
    number of pods.  ``budget_pod_scans`` staying 0 while ``pods`` grows is
    the observable proof that no full pod walk happens on these paths.
    """
    body: dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        try:
            body = await request.json()
        except ValueError:
            body = {}
    extra = max(0, int(body.get("pods", 500)))
    with STATE.lock:
        if extra:
            # Give the benchmark pods a label no seeded budget selects, so
            # they raise the pod count without changing any budget.
            for index in range(extra):
                pod = SimPod(
                    namespace="bench",
                    name=f"bench-{index:05d}",
                    node="node-c",
                    labels={"app": "bench"},
                )
                STATE.pods[pod.key] = pod
                STATE._attach_pod(pod)

        before = dict(STATE.metrics)
        pdb = STATE.pdbs.get(("shop", "web-pdb"))
        reads = 0
        edges = 0
        if pdb is not None:
            for _ in range(100):
                STATE.budget_status(pdb)
                reads += 1
        if pdb is not None and ("bench", "bench-00000") in STATE.pods:
            # Apply a grant edge and an exit edge to one bench pod; the index
            # maintenance cost is per-pod, not per-cluster.
            victim = STATE.pods[("bench", "bench-00000")]
            was_healthy = victim.healthy
            victim.terminating = True
            victim.deletion_timestamp = _now_iso()
            STATE._refresh_pod_health(victim, was_healthy)
            STATE._detach_pod(victim)
            del STATE.pods[victim.key]
            edges = 1
        after = dict(STATE.metrics)
        return JSONResponse(
            {
                "pods": len(STATE.pods),
                "budget_reads": reads,
                "edges": edges,
                "pod_scans_added": after["budget_pod_scans"]
                - before["budget_pod_scans"],
                "index_builds_total": after["budget_index_builds"],
                "index_updates_added": after["budget_index_updates"]
                - before["budget_index_updates"],
                "metrics": after,
            }
        )
        after = dict(STATE.metrics)
        return JSONResponse(
            {
                "pods": len(STATE.pods),
                "budget_reads": reads,
                "pod_scans_added": after["budget_pod_scans"]
                - before["budget_pod_scans"],
                "index_builds_total": after["budget_index_builds"],
                "index_updates_added": after["budget_index_updates"]
                - before["budget_index_updates"],
                "metrics": after,
            }
        )


@app.post("/api/sim/budgets/{namespace}/{name}")
async def update_budget(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pdb = STATE.pdbs.get((namespace, name))
        if pdb is None:
            return status_response(404, "NotFound", f'poddisruptionbudgets "{name}" not found')
        if "minAvailable" in body:
            value = body["minAvailable"]
            if value is None:
                pdb.min_available = None
            else:
                value = int(value)
                if value < 0:
                    return status_response(422, "Invalid", "minAvailable 不能为负")
                pdb.min_available = value
                pdb.max_unavailable = None
        elif "maxUnavailable" in body:
            value = int(body["maxUnavailable"])
            if value < 0:
                return status_response(422, "Invalid", "maxUnavailable 不能为负")
            pdb.max_unavailable = value
            pdb.min_available = None
        else:
            return status_response(422, "Invalid", "需要 minAvailable 或 maxUnavailable")
        pdb.generation += 1
        pdb.resource_version += 1
        # The selector did not change, so membership is unchanged; only the
        # desired-health formula changes.  Numbers are recomputed lazily from
        # the O(1) counts, so no pod walk is needed here.
        return JSONResponse(STATE.pdb_json(pdb))
        STATE._event(
            "budget",
            f"预算 {namespace}/{name} 调整为 "
            + (
                f"minAvailable={pdb.min_available}"
                if pdb.min_available is not None
                else f"maxUnavailable={pdb.max_unavailable}"
            ),
        )
        return JSONResponse(STATE.pdb_json(pdb))


@app.post("/api/sim/pods/{namespace}/{name}/replace")
async def replace_pod(namespace: str, name: str, request: Request) -> Response:
    """Re-create the pod under the same name with a new UID.

    This is how a StatefulSet behaves when its pod is evicted: the name is
    reused, the object identity is not.
    """
    body: dict[str, Any] = {}
    if request.headers.get("content-length") not in (None, "0"):
        body = await request.json()
    with STATE.lock:
        old = _find_pod(namespace, name)
        if old is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        node = body.get("node") or old.node
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        ready = bool(body.get("ready", True))
        replacement = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(old.labels),
            owner_kind=old.owner_kind,
            static=old.static,
            ready=ready,
        )
        STATE._detach_pod(old)
        STATE.pods[replacement.key] = replacement
        STATE._attach_pod(replacement)
        STATE._event(
            "replace",
            f"{namespace}/{name} 同名重建：UID {old.uid[:8]} → {replacement.uid[:8]}，节点 {node}",
        )
        return JSONResponse(
            {
                "previous_uid": old.uid,
                "pod": {
                    "namespace": namespace,
                    "name": name,
                    "uid": replacement.uid,
                    "node": node,
                    "ready": ready,
                },
            },
            status_code=201,
        )


@app.post("/api/sim/pods")
async def create_pod(request: Request) -> Response:
    """Add a pod directly, for scenarios such as a finished Job pod.

    This is a rehearsal convenience, not a Kubernetes API endpoint.
    """
    body = await request.json()
    namespace = str(body.get("namespace") or "default")
    name = str(body.get("name") or "")
    node = body.get("node") or NODE_NAMES[0]
    if not name:
        return status_response(422, "Invalid", "需要 name")
    with STATE.lock:
        if node not in STATE.nodes:
            return status_response(422, "Invalid", f"节点 {node} 不存在")
        if (namespace, name) in STATE.pods:
            return status_response(
                409, "AlreadyExists", f'pods "{name}" already exists'
            )
        pod = SimPod(
            namespace=namespace,
            name=name,
            node=node,
            labels=dict(body.get("labels") or {}),
            owner_kind=body.get("owner_kind"),
            static=bool(body.get("static", False)),
            ready=bool(body.get("ready", True)),
            phase=str(body.get("phase") or "Running"),
        )
        STATE.pods[pod.key] = pod
        STATE._attach_pod(pod)
        STATE._event("create", f"创建 Pod {namespace}/{name} 于 {node}")
        return JSONResponse(STATE.pod_json(pod), status_code=201)


@app.post("/api/sim/pods/{namespace}/{name}/ready")
async def set_ready(namespace: str, name: str, request: Request) -> Response:
    body = await request.json()
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        before = pod.healthy
        pod.ready = bool(body.get("ready", True))
        pod.resource_version += 1
        STATE._refresh_pod_health(pod, before)
        STATE._event(
            "ready",
            f"{namespace}/{name} 标记为{'就绪' if pod.ready else '未就绪'}",
        )
        return JSONResponse(STATE.pod_json(pod))


@app.post("/api/sim/pods/{namespace}/{name}/exit")
async def finish_terminating(namespace: str, name: str) -> Response:
    """Let a pod that already has a deletion timestamp finish terminating."""
    with STATE.lock:
        pod = _find_pod(namespace, name)
        if pod is None:
            return status_response(404, "NotFound", f'pods "{name}" not found')
        if not pod.terminating:
            return status_response(409, "Conflict", "Pod 尚未进入退出流程，拒绝直接移除")
        STATE._detach_pod(pod)
        del STATE.pods[pod.key]
        STATE._event("exit", f"{namespace}/{name} 已退出节点 {pod.node}")
        return JSONResponse({"exited": True, "namespace": namespace, "name": name})


@app.post("/api/sim/pods/exit-terminating")
def finish_all_terminating() -> dict[str, Any]:
    with STATE.lock:
        finished = [
            f"{pod.namespace}/{pod.name}"
            for pod in list(STATE.pods.values())
            if pod.terminating
        ]
        for pod in [p for p in STATE.pods.values() if p.terminating]:
            STATE._detach_pod(pod)
            STATE.pods.pop(pod.key, None)
        if finished:
            STATE._event("exit", f"{len(finished)} 个待退出 Pod 已退出")
        return {"exited": finished}


@app.post("/api/sim/reset")
def reset_cluster() -> dict[str, str]:
    STATE.reset()
    return {"status": "reset"}


@app.get("/", response_class=HTMLResponse)
def dashboard(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(request, "sim.html", {"request": request})


if __name__ == "__main__":  # pragma: no cover
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8080)
