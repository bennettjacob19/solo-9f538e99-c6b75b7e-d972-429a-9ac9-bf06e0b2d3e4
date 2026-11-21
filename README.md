# 节点排空值班台

检修 Kubernetes 节点前的排空工具：FastAPI 网页选节点，先封锁，再按轮次驱逐
Pod，只在集群确认后才提示“可下线”。

## 规则

| 要求 | 实现 |
| --- | --- |
| 先标记不可调度，再列出本轮 Pod | `DrainManager.start()` 先 `PATCH` 节点 `spec.unschedulable=true`，再 `GET` 该节点的 Pod；封锁失败则不会产生轮次、也不会展示任何待驱逐 Pod |
| 保留 DaemonSet 与静态 Pod | `Pod.exempt_reason`：`DaemonSet`、`静态 Pod`（mirror pod / config.source=file,http）、以及已结束（Succeeded/Failed）的 Pod 均不驱逐，也不阻挡“可下线” |
| 用 Eviction 按 UID 驱逐 | `POST /api/v1/namespaces/{ns}/pods/{name}/eviction`（`policy/v1`），`deleteOptions.preconditions.uid` 为列表时的 UID |
| 同名替代者留待续排 | 若同名 Pod 的 UID 已变化，旧 UID 的驱逐决定不迁移；新对象在执行中被标为 `留待续排`，在重新读取时进入新的一轮 |
| 两节点并行不超共用 PDB 额度 | 预算判定完全交给 API Server：它返回 429 即视为“受预算阻挡”。应用不做本地配额缓存，因此并行轮次共享同一份额度 |
| 演练端预算计数不随 Pod 总数增长 | `simulator` 维护增量 PDB 索引（按标签定位候选预算，逐 Pod 计数）：查 PDB 为 O(1)，改阈值为 O(1)，驱逐单个 Pod 为 O(标签数 + 相关预算数)，Pod 退出为 O(命中该 Pod 的预算数)，均不随集群 Pod 总数增长 |
| 演练页可核对四类读数 | 调整 `minAvailable` / `maxUnavailable` 后，演练页实时显示匹配数、健康数、期望健康数、允许驱逐数；值班台轮次页同样显示“最新读数” |
| 获准即更新健康数，退出才减匹配数 | 驱逐获准时健康数立即减一；Pod 实际退出（对象消失）时匹配数才减一。`maxUnavailable` 的期望健康数由当前匹配数推导，因此匹配数变化后同步更新 |
| 阈值与驱逐交错仍按生效时读数放行 | 演练端在一把锁内完成“取索引计数 → 判定 → 更新索引”。阈值修改和驱逐串行生效：每一次放行对应操作生效时的阈值和计数，被拒的驱逐不改 Pod 状态、不消耗额度 |
| 页面标出受预算阻挡的 Pod，同轮继续 | 429 只影响该 Pod 的状态行（`受预算阻挡`），循环继续处理下一个 Pod |
| 中断保留封锁与已发生的驱逐 | 中断只阻止后续驱逐；节点保持封锁，轮次记录与已发出的驱逐都保留 |
| 继续排空重新读取集群 | `resume()` 重新 `GET` 节点与 Pod，必要时补封锁，然后 `refresh()` 对账，不会沿用旧快照 |
| 获准驱逐但未离开显示“待退出” | 驱逐成功后状态为 `待退出`，直到再次检查发现对象已消失才变为 `已离开` |
| 复核后才显示可下线 | `_verify_offline()` 每次独立重读：节点当前仍不可调度 **且** 已无普通 Pod，才置为“可下线” |
| 本地演练端 | `simulator/` 提供本地 Kubernetes API 演练端（节点/Pod/PDB/Eviction 子资源），网页可调预算、同名替换、令 Pod 退出 |
| docker compose 启动 | `docker compose up --build` |

## 启动（docker compose）

```bash
docker compose up --build
```

* 值班台：<http://localhost:8000>
* 演练控制台：<http://localhost:8080>

值班台在 compose 中通过 `DRAIN_KUBE_HOST=http://simulator:8080` 直连演练端，
无需 kubeconfig。

## 连接真实集群

程序支持三种模式，均通过环境变量配置：

```bash
# 集群内（ServiceAccount）
DRAIN_KUBE_MODE=in-cluster

# 运维机上的 kubeconfig
DRAIN_KUBE_MODE=kubeconfig DRAIN_KUBECONFIG=~/.kube/config DRAIN_KUBE_CONTEXT=prod

# 显式 API Server + Bearer Token
DRAIN_KUBE_MODE=host DRAIN_KUBE_HOST=https://10.0.0.1:6443 \
  DRAIN_KUBE_TOKEN_FILE=/var/run/secrets/token DRAIN_KUBE_CA_CERT=/etc/ca.crt
```

所需权限（ClusterRole）：

```yaml
rules:
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list", "patch"]
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list"]
  - apiGroups: [""]
    resources: ["pods/eviction"]
    verbs: ["create"]
  - apiGroups: ["policy"]
    resources: ["poddisruptionbudgets"]
    verbs: ["get", "list"]
```

可选：`DRAIN_STATE_FILE=/data/rounds.json` 将轮次记录（封锁决定、已发出的
驱逐、状态行）落盘，进程重启后仍能“继续排空”。集群仍是唯一事实来源：续排
时一定会重新读取节点与 Pod。

## 演练端的预算读数与自测

演练控制台（<http://localhost:8080>）的预算表直接读取增量索引：每次改动阈值
后立刻能看到最新的匹配数、健康数、期望健康数和允许驱逐数。

计数变化有两条不同的边：

* **驱逐获准** —— 健康数立刻减一，匹配数不变（Pod 对象仍在，只是进入待退出）。
* **Pod 实际退出** —— 匹配数减一。若预算用 `maxUnavailable`，期望健康数
  `匹配数 - maxUnavailable` 同步更新。

“预算索引自检”面板提供两个按钮：

* **全量核对索引**：遍历全部 Pod，用参考实现重算并比较；这是唯一成本随
  Pod 数增长的路径，只用于自测。
* **增加 Pod 总数并测遍历**：批量加入无关 Pod 后做 100 次预算读取及一次
  驱逐/退出边更新。返回 `pod_scans_added=0`，说明这些操作没有遍历全部 Pod。

对应测试：`tests/test_budget_index.py`。

## 本地开发

```bash
python3 -m pip install -r requirements.txt
python3 -m pip install -r tests/requirements-dev.txt   # pytest + httpx
python3 -m pytest
```

不装依赖直接跑演练端与值班台：

```bash
uvicorn simulator.main:app --port 8080
DRAIN_KUBE_HOST=http://127.0.0.1:8080 DRAIN_SIM_URL=http://127.0.0.1:8080 \
  uvicorn app.main:app --port 8000
```

## 页面状态含义

| 状态 | 含义 |
| --- | --- |
| 待驱逐 | 本轮尚未对其发出 Eviction |
| 待退出 | Eviction 已被接受，Pod 对象仍在（含删除中） |
| 已离开 | 重新读取后该 UID 已不在节点上 |
| 受预算阻挡 | API Server 以 429 拒绝，通常因 PDB；同轮其他 Pod 继续 |
| 留待续排 | 同名替代者或驱逐冲突，留给下一轮处理 |

“可下线”只在轮次页面显示，并且必须同时满足：节点不可调度、节点上已无普通
Pod。DaemonSet、静态 Pod 和已结束的 Pod 不计入。

## 目录

```
app/
  main.py      FastAPI 路由（表单 POST + redirect，刷新不会重复驱逐）
  drain.py     轮次编排、状态机、复核逻辑
  kube.py      Kubernetes 客户端封装（封锁、列表、Eviction、PDB）
  models.py    轮次与 Pod 状态模型
  store.py     可选的轮次落盘
  templates/   页面
simulator/
  main.py      本地 Kubernetes API 演练端 + 控制台
tests/         针对演练端的端到端测试
```

## 设计取舍

* **预算判定以 API Server 为准。** 页面展示的 PDB 数字仅供解释；是否放行由
  429 决定。这样多节点并行、多副本进程都不会超额。
* **UID 是对象身份。** 所有驱逐都带 UID 前置条件；同名重建不会被误驱逐。
* **复核是独立读。** “可下线”不依赖轮次内缓存，每次检查都重新读取节点与 Pod。
* **中断不清状态。** 中断只切换阶段；封锁和已发出的驱逐保留，续排时重新读取。
