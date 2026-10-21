# 单队列 pipeline worker（本地验证版本）

每个 pipeline 使用一个 Queue。PDB 是可选的实例可用性约束；纯按卡管理时无需创建 PDB，GPU 预算预留与重验仍必须保留。worker 不再自行选择 base/shared，也不固定哪一个 Pod 永远不可抢占。平台为每种 worker 设置 `desiredGPU` 和 `minAvailableGPU`；候选驱逐由当前 Ready GPU、在途预算预留和 PDB 共同决定。原 base/shared 工作负载继续使用原路由，本文不会自动迁移它们。

## 职责和行为

- `WorkloadRoutingPolicy`：用根对象的 pipeline + worker-type 匹配平台规则，自动注入 scheduler、Queue、节点范围、PriorityClass、pipeline 归属、worker-type、preemptible。继承的路由决策随控制器后代保留。显式冲突拒绝，不悄悄覆盖。
- `PipelinePolicy`：该 pipeline 的 `accountingQueues` 和 `preemptibleQueues` 都配置同一个队列。训练目标身份与 pipeline 身份按现有约定保持一致。训练仍须满足现有抢占授权及 GPU、优先级等条件。
- `WorkerBudget`：每个 pipeline/worker-type 一个预算。`minAvailableGPU` 是自定义驱逐时保留的 Ready GPU 下限；`desiredGPU` 是平时希望运行的卡数。
- 新增 `pipeline-worker-controller`：将 `desiredGPU / 每副本GPU` 转成 Deployment `/scale`，只管理显式绑定 UID 的 Deployment。它不删除 Pod、不改模板、不做抢占。现有 budget-controller 继续统计保底，两者保留对方 status 字段并用 resourceVersion 防止覆盖。
- 现有静态抢占插件：选取符合条件的 worker，保持 GPU/PDB 预算预留、重验、Eviction、失败恢复。训练进入同一队列，并以更高优先级获得腾出的资源。
- Deployment：驱逐后立即补 Pod；资源不足时替代 Pod Pending。训练结束释放资源后，原有替代 Pod 自动调度，无需手工恢复副本数。

`desiredGPU` 不是不可借用配额，`minAvailableGPU` 不是预留空闲卡；节点故障、人工删除或滚动发布可能使 Ready GPU 低于下限。下限约束的是此插件允许的主动驱逐，不是硬件可用性承诺。CPU/内存 requests 仍必须准确配置，GPU 充足不代表 CPU/内存也充足。

## 配置示例

`single-queue-example.json` 展示 12 卡节点池、单队列和 hand-detect 期望 8 / 最低 4 卡的预算。示例抢占授权为空，不能直接作为生产开关；Deployment UID 必须替换成创建后读取的真实值。

在平台管理的 WorkloadRoutingPolicy 中增加如下规则（目标中的字段是新增能力）：

```yaml
- name: hand-detect-prod
  match:
    labels:
      pipeline.simplesilicon.com/name: ego-pipeline-prod
      scheduling.simplesilicon.com/worker-type: hand-detect
  target:
    queue: ego-pipeline-prod
    nodeGroup: infra-ego-pipeline-prod
    poolRef: ego-pipeline-prod
    priorityClassName: ego-pipeline-worker
    worker:
      pipeline: ego-pipeline-prod
      namespace: infra-ego-pipeline
      type: hand-detect
```

需要配套 NodePoolPolicy 接受这条路由、节点标签、PriorityClass，以及指向同一 Queue 的训练规则。训练规则不要配置 `worker`，应保留训练的 `preemptionTarget` 和更高优先级。PriorityClass 必须预先存在。`worker` 与 `preemptionTarget`、`cpuOnly` 互斥。

业务 Deployment 只需声明 pipeline、worker-type、镜像、命令和每副本资源。初次创建可用 `replicas: 0`，避免预算接管前运行副本；随后创建含真实 Deployment UID 的 WorkerBudget。

```yaml
metadata:
  name: ego-hand-detect-prod
  namespace: infra-ego-pipeline
  labels:
    pipeline.simplesilicon.com/name: ego-pipeline-prod
    scheduling.simplesilicon.com/worker-type: hand-detect
spec:
  replicas: 0
  # selector、template、容器由业务清单提供
```

## 控制器部署产物

先升级 CRD（`budget-crds.json`、`workload-routing-crd.json`）和 workload-policy 镜像，再部署独立 worker controller。镜像构建入口 `build-local.sh` 已包含新二进制，独立镜像 Dockerfile 为 `Dockerfile.worker-controller`。打包上下文需包含 `amd64/pipeline-worker-controller` 或 `arm64/pipeline-worker-controller`，与 Docker 的 TARGETARCH 对应。渲染命令只输出本地文件，不执行集群操作：

```bash
python3 installer/pipeline/render-worker-controller.py \
  --image '<包含新二进制的镜像@digest>' \
  --worker-namespace infra-ego-pipeline \
  --namespace volcano-system \
  --recovery-journal volcano-system/worker-type-preemption-recovery \
  > worker-controller.json
```

新 controller 使用明确 namespace、独立 Lease 选主，只有该 namespace 的 Deployment scale 和 WorkerBudget status 写权限。运行在已排除业务路由的系统 namespace。需要在该 namespace 可以调度的节点上运行；镜像拉取凭据可通过 `--image-pull-secret` 传入。

本轮仅本地测试，未发布镜像、未应用上述生产清单。

## 第一版限制

1. 每个 WorkerBudget 绑定一个 Deployment（名称 + UID），同一个 Deployment 不允许多个 desiredGPU 预算拥有。类型应只对应这个 Deployment，避免其它同类型实例计入预算却不受期望值控制。
2. 只支持 `nvidia.com/gpu` 整卡；每副本卡量必须固定，期望卡数必须能整除每副本卡数。GPU initContainer/overhead 不支持。多卡 Pod 只能整 Pod 驱逐，可能保留多于最低卡数。
3. 自动扩容、训练结束恢复支持。主动降低 desiredGPU 时，如果该 pipeline 仍允许抢占，返回 `DownscaleDeferred`，不直接缩容。需先将 `allowedPreemptorNamespaces` 设为显式空数组，等待当前副本全部更新且 Ready，且配置的调度器 recovery journal 为空。controller 的 `--recovery-journal=namespace/name` 必须与调度器一致；未配置、读不到或内容无效时都延迟缩容。这样避免普通 Deployment 缩容（不受 PDB 约束）与插件驱逐并发突破下限。若要带抢占持续缩容，需要后续统一驱逐事务，当前版本不宣称支持。
4. 不与 HPA 同时管理 Deployment。Argo CD 应忽略该 Deployment 的 `/spec/replicas`，并启用 `RespectIgnoreDifferences=true`；初次创建的 replicas 仍要显式设计。平台改 WorkerBudget，业务不再手工改 replicas。
5. 旧决策不会自动注入新属性；升级后需以新 Deployment 灰度迁移。不要直接改正在运行 Pod 的调度身份或删除旧 base 队列。
6. 保底总量超过节点池可用容量、单节点碎片、CPU/内存不足时可能 Pending。部署前核对各类型最低值之和及每个 Pod 的可放置性。平台期望量也可以超过容量，此时未运行副本排队。

## 回滚顺序

先关闭该新 pipeline 的抢占授权并确认恢复 journal 已收敛，再停用新 worker controller（副本设为 0），保留 WorkerBudget/CRD 和已冻结的路由决策。停止 controller 不会删除 worker，也不会更改当前 replicas。

恢复人工/Git 管理前，把当前 Deployment replicas 写回 Git。已接管 Pod 的重建仍需要支持新 decision 字段的 workload-policy；不能先回退成不认识新字段的 webhook。要完全撤销新模型，应先停止新入口、迁移其 Deployment，确认没有新格式决策引用，再回退镜像/策略。旧 UID 绑定不适用于同名重建的 Deployment，须显式更新预算引用。

## 本地测试

`test/e2e/singlequeue/run.py` 只接受 `https://127.0.0.1:16473`，使用隔离命名资源和实际 admission/Deployment/PodGroup/PDB/scale/调度器。GPU 节点容量和 Ready 状态模拟；没有真实 CUDA 执行，也不能替代生产 wuying 驱逐/业务幂等性验收。

详细通过项与证据目录见本轮 `outputs/single-queue-worker-20261021` 报告。运行前需要构建并导入包含 webhook、scale controller、budget controller、PodGroup controller 和静态 scheduler 的本地镜像；不允许将该脚本指向生产。

## PDB 是否必需

按卡管理的默认方案不创建 PDB。保留 WorkerBudget 和插件的并发预留即可限制此插件的主动驱逐。PDB 只在另有实例可用性需求时叠加，例如至少保留两个服务实例，或希望使用 Eviction 的运维 drain 也遵守实例下限。PDB 与 GPU 最低值同时存在时都必须满足；PDB 不按卡记账，也不阻止直接删除/缩容。

移除 PDB 不等于允许删去持久化恢复记录、GPU 预算预留或驱逐前重验。现有线上 PDB 不会由新 controller 自动删除。
