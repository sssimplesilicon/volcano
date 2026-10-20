# Unified workload-policy

## 本次变化

一个 Deployment / Service、一个准入引擎处理 Arena/PyTorchJob 和普通工作负载。
标准二进制为 `workload-policy-webhook`。旧二进制名
`workload-routing-webhook` 和 `/mutate` URL 保留为兼容入口，调用同一引擎，
新根对象均写入 v2 冻结决策，不再维护另一套 PyTorch 路由算法。

继续使用两个 CRD：

- `WorkloadRoutingPolicy`：namespace 生效范围、系统排除、标签/批次规则、
  scheduler、queue、nodegroup、poolRef 和可信控制器身份。
- `NodePoolPolicy`：路由与节点池的对应关系、其他任务的节点排除、
  历史控制器 UID 登记和系统例外。

可以保留 `business-routing` 和 `arena-pytorch` 两个策略对象，分别配置不重叠
的 workloadKinds；它们不再需要独立服务。既有策略名字应保留，因为已接管
对象的冻结决策引用这些名字。

## 队列不再按 namespace 授权

准入服务不读取 QueueAccessPolicy，也不需要该 CRD 或相关 RBAC。
namespaceScope 仍决定灰度范围和系统排除，但不再是队列权限表。
同样的 batch/route 在不同生效 namespace 中选择同样的队列。

旧配置中的 queueAccessPolicyRef 可在切换期间保留：新服务忽略该字段并返回
明确的 AdmissionResponse warning，避免服务迁移和配置更新无法原子提交的问题。
新生成清单必须去掉该字段。旧服务仍在运行时不要提前删除它依赖的策略。

独立 scheduler 的 `queue-access` 插件和 `ego-queue-access` webhook 是另外
两处授权执行点，升级这个镜像不会自动关闭它们。它们的退役需要单独的受保护
配置变更。不要删除 Queue、配额、capacity/nodegroup/binpack，也不要启用抢占。
原有抢占插件的可选队列检查和跨 namespace 抢占授权不在本次准入重构中修改。

## 保留的行为

- PyTorch：根 batch 标签优先；无标签时使用副本 nodeSelector.batch。
  各副本 selector.batch 不一致仍拒绝，即使根标签存在。
- 用户已有 selector、toleration、镜像、命令和资源声明保留；不自动追加 wuying。
- 普通 Pod、Deployment、StatefulSet、Job、CronJob、Workflow 和 PyTorchJob
  共用规则。ReplicaSet 是可信后代，不作为独立新根接管。
- CPU route 不能申请 NVIDIA GPU，直接 nodeName 绑定与冲突选择器仍会被拒绝。
- 新根决策冻结在根对象和模板上。变更规则不会迁移已经接管的任务。
- 子对象只继承实际 UID 匹配的祖先决策，并检查创建它的控制器身份。
- 旧根 UPDATE 不接管。旧 v1 根/旧控制器及其后代重建需要保留已有兼容链路：
  将真实祖先 UID 纳入 NodePoolPolicy 的 legacyRegistry，配置相应 controllerUsers。
  不把客户提交的 v1 标签作为独立放行凭据。
- 系统 namespace 在 API server webhook selector 层排除；准入服务故障时仍不命中。
- 范围内新任务/已接管后代遇到服务故障按 Fail 策略拒绝，恢复后控制器可重试。

## 离线生成新安装清单

先填写两个策略的 controllerUsers（包含实际 Trainer、Kubernetes 和 Argo
控制器身份），核对 namespaceScope 和历史登记。示例保持 Preview/Audit。
renderer 不访问集群，所有 webhook 门控默认 false。

```sh
python3 installer/pipeline/render-workload-policy.py \
  --policy installer/pipeline/business-routing-example.json \
  --additional-policy installer/pipeline/workload-routing-example.json \
  --pools installer/pipeline/nodepool-policy-example.json \
  --image "$POLICY_IMAGE" \
  > unified-disabled.json
```

示例 renderer 要求两个路由策略的 namespaceScope 一致且 workloadKinds 不重叠；
不自动扩大 namespace 白名单，也不自动合并不同的系统排除列表。
已有集群迁移可以保留各 webhook 原有 selector，通过多个入口指向同一个服务。

## 已有部署的迁移顺序（需要后续生产快照生成清单）

本说明不是生产执行脚本。本次开发和 E2E 没有操作生产。

1. 保存路由策略、节点隔离策略、历史登记、webhook、调度器实际挂载的 ConfigMap、
   Deployment 和系统排除列表。不要误备份未挂载的旧 ConfigMap。
2. 保留两个策略对象名字。给 Arena 对应目标补上 poolRef，在 NodePoolPolicy
   acceptedRoutes 中登记其规则，并配置真实 Trainer 身份。历史 v1 祖先保留登记。
   这些配置先通过 dry-run 检查，保持旧任务的 spec 不变。
3. 升级统一 workload-policy 后端，先验证关闭/灰度入口。旧 `/mutate` URL 的
   输出已变为 v2，因此切换前要确保 Kyverno 兼容策略跳过已有冻结决策，
   普通 workload webhook 的 namespace/后代门控会覆盖这些新 PyTorch 后代。
4. 使用保留 UID/原字段 test 的补丁把 Arena webhook 后端指向 workload-policy，
   保持已有 namespace 选择和新根筛选条件。对应 clientConfig.caBundle 必须
   使用 workload-policy 的证书 CA；不能只修改 Service 名称。
   避免保留两套同时命中的旧/新 PyTorch 计算入口；验证兼容 URL 与主入口相同。
5. 单独关闭 ego-queue-access 准入检查，从实际 scheduler 配置删除 queue-access
   插件；保持动作列表及其他插件不变，验证没有 preempt/reclaim/worker-type-preempt。
6. 确认全部 webhook 引用已离开旧 Service 后，才退役旧 workload-routing 和
   ego-queue-access 专用 Deployment/Service/证书，清理已无引用的队列授权对象。
   **保留共享 ego-queue-access-ca 的 Issuer/Secret/Certificate 和 Reloader**：
   workload-policy 的 TLS 仍依赖它们。不要按资源名称前缀批量删除。

## 回滚语义

- 尚无新 v2 任务时，可以按快照逆序恢复 webhook 后端、策略和配置。
- 已有新 v2 任务时，先将相应路由策略 mode 改成 Disabled，停止新根接管；
  保留统一服务、策略对象、节点隔离和已接管后代/UPDATE 的 webhook 入口。
  它们继续继承冻结决策，删 Pod、扩副本不会变成另一条路由。
- 此时不能直接删除统一服务、降级到只认识 v1 的旧镜像，或重新开启会拒绝
  已接管 namespace 的队列 ACL。必须先排空或明确迁移已接管对象，再完全回退。
- 回滚不删除业务工作负载。完整清理只针对本地测试对象。

## 本地验证

入口是 `test/e2e/workloadpolicy/run.py`，固定拒绝非
`https://127.0.0.1:16473` 的 kubeconfig。旧 workloadrouting runner 转调同一套测试。
测试镜像为 workload-policy:local，需导入本地所有 k3s 节点。

测试使用不存在的节点选择器和零 GPU 请求，验证真实 API、Trainer、Deployment、
StatefulSet、Job、CronJob 和 Argo 控制器行为，不验证 GPU 抢占或实际装箱性能。
Arena CLI 必须显式传入同一个本地 kubeconfig。新 suite 不给 webhook 授予
QueueAccessPolicy 读取权限；单元测试还覆盖该 API 不存在的情形。

检查结果和完整限制见 `test/e2e/workloadpolicy/VALIDATION.md`。

## 现有生产集群：第一阶段镜像升级命令

本阶段只替换 `volcano-system/workload-policy` 的镜像。普通任务沿用现有
路由、系统排除和 v2 冻结决策；Arena 仍由旧 `workload-routing` 服务处理。
不会完成 Arena 入口合并，也不会撤销现有队列 namespace ACL。
不能把新镜像直接同时设置给两个 Deployment：旧 `/mutate` 的行为已经变更。

发布时提供 digest 固定的 `POLICY_IMAGE`。在本仓库根目录执行：

```sh
: "${CTX:?请设置生产 context}"
: "${POLICY_IMAGE:?请填本次发布的镜像 digest}"
export UNIFIED_STATE="$HOME/volcano/unified-policy-$(date +%Y%m%d-%H%M%S)"
python3 installer/pipeline/prepare-unified-image-upgrade.py prepare \
  --context "$CTX" --state "$UNIFIED_STATE" --image "$POLICY_IMAGE"
cat "$UNIFIED_STATE/plan.json"
```

`prepare` 只执行 GET/list，在本地保存配置，既不读 Secret，也不更改生产。
目录已存在、Arena 已经切至统一后端、非双副本后端或调度动作已含抢占时会停止。
运行前确认使用原有已验证的镜像拉取凭据。以下两步分别预览和执行镜像升级：

```sh
kubectl --context "$CTX" -n volcano-system patch deployment workload-policy \
  --type=json --patch-file "$UNIFIED_STATE/upgrade-image.json" \
  --dry-run=server -o name
kubectl --context "$CTX" -n volcano-system patch deployment workload-policy \
  --type=json --patch-file "$UNIFIED_STATE/upgrade-image.json"
kubectl --context "$CTX" -n volcano-system rollout status \
  deployment/workload-policy --timeout=180s
python3 installer/pipeline/prepare-unified-image-upgrade.py verify \
  --context "$CTX" --state "$UNIFIED_STATE"
kubectl --context "$CTX" -n volcano-system logs deployment/workload-policy \
  --since=5m --tail=80
```

补丁校验 Deployment UID 和完整原 spec；若期间发生配置变化，停止核对，不能
删除 test 强行执行。滚动更新会重建 webhook 自身的 Pod，不修改业务工作负载。
Ready 只证明服务可用；本阶段 verify 还检查快照内的 webhook 门控、策略、队列、
旧 Arena 后端、两个调度器和实际挂载 ConfigMap 没有变化。它不等于完整切换验收。

本阶段失败时仅回退镜像（必须在后续 Arena 入口切换之前）：

```sh
kubectl --context "$CTX" -n volcano-system patch deployment workload-policy \
  --type=json --patch-file "$UNIFIED_STATE/rollback-image.json"
kubectl --context "$CTX" -n volcano-system rollout status \
  deployment/workload-policy --timeout=180s
```

后续用本阶段快照核对 Trainer 用户、Arena poolRef/acceptedRoutes、历史 UID 登记
和后代门控，再生成 Arena 切换与 ACL 退役补丁。不要直接 apply 示例策略，它们会
覆盖现有生产规则。完成后续切换后，应使用前文“停止新接管、保留已接管后代”的
回滚流程，而不是此处的镜像回退。
