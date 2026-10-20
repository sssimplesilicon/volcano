# ego-pipeline 灰度部署、验证和回滚

本文件是操作手册，提交它不会部署任何资源。先完成 A–E（准入灰度，**不开抢占**），
再单独评审 F（真实抢占）。不要安装 `test/e2e/pipeline/infrastructure.json` 到生产。
本地 P01–P23 已通过；使用模拟 GPU，不代表真实 worker 的优雅退出和业务恢复已验收。

目标：worker namespace 为 `infra-ego-pipeline`；base 仅允许该 namespace；shared
允许该 namespace、`infra-group`、`data-group`。只有后两个 namespace 的 GPU 训练
可以触发 shared 抢占，base 不动。权限按真实 namespace 判断，不鉴别提交是否来自 Arena。

## A. 快照和准入前检查

从仓库根目录执行。所有生产命令由操作者核对后执行。

```sh
: "${CTX:?设置生产 context}"
NS=volcano-system
DEP=volcano-infra-scheduler
STATE="$HOME/volcano/ego-access-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$STATE"
chmod 700 "$STATE"
export NS DEP STATE
kubectl --context "$CTX" config view --minify
kubectl --context "$CTX" -n "$NS" get deployment "$DEP" -o json > "$STATE/scheduler.json"
kubectl --context "$CTX" -n "$NS" get configmap -o json > "$STATE/configmaps.json"
kubectl --context "$CTX" get namespaces -o json > "$STATE/namespaces.json"
kubectl --context "$CTX" get queues.scheduling.volcano.sh -o json > "$STATE/queues.json"
kubectl --context "$CTX" get podgroups.scheduling.volcano.sh -A -o json > "$STATE/groups.json"
kubectl --context "$CTX" get pods -A -o json > "$STATE/pods.json"
kubectl --context "$CTX" get crd -o json > "$STATE/crds.json"
kubectl --context "$CTX" get validatingwebhookconfigurations -o json > "$STATE/webhooks.json"
jq '.spec.template.spec | {serviceAccountName,containers,volumes}' "$STATE/scheduler.json"
jq -r '.items[] | select(.spec.queue == "infra-ego-pipeline-base" or .spec.queue == "infra-ego-pipeline-shared") | [.metadata.namespace,.metadata.name,.spec.queue,.status.phase] | @tsv' "$STATE/groups.json"
```

检查这些受保护队列的现有使用者：base 若已有其他 namespace，或 shared 有三个允许
namespace 之外的任务，先解决权限归属，不要直接启用策略。撤权不删除 Running Pod，
但其后续重建会被拒绝。旧 scheduler 上的受保护队列任务也须先核对，旧 scheduler
没有新 ACL 插件，不能宣称它会执行新的撤权语义。

从上面的 Deployment 输出确认容器、配置卷对应的 ConfigMap 和 ServiceAccount，填写：

```sh
CONTAINER='填写调度器容器名称'
CM='填写调度器实际引用的ConfigMap'
SA='填写调度器ServiceAccount'
export CONTAINER CM SA
kubectl --context "$CTX" -n "$NS" get cm "$CM" -o json > "$STATE/config.json"
kubectl --context "$CTX" -n "$NS" get cm "$CM" \
  -o jsonpath='{.data.volcano-scheduler\.conf}' > "$STATE/scheduler-before.conf"
cp "$STATE/scheduler-before.conf" "$STATE/scheduler-acl.conf"
```

保留原来的 capacity/nodegroup/binpack 等配置；不能用测试配置覆盖它们。
核对调度器名称仍为 `volcano-infra`，无其他副本/独立实例调度同一名称。
以下步骤会短暂停止新任务调度，**不会主动删除已有 worker**；不承诺调度器滚动升级零暂停。
不要修改原 `volcano-scheduler`、PodGroup controller、节点标签/污点或 Kyverno 安全策略。

## B. 镜像和 CRD

本次代码推送不等于镜像发布。先构建与生产节点架构一致的镜像，推送到自己的镜像仓库，
取远端 digest；这里不提供一个未经发布验证的 tag。

```sh
TARGETARCH=amd64 ./installer/pipeline/build-local.sh
: "${IMAGE_TAG:?填写目标仓库和唯一tag}"
docker build --platform linux/amd64 -f installer/pipeline/Dockerfile \
  -t "$IMAGE_TAG" _output/pipeline
docker push "$IMAGE_TAG"
docker buildx imagetools inspect "$IMAGE_TAG"
# 从输出填入远端 digest，后续全部用不可变引用
IMAGE='目标仓库@sha256:填写远端digest'
export IMAGE
kubectl --context "$CTX" apply --dry-run=server -f installer/pipeline/budget-crds.json
kubectl --context "$CTX" apply --dry-run=server -f installer/pipeline/queue-access-crd.json
kubectl --context "$CTX" apply -f installer/pipeline/budget-crds.json
kubectl --context "$CTX" apply -f installer/pipeline/queue-access-crd.json
kubectl --context "$CTX" wait --for=condition=Established \
  crd/queueaccesspolicies.scheduling.simplesilicon.com --timeout=60s
kubectl --context "$CTX" get queueaccesspolicies -o yaml > "$STATE/access-before.yaml"
kubectl --context "$CTX" get pipelinepolicies -A -o yaml > "$STATE/pipelines-before.yaml"
```

若已有同名 policy，先审阅差异，不覆盖已有授权或保底。

## C. 先让独立 scheduler 支持 ACL（没有策略时不限制队列）

给独立 scheduler 增加只读授权；原有 scheduler RBAC 保留：

```sh
cat > "$STATE/acl-rbac.yaml" <<EOF_RBAC
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: ego-queue-access-reader
rules:
- apiGroups: [scheduling.simplesilicon.com]
  resources: [queueaccesspolicies]
  verbs: [get, list, watch]
- apiGroups: [scheduling.volcano.sh]
  resources: [podgroups]
  verbs: [get]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ego-queue-access-scheduler
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ego-queue-access-reader
subjects:
- kind: ServiceAccount
  name: $SA
  namespace: $NS
EOF_RBAC
kubectl --context "$CTX" apply -f "$STATE/acl-rbac.yaml"
```

编辑 `$STATE/scheduler-acl.conf`：只在第一层 `plugins` 增加 `- name: queue-access`。
`actions` 保持 `enqueue, allocate, backfill`；不得有 `preempt`、`reclaim` 或
`worker-type-preempt`。这一步仅开 ACL 插件，没有驱逐能力。

生成独立配置和更新清单，保留原调度器的其他设置；生成时不会访问集群：

```sh
python3 - <<'PY'
import json, os, pathlib
p=pathlib.Path(os.environ['STATE'])
d=json.loads((p/'scheduler.json').read_text())
conf=(p/'scheduler-acl.conf').read_text()
assert 'queue-access' in conf
assert 'worker-type-preempt' not in conf
assert '@sha256:' in os.environ['IMAGE'], 'Use a published digest'
vols=d['spec']['template']['spec'].get('volumes',[])
matched=[v for v in vols if v.get('configMap',{}).get('name')==os.environ['CM']]
assert len(matched)==1, 'Review configuration volume'
matched[0]['configMap']['name']='ego-queue-access-scheduler-config'
containers=d['spec']['template']['spec']['containers']
c=next(c for c in containers if c['name']==os.environ['CONTAINER'])
c['image']=os.environ['IMAGE']; c['command']=['/vc-scheduler']
a=[x for x in c.get('args',[]) if not x.startswith('--enable-worker-type-preemption')]
c['args']=a+['--enable-worker-type-preemption=true']
# One active process, including during upgrades.
d['spec']['replicas']=1; d['spec']['strategy']={'type':'Recreate'}
(p/'scheduler-acl-spec.json').write_text(json.dumps([{'op':'replace','path':'/spec','value':d['spec']}],indent=2))
cm={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'ego-queue-access-scheduler-config','namespace':os.environ['NS']},'data':{'volcano-scheduler.conf':conf}}
(p/'scheduler-acl-config.json').write_text(json.dumps(cm,indent=2))
PY
# Review both files and confirm existing command-line options are compatible.
kubectl --context "$CTX" apply --dry-run=server -f "$STATE/scheduler-acl-config.json"
kubectl --context "$CTX" -n "$NS" patch deployment "$DEP" --type=json \
  --patch-file "$STATE/scheduler-acl-spec.json" --dry-run=server -o yaml > "$STATE/scheduler-preview.yaml"
# After reviewing preview:
kubectl --context "$CTX" apply -f "$STATE/scheduler-acl-config.json"
kubectl --context "$CTX" -n "$NS" patch deployment "$DEP" --type=json \
  --patch-file "$STATE/scheduler-acl-spec.json"
kubectl --context "$CTX" -n "$NS" rollout status deployment/"$DEP" --timeout=180s
kubectl --context "$CTX" -n "$NS" logs deployment/"$DEP" --since=3m --tail=100
```

这个 flag 是注册开关；没有自定义 action 不会开启自定义抢占。
若部署原来有 `--leader-elect` 等参数，保留它；此方案依赖单个活动实例。
如果清单由 Helm/Argo CD 管理，必须同时把审阅后的更改纳入其源配置，避免被自动恢复。

## D. 独立验证 Webhook，先只覆盖灰度 namespace

使用专用服务 `ego-queue-access.volcano-system.svc`。由现有证书平台提供服务端证书、私钥
及 CA 文件，SAN 必须含该 DNS；不要复用本地 E2E 的两天证书。
当前二进制启动时加载证书，**不包含自动续期/热加载**。上线前须安排证书平台续期并滚动重启
`ego-queue-access`（或受控 reloader），监控到期时间。未配置这一点不应长期运行。

```sh
: "${TLS_CERT:?服务端证书文件}"
: "${TLS_KEY:?服务端私钥文件}"
: "${CA_CERT:?签发CA文件}"
kubectl --context "$CTX" -n "$NS" create secret tls ego-queue-access-tls \
  --cert="$TLS_CERT" --key="$TLS_KEY"
cat > "$STATE/webhook.yaml" <<EOF_WEBHOOK
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ego-queue-access
  namespace: volcano-system
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: ego-queue-access-webhook
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: ego-queue-access-reader
subjects:
- kind: ServiceAccount
  name: ego-queue-access
  namespace: volcano-system
---
apiVersion: v1
kind: Service
metadata:
  name: ego-queue-access
  namespace: volcano-system
spec:
  selector: {app: ego-queue-access}
  ports: [{port: 443, targetPort: 8443}]
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ego-queue-access
  namespace: volcano-system
spec:
  replicas: 2
  selector:
    matchLabels: {app: ego-queue-access}
  template:
    metadata:
      labels: {app: ego-queue-access}
    spec:
      serviceAccountName: ego-queue-access
      schedulerName: default-scheduler
      containers:
      - name: webhook
        image: $IMAGE
        command: [/queue-access-webhook]
        args: [--scheduler-names=volcano-infra]
        resources:
          requests: {cpu: 100m, memory: 64Mi}
          limits: {memory: 256Mi}
        readinessProbe:
          httpGet: {scheme: HTTPS, path: /healthz, port: 8443}
        volumeMounts:
        - {name: tls, mountPath: /tls, readOnly: true}
      volumes:
      - name: tls
        secret: {secretName: ego-queue-access-tls}
EOF_WEBHOOK
# Private registry: add your approved imagePullSecrets before applying.
kubectl --context "$CTX" apply --dry-run=server -f "$STATE/webhook.yaml"
kubectl --context "$CTX" apply -f "$STATE/webhook.yaml"
kubectl --context "$CTX" -n "$NS" rollout status deployment/ego-queue-access --timeout=180s
export CA_CERT
python3 - <<'PY'
import base64,json,os,pathlib
p=pathlib.Path(os.environ['STATE'])
w={'apiVersion':'admissionregistration.k8s.io/v1','kind':'ValidatingWebhookConfiguration','metadata':{'name':'ego-queue-access'},'webhooks':[{
'name':'queue-access.simplesilicon.com','admissionReviewVersions':['v1'],'sideEffects':'None','failurePolicy':'Fail','timeoutSeconds':10,
'namespaceSelector':{'matchLabels':{'scheduling.simplesilicon.com/queue-access':'canary'}},
'clientConfig':{'service':{'namespace':'volcano-system','name':'ego-queue-access','path':'/validate'},'caBundle':base64.b64encode(pathlib.Path(os.environ['CA_CERT']).read_bytes()).decode()},
'rules':[{'operations':['CREATE','UPDATE'],'apiGroups':[''],'apiVersions':['v1'],'resources':['pods']},{'operations':['CREATE','UPDATE'],'apiGroups':['scheduling.volcano.sh'],'apiVersions':['v1beta1'],'resources':['podgroups']}]}]}
(p/'webhook-registration.json').write_text(json.dumps(w,indent=2))
PY
kubectl --context "$CTX" apply --dry-run=server -f "$STATE/webhook-registration.json"
kubectl --context "$CTX" apply -f "$STATE/webhook-registration.json"
```

确认无 namespace 已有上述灰度标签；系统 namespace 不添加它。不要给普通业务用户
namespace 标签、QueueAccessPolicy 或 PipelinePolicy 的写权限。

创建队列策略，同时明确关闭抢占授权（**不要直接 apply 开放授权的示例文件**）：

```sh
sed '/  allowedPreemptorNamespaces:/,$d' installer/pipeline/queue-access-example.yaml \
  > "$STATE/ego-access.yaml"
printf '  allowedPreemptorNamespaces: []\n' >> "$STATE/ego-access.yaml"
cat "$STATE/ego-access.yaml"
kubectl --context "$CTX" apply --dry-run=server -f "$STATE/ego-access.yaml"
kubectl --context "$CTX" apply -f "$STATE/ego-access.yaml"
kubectl --context "$CTX" label namespace infra-group \
  scheduling.simplesilicon.com/queue-access=canary
```

**范围区别**：scheduler 的 QueueAccessPolicy 按队列全局生效；Webhook 标签只控制准入
灰度范围，不把 scheduler ACL 限制在 infra-group。必须先核对所有 protected queue 使用者。
namespace 灰度期间，未纳入的 namespace 尚未受这个 Webhook 保护，不能宣称全群已隔离。

## E. 验证（不占 GPU，不驱逐）

```sh
# infra-group 有权访问 shared，dry-run 应通过。
kubectl --context "$CTX" create --dry-run=server -f - <<'YAML'
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  generateName: ego-access-shared-check-
  namespace: infra-group
spec:
  minMember: 1
  queue: infra-ego-pipeline-shared
YAML
# infra-group 无权访问 base，下面必须返回 Forbidden / namespace not allowed。
kubectl --context "$CTX" create --dry-run=server -f - <<'YAML'
apiVersion: scheduling.volcano.sh/v1beta1
kind: PodGroup
metadata:
  generateName: ego-access-base-denied-
  namespace: infra-group
spec:
  minMember: 1
  queue: infra-ego-pipeline-base
YAML
kubectl --context "$CTX" -n "$NS" logs deployment/ego-queue-access --since=5m
kubectl --context "$CTX" -n "$NS" logs deployment/"$DEP" --since=5m --tail=100
kubectl --context "$CTX" -n infra-ego-pipeline get pipelinepolicy ego-pipeline -o yaml
```

继续用一份**新名字**的 CPU canary worker 验证创建、调度和删除，不重启真实 worker。
保留 safeguard、安全上下文等现有准入要求。使用 scheduler `volcano-infra`、shared 队列，
CPU requests 明确填写，GPU request 不设置。不能用 CPU canary 宣称真实 GPU 抢占已验证。

验收已有 Pod 的 UID、nodeName、容器 restartCount 与快照对比；自然结束/故障导致变化
需查事件，不能把所有变化直接归因于插件。旧任务重建在本地已测试：权限未撤销时正常，
撤销后新 Pod 被拒绝；生产不删除旧 Pod 来做这个测试。

灰度确认后逐个标记 `data-group`、`infra-ego-pipeline`，再覆盖**其他业务 namespace**，
这样非法 namespace 的准入也会被检查。排除系统 namespace 并保留原 Kyverno 安全策略。
扩范围前保存各 namespace 原标签。未纳入范围的 namespace 不属于准入隔离保证。

## F. 之后才开启真实抢占（独立评审）

当前到 E 为止无抢占。以下配置准备好再开，不能仅凭 ACL 测试通过直接驱逐真实业务。

1. 每类 worker 的 Pod 都有 `pipeline.simplesilicon.com/name: ego-pipeline` 和
   `scheduling.simplesilicon.com/worker-type: <类型>`；base/shared 都按同一类型统计。
2. 每类建立 WorkerBudget（namespace `infra-ego-pipeline`、pipelineRef.name 为
   `ego-pipeline`、resourceName `nvidia.com/gpu`、minAvailableGPU 由业务填写）和 PDB。
   PDB selector 同时含 pipeline name 与 worker-type。参考 `pipeline-example.yaml`，
   **示例 8 卡/2 副本不是生产建议值**。缺少 budget、未 Ready 或已有缺口应先处理。
3. 安装一个 `/budget-controller`，专用 SA：集群只读 Pods、PipelinePolicy、WorkerBudget，
   可 update WorkerBudget/status；配置资源和镜像拉取权限。它仅观察，不扩缩容。
4. 创建专用持久 ConfigMap `volcano-system/ego-preemption-recovery`，初始
   `data.intents.json: "{}"`。只允许首次 create，绝不 apply 空内容覆盖已存在记录。
   scheduler 需要该 ConfigMap 的 get/update 权限、Pods/PDB/策略只读权限及
   `pods/eviction` create 权限；controller 的 status 权限不要误授给 Webhook。
5. 单活动 scheduler；不得与原生 preempt/reclaim 或其他会驱逐相同 workers 的路径并用。
   保留当前 capacity/nodegroup/binpack，仅增加以下配置：

```yaml
# 第一层 plugins 增加（保留 queue-access）
- name: worker-type-preemption
  arguments:
    victimQueue: infra-ego-pipeline-shared
    pipelineBudgets: true
# 顶层 actions 改为：
# actions: "enqueue, allocate, worker-type-preempt, backfill"
# 顶层 configurations 增加：
# - name: worker-type-preempt
#   arguments:
#     pipelineBudgets: true
#     queueAccess: true
#     recoveryNamespace: volcano-system
#     recoveryName: ego-preemption-recovery
#     evictionTimeoutSeconds: 10
```

6. 训练 Pod（含 PyTorchJob 每个副本 template）须有 pipeline name 和
   `pipeline.simplesilicon.com/namespace: infra-ego-pipeline`，shared 队列，实际 GPU
   request 和高于 victim 的 PriorityClass。不额外放开其他 namespace 或伪造请求。
   worker 标签应通过 Git 发布；修改 template 会滚动更新，须另安排窗口。
7. 确认不存在历史 pending GPU 训练会因授权立即触发真实驱逐，先用隔离 canary worker
   验证信号、业务租约、恢复和预算，再允许真实训练。两次授权命令分别是：

```sh
# 第一阶段只授权 infra-group
kubectl --context "$CTX" -n infra-ego-pipeline patch pipelinepolicy ego-pipeline \
  --type=merge -p '{"spec":{"allowedPreemptorNamespaces":["infra-group"]}}'
# 灰度验收后扩到目标两个 namespace
kubectl --context "$CTX" -n infra-ego-pipeline patch pipelinepolicy ego-pipeline \
  --type=merge -p '{"spec":{"allowedPreemptorNamespaces":["infra-group","data-group"]}}'
```

验收类型占卡排序、每次选择后的重排、PDB/卡保底、base UID 不变、训练取消和恢复记录
最终为空。保底不覆盖节点故障或其他控制器直接删除；本地验证不替代真实业务恢复验收。

## G. 回滚

### 只完成 A–E（尚无自定义抢占）

先暂停扩范围。关闭**本次独立** Webhook 的 namespace 匹配，不动其他安全策略：

```sh
kubectl --context "$CTX" patch validatingwebhookconfiguration ego-queue-access \
  --type=json -p '[{"op":"add","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-disabled","expression":"false"}]}]'
```

这会暂时撤掉这一个准入防线；scheduler ACL 仍在。错误撤权优先恢复 policy 的原 allowlist，
而不是删除 policy（删除代表不再管理该队列）。恢复 scheduler 前核对 Deployment UID
与快照一致、期间没有他人更新；以下回滚覆盖其 spec，应先审阅 dry-run：

```sh
jq '[{op: "replace", path: "/spec", value: .spec}]' "$STATE/scheduler.json" > "$STATE/scheduler-rollback.json"
kubectl --context "$CTX" -n "$NS" patch deployment "$DEP" --type=json \
  --patch-file "$STATE/scheduler-rollback.json" --dry-run=server -o yaml > "$STATE/rollback-preview.yaml"
# Review, then restore original image/args/config volume/strategy:
kubectl --context "$CTX" -n "$NS" patch deployment "$DEP" --type=json \
  --patch-file "$STATE/scheduler-rollback.json"
kubectl --context "$CTX" -n "$NS" rollout status deployment/"$DEP" --timeout=180s
```

JSON Patch 整体恢复 Deployment spec，包括原 strategy；先确认没有需要保留的并行修改。
本方案使用新的 ConfigMap，原 ConfigMap 没被修改，不需要恢复其数据。
恢复官方 scheduler 后它不执行 QueueAccessPolicy，独立策略仍保留但不能宣称继续强制执行。
不删除旧 worker、队列、CRD。确认不再引用后可删除本次新增 Webhook Deployment/Service，
证书 Secret 和专用 RBAC；暂保留策略及快照便于再次部署。

### 已完成 F（可能发生驱逐）

先撤销新抢占权限：

```sh
kubectl --context "$CTX" -n infra-ego-pipeline patch pipelinepolicy ego-pipeline \
  --type=merge -p '{"spec":{"allowedPreemptorNamespaces":[]}}'
kubectl --context "$CTX" -n volcano-system get cm ego-preemption-recovery \
  -o jsonpath='{.data.intents\.json}' | jq .
```

保持 custom action、budget-controller 和日志可用，排查每条未决记录。撤权不能撤回已经
提交的 Eviction。超时不明确的记录可能暂时阻塞后续自定义抢占，不能清空 journal 强行恢复；
根据真实 victim/demand 状态等待解析，必要时由业务方取消该 demand。**确认 journal 为空且
无在途驱逐后**，才移除 custom action/plugin，恢复 C 的 ACL-only 配置，再按需要回滚 A–E。
移除 action 会停止恢复处理，它不是“排空”命令。已被驱逐 worker 的重建由原控制器负责。

### 证书续期

用证书平台续期后的同域名证书更新 Secret，CA 轮换时同步更新 VWC caBundle（重叠信任
窗口），然后滚动重启 validator 并执行 E。不要使用示例测试证书长期上线。
Webhook 故障时 Fail 会阻止纳入范围内匹配的 Pod/PodGroup 请求（含普通队列），应告警；
紧急停止灰度用上述唯一 VWC 的 matchConditions=false，不修改全局 Kyverno failurePolicy。
