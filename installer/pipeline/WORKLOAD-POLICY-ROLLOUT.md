# 普通工作负载路由与节点隔离：infra-group 发布步骤

本次使用独立 `workload-policy` Webhook 服务。**不替换调度器，不修改 Queue、
抢占配置、节点标签/污点、已有工作负载或其他 Kyverno 安全策略。**
以下命令由生产操作者执行。先部署关闭状态，再仅启用 dry-run；这不是全量切换脚本。

## 1. 固定版本、镜像与备份目录

从本次发布的 Git commit checkout 根目录执行。`POLICY_IMAGE` 使用发布记录中的
`ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/infra/volcano@sha256:...`。
这是 Webhook 镜像，**不能用于 volcano-infra-scheduler**。

```sh
: "${CTX:?请设置生产 context}"
: "${POLICY_IMAGE:?请设置本次发布的 workload-policy 镜像 digest}"
export SRC="$PWD"
export WP_STATE="$HOME/volcano/workload-policy-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$WP_STATE"
chmod 700 "$WP_STATE"
test -f "$SRC/installer/pipeline/render-workload-policy.py"

kubectl --context "$CTX" get crd workloadroutingpolicies.scheduling.simplesilicon.com \
  -o json > "$WP_STATE/routing-crd-before.json"
kubectl --context "$CTX" get workloadroutingpolicies -o json > "$WP_STATE/routes-before.json"
kubectl --context "$CTX" get queueaccesspolicies -o json > "$WP_STATE/access-before.json"
kubectl --context "$CTX" get clusterpolicies.kyverno.io -o json > "$WP_STATE/kyverno-before.json"
kubectl --context "$CTX" get mutatingwebhookconfigurations,validatingwebhookconfigurations \
  -o json > "$WP_STATE/webhooks-before.json"
kubectl --context "$CTX" -n kyverno get cm pipeline-isolation-grandfather \
  -o json > "$WP_STATE/registry-before.json"
kubectl --context "$CTX" -n infra-group get pods,deployments,replicasets,statefulsets,daemonsets,jobs,cronjobs,pytorchjobs \
  -o json > "$WP_STATE/workloads-before.json"
kubectl --context "$CTX" -n infra-group get workflows.argoproj.io \
  -o json > "$WP_STATE/workflows-before.json"

kubectl --context "$CTX" -n volcano-system get issuer ego-queue-access-ca
kubectl --context "$CTX" -n volcano-system get secret acr-credential-secret-aggregation -o name
kubectl --context "$CTX" get nodes -l volcano.sh/nodegroup-name=general
kubectl --context "$CTX" get queues.scheduling.volcano.sh \
  default infra-ego-pipeline-base infra-ego-pipeline-shared infra-ego-pipeline-cpu
```

要求：Issuer Ready，至少两个可放置 Webhook 的 general 节点，四个 Queue 已存在。
若未安装 Workflow CRD，跳过该读取并在后面的工作负载类型中删除 Workflow；
若已安装但读取失败，不要误当成空列表。快照包含业务清单，勿提交到 Git。
若已有同名 `business-routing`、`ego-pipeline` NodePoolPolicy 或 `workload-policy`
部署，先检查已有状态，不直接覆盖。既有路由策略必须只负责 PyTorchJob，不能与
本次普通工作负载类型重叠。

## 2. 生成关闭的清单

先生成仅 ego、仅 infra-group 的 Preview/Audit 配置。controllerUsers 暂为空，
因此本阶段不能启用真实接管；正式启用前必须从审计记录/现有配置核对控制器身份。

```sh
python3 - <<'PY'
import json, os
from pathlib import Path
src=Path(os.environ['SRC'])/'installer/pipeline'
out=Path(os.environ['WP_STATE'])
p=json.loads((src/'business-routing-example.json').read_text())
p['spec']['rules']=[r for r in p['spec']['rules'] if not r['name'].startswith('video-')]
pools=json.loads((src/'nodepool-policy-example.json').read_text())
pools['items']=[x for x in pools['items'] if x['metadata']['name']=='ego-pipeline']
(out/'business-policy.json').write_text(json.dumps(p,indent=2)+'\n')
(out/'nodepools.json').write_text(json.dumps(pools,indent=2)+'\n')
PY

python3 "$SRC/installer/pipeline/render-workload-policy.py" \
  --policy "$WP_STATE/business-policy.json" \
  --pools "$WP_STATE/nodepools.json" \
  --image "$POLICY_IMAGE" \
  --image-pull-secret acr-credential-secret-aggregation \
  > "$WP_STATE/install-disabled.json"

# 第一阶段普通路由入口不匹配 PyTorchJob；已有训练路由继续独立工作。
python3 - <<'PY'
import json,os
from pathlib import Path
p=Path(os.environ['WP_STATE'])/'install-disabled.json'
a=json.loads(p.read_text())
for o in a['items']:
 if o['kind'].endswith('WebhookConfiguration'):
  for w in o['webhooks']:
   w['rules']=[r for r in w['rules'] if 'kubeflow.org' not in r['apiGroups']]
p.write_text(json.dumps(a,indent=2)+'\n')
PY
```

## 3. CRD、证书和关闭状态服务

```sh
(
  set -e
  kubectl --context "$CTX" apply --dry-run=server \
    -f "$SRC/installer/pipeline/workload-routing-crd.json" \
    -f "$SRC/installer/pipeline/nodepool-policy-crd.json"
  kubectl --context "$CTX" apply \
    -f "$SRC/installer/pipeline/workload-routing-crd.json" \
    -f "$SRC/installer/pipeline/nodepool-policy-crd.json"
  kubectl --context "$CTX" wait --for=condition=Established \
    crd/nodepoolpolicies.scheduling.simplesilicon.com --timeout=60s
  kubectl --context "$CTX" create \
    -f "$SRC/installer/pipeline/workload-policy-certificate.yaml"
  kubectl --context "$CTX" -n volcano-system wait --for=condition=Ready \
    certificate/workload-policy-serving --timeout=180s
  kubectl --context "$CTX" create --dry-run=server --validate=strict \
    -f "$WP_STATE/install-disabled.json" -o name
  kubectl --context "$CTX" create -f "$WP_STATE/install-disabled.json"
  kubectl --context "$CTX" -n volcano-system rollout status \
    deployment/workload-policy --timeout=180s
)
```

使用 create 是为了在同名资源存在时停止，避免误覆盖。部分失败时核对已创建的资源再继续，
不要直接重跑或改成 apply 整套覆盖。证书复用 `ego-queue-access-ca` Issuer，
Secret 标签复用已有 Reloader 的筛选标签，Deployment 只订阅 `workload-policy-tls`。

```sh
kubectl --context "$CTX" -n volcano-system get pods -l app=workload-policy -o wide
kubectl --context "$CTX" -n volcano-system get endpointslices \
  -l kubernetes.io/service-name=workload-policy
kubectl --context "$CTX" -n volcano-system logs deployment/workload-policy --since=5m --tail=50
kubectl --context "$CTX" get mutatingwebhookconfiguration,validatingwebhookconfiguration \
  workload-policy -o json > "$WP_STATE/installed-webhooks.json"
kubectl --context "$CTX" -n volcano-system get secret workload-policy-tls \
  -o jsonpath='{.data.ca\.crt}' > "$WP_STATE/serving-ca.b64"

python3 - <<'PY'
import json,os
from pathlib import Path
p=Path(os.environ['WP_STATE'])
a=json.loads((p/'installed-webhooks.json').read_text())
ca=(p/'serving-ca.b64').read_text(); assert ca
assert len(a['items'])==2
for o in a['items']:
 assert len(o['webhooks'])==2
 for w in o['webhooks']:
  assert w['clientConfig']['caBundle']==ca, '等待 cainjector 注入 CA 后重查'
  assert w['matchConditions']==[{'name':'rollout-enabled','expression':'false'}]
  assert any(e['operator']=='In' and e['values']==['infra-group'] for e in w['namespaceSelector']['matchExpressions'])
print('CA、infra-group 范围、关闭状态检查通过')
PY
```

## 4. 仅打开 dry-run Preview

两种 webhook 都仅处理 dryRun 请求；真实提交继续走现有策略。修改带旧值测试，
若失败不要去掉 test，需要重新核对状态。

```sh
(
  set -e
  for KIND in mutatingwebhookconfiguration validatingwebhookconfiguration; do
    kubectl --context "$CTX" patch "$KIND" workload-policy --type=json -p='[
      {"op":"test","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"false"}]},
      {"op":"test","path":"/webhooks/1/matchConditions","value":[{"name":"rollout-enabled","expression":"false"}]},
      {"op":"replace","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]},
      {"op":"replace","path":"/webhooks/1/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]}
    ]'
  done
)

kubectl --context "$CTX" create --dry-run=server -f - -o json <<'YAML'
apiVersion: apps/v1
kind: Deployment
metadata:
  name: workload-policy-preview
  namespace: infra-group
  labels:
    safeguard: infra-test
    scheduling.simplesilicon.com/route: shared
spec:
  replicas: 0
  selector:
    matchLabels:
      app: workload-policy-preview
  template:
    metadata:
      labels:
        app: workload-policy-preview
        safeguard: infra-test
    spec:
      containers:
      - name: worker
        image: busybox:1.36
        command: [sh, -c, 'echo preview']
        resources:
          requests: {cpu: 100m, memory: 64Mi}
YAML
```

预期出现 `routing preview` 警告，决定为 shared/volcano-infra。由于仍是 Preview，
不会写入 v2 decision；返回对象仍可能被现有 Kyverno 改写，这不是新策略正式生效。
此命令不创建 Deployment/Pod，也不占 GPU。通过后将入口恢复 false，见第 6 节。

## 5. 真实接管前的必要检查

**不要把上述 dry-run gate 直接改成 true。** 安装和 Preview 成功还不能验证重建身份、
旧任务豁免与双路由共存。下一阶段用第一节快照生成最终配置/补丁：

1. 核对 kube-controller-manager/各控制器和 Argo 的实际请求用户名，填写精确
   controllerUsers。工作负载自己的 serviceAccountName 不是创建它的控制器用户名。
2. 旧 PyTorch 根对象仍走原 `/mutate`，缺少 v2 decision。隔离接管前，必须完成
   `WORKLOAD-POLICY.md` 中 PyTorch bridge，或者在新隔离入口明确排除该类训练 Pod，
   并保持原隔离策略对它们继续生效。不能仅因 ordinary worker dry-run 成功就删除旧隔离。
3. 合并 Workflows 与第一节 workload List，运行 `prepare-workload-handoff.py`，
   审阅 protected Queue 旧控制器 UID 与系统组件例外。新路由开启后、隔离交接前再补一次
   旧根对象快照，避免准备期间新建的旧格式任务漏登记。
4. 路由进入 Enforce，最终校验入口就绪，按 reviewed namespace+v2 decision 对旧
   Kyverno 三条策略做 guarded skip。两套准入不是原子事务；先验证 dry-run，
   不能假定任意 webhook 顺序都不会拒绝冲突约束。
5. 真实创建一个不占 GPU 的 CPU worker，验证实际 owner 链、创建/重建、外部 namespace
   保持原状，再扩展 scope。保留 NodePool 和服务支撑已接管对象重建。

这一阶段不改 QueueAccessPolicy 白名单，不扩大全量队列授权，也不开抢占。
生产快照中若发现既有同名配置或与示例不符，应据快照生成补丁，不能覆盖示例。

## 6. 本次安装/Preview 的回滚

在尚无 v2 根对象接管的这个阶段，可以关闭入口：

```sh
(
  set -e
  for KIND in mutatingwebhookconfiguration validatingwebhookconfiguration; do
    kubectl --context "$CTX" patch "$KIND" workload-policy --type=json -p='[
      {"op":"test","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]},
      {"op":"test","path":"/webhooks/1/matchConditions","value":[{"name":"rollout-enabled","expression":"has(request.dryRun) && request.dryRun == true"}]},
      {"op":"replace","path":"/webhooks/0/matchConditions","value":[{"name":"rollout-enabled","expression":"false"}]},
      {"op":"replace","path":"/webhooks/1/matchConditions","value":[{"name":"rollout-enabled","expression":"false"}]}
    ]'
  done
)
```

若入口本来就是 false，无需执行上述 patch。关闭后可以保留服务，或在确认只进行过本手册
Preview、无依赖新对象的前提下，删除本次 create 的资源：

```sh
kubectl --context "$CTX" delete -f "$WP_STATE/install-disabled.json"
kubectl --context "$CTX" -n volcano-system delete certificate workload-policy-serving
```

不删除 CRD、原 CA Issuer、旧服务、其他路由策略、历史 registry。Certificate 删除后
Secret 是否保留取决于 cert-manager 配置；仅确认无引用时才手动清理该新 Secret。

**未来真实接管后的回滚不同：** 将 `business-routing.spec.mode` 设置为 Disabled 只停止
新接管，保留 scopes、控制器身份、NodePoolPolicy、校验 Webhook 和 Kyverno 兼容 skip。
已接管控制器还要依赖它们重建，不能直接运行上面的卸载命令。
