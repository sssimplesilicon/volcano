# ego-pipeline-prod：三台机器、单队列、GitOps 分批迁移

## 发布与范围

运行时代码：`4984256143df100e8f67ae702dcd2ac9a9baf046`。三个角色镜像均已推送至 `ss-container-lib-registry.cn-shanghai.cr.aliyuncs.com/infra/volcano`，均包含 linux/amd64、linux/arm64。不可变 digest 见 `releases/single-queue-49842561.json`。

生产只做了只读检查，尚未升级、迁节点或同步应用。

| 批次 | 节点 | 原 Deployment | 新 worker 期望 GPU / 保底 |
|---|---|---|---|
| 1 | ws-0buj1s9hweo2h3z8c | egov2-hawor-vo-grpc-5022 | 4 / 2 |
| 2 | ws-0buj1sdt3w21c5hso | egov2-hawor-vo-grpc-5023 | 8 / 4 |
| 3 | ws-0buj1sdt3w21c5hsn | egov2-hawor-vo-grpc-5024 | 12 / 6 |

- 新 Queue、nodegroup、pool 标签值：`infra-ego-pipeline-prod`。
- PipelinePolicy：`infra-ego-pipeline/ego-pipeline-prod`。
- 新 Deployment：`infra-ego-pipeline/egov2-hawor-vo-grpc-prod`，worker-type 保持 `hand-detect`。
- 三份业务模板经只读核对一致，仅 app 和 SSH 端口节点选择不同，合并为一个 Deployment。保留业务镜像、环境、挂载和每副本资源，新 worker 通过路由注入的 nodegroup 限制到新节点池，不固定节点名；本次脚本只操作上述三台节点。
- 每批先由 GitOps 将旧副本降到 0，等包括 Terminating 在内的旧 GPU Pod 全部退出，才增加新 desiredGPU。该批会有停机窗口；迁移过程可以低于 12 卡，但不会通过脚本增加到 12 卡以上。
- 不创建 PDB；原 `ego-preemption-canary` PDB 的选择器不匹配新 pipeline，保留原样。
- 不修改其他 worker 的 Deployment、业务镜像或副本数。不整体同步 Argo Application。
- 两条绑定目标节点的 CPFS 巡检 CronJob 随批次切换为新名字；旧 CronJob suspend，已创建 Job 不删除。新监控仍然是 CPU 任务。
- 新 PipelinePolicy 的 `allowedPreemptorNamespaces: []` 始终保持关闭。本次不开放抢占，也不改变已有 scheduler actions。旧 runtime 镜像会升级，两个既有 admission/backend 的副本会滚动替换。

## 谁管理 replicas

GitOps 管新 Deployment 的业务模板，GPU controller 只管其 `/scale`。WorkerBudget 显式绑定这个新 Deployment UID，不接管其他 worker。

Argo Application 仅对新 Deployment 忽略 `/spec/replicas`，加 `RespectIgnoreDifferences=true`。Git 中初始 replicas=0 负责安全创建；随后 desiredGPU 由平台 WorkerBudget 驱动。不要再用 Git replicas 调整新 worker，也不要配置 HPA。此轮 WorkerBudget 由平台迁移脚本创建，并非由 Application 管理。

训练抢占时 Deployment 的期望副本不减少，替代 Pod 可以 Pending，训练结束后重新调度。GPU 保底只约束自定义驱逐，不阻止手动删 Pod、节点故障或 Deployment 发布。本版本主动降低 desiredGPU 需关闭抢占授权、worker 稳定 Ready 且恢复日志为空。

## 1. 获取文件，保持手动同步

```bash
export PLATFORM_REPO='/Users/zoisite/Documents/Codex/2026-10-10/volcano/work/volcano-ss'
export CD_REPO='/Users/zoisite/Documents/Codex/2026-10-10/volcano/work/infra-ego-pipeline-cd'
export SQ_TOOL="$PLATFORM_REPO/installer/pipeline/migrate-ego-single-queue.py"
export SQ_RELEASE="$PLATFORM_REPO/installer/pipeline/releases/single-queue-49842561.json"
export SQ_STATE="$HOME/volcano/ego-single-queue-$(date +%Y%m%d-%H%M%S)"
: "${CTX:?设置生产 context}"

cd "$PLATFORM_REPO"
git pull --ff-only origin ss/v1.15.3-pipeline-preemption

cd "$CD_REPO"
git fetch origin
# 此分支只添加暂未被根 kustomization 引用的迁移资产。
# 如 main 已前进，不能 fast-forward 时先人工合并审阅，不强推。
git switch main
git pull --ff-only origin main
git merge --ff-only origin/ss/single-queue-prod-rollout
git push origin main

kubectl --context "$CTX" -n argocd get application infra-ego-pipeline -o json |
jq -e '.spec.source.targetRevision == "main" and
       (.spec.syncPolicy.automated == null) and (.operation == null)'
```

`true` 才继续。不要打开自动同步、prune 或 selfHeal。GitOps 本地 stage 状态保存在该 checkout 的 `.git/ego-single-queue-stage.json`，中途不要更换 checkout 或丢失状态目录。脚本不重写未知 Git 改动。

## 2. 平台前置：只读准备，再升级和配置

```bash
python3 "$SQ_TOOL" prepare --context "$CTX" --state "$SQ_STATE" --release "$SQ_RELEASE"
cat "$SQ_STATE/plan.json"

# 接下来才会修改生产。更新 additive CRD 和三个平台 Deployment 的镜像。
python3 "$SQ_TOOL" upgrade --context "$CTX" --state "$SQ_STATE"
# 新队列、PriorityClass、prod 专用路由与空授权；尚不创建 worker。
python3 "$SQ_TOOL" configure --context "$CTX" --state "$SQ_STATE"
```

前置会检查三个老 Deployment 各 4 个单卡 Pod、节点未迁移、模板一致、无 Service/HPA 依赖、旧 prod 队列无 PodGroup。旧 `base-prod/share-prod` 队列保留为空，不删除旧池的 base/shared 队列。

任何命令失败立即停下，不要继续下一批；保留 SQ_STATE。平台升级涉及 API admission/backend 的滚动更新，不会主动重启业务 worker。

## 3. GitOps 阶段 0：只创建零副本 worker

```bash
cd "$CD_REPO"
python3 ops/single-queue/stage.py 0
git diff -- kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
kubectl kustomize . > "$SQ_STATE/gitops-stage-0.yaml"
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m 'deploy: prepare zero-replica single-queue ego worker'
git push origin main

# Application 本身不在 root Kustomization 中，因此单独应用 ignoreDifferences。
kubectl --context "$CTX" apply -f bootstrap/application.json
python3 ops/single-queue/sync.py 0 --context "$CTX" --revision "$(git rev-parse HEAD)"

# 绑定刚由 Argo 创建的新 Deployment UID，安装 worker controller，desiredGPU=0。
python3 "$SQ_TOOL" attach --context "$CTX" --state "$SQ_STATE"
python3 "$SQ_TOOL" verify --context "$CTX" --state "$SQ_STATE"
```

`sync.py` 用 Argo Application.operation 指定资源同步，不要求安装 argocd CLI。阶段 0 只同步新 Deployment 和两条 suspend=true 的新监控，不同步其他资源。

## 4. 每次仅一批

以下先执行 `BATCH=1`，验收业务日志、数据库消费恢复后，再分别执行 `BATCH=2`、`BATCH=3`。不要直接用循环跑完。

```bash
BATCH=1
cd "$CD_REPO"
python3 ops/single-queue/stage.py "$BATCH"
git diff -- kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml
kubectl kustomize . > "$SQ_STATE/gitops-stage-$BATCH.yaml"
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m "deploy: retire old ego GPU worker batch $BATCH"
git push origin main
python3 ops/single-queue/sync.py "$BATCH" --context "$CTX" --revision "$(git rev-parse HEAD)"

# 确认 GitOps 旧副本为零；等待旧 Pod 完全退出；只改该节点两个标签；增加新预算。
python3 "$SQ_TOOL" batch --batch "$BATCH" --context "$CTX" --state "$SQ_STATE"
python3 "$SQ_TOOL" verify --context "$CTX" --state "$SQ_STATE"

kubectl --context "$CTX" -n infra-ego-pipeline get workerbudget ego-hand-detect-prod -o yaml
kubectl --context "$CTX" -n infra-ego-pipeline get pods -l app=egov2-hawor-vo-grpc-prod -o wide
kubectl --context "$CTX" -n infra-ego-pipeline logs deployment/egov2-hawor-vo-grpc-prod -c hawor --since=5m --tail=100
```

每批最终分别为：旧/新 GPU 副本 `8/4`、`4/8`、`0/12`。保底分别 `2/4/6`。检查 WorkerBudget `.status.readyGPU` 和 `.status.scaling` 收敛，不只检查 Pod Running。新 Pod 的业务处理效果仍需通过业务日志确认。

迁移期间不要提交显式 `pipeline.simplesilicon.com/name=ego-pipeline-prod` 的 Arena 训练，防止其占用本批容量使验收等待。现有旧 pipeline 的训练路由不变。新 pipeline 训练路由已经配到同一 Queue，但抢占授权为空。

## 5. 回滚：必须先退新，再恢复旧

若尚未执行阶段 0，且没有创建新 worker，可直接使用平台 rollback。若已执行阶段 0 或任意一批：

```bash
# A. 停止新 worker 控制器，解除 desiredGPU 自动控制；不触碰 GitOps worker。
python3 "$SQ_TOOL" stop --context "$CTX" --state "$SQ_STATE"

# B. 从 Git 中移除新 worker/新监控，保留已经归零的旧副本为零。
cd "$CD_REPO"
python3 ops/single-queue/stage.py rollback-stop
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m 'rollback: remove new ego worker before restoring old GPU workers'
git push origin main
python3 ops/single-queue/sync.py rollback-stop --context "$CTX" --revision "$(git rev-parse HEAD)"
# 此同步只对新 Deployment 和两个新 CronJob 开 prune，不清理其他资源。

# C. 等新 worker Pod 全部消失，再恢复已迁移节点标签、旧路由和平台镜像。
python3 "$SQ_TOOL" rollback --context "$CTX" --state "$SQ_STATE"

# D. Git 恢复旧 worker 的 4/4/4 副本和旧监控。
python3 ops/single-queue/stage.py rollback-restore
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m 'rollback: restore original ego worker replicas and monitors'
git push origin main
kubectl --context "$CTX" apply -f bootstrap/application.json
python3 ops/single-queue/sync.py rollback-restore --context "$CTX" --revision "$(git rev-parse HEAD)"
for PORT in 5022 5023 5024; do
  kubectl --context "$CTX" -n infra-ego-pipeline rollout status \
    "deployment/egov2-hawor-vo-grpc-$PORT" --timeout=360s
done
```

回滚保留新增 CRD 字段、空 Queue、PriorityClass、零副本 worker-controller、RBAC 和已解除 desired 控制的 WorkerBudget 供检查，不自动删除集群级资源。旧 Deployment UID 和模板保持不变。停止状态若出现未知 UID、策略被第三方修改或未完成创建的记录，脚本拒绝覆盖，应先检查状态文件。

## 验证证据与限制

- 运行时代码上轮本地 k3s：8 个单队列流程检查 + 10 个旧抢占回归场景最终通过；GPU 是模拟资源，不是生产 GPU 性能试验。
- 本轮重新通过相关 Go 单元测试与安装脚本测试；迁移脚本模拟三批和半途失败回滚，检查目标总副本从不超过 12。
- GitOps 阶段 0/1/2/3/rollback-stop/rollback-restore 全部本地 Kustomize 渲染；非目标资源内容不变，最终回滚渲染等于原清单。
- 生产只读准备发现三份 worker 模板可合并、原 PDB 不匹配新 worker，并识别了两条必须一并迁移的节点监控。
- 尚未在生产执行真实迁移。不要把上述验证解释为已验证生产挂载、驱动或业务消费恢复。

## CRD 字段所有权冲突后的重试

若旧版 `upgrade` 报 `conflict with "kubectl-create": .spec.versions`，不要删除 CRD，也不要直接添加 `--force-conflicts`。更新迁移脚本后使用原来的 `SQ_STATE` 重试；不要重新运行 prepare，以免丢失原始回滚基线。

新版 upgrade 会备份三份现有 CRD，保留线上额外 schema 字段，合并本次新增字段。对已有字段的不兼容修改会停止。所有变更先 server dry-run，再用 UID 和完整 spec 的 JSON Patch test 防止并发覆盖。CRD 不变时跳过；如果上次已更新部分平台镜像，则确认配置符合本次目标后继续等待 rollout。

```bash
# 在 Volcano fork 的 ss/v1.15.3-pipeline-preemption 分支执行
 git pull --ff-only origin ss/v1.15.3-pipeline-preemption
 export SQ_TOOL="$PWD/installer/pipeline/migrate-ego-single-queue.py"
 : "${CTX:?请设置生产 context}"
 : "${SQ_STATE:?请保留原迁移状态目录}"
 jq '{upgraded,configured,created,started,completed}' "$SQ_STATE/progress.json"
 (
   set -e
   python3 "$SQ_TOOL" upgrade --context "$CTX" --state "$SQ_STATE"
   python3 "$SQ_TOOL" configure --context "$CTX" --state "$SQ_STATE"
 )
```

上述适用于 upgrade 未完成、configure 尚未创建资源的情况。若 `configured=true` 或 `created` 非空，不重复 configure，应检查原执行进度。升级只改变平台镜像和新增 CRD schema；configure 创建平台策略，不迁移节点或缩放 worker。之后继续阶段 0 的 GitOps 同步。

该修复通过 23 个安装脚本测试，并在本地 k3s 使用独立测试 CRD 复现原 SSA 冲突，验证升级、保留已有实例、重复执行和拒绝陈旧 patch；未修改生产配置。

## 一次切换：先停全部三个旧 worker，再启动新 worker

这是替代第 4 节逐批迁移的选项。仍使用原来的 SQ_STATE、GitOps checkout 和 stage 状态。先完成阶段 0（包括 attach，预算 0/0）；不要重跑 prepare、configure 或 stage.py 0。固定节点名的旧清单已修正，新 worker 只依赖平台注入的 nodegroup；切换脚本仍严格限定本次三台节点。

本方案有三个旧 worker 全部停止的业务窗口。新 pipeline 抢占授权保持为空，PDB 不新增，其他 worker 不修改。禁止在切换期间向新池提交训练任务。

```bash
# 先更新两个仓库，阶段 0 失败的先重试 sync.py 0，再 attach/verify。
cd "$PLATFORM_REPO"
ALL_PROXY='socks5://127.0.0.1:10808' git pull --ff-only origin ss/v1.15.3-pipeline-preemption
cd "$CD_REPO"
ALL_PROXY='socks5://127.0.0.1:10808' git pull --ff-only origin main

# A. 只在阶段 0 完成后执行。生成并提交停旧清单。
python3 ops/single-queue/stage.py stop-old
git diff -- kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
kubectl kustomize . > "$SQ_STATE/gitops-stop-old.yaml"
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m 'deploy: stop three old ego workers before single-queue cutover'
ALL_PROXY='socks5://127.0.0.1:10808' git push origin main
ALL_PROXY='socks5://127.0.0.1:10808' python3 ops/single-queue/sync.py stop-old --context "$CTX" --revision "$(git rev-parse HEAD)"

# B. 确认 A 成功后：等待旧 Pod 全退出，检查没有其他 GPU Pod，切换三节点并启动新副本。
python3 "$SQ_TOOL" cutover --context "$CTX" --state "$SQ_STATE"
python3 "$SQ_TOOL" verify --context "$CTX" --state "$SQ_STATE"

# C. 新 worker 12 个 Ready、业务恢复后，再启用新巡检。
cd "$CD_REPO"
python3 ops/single-queue/stage.py start-monitors
git add kustomization.yaml workloads/hawor-vo-grpc/kustomization.yaml bootstrap/application.json
git commit -m 'deploy: start prod monitors after ego cutover'
ALL_PROXY='socks5://127.0.0.1:10808' git push origin main
ALL_PROXY='socks5://127.0.0.1:10808' python3 ops/single-queue/sync.py start-monitors --context "$CTX" --revision "$(git rev-parse HEAD)"
```

每条命令成功后再执行下一条，脚本 stage 生成、commit、attach 都不要盲目重复。中途失败保留状态，按第 5 节原有回滚：stop 控制器 → GitOps rollback-stop 删除新 worker/监控 → 平台 rollback 恢复节点和策略 → GitOps rollback-restore 恢复旧 4/4/4。cutover 中途失败可能保留 cordon 的节点，回滚会恢复本次三节点；不要直接启动旧副本与新副本并存。

验证：27 个平台安装脚本单元测试、GitOps 渲染流程和本地 k3s 阶段 0 服务端 dry-run 通过；该一次切换流程的生产业务运行效果尚未验证。
