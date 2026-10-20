# Static v7 integration validation

Base: Volcano `v1.15.3` (`d2dd7024f4af3070f9d99d68a78973893908a743`).

Local k3s only; no production operations or remote image push. Tests used the native Linux ARM64, CGO-disabled static binaries. The original local Volcano Deployment UID and spec were unchanged.

| Suite | PASS | SKIP | FAIL |
| --- | ---: | ---: | ---: |
| default-off | 9 | 0 | 0 |
| pipeline | 17 | 0 | 0 |
| legacy | 77 | 10 | 0 |

Go package tests passed for workerpreemption (including guardpreempt and pipelinebudget), scheduler, scheduler options, budget controller build and faultproxy. Python offline safety tests: **14 passed**.

## Default-off comparison

Official v1.15.3, static image with no opt-in flag, and static image with explicit false each ran the same three scenarios: free capacity allocation, full capacity without eviction, and unsatisfied node selector. Outcomes matched. No custom CRDs were installed. No custom Eviction or recovery journal changes occurred.

## Coverage

Worker-type ranking and re-ranking; base protection; priority and authorization; CPU/memory/GPU fragmentation; gang feasibility; sequential and concurrent PDB budgets; external Eviction competition; GPU floors; multiple pipelines; fail-closed policies; journal conflicts and corruption; 403/429/5xx; timeout before/after acceptance; partial success; restart; UID replacement; cancelled training; runtime termination and replacement; pure CPU demand protection; admitted requests accounting.

## Explicit gaps

- `B10`: 需要在选定到提交之间的确定性资源变化故障点
- `C08`: 需要暂停/恢复真实 PDB controller；不允许伪造 PDB.status
- `C09`: 需要捕获真实 disruptedPods 在途窗口，不能人工写 PDB.status
- `D12`: 需要专用 PDB controller 的暂停恢复控制
- `E07`: 需要真实业务数据库协议、worker 镜像及租约契约
- `E08`: 需要业务幂等键及提交响应丢失注入，不用打印日志代替业务验证
- `E10`: wuying 运行时必须另行授权真实环境测试，禁止此本地入口访问生产
- `F05`: 需要两份明确版本镜像及 journal schema 兼容验收
- `F06`: 入口已拒绝原生 preempt/reclaim；尚无集群配置告警 e2e
- `F07`: 入口只允许单活；多活/leader election 部署验收尚未实现

These results use simulated GPU nodes and real Kubernetes PDB/Eviction APIs. They do not prove physical GPU/wuying behavior, business database idempotency, or multi-active scheduler safety. Disabling the extension preserves tested upstream behavior; it is not a proof of equivalence for every upstream configuration.

## Artifacts

- Local image: `volcano-ss:v1.15.3-ss.1-local`
- Image index: `sha256:d81c30a396fc1da18cc3c0fc63404fcbbfbc19fcaa2e8b5ce1f2ca0b4b522efd`
- Linux ARM64 manifest: `sha256:b01a7667dafa20d03ed52d832ca9423452221cce7653a3daa04569ae8f8cf8e4`
- Raw results, timelines, journal snapshots and logs: ignored `test/e2e/pipeline/evidence/` directories.
- The fixture fault-proxy uses the prior v7 test image; scheduler and budget controller both use the newly built static image.

- `vc-scheduler` SHA-256: `fd61e33f887f51968e2093f52145c6e91d9e07daa58263899ea32f81bc6beacf`
- `budget-controller` SHA-256: `b5cee097167b1d5aa1e0a708306204ec80c3c5fd572848c38e9c5e013b0f1295`

## Queue access extension — 2026-10-20

Final local k3s run: **23 PASS, 0 FAIL, 0 SKIP** (P01–P17 regression plus
P18–P23 authorization tests). Related Go unit tests and 14 offline runner tests
passed. Original local Volcano Deployment UID/spec unchanged. Temporary test
webhook and custom scheduler removed after confirming an empty recovery journal;
CRDs and journal storage retained. No production changes or remote publication.

Evidence: `outputs/queue-access-e2e-20261020-final` under the parent Volcano
workspace, including `SUMMARY.md`, `results.json`, `junit.xml`, per-case logs and
timelines, and image/runtime snapshots. See `QUEUE-ACCESS.md` for enforcement
scope and revocation limitations. These tests use synthetic GPUs and a single
active scheduler, not physical GPUs or multi-active scheduler HA.
