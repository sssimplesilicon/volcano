> Historical ACL-based release results. The current runner delegates to the
> unified workload-policy suite; see ../workloadpolicy/VALIDATION.md for current results.

# Local validation — 2026-10-20

Endpoint: local k3s `https://127.0.0.1:16473`, v1.35.5+k3s1.
No production API calls or production mutations. New standalone arm64 image
`workload-routing:local` imported into the local server; no registry push.

38 end-to-end assertions passed, including 12 concurrent admission requests:

- ego legacy root-label batch and replica-selector fallback;
- explicit ego / video pipeline routing and unknown explicit pipeline fallback;
- unauthorized namespace fallback; independent per-pipeline ACLs;
- new batch and missing batch fallback, preservation of user tolerations;
- namespace scope exclusion and old-controller bypass;
- concurrent authorized/unauthorized requests across pipeline types;
- historical unmarked root created before activation: UID/spec unchanged;
- real ReplicaSet child deletion/recreation with unchanged original routing;
- overlapping rules and overlapping policy scopes rejected;
- schema rejects inconsistent queue policy reference;
- ACL revocation changes new routing but does not mutate existing root templates;
- existing routed template's child recreated after ACL revocation;
- missing ACL is a configuration error, not unrestricted access;
- inconsistent replica batch selectors rejected;
- webhook outage blocks enrolled CREATE but not outside namespace;
- disabling admission restores new submissions without modifying historical spec.

The old local migration adapter was temporarily excluded only from three test
namespaces, then restored exactly. Real Kyverno was also tested with an isolated copy of the old Arena route:
marker handoff, continued old routing outside scope, disabling the new router,
and guarded rollback all passed. Production ownership-adapter coexistence still
requires the production dry-run in the rollout guide. Real GPU allocation, production
wuying placement, running training and preemption are not exercised. Child Pods
intentionally remained Pending on synthetic node selectors. Preserving child
routing does not bypass a revoked QueueAccessPolicy in the scheduler/validator.

Also passed:

- Go tests for new workloadrouting and existing queueaccess admission/plugins.
- strict server-side dry-run for generated installation manifests.
- `git diff --check`.

Final cleanup verified: suspended PyTorchJobs, ReplicaSets and child Pods removed;
test webhook condition is false; original migration-adapter matchConditions
restored. Test infrastructure/policies remain in wr-e2e scope for inspection.
