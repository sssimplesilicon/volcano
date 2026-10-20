"""Offline contract tests for the single-service deployment bundle."""
import json
import pathlib
import subprocess
import sys
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
INSTALLER = ROOT / "installer/pipeline"


class UnifiedBundle(unittest.TestCase):
    def test_one_backend_no_queue_acl_and_disabled_gates(self):
        result = subprocess.run([
            sys.executable, str(INSTALLER / "render-workload-policy.py"),
            "--policy", str(INSTALLER / "business-routing-example.json"),
            "--additional-policy", str(INSTALLER / "workload-routing-example.json"),
            "--pools", str(INSTALLER / "nodepool-policy-example.json"),
            "--image", "example.invalid/workload-policy:test",
        ], capture_output=True, text=True, check=True)
        items = json.loads(result.stdout)["items"]
        deployments = [x for x in items if x["kind"] == "Deployment"]
        services = [x for x in items if x["kind"] == "Service"]
        self.assertEqual(len(deployments), 1)
        self.assertEqual(len(services), 1)
        self.assertEqual(services[0]["metadata"]["name"], "workload-policy")
        self.assertEqual(deployments[0]["spec"]["template"]["spec"]["schedulerName"],
                         "default-scheduler")
        self.assertEqual(
            {x["metadata"]["name"] for x in items if x["kind"] == "WorkloadRoutingPolicy"},
            {"business-routing", "arena-pytorch"})
        self.assertFalse(any(x["kind"] == "QueueAccessPolicy" for x in items))
        for obj in items:
            if obj["kind"] == "ClusterRole":
                resources = {r for rule in obj["rules"] for r in rule.get("resources", [])}
                self.assertNotIn("queueaccesspolicies", resources)
                self.assertIn("pytorchjobs", resources)
                self.assertIn("replicasets", resources)
            if obj["kind"].endswith("WebhookConfiguration"):
                for webhook in obj["webhooks"]:
                    self.assertEqual(webhook["clientConfig"]["service"]["name"], "workload-policy")
                    self.assertEqual(webhook["matchConditions"],
                                     [{"name": "rollout-enabled", "expression": "false"}])


if __name__ == "__main__":
    unittest.main()
