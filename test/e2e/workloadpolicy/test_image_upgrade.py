"""Safety contract for the image-only release stage (no Kubernetes required)."""
import copy
import importlib.util
import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[3]
spec = importlib.util.spec_from_file_location('upgrade', ROOT / 'installer/pipeline/prepare-unified-image-upgrade.py')
upgrade = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upgrade)


class ImageStage(unittest.TestCase):
    def deployment(self):
        return {'metadata': {'uid': 'original-uid'}, 'spec': {'replicas': 2,
            'strategy': {'type': 'RollingUpdate'},
            'template': {'spec': {'schedulerName': 'default-scheduler', 'containers': [
                {'name': 'webhook', 'image': 'old@sha256:123',
                 'readinessProbe': {'httpGet': {'path': '/healthz'}}}]}}}}

    def hooks(self):
        return {'items': [{'webhooks': [
            {'clientConfig': {'service': {'namespace': 'volcano-system', 'name': 'workload-policy',
                                         'path': '/mutate-workload'}},
             'rules': [{'apiGroups': [''], 'resources': ['pods']}]},
            {'clientConfig': {'service': {'namespace': 'volcano-system', 'name': 'workload-routing',
                                         'path': '/mutate'}}}]}]}

    def test_image_patch_and_inverse_guard_the_full_spec(self):
        d = self.deployment()
        original = copy.deepcopy(d)
        forward, rollback = upgrade.patch_image(d, 'new@sha256:456')
        self.assertEqual(d, original)
        self.assertEqual(forward[0], {'op': 'test', 'path': '/metadata/uid', 'value': 'original-uid'})
        self.assertEqual(forward[1]['value'], d['spec'])
        self.assertEqual(rollback[1]['value']['template']['spec']['containers'][0]['image'], 'new@sha256:456')
        self.assertEqual(rollback[2]['value'], 'old@sha256:123')
        self.assertEqual(len(forward), 3)

    def test_refuses_nonredundant_backend(self):
        d = self.deployment()
        d['spec']['replicas'] = 1
        with self.assertRaisesRegex(ValueError, 'at least two'):
            upgrade.patch_image(d, 'new')

    def test_refuses_arena_handoff_or_wildcard_already_present(self):
        upgrade.check_split(self.hooks())
        for resource in ['pytorchjobs', '*', '*/*', 'pytorchjobs/*']:
            h = self.hooks()
            h['items'][0]['webhooks'][0]['rules'] = [
                {'apiGroups': ['kubeflow.org'], 'resources': [resource]}]
            with self.assertRaisesRegex(ValueError, 'already handled'):
                upgrade.check_split(h)

    def test_validating_webhook_entry_reorder_is_not_drift(self):
        old = self.hooks()['items'][0]
        old['kind'] = 'ValidatingWebhookConfiguration'
        for i, w in enumerate(old['webhooks']):
            w['name'] = f'validate-{i}.example.test'
        new = copy.deepcopy(old)
        new['webhooks'].reverse()
        self.assertEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(new))
        # A reorder must not hide real security/routing changes in the moved entry.
        for key, value in [
            ('failurePolicy', 'Ignore'),
            ('namespaceSelector', {'matchLabels': {'test': 'changed'}}),
            ('matchConditions', [{'name': 'gate', 'expression': 'false'}]),
            ('rules', [{'apiGroups': ['*'], 'resources': ['*'], 'operations': ['*']}]),
        ]:
            changed = copy.deepcopy(new)
            changed['webhooks'][0][key] = value
            self.assertNotEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(changed))
        changed = copy.deepcopy(new)
        changed['webhooks'][0]['clientConfig']['service']['name'] = 'different-backend'
        self.assertNotEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(changed))
        changed = copy.deepcopy(new)
        changed['webhooks'].pop()
        self.assertNotEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(changed))

    def test_mutating_webhook_entry_order_remains_guarded(self):
        old = self.hooks()['items'][0]
        old['kind'] = 'MutatingWebhookConfiguration'
        for i, w in enumerate(old['webhooks']):
            w['name'] = f'mutate-{i}.example.test'
        new = copy.deepcopy(old)
        new['webhooks'].reverse()
        self.assertNotEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(new))

    def test_certificate_rotation_ignored_but_gates_checked(self):
        old = self.hooks()['items'][0]
        new = copy.deepcopy(old)
        new['webhooks'][0]['clientConfig']['caBundle'] = 'rotated'
        self.assertEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(new))
        new['webhooks'][0]['matchConditions'] = [{'name': 'gate', 'expression': 'false'}]
        self.assertNotEqual(upgrade.unchanged_projection(old), upgrade.unchanged_projection(new))


if __name__ == '__main__':
    unittest.main()
