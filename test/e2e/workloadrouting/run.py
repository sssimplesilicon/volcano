#!/usr/bin/env python3
"""Compatibility entrypoint: PyTorch and ordinary workloads now share one suite."""
import os,pathlib,runpy
os.environ.setdefault('WORKLOAD_POLICY_E2E_KUBECONFIG',os.environ.get('ROUTING_E2E_KUBECONFIG','/nonexistent/local-only'))
if 'ROUTING_E2E_OUTPUT' in os.environ:
 os.environ.setdefault('WORKLOAD_POLICY_E2E_OUTPUT',os.environ['ROUTING_E2E_OUTPUT'])
print('Running unified workload-policy suite (historical namespace ACL behavior retired).',flush=True)
runpy.run_path(str(pathlib.Path(__file__).resolve().parents[1]/'workloadpolicy/run.py'),run_name='__main__')
