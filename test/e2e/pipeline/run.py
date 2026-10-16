#!/usr/bin/env python3
"""Opt-in local-only E2E runner. --list performs no Kubernetes operations."""
import argparse
import datetime
import fcntl
import json
import pathlib
import sys
import traceback
import xml.etree.ElementTree as ET

from harness import Cluster, Fixture, require
from cases import CASES
import runtime_cases  # registers real-process tests
import pipeline_cases  # registers CRD budget tests

# Explicit gaps, never represented as passing executable tests.
DEFERRED = {
    'A10': '需要第二个隔离 namespace 和对应 Eviction RBAC；尚未实现跨 namespace fixture',
    'B09': '需要多副本 PodGroup/gang fixture，当前 fixture 是 minMember=1',
    'B10': '需要在选定到提交之间的确定性资源变化故障点',
    'C05': '替代 worker Ready 后预算恢复用例待补（现有 fixture 不自动创建替代副本）',
    'C08': '需要暂停/恢复真实 PDB controller；不允许伪造 PDB.status',
    'C09': '需要捕获真实 disruptedPods 在途窗口，不能人工写 PDB.status',
    'C12': '需要带同步屏障的外部 Eviction 客户端',
    'C14': '百分比/maxUnavailable 需要带 scale 子资源的真实控制器 fixture',
    'D01': '需要 journal 写入前确定性故障点',
    'D02': '需要独立最小权限 scheduler SA；不得误改复用 SA 的权限',
    'D03': '需要 journal 写入后、POST 前的确定性暂停点',
    'D06': '需要独立 RBAC fixture 和 API 429 拒绝证据，未复用不安全的旧权限脚本',
    'D07': '当前故障代理仅支持 before/after，需增加明确 5xx 注入模式',
    'D10': '需延长 before 故障窗口，在重试前确定完成同名 UID 替换',
    'D12': '需要专用 PDB controller 的暂停恢复控制',
    'D15': '故障代理尚不支持连续多次超时；单次超时由 D04/D09 覆盖',
    'D16': '需要只拦截 journal 清理写入的故障点',
    'E07': '需要真实业务数据库协议、worker 镜像及租约契约',
    'E08': '需要业务幂等键及提交响应丢失注入，不用打印日志代替业务验证',
    'E09': '需要真实控制器重建和训练结束 fixture',
    'E10': 'wuying 运行时必须另行授权真实环境测试，禁止此本地入口访问生产',
    'F02': '需要三个实际 PriorityClass 和确定性并发释放屏障',
    'F03': '需要持续到达请求的负载生成器',
    'F04': '需要 scheduler 配置热加载/重启 fixture',
    'F05': '需要两份明确版本镜像及 journal schema 兼容验收',
    'F06': '入口已拒绝原生 preempt/reclaim；尚无集群配置告警 e2e',
    'F07': '入口只允许单活；多活/leader election 部署验收尚未实现',
    'F08': '需要外部删除/缩容/节点故障审计归因 fixture',
}
DEFERRED = {k: v for k, v in DEFERRED.items() if k not in CASES and not any(c.startswith(k + '-') for c in CASES)}
KNOWN = {}  # D17 is a required regression after demand-aware recovery.


def write_reports(output, rows):
    (output / 'results.json').write_text(json.dumps(rows, indent=2, ensure_ascii=False))
    suite = ET.Element('testsuite', name='worker-type-preemption-e2e', tests=str(len(rows)),
                       failures=str(sum(r['status'] in ('FAIL', 'XPASS') for r in rows)),
                       skipped=str(sum(r['status'] in ('SKIP', 'XFAIL') for r in rows)))
    for row in rows:
        node = ET.SubElement(suite, 'testcase', classname='volcano.preemption', name=row['id'])
        if row['status'] in ('SKIP', 'XFAIL'):
            ET.SubElement(node, 'skipped', message=row.get('detail', row['status']))
        elif row['status'] in ('FAIL', 'XPASS'):
            ET.SubElement(node, 'failure', message=row.get('detail', row['status'])).text = row.get('traceback', '')
    ET.ElementTree(suite).write(output / 'junit.xml', encoding='utf-8', xml_declaration=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--list', action='store_true')
    parser.add_argument('--case', action='append', default=[], help='ID, repeatable; defaults to all implemented cases')
    parser.add_argument('--pipeline-budgets', action='store_true', help='Run v7 CRD scenarios; requires pipelineBudgets enabled')
    parser.add_argument('--all', action='store_true', help='Include explicit deferred cases in coverage report')
    parser.add_argument('--execute-local', action='store_true', help='Authorize mutation of dedicated local fixtures only')
    parser.add_argument('--output', type=pathlib.Path)
    parser.add_argument('--require-all-pass', action='store_true', help='Nonzero for SKIP/XFAIL as well as failures; release gate')
    args = parser.parse_args(argv)
    all_ids = set(CASES) | set(DEFERRED)
    if args.list:
        for ident in sorted(all_ids):
            state = 'DEFERRED' if ident in DEFERRED else ('KNOWN-FAIL' if ident in KNOWN else 'EXECUTABLE')
            print(ident, state, DEFERRED[ident] if ident in DEFERRED else CASES[ident][0], sep='\t')
        return 0
    selected = args.case or sorted(x for x in (all_ids if args.all else CASES) if x.startswith('P') == args.pipeline_budgets)
    unknown = set(selected) - all_ids
    if unknown:
        parser.error('Unknown case IDs: ' + ','.join(sorted(unknown)))
    if len(selected) != len(set(selected)):
        parser.error('Duplicate case IDs')
    if not args.execute_local:
        parser.error('No cluster operations performed. Pass --execute-local explicitly or use --list.')
    # Single local runner; concurrent suites must not delete each other's fixtures.
    lock_path = pathlib.Path(__file__).parent / '.run.lock'
    lock = lock_path.open('a')
    try:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        parser.error('Another e2e run owns the local fixture lock')
    c = Cluster()
    c.preflight()
    conf = c.get('cm', 'worker-type-test', 'volcano-system')['data']['volcano-scheduler.conf']
    require(all(x.startswith('P') for x in selected) or all(not x.startswith('P') for x in selected), 'Run legacy and pipeline modes separately')
    require(('pipelineBudgets: true' in conf) == all(x.startswith('P') for x in selected), 'Selected tests do not match scheduler pipeline budget mode')
    output = args.output or pathlib.Path(__file__).parent / 'evidence' / datetime.datetime.now().strftime('%Y%m%d-%H%M%S')
    output.mkdir(parents=True, exist_ok=False)
    old = c.get('deployment', 'volcano-scheduler', 'volcano-system')
    (output / 'old-scheduler-before.json').write_text(json.dumps(old, indent=2))
    rows = []
    for ident in selected:
        if ident in DEFERRED:
            rows.append({'id': ident, 'status': 'SKIP', 'detail': DEFERRED[ident]})
            write_reports(output, rows)
            continue
        f = Fixture(c, output / ident)
        row = {'id': ident, 'status': 'PASS'}
        started = False
        try:
            c.preflight()  # No leftovers or unresolved intents between cases.
            started = True
            f.setup()
            CASES[ident][1](f, ident)
            f.protected()
            require(not f.errors, str(f.errors))
            if ident in KNOWN:
                row.update(status='XPASS', detail='Known gap unexpectedly passed; review and promote test')
        except Exception as exc:
            # Only the exact product-behavior assertion is expected; setup/network failures are FAIL.
            expected = ident in KNOWN and isinstance(exc, AssertionError) and str(exc) == KNOWN[ident]
            row.update(status='XFAIL' if expected else 'FAIL', detail=str(exc), traceback=traceback.format_exc())
        finally:
            try:
                if started:
                    f.finish()
            except Exception as exc:
                row.update(status='FAIL', detail=row.get('detail', '') + '\nCleanup/evidence error: ' + str(exc))
        rows.append(row)
        print(ident, row['status'], row.get('detail', ''), flush=True)
        write_reports(output, rows)
        if row['status'] in ('FAIL', 'XPASS'):
            break  # Preserve evidence; do not cascade through an unhealthy fixture.
    new = c.get('deployment', 'volcano-scheduler', 'volcano-system')
    (output / 'old-scheduler-after.json').write_text(json.dumps(new, indent=2))
    if old['metadata']['uid'] != new['metadata']['uid'] or old['spec'] != new['spec']:
        rows.append({'id': 'OLD-SCHEDULER', 'status': 'FAIL', 'detail': 'Original local scheduler changed'})
    completed = {r['id'] for r in rows}
    for ident in selected:
        if ident not in completed:
            rows.append({'id': ident, 'status': 'SKIP', 'detail': 'Not run after earlier failure'})
    write_reports(output, rows)
    print('Evidence:', output)
    print('PASS excludes SKIP/XFAIL; synthetic GPU results do not prove physical GPU behavior.')
    return int(any(r['status'] != 'PASS' if args.require_all_pass else r['status'] in ('FAIL', 'XPASS') for r in rows))


if __name__ == '__main__':
    sys.exit(main())
