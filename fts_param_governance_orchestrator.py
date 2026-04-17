# -*- coding: utf-8 -*-
"""Parameter governance orchestrator v3.

Single admin/control entry for:
- candidate AI judgement by scope
- paper/shadow evidence refresh and release-field writeback
- release-gate check by scope
- approved param mount summary

It does not promote live unless release gate is explicitly forced/configured.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')

DEFAULT_SCOPES = [
    'trainer::default',
    'label_policy::default',
    'strategy_signal::default',
    'execution_policy::default',
]

REPORT_PATH = Path('runtime') / 'param_governance_orchestrator.json'


def run_param_governance(
    scopes: list[str] | None = None,
    refresh_evidence: bool = True,
    run_release_gate: bool = False,
    force_release: bool = False,
) -> dict[str, Any]:
    scopes = scopes or list(DEFAULT_SCOPES)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []

    for scope in scopes:
        item: dict[str, Any] = {'scope': scope}
        try:
            from fts_candidate_ai_judge import judge_latest
            item['ai_judge'] = judge_latest(scope=scope, auto_apply=True)
        except Exception as exc:
            item['ai_judge'] = {'status': 'error', 'error': repr(exc)}

        if refresh_evidence:
            try:
                from fts_param_evidence_collector import collect_runtime_evidence
                item['evidence'] = collect_runtime_evidence(scope=scope, write_back=True)
            except Exception as exc:
                item['evidence'] = {'status': 'error', 'error': repr(exc)}

        if run_release_gate and scope in {'strategy_signal::default', 'execution_policy::default'}:
            try:
                from fts_param_release_gate import run_release_gate as _run_release_gate
                item['release_gate'] = _run_release_gate(scope=scope, force=force_release, refresh_evidence=False)
            except Exception as exc:
                item['release_gate'] = {'status': 'error', 'error': repr(exc)}
        results.append(item)

    try:
        from fts_approved_param_mount import build_mount_summary
        mount_summary = build_mount_summary()
    except Exception as exc:
        mount_summary = {'status': 'error', 'error': repr(exc)}

    payload = {
        'generated_at': now_str(),
        'status': 'param_governance_orchestrator_ready_v3',
        'scopes': scopes,
        'refresh_evidence': bool(refresh_evidence),
        'run_release_gate': bool(run_release_gate),
        'force_release': bool(force_release),
        'results': results,
        'mount_summary': mount_summary,
        'live_auto_promotion_note': 'live promotion remains blocked unless release gate and explicit config allow it',
    }
    REPORT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--scope', action='append', default=[], help='scope to process; can be repeated')
    parser.add_argument('--all-scopes', action='store_true')
    parser.add_argument('--no-refresh-evidence', action='store_true')
    parser.add_argument('--release-gate', action='store_true')
    parser.add_argument('--force-release', action='store_true')
    args = parser.parse_args(argv)
    scopes = list(DEFAULT_SCOPES) if args.all_scopes or not args.scope else list(args.scope)
    payload = run_param_governance(
        scopes=scopes,
        refresh_evidence=not args.no_refresh_evidence,
        run_release_gate=bool(args.release_gate),
        force_release=bool(args.force_release),
    )
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
