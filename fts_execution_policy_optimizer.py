# -*- coding: utf-8 -*-
"""Execution policy optimizer v3.

Generates execution_policy::default candidates for paper/shadow evaluation.
It never changes broker/live settings directly.
"""
from __future__ import annotations

import argparse
import json
import random
from pathlib import Path
from typing import Any

try:
    from fts_utils import now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')

from param_storage import save_candidate_params

REPORT_PATH = Path('runtime') / 'execution_policy_optimizer_report.json'

EXECUTION_POLICY_SPACE: dict[str, list[Any]] = {
    'EXECUTION_STYLE': ['TWAP3', 'LIMIT', 'BRACKET_TWAP3'],
    'TWAP3_CHILD_COUNT': [2, 3, 4],
    'EXECUTION_MIN_LIQUIDITY_SCORE': [35, 45, 55, 65],
    'EXECUTION_MIN_ADV20': [5000000, 10000000, 20000000],
    'EXECUTION_MAX_ADV20_PARTICIPATION': [0.005, 0.01, 0.02],
    'PAPER_BROKER_DEFAULT_SLIPPAGE_BPS': [5, 8, 10, 15],
    'PAPER_BROKER_PARTIAL_FILL_RATIO': [0.50, 0.70, 0.85, 1.00],
    'PAPER_BROKER_PARTIAL_FILL_THRESHOLD_VALUE': [300000, 500000, 1000000],
}


def _read_json(path: Path) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    except Exception:
        return {}
    return {}


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        if v in (None, ''):
            return default
        return float(v)
    except Exception:
        return default


def _sample_policy() -> dict[str, Any]:
    return {k: random.choice(v) for k, v in EXECUTION_POLICY_SPACE.items()}


def _runtime_health() -> dict[str, Any]:
    runtime = Path('runtime')
    ledger = _read_json(runtime / 'execution_ledger_summary.json')
    shadow = _read_json(runtime / 'shadow_runtime_evidence.json')
    liquidity = _read_json(runtime / 'price_liquidity_snapshot_summary.json')
    reject_rate = None
    if isinstance(shadow, dict):
        reject_rate = shadow.get('reject_rate')
    if reject_rate is None and isinstance(ledger, dict):
        reject_rate = ledger.get('reject_rate')
    return {
        'reject_rate': _safe_float(reject_rate, 0.0),
        'runtime_observed': bool(shadow.get('runtime_observed', False)) if isinstance(shadow, dict) else False,
        'liquidity_snapshot_available': bool(liquidity),
    }


def _score_policy(policy: dict[str, Any], health: dict[str, Any]) -> dict[str, Any]:
    slippage = _safe_float(policy.get('PAPER_BROKER_DEFAULT_SLIPPAGE_BPS'), 10.0)
    child_count = int(policy.get('TWAP3_CHILD_COUNT', 3) or 3)
    participation = _safe_float(policy.get('EXECUTION_MAX_ADV20_PARTICIPATION'), 0.01)
    liq_score = _safe_float(policy.get('EXECUTION_MIN_LIQUIDITY_SCORE'), 45.0)
    reject_rate = _safe_float(health.get('reject_rate'), 0.0)
    reasons: list[str] = []
    hard_pass = True
    if slippage > 25:
        hard_pass = False
        reasons.append('slippage_bps_too_high')
    if child_count < 1 or child_count > 6:
        hard_pass = False
        reasons.append('twap_child_count_out_of_range')
    if participation > 0.03:
        hard_pass = False
        reasons.append('adv20_participation_too_high')
    if liq_score < 25:
        hard_pass = False
        reasons.append('liquidity_gate_too_weak')
    if reject_rate > 0.20:
        hard_pass = False
        reasons.append('runtime_reject_rate_too_high')

    score = 100.0 * (
        0.20 * max(0.0, 1.0 - slippage / 25.0)
        + 0.20 * max(0.0, 1.0 - reject_rate / 0.20)
        + 0.15 * min(child_count / 3.0, 1.0)
        + 0.15 * max(0.0, 1.0 - participation / 0.03)
        + 0.15 * min(liq_score / 65.0, 1.0)
        + 0.10 * (1.0 if health.get('runtime_observed') else 0.6)
        + 0.05 * (1.0 if hard_pass else 0.0)
    )
    return {
        'ai_score': round(float(score), 3),
        'hard_gate_pass': bool(hard_pass),
        'reason': reasons or ['execution_policy_candidate_health_ok'],
        'runtime_health': health,
    }


def run_execution_policy_optimizer(iterations: int = 24, auto_judge: bool = True) -> dict[str, Any]:
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    health = _runtime_health()
    rows: list[dict[str, Any]] = []
    for _ in range(max(1, int(iterations))):
        p = _sample_policy()
        rows.append({'params': p, **_score_policy(p, health)})
    best = sorted(rows, key=lambda r: (bool(r.get('hard_gate_pass')), float(r.get('ai_score', 0.0))), reverse=True)[0]
    candidate = save_candidate_params(
        scope_name='execution_policy::default',
        best_params=best['params'],
        metrics={k: v for k, v in best.items() if k != 'params'},
        source_module='fts_execution_policy_optimizer.py',
        note='execution policy candidate; requires paper/shadow evidence before release',
    )
    judge_payload = None
    if auto_judge:
        try:
            from fts_candidate_ai_judge import judge_candidate_by_id  # type: ignore
            judge_payload = judge_candidate_by_id(candidate['candidate_id'])
        except Exception as exc:
            judge_payload = {'status': 'auto_judge_failed', 'error': repr(exc)}
    report = {
        'generated_at': now_str(),
        'status': 'execution_policy_candidate_generated',
        'scope': 'execution_policy::default',
        'candidate_id': candidate.get('candidate_id'),
        'best': best,
        'leaderboard': rows[:50],
        'auto_judge': judge_payload,
        'writes_production_config': False,
        'requires_paper_shadow_release_gate': True,
    }
    REPORT_PATH.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--iterations', type=int, default=24)
    parser.add_argument('--no-auto-judge', action='store_true')
    args = parser.parse_args(argv)
    payload = run_execution_policy_optimizer(args.iterations, auto_judge=not args.no_auto_judge)
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
