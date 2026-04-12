# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Callable

from fts_prelive_runtime import PATHS, now_str, load_json, write_json
from fts_live_watchlist_promoter import LiveWatchlistPromoter
from fts_live_watchlist_loader import LiveWatchlistLoader
from fts_execution_state_machine import DirectionalExecutionStateMachine
from fts_execution_ledger import ExecutionLedger
from fts_reconciliation_engine import ReconciliationEngine

LANES = ['LONG', 'SHORT', 'RANGE']
STAGES = ['promotion', 'watchlist_load', 'model_loading', 'candidate_filter', 'callback_pipeline', 'state_machine', 'ledger', 'reconciliation', 'repair_execution']

class TriLaneOrchestrator:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'tri_lane_orchestrator.json'

    def _model_status(self, lane: str) -> dict[str, Any]:
        feature_path = PATHS.model_dir / f'selected_features_{lane.lower()}.pkl'
        model_candidates = list(PATHS.model_dir.glob(f'model_{lane.lower()}_*.pkl'))
        if feature_path.exists() and model_candidates:
            return {'status': 'ready', 'feature_path': str(feature_path), 'model_count': len(model_candidates)}
        return {'status': 'fallback', 'detail': 'directional model artifact missing'}

    def _load_shared(self) -> dict[str, Any]:
        return {
            'loader': load_json(PATHS.runtime_dir / 'live_watchlist_loader.json', default={}) or {},
            'sm': DirectionalExecutionStateMachine()._load(),
            'ledger_summary': ExecutionLedger().build_summary()[1],
            'recon': ReconciliationEngine().reconcile([], [], [], [], [], [], 0.0, 0.0)[1],
        }

    def _run_promotion_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        payload = load_json(PATHS.runtime_dir / 'live_watchlist_promoter.json', default={}) or {}
        return {'status': 'ready' if payload else 'fallback', 'path': str(PATHS.runtime_dir / 'live_watchlist_promoter.json')}

    def _run_watchlist_load_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        items = shared['loader'].get('lanes', {}).get(lane, []) if isinstance(shared['loader'].get('lanes'), dict) else []
        return {'status': 'ready' if items else 'fallback', 'count': len(items), 'path': str(PATHS.runtime_dir / 'live_watchlist_loader.json')}

    def _run_model_loading_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        return self._model_status(lane)

    def _run_candidate_filter_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        items = shared['loader'].get('lanes', {}).get(lane, []) if isinstance(shared['loader'].get('lanes'), dict) else []
        return {'status': 'ready' if items else 'fallback', 'candidate_count': len(items)}

    def _run_callback_pipeline_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        p = PATHS.runtime_dir / 'callback_event_store_summary.json'
        return {'status': 'ready' if p.exists() else 'fallback', 'path': str(p)}

    def _run_state_machine_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        orders = shared['sm'].get('lanes', {}).get(lane, {})
        return {'status': 'ready' if orders else 'fallback', 'order_count': len(orders)}

    def _run_ledger_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        payload = shared['ledger_summary']
        return {'status': 'ready', 'event_count': int((payload.get('lane_event_counts', {}) or {}).get(lane, 0)), 'order_count': int((payload.get('lane_order_counts', {}) or {}).get(lane, 0))}

    def _run_reconciliation_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        recon = shared['recon']
        return {'status': 'ready', 'issue_count': int((recon.get('lane_issue_breakdown', {}) or {}).get(lane, 0))}

    def _run_repair_execution_lane(self, lane: str, shared: dict[str, Any]) -> dict[str, Any]:
        recon = shared['recon']
        actions = [x for x in recon.get('directional_repair_actions', []) if x.get('lane') == lane]
        return {'status': 'ready', 'action_count': len(actions)}

    def _runner_map(self) -> dict[str, dict[str, Callable[[str, dict[str, Any]], dict[str, Any]]]]:
        return {stage: {lane: getattr(self, f'_run_{stage}_lane') for lane in LANES} for stage in STAGES}

    def build(self) -> tuple[str, dict[str, Any]]:
        LiveWatchlistPromoter().build()
        LiveWatchlistLoader().resolve_live_watchlist()
        shared = self._load_shared()
        stage_runs = {lane: [] for lane in LANES}
        lanes_payload: dict[str, dict[str, Any]] = {lane: {} for lane in LANES}
        runner_map = self._runner_map()
        for stage in STAGES:
            for lane in LANES:
                lanes_payload[lane][stage] = runner_map[stage][lane](lane, shared)
                stage_runs[lane].append(stage)
        execution_status = {lane: {'ready_stages': sum(1 for _, v in lanes_payload[lane].items() if v.get('status') == 'ready'), 'fallback_stages': [k for k, v in lanes_payload[lane].items() if v.get('status') != 'ready']} for lane in LANES}
        payload = {'generated_at': now_str(), 'status': 'tri_lane_orchestrator_ready', 'stage_runs': stage_runs, 'lanes': lanes_payload, 'tri_lane_execution_status': execution_status}
        write_json(self.path, payload)
        return str(self.path), payload
