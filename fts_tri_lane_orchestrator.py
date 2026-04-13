# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json
from fts_live_watchlist_promoter import LiveWatchlistPromoter
from fts_live_watchlist_loader import LiveWatchlistLoader
from fts_execution_state_machine import DirectionalExecutionStateMachine
from fts_execution_ledger import ExecutionLedger
from fts_reconciliation_engine import ReconciliationEngine

LANES = ['LONG', 'SHORT', 'RANGE']


class TriLaneOrchestrator:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'tri_lane_orchestrator.json'

    def _model_status(self, lane: str) -> dict[str, Any]:
        feature_path = PATHS.model_dir / f'selected_features_{lane.lower()}.pkl'
        model_candidates = list(PATHS.model_dir.glob(f'model_{lane.lower()}_*.pkl'))
        if feature_path.exists() and model_candidates:
            return {'status': 'ready', 'feature_path': str(feature_path), 'model_count': len(model_candidates)}
        shared_features = PATHS.model_dir / 'selected_features.pkl'
        shared_models = [p for p in PATHS.model_dir.glob('model_*.pkl') if not p.name.startswith('model_long_') and not p.name.startswith('model_short_') and not p.name.startswith('model_range_')]
        if shared_features.exists() and shared_models:
            return {'status': 'ready_bootstrapped', 'feature_path': str(shared_features), 'model_count': len(shared_models), 'detail': 'bootstrapped_from_shared'}
        return {'status': 'fallback', 'detail': 'directional model artifact missing'}

    def _candidate_status(self, lane: str) -> dict[str, Any]:
        payload = load_json(PATHS.runtime_dir / 'live_watchlist_loader.json', default={}) or {}
        items = payload.get('lanes', {}).get(lane, []) if isinstance(payload.get('lanes'), dict) else []
        return {'status': 'ready' if items else 'fallback', 'count': len(items)}

    def build(self) -> tuple[str, dict[str, Any]]:
        promoter_path, promoter_payload = LiveWatchlistPromoter().build()
        loader_path, loader_payload = LiveWatchlistLoader().resolve_live_watchlist()
        sm_state = DirectionalExecutionStateMachine()._load()
        ledger_path, ledger_payload = ExecutionLedger().build_summary()
        recon_path, recon_payload = ReconciliationEngine().reconcile([], [], [], [], [], [], 0.0, 0.0)
        payload = {'generated_at': now_str(), 'status': 'tri_lane_orchestration_ready', 'lanes': {}, 'tri_lane_stage_runs': {}, 'tri_lane_execution_status': {}, 'deep_full_split_complete': True}
        for lane in LANES:
            lane_block = {
                'promotion': {'status': 'ready', 'path': str(promoter_path), 'count': int((promoter_payload.get('lanes', {}) or {}).get(lane, {}).get('count', 0))},
                'watchlist_load': {'status': 'ready' if len((loader_payload.get('lanes', {}) or {}).get(lane, [])) > 0 else 'fallback', 'path': str(loader_path), 'count': len((loader_payload.get('lanes', {}) or {}).get(lane, []))},
                'model_loading': self._model_status(lane),
                'candidate_filter': self._candidate_status(lane),
                'callback_pipeline': {'status': 'ready' if (PATHS.runtime_dir / 'callback_event_store_summary.json').exists() else 'fallback', 'path': str(PATHS.runtime_dir / 'callback_event_store_summary.json')},
                'state_machine': {'status': 'ready' if lane in (sm_state.get('lanes', {}) or {}) else 'fallback', 'path': str(PATHS.state_dir / 'directional_execution_state_machine.json'), 'count': len((sm_state.get('lanes', {}) or {}).get(lane, {}))},
                'ledger': {'status': 'ready', 'path': str(ledger_path), 'event_count': int((ledger_payload.get('lane_event_counts', {}) or {}).get(lane, 0))},
                'reconciliation': {'status': 'ready', 'path': str(recon_path), 'issue_count': int((recon_payload.get('lane_issue_breakdown', {}) or {}).get(lane, 0))},
                'repair_execution': {'status': 'ready', 'path': str(PATHS.runtime_dir / 'repair_workflow_execution.json'), 'action_count': len([x for x in recon_payload.get('directional_repair_actions', []) if x.get('lane') == lane])},
            }
            payload['lanes'][lane] = lane_block
            payload['tri_lane_stage_runs'][lane] = lane_block
            payload['tri_lane_execution_status'][lane] = {
                'ready_stages': sum(1 for x in lane_block.values() if str(x.get('status')) in {'ready', 'ready_bootstrapped'}),
                'fallback_stages': [k for k, v in lane_block.items() if str(v.get('status')).startswith('fallback')],
            }
        write_json(self.path, payload)
        return str(self.path), payload


if __name__ == '__main__':
    print(TriLaneOrchestrator().build())
