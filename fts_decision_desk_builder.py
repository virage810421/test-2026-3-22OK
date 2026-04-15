# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from config import PARAMS
from fts_config import PATHS
from fts_utils import now_str, log, safe_float
from fts_watchlist_service import WatchlistService
from fts_market_data_service import MarketDataService
from fts_market_climate_service import MarketClimateService
from fts_screening_engine import ScreeningEngine
from fts_signal_gate import evaluate_signal_gate
from fts_price_snapshot_auto_builder import AutoPriceSnapshotBuilder

_ALLOWED_ENTRY_STATES = {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY', 'NO_ENTRY'}


def _sf(value: Any, default: float = 0.0) -> float:
    try:
        if pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def _normalize_signal_conf(value: Any, fallback: float = 0.0) -> float:
    conf = _sf(value, fallback)
    if conf > 1.5:
        conf = conf / 100.0
    return conf


def infer_entry_state_from_row(row: dict[str, Any], params: dict[str, Any] | None = None) -> tuple[str, str, str, bool, dict[str, float]]:
    params = params or PARAMS
    stage = str(row.get('Entry_State') or row.get('Entry_Action') or '').strip().upper()
    early_state = str(row.get('Early_Path_State') or '').strip().upper()
    confirm_state = str(row.get('Confirm_Path_State') or '').strip().upper()
    if stage in _ALLOWED_ENTRY_STATES:
        metrics = {
            'ai_proba': _sf(row.get('AI_Proba', 0.0), 0.0),
            'signal_confidence': _normalize_signal_conf(row.get('Signal_Confidence', row.get('SignalConfidence', row.get('訊號信心分數(%)', 0.0))), 0.0),
            'score': _sf(row.get('Score', row.get('System_Score', 0.0)), 0.0),
            'score_gap': abs(_sf(row.get('Score_Gap', 0.0), 0.0)),
            'entry_readiness': _sf(row.get('Entry_Readiness', 0.0), 0.0),
            'preentry_score': _sf(row.get('PreEntry_Score', row.get('Entry_Readiness', 0.0)), 0.0),
            'confirm_score': _sf(row.get('Confirm_Entry_Score', row.get('AI_Proba', 0.0)), 0.0),
        }
        if not early_state:
            early_state = 'PREPARE' if stage in {'PILOT_ENTRY', 'FULL_ENTRY'} else stage
        if not confirm_state:
            confirm_state = 'FULL_READY' if stage == 'FULL_ENTRY' else ('READY' if stage == 'PILOT_ENTRY' else 'WAIT_CONFIRM')
        return stage, early_state, confirm_state, False, metrics

    ai_proba = _sf(row.get('AI_Proba', 0.0), 0.0)
    signal_conf = _normalize_signal_conf(row.get('Signal_Confidence', row.get('SignalConfidence', row.get('訊號信心分數(%)', ai_proba))), ai_proba)
    score = _sf(row.get('Score', row.get('System_Score', 0.0)), 0.0)
    score_gap = abs(_sf(row.get('Score_Gap', 0.0), 0.0))
    entry_readiness = max(
        _sf(row.get('Entry_Readiness', 0.0), 0.0),
        signal_conf if score_gap >= 1.0 else 0.0,
        score if score_gap >= 1.0 else 0.0,
    )
    preentry_score = max(_sf(row.get('PreEntry_Score', 0.0), 0.0), entry_readiness)
    confirm_score = max(_sf(row.get('Confirm_Entry_Score', 0.0), 0.0), ai_proba)

    prepare_min = float(params.get('PREENTRY_PILOT_THRESHOLD', 0.52))
    full_min = float(params.get('CONFIRM_FULL_THRESHOLD', 0.60))
    readiness_min = float(params.get('ENTRY_READINESS_PREPARE_MIN', 0.40))
    long_min_proba = float(params.get('LONG_MIN_PROBA', 0.52))
    long_min_conf = float(params.get('LONG_MIN_CONFIDENCE', 0.50))
    pilot_ai_min = max(0.45, long_min_proba - float(params.get('PILOT_MIN_PROBA_BUFFER', 0.06)))
    pilot_conf_min = max(0.40, long_min_conf - float(params.get('PILOT_MIN_CONF_BUFFER', 0.06)))
    full_ai_min = float(params.get('FULL_ENTRY_MIN_AI_PROBA', max(0.50, full_min - 0.08)))
    full_score_gap_min = float(params.get('FULL_ENTRY_MIN_SCORE_GAP', 2.0))

    inferred = True
    if confirm_score >= full_min and ai_proba >= full_ai_min and signal_conf >= readiness_min and score_gap >= full_score_gap_min:
        stage = 'FULL_ENTRY'
        early_state = early_state or 'PREPARE'
        confirm_state = confirm_state or 'FULL_READY'
    elif preentry_score >= prepare_min and (ai_proba >= pilot_ai_min or signal_conf >= pilot_conf_min or score_gap >= 2.0):
        stage = 'PILOT_ENTRY'
        early_state = early_state or 'PREPARE'
        confirm_state = confirm_state or 'READY'
    elif entry_readiness >= readiness_min or signal_conf >= readiness_min or score_gap >= 1.0:
        stage = 'PREPARE'
        early_state = early_state or 'PREPARE'
        confirm_state = confirm_state or 'WAIT_CONFIRM'
    else:
        stage = 'NO_ENTRY'
        early_state = early_state or 'NO_ENTRY'
        confirm_state = confirm_state or 'WAIT_CONFIRM'

    metrics = {
        'ai_proba': ai_proba,
        'signal_confidence': signal_conf,
        'score': score,
        'score_gap': score_gap,
        'entry_readiness': entry_readiness,
        'preentry_score': preentry_score,
        'confirm_score': confirm_score,
    }
    return stage, early_state, confirm_state, inferred, metrics


class DecisionDeskBuilder:
    MODULE_VERSION = "v102_decision_desk_builder_prerisk_contract_fixed"

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / "decision_desk_builder.json"
        self.output_path = PATHS.data_dir / "normalized_decision_output.csv"
        self.enriched_path = PATHS.data_dir / "normalized_decision_output_enriched.csv"
        self.watchlist = WatchlistService()
        self.market = MarketClimateService()
        self.market_data = MarketDataService()
        self.screen = ScreeningEngine()
        self.price_builder = AutoPriceSnapshotBuilder()

    def _normalize_existing(self, path) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(path)
        rename_map = {}
        if 'Ticker SYMBOL' in df.columns and 'Ticker' not in df.columns:
            rename_map['Ticker SYMBOL'] = 'Ticker'
        if '結構' in df.columns and 'Structure' not in df.columns:
            rename_map['結構'] = 'Structure'
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _upstream_candidates(self) -> list:
        return [
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
            PATHS.base_dir / 'daily_decision_desk_prerisk.csv',
            PATHS.data_dir / 'daily_decision_desk_prerisk.csv',
        ]

    def _load_upstream(self) -> tuple[pd.DataFrame, str]:
        for path in self._upstream_candidates():
            if not path.exists():
                continue
            try:
                df = self._normalize_existing(path)
            except Exception:
                continue
            if df is not None and not df.empty:
                return df, path.name
        return pd.DataFrame(), 'none'

    def _default_target_qty(self, price: float, stage: str) -> int:
        if price <= 0:
            return 0
        allow_odd = bool(PARAMS.get('AUTO_DECISION_ALLOW_ODD_LOT_PAPER', True))
        pilot_notional = float(PARAMS.get('AUTO_DECISION_TARGET_NOTIONAL_PILOT', 120000.0))
        full_notional = float(PARAMS.get('AUTO_DECISION_TARGET_NOTIONAL_FULL', 250000.0))
        budget = full_notional if stage == 'FULL_ENTRY' else pilot_notional
        if allow_odd:
            return max(1, int(budget // max(price, 1.0)))
        lot = 1000
        lots = max(1, int(budget // max(price * lot, 1)))
        return lots * lot

    def _infer_action(self, row: dict[str, Any], entry_state: str) -> str:
        raw = str(row.get('Action', row.get('Decision', row.get('Signal', ''))) or '').upper().strip()
        if raw in {'BUY', 'SELL', 'SHORT', 'COVER', 'HOLD'}:
            return raw
        direction_text = ' '.join(str(row.get(k, '')) for k in ['Direction', 'Golden_Type', 'Structure', 'Regime', 'Exit_State', 'Exit_Action', 'Reason']).upper()
        is_short_bias = ('SHORT' in direction_text or '空' in direction_text)
        has_exit_intent = any(token in direction_text for token in ['SELL', 'EXIT', 'REDUCE', 'DEFEND', 'COVER'])
        if entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}:
            return 'SHORT' if is_short_bias else 'BUY'
        if entry_state == 'PREPARE':
            return 'HOLD'
        if has_exit_intent:
            return 'SELL'
        return 'HOLD'

    def _fetch_reference_price(self, ticker: str) -> float:
        ticker = str(ticker or '').strip().upper()
        if not ticker:
            return 0.0
        try:
            price, _ = self.market_data.get_latest_reference_price(ticker, allow_online=True, period='6mo')
            return safe_float(price, 0.0)
        except Exception:
            return 0.0

    def _synthesize_execution_fields(self, row: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
        action = str(row.get('Action', 'HOLD') or 'HOLD').upper()
        entry_state = str(row.get('Entry_State', 'NO_ENTRY') or 'NO_ENTRY').upper()
        ref_price = safe_float(row.get('Reference_Price', row.get('Close', row.get('Current_Close', 0.0))), 0.0)
        if ref_price <= 0 and row.get('Ticker'):
            ref_price = self._fetch_reference_price(str(row.get('Ticker')))
        qty = int(_sf(row.get('Target_Qty', row.get('TargetQty', 0)), 0))
        active_qty = int(_sf(row.get('Active_Position_Qty', row.get('Position_Qty', row.get('持倉張數', 0))), 0))
        fallback_exec = bool(PARAMS.get('FALLBACK_DECISION_ALLOW_PAPER_EXECUTION', True))
        if action in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'} and ref_price > 0 and qty <= 0 and fallback_exec:
            qty = self._default_target_qty(ref_price, entry_state)
            if qty > 0:
                row['TargetQtySynthesized'] = True
                row['Target_Qty_Source'] = 'decision_desk_synthetic_default_qty'
        if action in {'SELL', 'COVER'} and qty <= 0 and active_qty > 0:
            qty = active_qty
            row['Target_Qty_Source'] = 'active_position_fallback'
        row['Reference_Price'] = ref_price
        row['Target_Qty'] = int(max(qty, 0))
        executable = False
        if action in {'BUY', 'SHORT'}:
            executable = bool(gate.get('passed')) and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'} and ref_price > 0 and row['Target_Qty'] > 0
            if row.get('FallbackBuild', False):
                executable = executable and fallback_exec
        elif action in {'SELL', 'COVER'}:
            executable = ref_price > 0 and row['Target_Qty'] > 0
        row['DeskUsable'] = bool(row.get('DeskUsable', True))
        row['ExecutionEligible'] = bool(executable)
        row['CanAutoSubmit'] = bool(executable)
        row['RequiresReview'] = bool(row.get('RequiresReview', False) or (not executable and action in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}))
        return row

    def _apply_entry_contract(self, row: dict[str, Any]) -> dict[str, Any]:
        entry_state, early_state, confirm_state, inferred, metrics = infer_entry_state_from_row(row)
        row['Entry_State'] = entry_state
        row['Early_Path_State'] = early_state
        row['Confirm_Path_State'] = confirm_state
        row['EntryStateInferred'] = inferred
        row['Entry_Readiness'] = max(_sf(row.get('Entry_Readiness', 0.0), 0.0), metrics['entry_readiness'])
        row['PreEntry_Score'] = max(_sf(row.get('PreEntry_Score', 0.0), 0.0), metrics['preentry_score'])
        row['Confirm_Entry_Score'] = max(_sf(row.get('Confirm_Entry_Score', 0.0), 0.0), metrics['confirm_score'])
        row.setdefault('Score', metrics['score'])
        row.setdefault('Signal_Confidence', metrics['signal_confidence'])
        row['Action'] = self._infer_action(row, entry_state)
        return row

    def _fallback_row(self, ticker: str, result: dict[str, Any]) -> dict[str, Any]:
        direction = str(result.get('Golden_Type', result.get('Direction', 'LONG')) or 'LONG')
        row = {
            'Ticker': ticker,
            'Structure': result.get('Structure', 'AI訊號'),
            'Regime': result.get('Regime', '區間盤整'),
            'Direction': direction,
            'Golden_Type': direction,
            'AI_Proba': safe_float(result.get('AI_Proba', 0.5), 0.5),
            'Realized_EV': safe_float(result.get('Realized_EV', result.get('Expected_Return', 0.0)), 0.0),
            'Expected_Return': safe_float(result.get('Expected_Return', result.get('Realized_EV', 0.0)), 0.0),
            'Sample_Size': int(_sf(result.get('Sample_Size', result.get('歷史訊號樣本數', 0)), 0)),
            'Weighted_Buy_Score': safe_float(result.get('Weighted_Buy_Score', result.get('Buy_Score', 0.0)), 0.0),
            'Weighted_Sell_Score': safe_float(result.get('Weighted_Sell_Score', result.get('Sell_Score', 0.0)), 0.0),
            'Score_Gap': safe_float(result.get('Score_Gap', 0.0), 0.0),
            'Kelly_Pos': safe_float(result.get('Kelly_Pos', 0.0), 0.0),
            'Entry_Readiness': safe_float(result.get('Entry_Readiness', 0.0), 0.0),
            'PreEntry_Score': safe_float(result.get('PreEntry_Score', result.get('Entry_Readiness', 0.0)), 0.0),
            'Confirm_Entry_Score': safe_float(result.get('Confirm_Entry_Score', result.get('AI_Proba', 0.0)), 0.0),
            'Breakout_Risk_Next3': safe_float(result.get('Breakout_Risk_Next3', 0.0), 0.0),
            'Reversal_Risk_Next3': safe_float(result.get('Reversal_Risk_Next3', 0.0), 0.0),
            'Exit_Hazard_Score': safe_float(result.get('Exit_Hazard_Score', 0.0), 0.0),
            'Transition_Label': str(result.get('Transition_Label', 'Stable') or 'Stable'),
            'Hysteresis_Regime_Label': str(result.get('Hysteresis_Regime_Label', result.get('Regime', '區間盤整')) or '區間盤整'),
            'Reference_Price': safe_float(result.get('Reference_Price', result.get('Close', 0.0)), 0.0),
            'Target_Qty': int(_sf(result.get('Target_Qty', 0), 0)),
            'Health': str(result.get('Health', 'REVIEW_REQUIRED') or 'REVIEW_REQUIRED'),
            'DecisionSource': 'fallback_watchlist_build',
            'RequiresReview': True,
            'FallbackBuild': True,
            'CanAutoSubmit': False,
            'DeskUsable': True,
            'ExecutionEligible': False,
            'DeskIntegrity': 'fallback_watchlist_rebuilt',
            'ReviewSeverity': 'REVIEW',
            'FallbackReason': 'upstream_decision_csv_missing_empty_or_unusable',
            'UpstreamHealthy': False,
        }
        row = self._apply_entry_contract(row)
        gate = evaluate_signal_gate(row)
        row['SignalGatePassed'] = bool(gate['passed'])
        row['SignalGateNote'] = gate['note']
        row['SignalGateBlockers'] = '|'.join(gate['blockers'])
        row['SignalGateWarnings'] = '|'.join(gate['warnings'])
        row['HeuristicRole'] = gate['heuristic_role']
        row['NearMissFlag'] = int((not gate['passed']) and row['Entry_State'] in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
        row = self._synthesize_execution_fields(row, gate)
        return row

    def _enrich_existing(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        for col, default in [
            ('DeskUsable', True), ('ExecutionEligible', False), ('CanAutoSubmit', False), ('Health', 'KEEP'),
            ('FallbackBuild', False), ('Reference_Price', 0.0), ('Target_Qty', 0), ('Score', 0.0),
            ('AI_Proba', 0.0), ('Realized_EV', 0.0), ('Entry_Readiness', 0.0), ('PreEntry_Score', 0.0),
            ('Confirm_Entry_Score', 0.0), ('Breakout_Risk_Next3', 0.0), ('Reversal_Risk_Next3', 0.0),
            ('Exit_Hazard_Score', 0.0), ('Signal_Confidence', 0.0), ('Score_Gap', 0.0),
        ]:
            if col not in out.columns:
                out[col] = default
        tickers = out.get('Ticker', pd.Series(dtype=str)).astype(str).tolist() if 'Ticker' in out.columns else []
        snapshot_path, snapshot_payload, snapshot_map = self.price_builder.build(tickers)
        rows = []
        for _, series in out.iterrows():
            row = series.to_dict()
            ticker = str(row.get('Ticker', '')).strip().upper()
            row = self._apply_entry_contract(row)
            if safe_float(row.get('Reference_Price', 0.0), 0.0) <= 0 and ticker:
                row['Reference_Price'] = safe_float(snapshot_map.get(ticker, 0.0), 0.0)
            gate = evaluate_signal_gate(row)
            row['SignalGatePassed'] = bool(gate['passed'])
            row['SignalGateNote'] = gate['note']
            row['SignalGateBlockers'] = '|'.join(gate['blockers'])
            row['SignalGateWarnings'] = '|'.join(gate['warnings'])
            row['HeuristicRole'] = gate['heuristic_role']
            row['NearMissFlag'] = int((not gate['passed']) and row['Entry_State'] in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
            row = self._synthesize_execution_fields(row, gate)
            row['PriceSnapshotPath'] = str(snapshot_path)
            row['PriceAutoStatus'] = str(snapshot_payload.get('status', 'unknown'))
            row['UpstreamSource'] = row.get('UpstreamSource', 'existing_csv')
            rows.append(row)
        return pd.DataFrame(rows)

    def build_decision_desk(self, limit: int = 12) -> pd.DataFrame:
        upstream_df, upstream_source = self._load_upstream()
        df = self._enrich_existing(upstream_df) if upstream_df is not None and not upstream_df.empty else pd.DataFrame()
        executable_rows = int(((df.get('ExecutionEligible', pd.Series(dtype=bool)).fillna(False)) & (df.get('Reference_Price', pd.Series(dtype=float)).fillna(0) > 0) & (pd.to_numeric(df.get('Target_Qty', pd.Series(dtype=float)), errors='coerce').fillna(0) > 0)).sum()) if not df.empty else 0
        if not df.empty:
            df['UpstreamSource'] = upstream_source
        if df.empty or int(df.get('SignalGatePassed', pd.Series(dtype=bool)).fillna(False).sum()) <= 0 or executable_rows <= 0:
            rows = []
            for ticker in self.watchlist.build_final_watchlist(limit=limit)[:limit]:
                result = self.screen.inspect_stock(ticker)
                if not result:
                    continue
                rows.append(self._fallback_row(ticker, result))
            if rows:
                fallback_df = pd.DataFrame(rows)
                fallback_df['UpstreamSource'] = 'fallback_watchlist_build'
                df = pd.concat([df, fallback_df], ignore_index=True) if not df.empty else fallback_df
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
            df.to_csv(self.enriched_path, index=False, encoding='utf-8-sig')
        except Exception:
            pass
        return df

    def build_summary(self) -> tuple[Any, dict[str, Any]]:
        desk = self.build_decision_desk()
        climate = self.market.analyze_market_climate()
        fallback_rows = int((desk.get('FallbackBuild', pd.Series(dtype=bool)).fillna(False)).sum()) if not desk.empty else 0
        usable_rows = int((desk.get('DeskUsable', pd.Series(dtype=bool)).fillna(False)).sum()) if not desk.empty else 0
        signal_gate_passed = int((desk.get('SignalGatePassed', pd.Series(dtype=bool)).fillna(False)).sum()) if not desk.empty else 0
        near_miss = int((desk.get('NearMissFlag', pd.Series(dtype=int)).fillna(0).astype(int) > 0).sum()) if not desk.empty else 0
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'output_path': str(self.output_path),
            'enriched_output_path': str(self.enriched_path),
            'row_count': int(len(desk)),
            'fallback_row_count': fallback_rows,
            'usable_row_count': usable_rows,
            'signal_gate_passed_count': signal_gate_passed,
            'near_miss_count': near_miss,
            'market_climate': climate,
            'status': 'decision_desk_blocked_fallback_only' if fallback_rows > 0 and usable_rows == 0 else ('decision_desk_degraded_mixed' if fallback_rows > 0 else 'decision_desk_ready'),
            'fallback_policy': {
                'kelly_default': 0.0,
                'health_default': 'FALLBACK_BUILD',
                'auto_submit_allowed': False,
                'execution_eligible_default': False,
                'desk_usable_default': True,
            },
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧭 decision desk builder ready: {self.runtime_path}')
        return self.runtime_path, payload
