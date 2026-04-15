# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log, resolve_decision_csv, safe_float
from fts_watchlist_service import WatchlistService
from fts_market_climate_service import MarketClimateService
from fts_screening_engine import ScreeningEngine
from fts_signal_gate import evaluate_signal_gate


class DecisionDeskBuilder:
    MODULE_VERSION = "v99_decision_desk_builder_near_miss_upgrade"

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / "decision_desk_builder.json"
        self.output_path = PATHS.data_dir / "normalized_decision_output.csv"
        self.watchlist = WatchlistService()
        self.market = MarketClimateService()
        self.screen = ScreeningEngine()

    def _normalize_existing(self, path) -> pd.DataFrame:
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(path)
        rename_map = {}
        if "Ticker SYMBOL" in df.columns and "Ticker" not in df.columns:
            rename_map["Ticker SYMBOL"] = "Ticker"
        if "結構" in df.columns and "Structure" not in df.columns:
            rename_map["結構"] = "Structure"
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _infer_entry_state(self, row: dict[str, Any]) -> tuple[str, str, str, bool]:
        entry_state = str(row.get('Entry_State', '') or '').upper()
        early_state = str(row.get('Early_Path_State', '') or '').upper()
        confirm_state = str(row.get('Confirm_Path_State', '') or '').upper()
        pre = safe_float(row.get('PreEntry_Score', row.get('Entry_Readiness', 0.0)), 0.0)
        confirm = safe_float(row.get('Confirm_Entry_Score', row.get('AI_Proba', 0.0)), 0.0)
        fallback = False
        if entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY', 'NO_ENTRY'}:
            return entry_state, early_state or entry_state, confirm_state or ('WAIT_CONFIRM' if entry_state != 'FULL_ENTRY' else 'FULL_READY'), fallback
        if confirm >= 0.66:
            entry_state = 'FULL_ENTRY'
            early_state = early_state or 'PREPARE'
            confirm_state = confirm_state or 'FULL_READY'
            fallback = True
        elif pre >= 0.58 or safe_float(row.get('Entry_Readiness', 0.0), 0.0) >= 0.55:
            entry_state = 'PILOT_ENTRY' if confirm >= 0.52 else 'PREPARE'
            early_state = early_state or 'PREPARE'
            confirm_state = confirm_state or ('READY' if entry_state == 'PILOT_ENTRY' else 'WAIT_CONFIRM')
            fallback = True
        else:
            entry_state = 'NO_ENTRY'
            early_state = early_state or 'NO_ENTRY'
            confirm_state = confirm_state or 'WAIT_CONFIRM'
            fallback = True
        return entry_state, early_state, confirm_state, fallback

    def _infer_action(self, row: dict[str, Any], entry_state: str) -> str:
        raw = str(row.get('Action', row.get('Decision', row.get('Signal', ''))) or '').upper().strip()
        if raw in {'BUY', 'SELL', 'SHORT', 'COVER', 'HOLD'}:
            return raw
        direction_text = ' '.join(str(row.get(k, '')) for k in ['Direction', 'Golden_Type', 'Structure', 'Regime']).upper()
        is_short_bias = ('SHORT' in direction_text or '空' in direction_text)
        has_exit_intent = any(token in direction_text for token in ['SELL', 'EXIT', 'REDUCE', 'DEFEND', 'COVER'])
        if entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}:
            return 'SHORT' if is_short_bias else 'BUY'
        if entry_state == 'PREPARE':
            return 'HOLD'
        if has_exit_intent:
            return 'SELL'
        return 'HOLD'

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
            'Sample_Size': int(safe_float(result.get('Sample_Size', result.get('歷史訊號樣本數', 0)), 0)),
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
            'Target_Qty': int(safe_float(result.get('Target_Qty', 0), 0)),
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
        entry_state, early_state, confirm_state, inferred = self._infer_entry_state(row)
        row['Entry_State'] = entry_state
        row['Early_Path_State'] = early_state
        row['Confirm_Path_State'] = confirm_state
        row['EntryStateInferred'] = inferred
        row['Action'] = self._infer_action(row, entry_state)
        gate = evaluate_signal_gate(row)
        row['SignalGatePassed'] = bool(gate['passed'])
        row['SignalGateNote'] = gate['note']
        row['SignalGateBlockers'] = '|'.join(gate['blockers'])
        row['SignalGateWarnings'] = '|'.join(gate['warnings'])
        row['HeuristicRole'] = gate['heuristic_role']
        row['NearMissFlag'] = int((not gate['passed']) and entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
        return row

    def _enrich_existing(self, df: pd.DataFrame) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame()
        out = df.copy()
        for col, default in [('DeskUsable', True), ('ExecutionEligible', False), ('CanAutoSubmit', False), ('Health', 'KEEP'), ('FallbackBuild', False), ('Reference_Price', 0.0), ('Target_Qty', 0), ('Score', 0.0), ('AI_Proba', 0.0), ('Realized_EV', 0.0), ('Entry_Readiness', 0.0), ('PreEntry_Score', 0.0), ('Confirm_Entry_Score', 0.0), ('Breakout_Risk_Next3', 0.0), ('Reversal_Risk_Next3', 0.0), ('Exit_Hazard_Score', 0.0)]:
            if col not in out.columns:
                out[col] = default
        rows = []
        for _, series in out.iterrows():
            row = series.to_dict()
            entry_state, early_state, confirm_state, inferred = self._infer_entry_state(row)
            row['Entry_State'] = entry_state
            row['Early_Path_State'] = early_state
            row['Confirm_Path_State'] = confirm_state
            row['EntryStateInferred'] = inferred
            row['Action'] = self._infer_action(row, entry_state)
            if row['Action'] in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}:
                row['DeskUsable'] = bool(row.get('DeskUsable', True))
                row['ExecutionEligible'] = bool(row.get('ExecutionEligible', True))
            gate = evaluate_signal_gate(row)
            row['SignalGatePassed'] = bool(gate['passed'])
            row['SignalGateNote'] = gate['note']
            row['SignalGateBlockers'] = '|'.join(gate['blockers'])
            row['SignalGateWarnings'] = '|'.join(gate['warnings'])
            row['HeuristicRole'] = gate['heuristic_role']
            row['NearMissFlag'] = int((not gate['passed']) and entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
            row['RequiresReview'] = bool(row.get('RequiresReview', False) or (not gate['passed']))
            rows.append(row)
        return pd.DataFrame(rows)

    def build_decision_desk(self, limit: int = 12) -> pd.DataFrame:
        existing = resolve_decision_csv()
        df = pd.DataFrame()
        if existing.exists():
            df = self._enrich_existing(self._normalize_existing(existing))
        if df.empty or int(df.get('SignalGatePassed', pd.Series(dtype=bool)).fillna(False).sum()) <= 0:
            rows = []
            for ticker in self.watchlist.build_final_watchlist(limit=limit)[:limit]:
                result = self.screen.inspect_stock(ticker)
                if not result:
                    continue
                rows.append(self._fallback_row(ticker, result))
            if rows:
                fallback_df = pd.DataFrame(rows)
                df = pd.concat([df, fallback_df], ignore_index=True) if not df.empty else fallback_df
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
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
