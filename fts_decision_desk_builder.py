# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any

import pandas as pd

from config import PARAMS
from fts_config import PATHS
from fts_utils import now_str, log, resolve_decision_csv, safe_float
from fts_watchlist_service import WatchlistService
from fts_market_data_service import MarketDataService
from fts_market_climate_service import MarketClimateService
from fts_screening_engine import ScreeningEngine
from fts_signal_gate import evaluate_signal_gate


class DecisionDeskBuilder:
    MODULE_VERSION = "v102_decision_desk_builder_structural_bias_and_block_domain"

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / "decision_desk_builder.json"
        self.output_path = PATHS.data_dir / "normalized_decision_output.csv"
        self.watchlist = WatchlistService()
        self.market = MarketClimateService()
        self.market_data = MarketDataService()
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
        prepare_min = float(PARAMS.get('PREENTRY_PILOT_THRESHOLD', 0.52))
        full_min = float(PARAMS.get('CONFIRM_FULL_THRESHOLD', 0.60))
        readiness_min = float(PARAMS.get('ENTRY_READINESS_PREPARE_MIN', 0.40))
        pre = safe_float(row.get('PreEntry_Score', row.get('Entry_Readiness', 0.0)), 0.0)
        confirm = safe_float(row.get('Confirm_Entry_Score', row.get('AI_Proba', 0.0)), 0.0)
        signal_conf = safe_float(row.get('Signal_Confidence', row.get('訊號信心分數(%)', 0.0)), 0.0)
        if signal_conf > 1.5:
            signal_conf /= 100.0
        score = safe_float(row.get('Score', row.get('System_Score', 0.0)), 0.0)
        score_gap = abs(safe_float(row.get('Score_Gap', 0.0), 0.0))
        pre = max(pre, signal_conf if score_gap >= 1.0 else 0.0, score if score_gap >= 1.0 else 0.0)
        pilot_confirm_hint = max(confirm, signal_conf if score_gap >= 2.0 and score >= readiness_min else 0.0)
        fallback = False
        if entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY', 'NO_ENTRY'}:
            return entry_state, early_state or entry_state, confirm_state or ('WAIT_CONFIRM' if entry_state != 'FULL_ENTRY' else 'FULL_READY'), fallback
        if confirm >= full_min:
            entry_state = 'FULL_ENTRY'
            early_state = early_state or 'PREPARE'
            confirm_state = confirm_state or 'FULL_READY'
            fallback = True
        elif pre >= prepare_min or safe_float(row.get('Entry_Readiness', 0.0), 0.0) >= readiness_min:
            pilot_confirm = max(prepare_min - 0.02, 0.48)
            entry_state = 'PILOT_ENTRY' if pilot_confirm_hint >= pilot_confirm else 'PREPARE'
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

    def _structural_bias(self, row: dict[str, Any]) -> dict[str, Any]:
        long_bias = safe_float(row.get('Structural_Long_Bias', 0.0), 0.0)
        short_bias = safe_float(row.get('Structural_Short_Bias', 0.0), 0.0)
        range_bias = safe_float(row.get('Structural_Range_Bias', 0.0), 0.0)
        if long_bias >= max(short_bias, range_bias):
            label = 'LONG'
        elif short_bias >= max(long_bias, range_bias):
            label = 'SHORT'
        else:
            label = 'RANGE'
        return {
            'Structural_Long_Bias': round(long_bias, 4),
            'Structural_Short_Bias': round(short_bias, 4),
            'Structural_Range_Bias': round(range_bias, 4),
            'Structural_Regime_Bias': str(row.get('Structural_Regime_Bias', label) or label),
            'Structural_Bias_Label': label,
        }

    def _block_domain_summary(self, gate: dict[str, Any], row: dict[str, Any]) -> tuple[str, str, str]:
        strategy_blockers = [str(x) for x in gate.get('strategy_blockers', []) if str(x)]
        engineering_blockers = [str(x) for x in gate.get('engineering_blockers', []) if str(x)]
        if safe_float(row.get('Kelly_Pos', 0.0), 0.0) <= 0:
            strategy_blockers.append('kelly_non_positive')
        if safe_float(row.get('Expected_Return', row.get('Realized_EV', 0.0)), 0.0) < float(PARAMS.get('LIVE_MIN_EXPECTED_RETURN', -0.0015)):
            strategy_blockers.append('expected_return_soft_negative_or_low')
        if safe_float(row.get('Target_Qty', row.get('TargetQty', 0)), 0.0) <= 0:
            engineering_blockers.append('target_qty_missing_or_zero')
        if safe_float(row.get('Reference_Price', row.get('Close', row.get('Current_Close', 0.0))), 0.0) <= 0:
            engineering_blockers.append('reference_price_missing_or_zero')
        strategy_blockers = list(dict.fromkeys(strategy_blockers))
        engineering_blockers = list(dict.fromkeys(engineering_blockers))
        if strategy_blockers and engineering_blockers:
            domain = 'mixed'
        elif strategy_blockers:
            domain = 'strategy'
        elif engineering_blockers:
            domain = 'engineering'
        elif gate.get('warnings'):
            domain = 'warning_only'
        else:
            domain = 'clean'
        summary = '|'.join((strategy_blockers + engineering_blockers)[:8])
        return domain, '|'.join(strategy_blockers), '|'.join(engineering_blockers) if engineering_blockers else summary

    def _fetch_reference_price(self, ticker: str) -> float:
        ticker = str(ticker or '').strip().upper()
        for candidate in [PATHS.data_dir / 'manual_price_snapshot_overrides.csv', PATHS.data_dir / 'last_price_snapshot.csv', PATHS.base_dir / 'last_price_snapshot.csv']:
            if not candidate.exists():
                continue
            try:
                snap = pd.read_csv(candidate)
                for col in ['Ticker', 'Ticker SYMBOL', 'ticker', 'symbol']:
                    if col in snap.columns:
                        rows = snap[snap[col].astype(str).str.upper() == ticker]
                        if not rows.empty:
                            for pcol in ['Reference_Price', 'Close', 'Last', 'Price']:
                                if pcol in rows.columns:
                                    price = safe_float(rows.iloc[0].get(pcol), 0.0)
                                    if price > 0:
                                        return price
            except Exception:
                continue
        try:
            df = self.market_data.smart_download(ticker, period='6mo')
            if df is not None and not df.empty and 'Close' in df.columns:
                return safe_float(df['Close'].dropna().iloc[-1], 0.0)
        except Exception:
            return 0.0
        return 0.0

    def _synthesize_execution_fields(self, row: dict[str, Any], gate: dict[str, Any]) -> dict[str, Any]:
        action = str(row.get('Action', 'HOLD') or 'HOLD').upper()
        entry_state = str(row.get('Entry_State', 'NO_ENTRY') or 'NO_ENTRY').upper()
        ref_price = safe_float(row.get('Reference_Price', row.get('Close', row.get('Current_Close', 0.0))), 0.0)
        if ref_price <= 0 and row.get('Ticker'):
            ref_price = self._fetch_reference_price(str(row.get('Ticker')))
        qty = int(safe_float(row.get('Target_Qty', row.get('TargetQty', 0)), 0))
        active_qty = int(safe_float(row.get('Active_Position_Qty', row.get('Position_Qty', row.get('持倉張數', 0))), 0))
        fallback_exec = bool(PARAMS.get('FALLBACK_DECISION_ALLOW_PAPER_EXECUTION', True))
        if action in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'} and ref_price > 0 and qty <= 0 and fallback_exec:
            qty = int(PARAMS.get('FALLBACK_DECISION_DEFAULT_TARGET_QTY', PARAMS.get('PAPER_DEFAULT_TARGET_QTY', 1000)))
            row['TargetQtySynthesized'] = True
            row['Target_Qty_Source'] = 'decision_desk_synthetic_paper_qty'
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
        row['ExecutionEligible'] = bool(row.get('ExecutionEligible', executable) or executable)
        row['CanAutoSubmit'] = bool(row.get('CanAutoSubmit', executable) or executable)
        row['RequiresReview'] = bool(row.get('RequiresReview', False) or (not executable and action in {'BUY', 'SHORT'} and entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}))
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
        row = self._synthesize_execution_fields(row, {'passed': entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}})
        gate = evaluate_signal_gate(row)
        row['SignalGatePassed'] = bool(gate['passed'])
        row['SignalGateNote'] = gate['note']
        row['SignalGateBlockers'] = '|'.join(gate['blockers'])
        row['SignalGateWarnings'] = '|'.join(gate['warnings'])
        row['HeuristicRole'] = gate['heuristic_role']
        row['NearMissFlag'] = int((not gate['passed']) and entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
        row.update(self._structural_bias(row))
        primary_domain, strategy_blockers, engineering_blockers = self._block_domain_summary(gate, row)
        row['PrimaryBlockDomain'] = primary_domain
        row['StrategyBlockers'] = strategy_blockers
        row['EngineeringBlockers'] = engineering_blockers
        row = self._synthesize_execution_fields(row, gate)
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
            row = self._synthesize_execution_fields(row, {'passed': entry_state in {'PILOT_ENTRY', 'FULL_ENTRY'}})
            gate = evaluate_signal_gate(row)
            row['SignalGatePassed'] = bool(gate['passed'])
            row['SignalGateNote'] = gate['note']
            row['SignalGateBlockers'] = '|'.join(gate['blockers'])
            row['SignalGateWarnings'] = '|'.join(gate['warnings'])
            row['HeuristicRole'] = gate['heuristic_role']
            row['NearMissFlag'] = int((not gate['passed']) and entry_state in {'PREPARE', 'PILOT_ENTRY', 'FULL_ENTRY'})
            row['RequiresReview'] = bool(row.get('RequiresReview', False) or (not gate['passed']))
            row = self._synthesize_execution_fields(row, gate)
            rows.append(row)
        return pd.DataFrame(rows)

    def build_decision_desk(self, limit: int = 12) -> pd.DataFrame:
        candidates = [resolve_decision_csv(), PATHS.base_dir / 'daily_decision_desk_prerisk.csv', PATHS.data_dir / 'daily_decision_desk_prerisk.csv']
        df = pd.DataFrame()
        for existing in candidates:
            if existing.exists():
                df = self._enrich_existing(self._normalize_existing(existing))
                if not df.empty:
                    break
        executable_rows = int(((df.get('ExecutionEligible', pd.Series(dtype=bool)).fillna(False)) & (df.get('Reference_Price', pd.Series(dtype=float)).fillna(0) > 0) & (pd.to_numeric(df.get('Target_Qty', pd.Series(dtype=float)), errors='coerce').fillna(0) > 0)).sum()) if not df.empty else 0
        if df.empty or int(df.get('SignalGatePassed', pd.Series(dtype=bool)).fillna(False).sum()) <= 0 or executable_rows <= 0:
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
        strategy_blocked = int((desk.get('PrimaryBlockDomain', pd.Series(dtype=str)).astype(str).isin(['strategy', 'mixed'])).sum()) if not desk.empty else 0
        engineering_blocked = int((desk.get('PrimaryBlockDomain', pd.Series(dtype=str)).astype(str).isin(['engineering', 'mixed'])).sum()) if not desk.empty else 0
        structural_bias_counts = desk.get('Structural_Bias_Label', pd.Series(dtype=str)).astype(str).value_counts().to_dict() if not desk.empty and 'Structural_Bias_Label' in desk.columns else {}
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'output_path': str(self.output_path),
            'row_count': int(len(desk)),
            'fallback_row_count': fallback_rows,
            'usable_row_count': usable_rows,
            'signal_gate_passed_count': signal_gate_passed,
            'near_miss_count': near_miss,
            'strategy_blocked_count': strategy_blocked,
            'engineering_blocked_count': engineering_blocked,
            'structural_bias_counts': structural_bias_counts,
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
