# -*- coding: utf-8 -*-
from __future__ import annotations

"""持倉生命週期服務 v94.

分層 lifecycle 版本：
- 區分 PREPARE / PILOT_ENTRY / FULL_ENTRY。
- 區分 LONG / SHORT。
- 區分 trend / range / neutral。
- 高波動部位自動放寬停損與 stale 判斷，避免被過早 DEFEND。
- action plan 依 profile 輸出不同 reduce fraction / 保護停損邏輯。
- v2：stage / regime 來源顯性化，避免猜不到就默認正式倉。
- v2：SHORT 出場語意改為 COVER，維持 long/short 閉環一致。
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_execution_journal_service import append_execution_journal_event


class PositionLifecycleService:
    MODULE_VERSION = 'v94_position_lifecycle_layered_profiles_v2'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'position_lifecycle.json'
        self.csv_path = PATHS.runtime_dir / 'position_lifecycle_snapshot.csv'
        self.action_plan_path = PATHS.runtime_dir / 'position_lifecycle_action_plan.json'
        self.stop_payload_csv_path = PATHS.data_dir / 'stop_replace_payloads.csv'
        self.stop_payload_runtime_path = PATHS.runtime_dir / 'stop_replace_payloads.csv'
        self.history_path = PATHS.runtime_dir / 'position_lifecycle_history.jsonl'

    def _read_csv_first(self, candidates: list[Path]) -> tuple[Path | None, pd.DataFrame]:
        for p in candidates:
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        return p, df
                except Exception:
                    continue
        return None, pd.DataFrame()

    def _load_positions(self) -> tuple[Path | None, pd.DataFrame]:
        return self._read_csv_first([
            PATHS.data_dir / 'active_positions.csv',
            PATHS.runtime_dir / 'active_positions.csv',
            PATHS.data_dir / getattr(CONFIG, 'active_positions_csv_filename', 'active_positions.csv'),
            PATHS.base_dir / 'active_positions.csv',
        ])

    def _load_decisions(self) -> pd.DataFrame:
        _, df = self._read_csv_first([
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
        ])
        return df

    def _load_prev(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                return data.get('positions', {}) or {}
        except Exception:
            return {}
        return {}

    @staticmethod
    def _ticker(row: pd.Series) -> str:
        return str(row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('ticker') or row.get('symbol') or '').strip().upper()

    @staticmethod
    def _num(row: pd.Series | dict[str, Any] | None, names: list[str], default: float = 0.0) -> float:
        if row is None:
            return default
        for name in names:
            try:
                if name in row and pd.notna(row.get(name)):
                    return float(row.get(name))
            except Exception:
                continue
        return default

    @staticmethod
    def _text(row: pd.Series | dict[str, Any] | None, names: list[str]) -> str:
        if row is None:
            return ''
        parts = []
        for n in names:
            try:
                if n in row:
                    v = row.get(n)
                    if pd.notna(v):
                        parts.append(str(v))
            except Exception:
                continue
        return ' '.join(parts)

    @staticmethod
    def _parse_date(v: Any) -> date | None:
        if v is None or str(v).strip() == '':
            return None
        try:
            return date.fromisoformat(str(v)[:10])
        except Exception:
            return None

    @staticmethod
    def _coalesce_text(*values: Any) -> str:
        for v in values:
            s = str(v or '').strip()
            if s:
                return s
        return ''

    def _combined_context_text(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> str:
        return ' '.join([
            self._text(row, ['Entry_State', 'Position_Stage', 'Stage', 'Position_Side', 'Direction', 'Regime', 'Setup_Tag', 'Signal', 'Action', 'Strategy_Tag', 'Golden_Type', '今日系統燈號']),
            self._text(drow, ['Entry_State', 'Exit_State', 'Exit_Action', 'Action', 'Signal', 'Direction', 'Regime', 'Setup_Tag', 'Strategy_Tag', 'Golden_Type', 'Reason', '今日系統燈號']),
            self._text(prev_item, ['entry_stage', 'position_side', 'regime_bucket', 'profile_name']),
        ])

    def _entry_stage_from_text(self, text: str) -> tuple[str, str, float]:
        raw = str(text or '')
        up = raw.upper()
        if any(tok in up for tok in ['FULL_ENTRY', 'FULL']) or '確認' in raw or 'CONFIRM' in up or '正式進場' in raw:
            return 'FULL_ENTRY', 'text_keyword_confirm', 0.60
        if any(tok in up for tok in ['PILOT_ENTRY', 'PILOT', 'PREEMPTIVE']) or '試單' in raw or '提早布局' in raw:
            return 'PILOT_ENTRY', 'text_keyword_pilot', 0.58
        if 'PREPARE' in up or '預備' in raw or '觀察' in raw or '布局' in raw:
            return 'PREPARE', 'text_keyword_prepare', 0.55
        return '', '', 0.0

    def _regime_from_text(self, text: str) -> tuple[str, str, float]:
        raw = str(text or '')
        up = raw.upper()
        if '區間' in raw or 'RANGE' in up or '盤整' in raw or 'MEAN_REVERSION' in up:
            return 'RANGE', 'text_keyword_range', 0.56
        if '趨勢' in raw or 'TREND' in up or '突破' in raw or '波段' in raw or 'MOMENTUM' in up:
            return 'TREND', 'text_keyword_trend', 0.56
        return '', '', 0.0

    def _infer_entry_stage_info(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> dict[str, Any]:
        explicit_fields = [
            ('row.Entry_State', row.get('Entry_State')),
            ('row.Position_Stage', row.get('Position_Stage')),
            ('row.Stage', row.get('Stage')),
            ('row.Approved_Entry_Stage', row.get('Approved_Entry_Stage')),
            ('row.Strategy_Stage_Final', row.get('Strategy_Stage_Final')),
            ('decision.Entry_State', drow.get('Entry_State') if drow is not None else ''),
            ('decision.Approved_Entry_Stage', drow.get('Approved_Entry_Stage') if drow is not None else ''),
            ('decision.Strategy_Stage_Final', drow.get('Strategy_Stage_Final') if drow is not None else ''),
        ]
        for source, value in explicit_fields:
            stage, _, _ = self._entry_stage_from_text(value)
            if stage:
                return {'entry_stage': stage, 'entry_stage_source': source, 'entry_stage_confidence': 0.98, 'entry_stage_raw': str(value or '').strip()}

        prev_stage = str((prev_item or {}).get('entry_stage') or '').strip()
        stage, _, conf = self._entry_stage_from_text(prev_stage)
        if stage:
            return {'entry_stage': stage, 'entry_stage_source': 'previous_snapshot.entry_stage', 'entry_stage_confidence': max(conf, 0.80), 'entry_stage_raw': prev_stage}

        text = self._combined_context_text(row, drow, prev_item)
        stage, source, conf = self._entry_stage_from_text(text)
        if stage:
            return {'entry_stage': stage, 'entry_stage_source': source, 'entry_stage_confidence': conf, 'entry_stage_raw': text[:160]}

        age_hint = int((prev_item or {}).get('age_days', 0) or 0)
        qty_hint = abs(int(round(self._num(row, ['Qty', 'qty', 'Quantity', 'shares', '進場股數', '股數'], 0.0))))
        if age_hint >= 5 and qty_hint > 0:
            return {'entry_stage': 'PILOT_ENTRY', 'entry_stage_source': 'fallback_active_position', 'entry_stage_confidence': 0.40, 'entry_stage_raw': f'age_days={age_hint};qty={qty_hint}'}
        return {'entry_stage': 'UNKNOWN', 'entry_stage_source': 'fallback_unknown', 'entry_stage_confidence': 0.20, 'entry_stage_raw': ''}

    def _infer_entry_stage(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> str:
        return str(self._infer_entry_stage_info(row, drow, prev_item).get('entry_stage') or 'UNKNOWN').upper()

    def _infer_position_side(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> str:
        explicit_fields = [
            row.get('Position_Side'), row.get('Direction'), row.get('Side'),
            drow.get('Position_Side') if drow is not None else '',
            drow.get('Direction') if drow is not None else '',
            (prev_item or {}).get('position_side'),
        ]
        for value in explicit_fields:
            text = str(value or '')
            up = text.upper()
            if any(tok in up for tok in ['SHORT', 'SELL_SHORT', 'COVER']):
                return 'SHORT'
            if '做空' in text or '空方' in text or '放空' in text:
                return 'SHORT'
            if any(tok in up for tok in ['LONG', 'BUY']):
                return 'LONG'
            if '做多' in text or '多方' in text:
                return 'LONG'
        text = self._combined_context_text(row, drow, prev_item)
        up = text.upper()
        if any(tok in up for tok in ['SHORT', 'SELL_SHORT', 'COVER']):
            return 'SHORT'
        if '做空' in text or '空方' in text or '放空' in text:
            return 'SHORT'
        if any(tok in up for tok in ['LONG', 'BUY']):
            return 'LONG'
        if '做多' in text or '多方' in text:
            return 'LONG'
        qty_sign = self._num(row, ['Signed_Qty', 'Net_Qty', 'signed_qty', 'net_qty'], 0.0)
        if qty_sign < 0:
            return 'SHORT'
        return 'LONG'

    def _infer_regime_bucket_info(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> dict[str, Any]:
        explicit_fields = [
            ('row.Regime_Bucket', row.get('Regime_Bucket')),
            ('row.Regime', row.get('Regime')),
            ('row.Policy_Regime', row.get('Policy_Regime')),
            ('row.Strategy_Regime', row.get('Strategy_Regime')),
            ('row.Final_Regime_Bucket', row.get('Final_Regime_Bucket')),
            ('decision.Regime_Bucket', drow.get('Regime_Bucket') if drow is not None else ''),
            ('decision.Regime', drow.get('Regime') if drow is not None else ''),
            ('decision.Policy_Regime', drow.get('Policy_Regime') if drow is not None else ''),
            ('decision.Strategy_Regime', drow.get('Strategy_Regime') if drow is not None else ''),
            ('decision.Final_Regime_Bucket', drow.get('Final_Regime_Bucket') if drow is not None else ''),
        ]
        for source, value in explicit_fields:
            regime, _, _ = self._regime_from_text(value)
            if regime:
                return {'regime_bucket': regime, 'regime_source': source, 'regime_confidence': 0.98, 'regime_raw': str(value or '').strip()}

        prev_regime = str((prev_item or {}).get('regime_bucket') or '').strip()
        regime, _, conf = self._regime_from_text(prev_regime)
        if regime:
            return {'regime_bucket': regime, 'regime_source': 'previous_snapshot.regime_bucket', 'regime_confidence': max(conf, 0.78), 'regime_raw': prev_regime}

        text = self._combined_context_text(row, drow, prev_item)
        regime, source, conf = self._regime_from_text(text)
        if regime:
            return {'regime_bucket': regime, 'regime_source': source, 'regime_confidence': conf, 'regime_raw': text[:160]}

        return {'regime_bucket': 'NEUTRAL', 'regime_source': 'fallback_neutral', 'regime_confidence': 0.25, 'regime_raw': ''}

    def _infer_regime_bucket(self, row: pd.Series, drow: pd.Series | None, prev_item: dict[str, Any]) -> str:
        return str(self._infer_regime_bucket_info(row, drow, prev_item).get('regime_bucket') or 'NEUTRAL').upper()

    def _volatility_inputs(self, row: pd.Series, drow: pd.Series | None, entry_stage: str = 'UNKNOWN', regime_bucket: str = 'NEUTRAL') -> dict[str, Any]:
        close = max(
            self._num(row, ['Current_Close', 'Close', 'Last', 'Reference_Price', '目前股價', '市價'], 0.0),
            self._num(drow, ['Reference_Price', 'Close', 'Current_Close'], 0.0),
            0.0,
        )
        atr = max(self._num(row, ['ATR', 'ATR14', 'atr', 'atr14'], 0.0), self._num(drow, ['ATR', 'ATR14', 'atr', 'atr14'], 0.0), 0.0)
        rv = max(
            self._num(row, ['RV', 'RealizedVol', 'realized_volatility', 'Volatility', 'HistoricalVol'], 0.0),
            self._num(drow, ['RV', 'RealizedVol', 'realized_volatility', 'Volatility', 'HistoricalVol'], 0.0),
            0.0,
        )
        if rv > 1.0:
            rv = rv / 100.0
        atr_ratio = (atr / close) if close > 0 and atr > 0 else 0.0
        vol_ratio = max(atr_ratio, rv)
        threshold = 0.035
        if str(regime_bucket).upper() == 'TREND':
            threshold += 0.004
        elif str(regime_bucket).upper() == 'RANGE':
            threshold -= 0.003
        if str(entry_stage).upper() in {'PREPARE', 'UNKNOWN'}:
            threshold -= 0.002
        threshold = min(0.05, max(0.025, threshold))
        high_vol = vol_ratio >= threshold and vol_ratio > 0
        confidence = 0.90 if (atr_ratio > 0 and rv > 0) else (0.72 if (atr_ratio > 0 or rv > 0) else 0.20)
        return {
            'volatility_ratio': round(vol_ratio, 6),
            'high_volatility': high_vol,
            'volatility_threshold': round(threshold, 6),
            'volatility_confidence': confidence,
            'atr_ratio': round(atr_ratio, 6),
            'rv_ratio': round(rv, 6),
            'volatility_mode': 'dynamic_threshold_v2',
        }

    def _build_profile(self, entry_stage: str, position_side: str, regime_bucket: str, row: pd.Series, drow: pd.Series | None) -> dict[str, Any]:
        base_trail = float(getattr(CONFIG, 'trailing_stop_pct', 0.05) or 0.05)
        base_defend = float(getattr(CONFIG, 'default_stop_loss_pct', 0.04) or 0.04)
        base_stale_days = int(getattr(CONFIG, 'position_max_age_without_progress_days', 12) or 12)
        base_progress = float(getattr(CONFIG, 'position_min_progress_pct', 0.01) or 0.01)
        stage_cfg = {
            'PREPARE': {'trail_mult': 1.08, 'defend_mult': 0.74, 'exit_mult': 1.08, 'stale_mult': 0.40, 'progress_mult': 0.70, 'reduce_fraction': 0.00},
            'PILOT_ENTRY': {'trail_mult': 1.00, 'defend_mult': 0.90, 'exit_mult': 1.20, 'stale_mult': 0.56, 'progress_mult': 0.82, 'reduce_fraction': 0.33},
            'FULL_ENTRY': {'trail_mult': 1.00, 'defend_mult': 1.00, 'exit_mult': 1.35, 'stale_mult': 1.00, 'progress_mult': 1.00, 'reduce_fraction': 0.50},
            'UNKNOWN': {'trail_mult': 1.02, 'defend_mult': 0.84, 'exit_mult': 1.12, 'stale_mult': 0.50, 'progress_mult': 0.82, 'reduce_fraction': 0.20},
        }
        regime_cfg = {
            'TREND': {'trail_mult': 1.18, 'defend_mult': 1.18, 'exit_mult': 1.18, 'stale_mult': 1.35, 'progress_mult': 0.70, 'reduce_fraction': 0.40},
            'RANGE': {'trail_mult': 0.88, 'defend_mult': 0.88, 'exit_mult': 0.92, 'stale_mult': 0.72, 'progress_mult': 1.10, 'reduce_fraction': 0.55},
            'NEUTRAL': {'trail_mult': 1.00, 'defend_mult': 1.00, 'exit_mult': 1.00, 'stale_mult': 1.00, 'progress_mult': 1.00, 'reduce_fraction': 0.50},
        }
        v = self._volatility_inputs(row, drow, entry_stage=entry_stage, regime_bucket=regime_bucket)
        vol_ratio = float(v.get('volatility_ratio', 0.0) or 0.0)
        high_vol = bool(v.get('high_volatility'))
        vol_mult = 1.18 if high_vol else 1.00
        s = stage_cfg.get(entry_stage, stage_cfg['UNKNOWN'])
        r = regime_cfg.get(regime_bucket, regime_cfg['NEUTRAL'])
        trailing_pct = max(0.018, base_trail * s['trail_mult'] * r['trail_mult'] * vol_mult)
        defend_loss_pct = max(0.012, base_defend * s['defend_mult'] * r['defend_mult'] * vol_mult)
        hard_exit_loss_pct = max(defend_loss_pct * 1.18, defend_loss_pct * s['exit_mult'] * r['exit_mult'])
        stale_days = max(2, int(round(base_stale_days * s['stale_mult'] * r['stale_mult'])))
        min_progress = max(0.002, base_progress * s['progress_mult'] * r['progress_mult'])
        reduce_fraction = r['reduce_fraction'] if entry_stage == 'FULL_ENTRY' else s['reduce_fraction']
        profile_name = f"{entry_stage}_{position_side}_{regime_bucket}".lower()
        return {
            'profile_name': profile_name,
            'entry_stage': entry_stage,
            'position_side': position_side,
            'regime_bucket': regime_bucket,
            'volatility_ratio': round(vol_ratio, 6),
            'high_volatility': high_vol,
            'volatility_threshold': v.get('volatility_threshold'),
            'volatility_confidence': v.get('volatility_confidence'),
            'atr_ratio': v.get('atr_ratio'),
            'rv_ratio': v.get('rv_ratio'),
            'volatility_mode': v.get('volatility_mode'),
            'trailing_pct': round(trailing_pct, 6),
            'defend_loss_pct': round(defend_loss_pct, 6),
            'hard_exit_loss_pct': round(hard_exit_loss_pct, 6),
            'stale_days': stale_days,
            'min_progress_pct': round(min_progress, 6),
            'reduce_fraction': round(reduce_fraction, 4),
        }

    def _calculate_protective_stop(
        self,
        entry: float,
        close: float,
        peak_price: float,
        trough_price: float,
        prev_stop: float,
        position_side: str,
        trailing_pct: float,
    ) -> float:
        if trailing_pct <= 0:
            return 0.0
        if position_side == 'SHORT':
            initial_stop = entry * (1.0 + trailing_pct) if entry > 0 else 0.0
            candidate = trough_price * (1.0 + trailing_pct) if trough_price > 0 else initial_stop
            anchor = min(x for x in [v for v in [prev_stop, initial_stop, candidate] if v > 0]) if any(v > 0 for v in [prev_stop, initial_stop, candidate]) else 0.0
            protective_stop = min(anchor, candidate) if anchor > 0 and candidate > 0 else max(anchor, candidate)
            if protective_stop > 0 and close > 0 and protective_stop <= close:
                protective_stop = close * 1.001
            return round(protective_stop, 4)
        initial_stop = entry * (1.0 - trailing_pct) if entry > 0 else 0.0
        candidate = peak_price * (1.0 - trailing_pct) if peak_price > 0 else initial_stop
        anchor = max(prev_stop, initial_stop, candidate)
        return round(anchor if anchor > 0 else 0.0, 4)

    def _evaluate_recommendation(
        self,
        entry_stage: str,
        position_side: str,
        regime_bucket: str,
        close: float,
        unrealized: float,
        age_days: int,
        protective_stop: float,
        signal_text: str,
        profile: dict[str, Any],
    ) -> tuple[str, str]:
        signal_upper = signal_text.upper()
        defend_loss_pct = float(profile.get('defend_loss_pct', 0.04) or 0.04)
        hard_exit_loss_pct = float(profile.get('hard_exit_loss_pct', defend_loss_pct * 1.2) or (defend_loss_pct * 1.2))
        stale_days = int(profile.get('stale_days', 12) or 12)
        min_progress = float(profile.get('min_progress_pct', 0.01) or 0.01)

        stop_hit = close > 0 and protective_stop > 0 and ((position_side == 'LONG' and close <= protective_stop) or (position_side == 'SHORT' and close >= protective_stop))
        if stop_hit:
            return 'EXIT', 'protective_stop_hit'
        if 'EXIT' in signal_upper or '出場' in signal_text or '平倉' in signal_text:
            return 'EXIT', 'model_or_policy_exit_signal'
        if unrealized <= -hard_exit_loss_pct:
            if entry_stage in {'PREPARE', 'PILOT_ENTRY'}:
                return 'EXIT', 'early_stage_loss_limit_hit'
            return 'DEFEND', 'hard_loss_limit_watch'
        if 'REDUCE' in signal_upper or '減碼' in signal_text:
            return 'REDUCE', 'model_or_policy_reduce_signal'
        if 'DEFEND' in signal_upper or '防守' in signal_text:
            return 'DEFEND', 'model_or_policy_defend_signal'
        if unrealized <= -defend_loss_pct:
            return ('EXIT', 'prepare_defend_escalated_to_exit') if entry_stage == 'PREPARE' else ('DEFEND', 'profile_loss_watch')
        if age_days >= stale_days and unrealized < min_progress:
            if entry_stage == 'FULL_ENTRY':
                return 'REDUCE', 'stale_position_no_progress'
            if regime_bucket == 'RANGE':
                return 'EXIT', 'range_probe_stale_exit'
            return 'DEFEND', 'early_stage_stale_watch'
        return 'HOLD', 'profile_hold'

    def _determine_action_qty(self, recommendation: str, qty: int, profile: dict[str, Any]) -> int:
        if recommendation == 'EXIT':
            return qty
        if recommendation != 'REDUCE':
            return 0
        reduce_fraction = float(profile.get('reduce_fraction', 0.5) or 0.5)
        return min(qty, max(1, int(round(qty * reduce_fraction)))) if qty > 0 else 0

    def _closing_action_for_side(self, recommendation: str, position_side: str) -> str:
        if recommendation not in {'EXIT', 'REDUCE'}:
            return ''
        return 'COVER' if str(position_side or '').upper() == 'SHORT' else 'SELL'

    def _signed_effect_qty(self, recommendation: str, position_side: str, qty: int) -> int:
        if recommendation not in {'EXIT', 'REDUCE'}:
            return 0
        return qty if str(position_side or '').upper() == 'SHORT' else (-qty)

    def _wrong_exit_stats(self, current_positions: dict[str, Any]) -> dict[str, Any]:

        """A conservative proxy: track EXIT/REDUCE recommendation reversals in recent history."""
        if not self.history_path.exists():
            return {'status': 'no_history_yet', 'wrong_exit_proxy_count': 0}
        try:
            rows = [json.loads(x) for x in self.history_path.read_text(encoding='utf-8').splitlines()[-2000:] if x.strip()]
        except Exception:
            return {'status': 'history_read_failed', 'wrong_exit_proxy_count': 0}
        exits = [r for r in rows if str(r.get('recommendation', '')).upper() == 'EXIT']
        proxy = []
        for e in exits[-200:]:
            t = str(e.get('ticker') or '').upper()
            cur = current_positions.get(t)
            if not isinstance(cur, dict):
                continue
            try:
                old_px = float(e.get('current_close') or 0)
                new_px = float(cur.get('current_close') or 0)
            except Exception:
                old_px = new_px = 0.0
            if old_px > 0 and new_px > old_px * 1.03:
                proxy.append({'ticker': t, 'exit_date': e.get('date'), 'exit_price': old_px, 'current_close': new_px, 'proxy_reason': 'reentry_or_remaining_position_above_exit_price_3pct'})
        return {'status': 'wrong_exit_proxy_scored', 'wrong_exit_proxy_count': len(proxy), 'items': proxy[:50]}

    def build(self) -> tuple[Path, dict[str, Any]]:
        source, positions_df = self._load_positions()
        decision_df = self._load_decisions()
        prev = self._load_prev()
        today = date.today().isoformat()

        if positions_df.empty:
            empty_plan = {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'status': 'position_lifecycle_action_plan_ready',
                'actions': [],
                'policy': {
                    'layered_profiles_enabled': True,
                    'profiles': ['prepare', 'pilot_entry', 'full_entry', 'unknown', 'long', 'short', 'trend', 'range', 'high_volatility'],
                },
            }
            self.action_plan_path.write_text(json.dumps(empty_plan, ensure_ascii=False, indent=2), encoding='utf-8')
            pd.DataFrame([]).to_csv(self.stop_payload_csv_path, index=False, encoding='utf-8-sig')
            pd.DataFrame([]).to_csv(self.stop_payload_runtime_path, index=False, encoding='utf-8-sig')
            payload = {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'module_version': self.MODULE_VERSION,
                'status': 'no_active_positions',
                'message': '尚無 active_positions.csv；生命週期服務已就緒，等有持倉後會開始逐日記錄。',
                'action_plan_json': str(self.action_plan_path),
                'stop_replace_payload_csv': str(self.stop_payload_csv_path),
                'stop_replace_payload_runtime_csv': str(self.stop_payload_runtime_path),
                'positions': {},
                'wrong_exit_statistics': self._wrong_exit_stats({}),
                'summary': {'position_count': 0, 'exit_count': 0, 'reduce_count': 0, 'defend_count': 0, 'action_plan_count': 0},
            }
            self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.path, payload

        decisions_by_ticker: dict[str, pd.Series] = {}
        if not decision_df.empty:
            for _, row in decision_df.iterrows():
                t = self._ticker(row)
                if t:
                    decisions_by_ticker[t] = row

        out_rows: list[dict[str, Any]] = []
        positions: dict[str, Any] = {}
        profile_summary = {
            'prepare_count': 0,
            'pilot_count': 0,
            'full_count': 0,
            'unknown_stage_count': 0,
            'long_count': 0,
            'short_count': 0,
            'trend_count': 0,
            'range_count': 0,
            'high_volatility_count': 0,
        }
        for _, row in positions_df.iterrows():
            ticker = self._ticker(row)
            if not ticker:
                continue
            entry = self._num(row, ['Entry_Price', 'entry_price', 'AvgCost', 'avg_cost', '進場價', '成本', '均價'], 0.0)
            qty = int(max(0, round(self._num(row, ['Qty', 'qty', 'Quantity', 'shares', '進場股數', '股數'], 0.0))))
            close = self._num(row, ['Current_Close', 'Close', 'Last', 'Reference_Price', '目前股價', '市價'], 0.0)
            entry_date = self._parse_date(row.get('Entry_Date') or row.get('entry_date') or row.get('建倉日期'))
            drow = decisions_by_ticker.get(ticker)
            if drow is not None and close <= 0:
                close = self._num(drow, ['Reference_Price', 'Close', 'Current_Close'], 0.0)
            prev_item = prev.get(ticker, {}) if isinstance(prev, dict) else {}
            entry_stage_info = self._infer_entry_stage_info(row, drow, prev_item)
            regime_info = self._infer_regime_bucket_info(row, drow, prev_item)
            entry_stage = str(entry_stage_info.get('entry_stage') or 'UNKNOWN').upper()
            position_side = self._infer_position_side(row, drow, prev_item)
            regime_bucket = str(regime_info.get('regime_bucket') or 'NEUTRAL').upper()
            profile = self._build_profile(entry_stage, position_side, regime_bucket, row, drow)

            if entry > 0 and close > 0:
                unrealized = ((close / entry) - 1.0) if position_side == 'LONG' else ((entry / close) - 1.0)
            else:
                unrealized = 0.0
            prev_peak = float(prev_item.get('peak_price', entry if entry > 0 else close) or 0)
            prev_trough = float(prev_item.get('trough_price', entry if entry > 0 else close) or 0)
            peak_price = max(prev_peak, close, entry)
            non_zero_candidates = [x for x in [prev_trough, close, entry] if x > 0]
            trough_price = min(non_zero_candidates) if non_zero_candidates else 0.0
            prev_stop = float(prev_item.get('trailing_stop_price', 0) or 0)
            trailing_stop = self._calculate_protective_stop(
                entry=entry,
                close=close,
                peak_price=peak_price,
                trough_price=trough_price,
                prev_stop=prev_stop,
                position_side=position_side,
                trailing_pct=float(profile.get('trailing_pct', 0.05) or 0.05),
            )
            if entry_date:
                age_days = max(0, (date.fromisoformat(today) - entry_date).days)
            else:
                age_days = int(prev_item.get('age_days', 0) or 0) + 1
            signal_text = self._text(drow, ['Exit_State', 'Exit_Action', 'Action', 'Signal', 'Reason', 'Golden_Type', 'Setup_Tag', '今日系統燈號'])
            recommendation, attribution = self._evaluate_recommendation(
                entry_stage=entry_stage,
                position_side=position_side,
                regime_bucket=regime_bucket,
                close=close,
                unrealized=unrealized,
                age_days=age_days,
                protective_stop=trailing_stop,
                signal_text=signal_text,
                profile=profile,
            )

            item = {
                'ticker': ticker,
                'date': today,
                'qty': qty,
                'entry_price': round(entry, 4),
                'current_close': round(close, 4),
                'unrealized_return': round(unrealized, 6),
                'unrealized_return_pct': round(unrealized * 100.0, 3),
                'age_days': age_days,
                'holding_days': age_days,
                'peak_price': round(peak_price, 4),
                'trough_price': round(trough_price, 4),
                'trailing_stop_price': round(trailing_stop, 4),
                'protective_stop_price': round(trailing_stop, 4),
                'entry_stage': entry_stage,
                'entry_stage_source': entry_stage_info.get('entry_stage_source'),
                'entry_stage_confidence': entry_stage_info.get('entry_stage_confidence'),
                'entry_stage_raw': entry_stage_info.get('entry_stage_raw'),
                'position_side': position_side,
                'regime_bucket': regime_bucket,
                'regime_source': regime_info.get('regime_source'),
                'regime_confidence': regime_info.get('regime_confidence'),
                'regime_raw': regime_info.get('regime_raw'),
                'profile_name': profile.get('profile_name'),
                'high_volatility': bool(profile.get('high_volatility')),
                'volatility_ratio': profile.get('volatility_ratio'),
                'lifecycle_profile': profile,
                'recommendation': recommendation,
                'exit_attribution': attribution,
                'reduce_reason': attribution if recommendation == 'REDUCE' else '',
                'exit_reason': attribution if recommendation == 'EXIT' else '',
                'defend_reason': attribution if recommendation == 'DEFEND' else '',
                'signal_text': signal_text[:300],
            }
            positions[ticker] = item
            out_rows.append(item)
            if entry_stage == 'PREPARE':
                profile_summary['prepare_count'] += 1
            elif entry_stage == 'PILOT_ENTRY':
                profile_summary['pilot_count'] += 1
            elif entry_stage == 'FULL_ENTRY':
                profile_summary['full_count'] += 1
            else:
                profile_summary['unknown_stage_count'] += 1
            if position_side == 'SHORT':
                profile_summary['short_count'] += 1
            else:
                profile_summary['long_count'] += 1
            if regime_bucket == 'TREND':
                profile_summary['trend_count'] += 1
            elif regime_bucket == 'RANGE':
                profile_summary['range_count'] += 1
            if bool(profile.get('high_volatility')):
                profile_summary['high_volatility_count'] += 1
            append_execution_journal_event(
                'POSITION_LIFECYCLE_EVALUATED',
                source='position_lifecycle_service',
                ticker=ticker,
                status=recommendation,
                qty=qty,
                reference_price=close,
                reason=attribution,
                stage=entry_stage,
                side=position_side,
                regime=regime_bucket,
            )

        if out_rows:
            pd.DataFrame(out_rows).to_csv(self.csv_path, index=False, encoding='utf-8-sig')
            with self.history_path.open('a', encoding='utf-8') as fh:
                for r in out_rows:
                    fh.write(json.dumps(r, ensure_ascii=False) + '\n')

        action_plan = []
        stop_replace_rows = []
        for r in out_rows:
            rec = str(r.get('recommendation') or '').upper()
            if rec in {'EXIT', 'REDUCE', 'DEFEND'}:
                qty = int(r.get('qty') or 0)
                profile = r.get('lifecycle_profile') if isinstance(r.get('lifecycle_profile'), dict) else {}
                action_qty = self._determine_action_qty(rec, qty, profile)
                defend_action = 'TIGHTEN_STOP_AND_REPLACE_PROTECTIVE_ORDER' if rec == 'DEFEND' else ''
                closing_action = self._closing_action_for_side(rec, r.get('position_side'))
                signed_effect_qty = self._signed_effect_qty(rec, r.get('position_side'), action_qty)
                action = {
                    'ticker': r.get('ticker'),
                    'recommendation': rec,
                    'action': (closing_action if rec in {'EXIT', 'REDUCE'} else defend_action),
                    'qty': action_qty,
                    'signed_effect_qty': signed_effect_qty,
                    'reference_price': r.get('current_close'),
                    'attribution': r.get('exit_attribution'),
                    'source': 'position_lifecycle_service',
                    'tighten_stop': rec == 'DEFEND',
                    'replace_protective_order': rec == 'DEFEND',
                    'reduce_risk': rec in {'REDUCE', 'DEFEND'},
                    'trailing_stop_price': r.get('trailing_stop_price'),
                    'entry_stage': r.get('entry_stage'),
                    'entry_stage_source': r.get('entry_stage_source'),
                    'entry_stage_confidence': r.get('entry_stage_confidence'),
                    'position_side': r.get('position_side'),
                    'regime_bucket': r.get('regime_bucket'),
                    'regime_source': r.get('regime_source'),
                    'regime_confidence': r.get('regime_confidence'),
                    'profile_name': r.get('profile_name'),
                }
                action_plan.append(action)
                if rec == 'DEFEND' and float(r.get('trailing_stop_price') or 0) > 0 and qty > 0:
                    stop_replace_rows.append({
                        'Ticker': r.get('ticker'),
                        'Position_Side': r.get('position_side'),
                        'Current_Position_Qty': qty,
                        'Desired_Stop_Price': float(r.get('trailing_stop_price') or 0),
                        'Target_Position_Multiplier': round(float(getattr(CONFIG, 'EXIT_DEFEND_POSITION_MULTIPLIER', 0.60) or 0.60), 4),
                        'Exit_State': 'DEFEND',
                        'Exit_Action': 'TIGHTEN_STOP',
                        'Should_Replace_Stop': True,
                        'Stop_Workflow_Mode': 'UPSERT_PROTECTIVE_STOP',
                        'Reference_Price': r.get('current_close'),
                        'Client_Order_ID': f"STOP-{r.get('ticker')}-{today.replace('-', '')}",
                        'Broker_Stop_Order_ID': '',
                        'Note': str(r.get('exit_attribution') or 'position_lifecycle_defend'),
                    })
                append_execution_journal_event(
                    'POSITION_LIFECYCLE_ACTION_PLANNED',
                    source='position_lifecycle_service',
                    ticker=r.get('ticker'),
                    status=rec,
                    action=action['action'],
                    qty=action_qty,
                    reference_price=r.get('current_close'),
                    reason=r.get('exit_attribution'),
                    stage=r.get('entry_stage'),
                    side=r.get('position_side'),
                    regime=r.get('regime_bucket'),
                )
        self.action_plan_path.write_text(json.dumps({
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'position_lifecycle_action_plan_ready',
            'actions': action_plan,
            'policy': {
                'layered_profiles_enabled': True,
                'notes': [
                    'PREPARE/PILOT/FULL_ENTRY use different stale-loss logic',
                    'LONG/SHORT use side-aware protective stop',
                    'TREND tolerates wider movement than RANGE',
                    'high_volatility widens trailing and defend thresholds',
                ],
            },
        }, ensure_ascii=False, indent=2), encoding='utf-8')
        stop_df = pd.DataFrame(stop_replace_rows)
        stop_df.to_csv(self.stop_payload_csv_path, index=False, encoding='utf-8-sig')
        stop_df.to_csv(self.stop_payload_runtime_path, index=False, encoding='utf-8-sig')
        wrong_exit_statistics = self._wrong_exit_stats(positions)
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': 'position_lifecycle_ready',
            'source': str(source),
            'snapshot_csv': str(self.csv_path),
            'action_plan_json': str(self.action_plan_path),
            'stop_replace_payload_csv': str(self.stop_payload_csv_path),
            'stop_replace_payload_runtime_csv': str(self.stop_payload_runtime_path),
            'history_jsonl': str(self.history_path),
            'positions': positions,
            'wrong_exit_statistics': wrong_exit_statistics,
            'summary': {
                'position_count': len(out_rows),
                'exit_count': sum(1 for r in out_rows if r['recommendation'] == 'EXIT'),
                'reduce_count': sum(1 for r in out_rows if r['recommendation'] == 'REDUCE'),
                'defend_count': sum(1 for r in out_rows if r['recommendation'] == 'DEFEND'),
                'hold_count': sum(1 for r in out_rows if r['recommendation'] == 'HOLD'),
                'action_plan_count': len(action_plan),
                **profile_summary,
            },
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = PositionLifecycleService().build()
    print(f'📒 持倉生命週期完成：{path} | status={payload.get("status")} positions={payload.get("summary",{}).get("position_count")}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
