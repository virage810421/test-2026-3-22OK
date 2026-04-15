# -*- coding: utf-8 -*-
from __future__ import annotations

"""持倉生命週期服務 v92.

補上：每日持倉狀態、移動停損、REDUCE/DEFEND/EXIT 歸因、action plan、execution journal、簡易錯誤出場統計。
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_execution_journal_service import append_execution_journal_event


class PositionLifecycleService:
    MODULE_VERSION = 'v92_position_lifecycle_closed_loop'

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
    def _num(row: pd.Series, names: list[str], default: float = 0.0) -> float:
        for name in names:
            if name in row and pd.notna(row.get(name)):
                try:
                    return float(row.get(name))
                except Exception:
                    continue
        return default

    @staticmethod
    def _text(row: pd.Series | None, names: list[str]) -> str:
        if row is None:
            return ''
        return ' '.join(str(row.get(n, '')) for n in names if n in row)

    @staticmethod
    def _parse_date(v: Any) -> date | None:
        if v is None or str(v).strip() == '':
            return None
        try:
            return date.fromisoformat(str(v)[:10])
        except Exception:
            return None

    def _wrong_exit_stats(self, current_positions: dict[str, Any]) -> dict[str, Any]:
        """A conservative proxy: track EXIT/REDUCE recommendation reversals in recent history."""
        if not self.history_path.exists():
            return {'status': 'no_history_yet', 'wrong_exit_proxy_count': 0}
        try:
            rows = [json.loads(x) for x in self.history_path.read_text(encoding='utf-8').splitlines()[-2000:] if x.strip()]
        except Exception:
            return {'status': 'history_read_failed', 'wrong_exit_proxy_count': 0}
        exits = [r for r in rows if str(r.get('recommendation', '')).upper() == 'EXIT']
        # 若近期曾 EXIT，但目前又有同 ticker 持倉且價格高於當時價格，標示為需人工檢討的 proxy。
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
            empty_plan = {'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'position_lifecycle_action_plan_ready', 'actions': []}
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
        trailing_pct = float(getattr(CONFIG, 'trailing_stop_pct', 0.05) or 0.05)
        max_age_without_progress = int(getattr(CONFIG, 'position_max_age_without_progress_days', 12) or 12)
        min_profit_progress = float(getattr(CONFIG, 'position_min_progress_pct', 0.01) or 0.01)
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
            unrealized = ((close / entry) - 1.0) if entry > 0 and close > 0 else 0.0
            prev_item = prev.get(ticker, {}) if isinstance(prev, dict) else {}
            prev_peak = float(prev_item.get('peak_price', entry if entry > 0 else close) or 0)
            prev_trough = float(prev_item.get('trough_price', entry if entry > 0 else close) or 0)
            peak_price = max(prev_peak, close, entry)
            trough_price = min(x for x in [prev_trough or close or entry, close or entry, entry or close] if x > 0) if any(x > 0 for x in [prev_trough or 0, close or 0, entry or 0]) else 0.0
            prev_stop = float(prev_item.get('trailing_stop_price', 0) or 0)
            trailing_stop = max(prev_stop, peak_price * (1.0 - trailing_pct)) if peak_price > 0 else 0.0
            if entry_date:
                age_days = max(0, (date.fromisoformat(today) - entry_date).days)
            else:
                age_days = int(prev_item.get('age_days', 0) or 0) + 1
            signal_text = self._text(drow, ['Exit_State', 'Exit_Action', 'Action', 'Signal', 'Reason', 'Golden_Type', 'Setup_Tag', '今日系統燈號'])
            signal_upper = signal_text.upper()
            recommendation = 'HOLD'
            attribution = 'no_exit_signal'
            if close > 0 and trailing_stop > 0 and close <= trailing_stop:
                recommendation = 'EXIT'
                attribution = 'trailing_stop_hit'
            elif 'EXIT' in signal_upper or '出場' in signal_text:
                recommendation = 'EXIT'
                attribution = 'model_or_policy_exit_signal'
            elif 'REDUCE' in signal_upper or '減碼' in signal_text:
                recommendation = 'REDUCE'
                attribution = 'model_or_policy_reduce_signal'
            elif 'DEFEND' in signal_upper or '防守' in signal_text:
                recommendation = 'DEFEND'
                attribution = 'model_or_policy_defend_signal'
            elif unrealized < -float(getattr(CONFIG, 'default_stop_loss_pct', 0.04) or 0.04):
                recommendation = 'DEFEND'
                attribution = 'loss_exceeds_default_stop_watch'
            elif age_days >= max_age_without_progress and unrealized < min_profit_progress:
                recommendation = 'REDUCE'
                attribution = 'stale_position_no_progress'
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
                'recommendation': recommendation,
                'exit_attribution': attribution,
                'reduce_reason': attribution if recommendation == 'REDUCE' else '',
                'exit_reason': attribution if recommendation == 'EXIT' else '',
                'defend_reason': attribution if recommendation == 'DEFEND' else '',
                'signal_text': signal_text[:300],
            }
            positions[ticker] = item
            out_rows.append(item)
            append_execution_journal_event('POSITION_LIFECYCLE_EVALUATED', source='position_lifecycle_service', ticker=ticker, status=recommendation, qty=qty, reference_price=close, reason=attribution)

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
                action_qty = qty if rec == 'EXIT' else (max(1, qty // 2) if rec == 'REDUCE' else 0)
                defend_action = 'TIGHTEN_STOP_AND_REPLACE_PROTECTIVE_ORDER' if rec == 'DEFEND' else ''
                action = {'ticker': r.get('ticker'), 'recommendation': rec, 'action': ('SELL' if rec in {'EXIT', 'REDUCE'} else defend_action), 'qty': action_qty, 'reference_price': r.get('current_close'), 'attribution': r.get('exit_attribution'), 'source': 'position_lifecycle_service', 'tighten_stop': rec == 'DEFEND', 'replace_protective_order': rec == 'DEFEND', 'reduce_risk': rec in {'REDUCE', 'DEFEND'}, 'trailing_stop_price': r.get('trailing_stop_price')}
                action_plan.append(action)
                if rec == 'DEFEND' and float(r.get('trailing_stop_price') or 0) > 0 and qty > 0:
                    position_side = 'SHORT' if ('空' in str(decisions_by_ticker.get(str(r.get('ticker') or '').upper(), {}).get('Direction', '')) or 'SHORT' in str(decisions_by_ticker.get(str(r.get('ticker') or '').upper(), {}).get('Direction', '')).upper()) else 'LONG'
                    stop_replace_rows.append({
                        'Ticker': r.get('ticker'),
                        'Position_Side': position_side,
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
                append_execution_journal_event('POSITION_LIFECYCLE_ACTION_PLANNED', source='position_lifecycle_service', ticker=r.get('ticker'), status=rec, action=action['action'], qty=action_qty, reference_price=r.get('current_close'), reason=r.get('exit_attribution'))
        self.action_plan_path.write_text(json.dumps({'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'), 'status': 'position_lifecycle_action_plan_ready', 'actions': action_plan}, ensure_ascii=False, indent=2), encoding='utf-8')
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
