# -*- coding: utf-8 -*-
from __future__ import annotations

"""進場後追蹤驗證服務 v92.

PREPARE / PILOT_ENTRY / FULL_ENTRY 不再只是單日訊號：
- 每天延續狀態與年齡。
- PREPARE 過期未確認 => 取消候選。
- PILOT 過期未轉強 => 降風險/取消候選。
- 產出 action_plan 供 control tower Gate 使用。
"""

import json
from datetime import datetime, date
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_execution_journal_service import append_execution_journal_event


class EntryTrackingService:
    MODULE_VERSION = 'v92_entry_tracking_closed_loop_gate'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'entry_tracking_journal.json'
        self.action_plan_path = PATHS.runtime_dir / 'entry_tracking_action_plan.json'
        self.max_prepare_days = 5
        self.max_pilot_days = 4
        self.max_missing_days = 2

    def _load_decisions(self) -> tuple[Path | None, pd.DataFrame]:
        candidates = [
            PATHS.data_dir / 'normalized_decision_output_enriched.csv',
            PATHS.data_dir / 'normalized_decision_output.csv',
            PATHS.base_dir / 'daily_decision_desk.csv',
            PATHS.data_dir / 'daily_decision_desk.csv',
        ]
        for p in candidates:
            if p.exists():
                try:
                    df = pd.read_csv(p)
                    if not df.empty:
                        return p, df
                except Exception:
                    continue
        return None, pd.DataFrame()

    def _load_state(self) -> dict[str, Any]:
        if not self.path.exists():
            return {'positions': {}, 'events': []}
        try:
            data = json.loads(self.path.read_text(encoding='utf-8'))
            if isinstance(data, dict):
                return {'positions': data.get('positions', {}), 'events': data.get('events', [])}
        except Exception:
            pass
        return {'positions': {}, 'events': []}

    @staticmethod
    def _stage_from_row(row: pd.Series) -> str:
        text = ' '.join(str(row.get(c, '')) for c in [
            'Entry_State', 'Entry_Action', 'Action', 'Signal', 'Golden_Type', 'Decision',
            'Early_Path_State', 'Confirm_Path_State', 'Setup_Tag', 'System_Light', '今日系統燈號'
        ])
        up = text.upper()
        if 'FULL_ENTRY' in up or 'CONFIRM' in up or '確認' in text or '強勢' in text:
            return 'FULL_ENTRY'
        if 'PILOT' in up or '試單' in text or 'PREEMPTIVE' in up or '提早布局' in text:
            return 'PILOT_ENTRY'
        if 'PREPARE' in up or '觀察' in text or '布局' in text:
            return 'PREPARE'
        try:
            qty = float(row.get('Target_Qty', row.get('TargetQty', 0)) or 0)
            if bool(row.get('ExecutionEligible', False)) and qty > 0:
                return 'FULL_ENTRY'
        except Exception:
            pass
        return 'WATCH'

    @staticmethod
    def _ticker(row: pd.Series) -> str:
        return str(row.get('Ticker') or row.get('Ticker SYMBOL') or row.get('ticker') or '').strip().upper()

    @staticmethod
    def _age(today: str, d: str | None) -> int:
        if not d:
            return 0
        try:
            return (date.fromisoformat(today) - date.fromisoformat(d)).days
        except Exception:
            return 0

    def _roll_forward_missing(self, positions: dict[str, Any], today: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        events: list[dict[str, Any]] = []
        action_plan: list[dict[str, Any]] = []
        for ticker, prev in list(positions.items()):
            if not isinstance(prev, dict):
                continue
            if prev.get('last_seen_at') == today:
                continue
            stage = str(prev.get('stage') or '')
            if stage not in {'PREPARE', 'PILOT_ENTRY'}:
                continue
            missing_days = self._age(today, prev.get('last_seen_at'))
            prepare_age = self._age(today, prev.get('first_prepare_at'))
            pilot_age = self._age(today, prev.get('first_pilot_at'))
            followup_status = str(prev.get('followup_status') or 'tracking')
            if stage == 'PREPARE' and (prepare_age > self.max_prepare_days or missing_days > self.max_missing_days):
                followup_status = 'expired_cancel_prepare'
            elif stage == 'PILOT_ENTRY' and (pilot_age > self.max_pilot_days or missing_days > self.max_missing_days):
                followup_status = 'expired_cancel_pilot'
            prev['missing_days'] = missing_days
            prev['prepare_age_days'] = prepare_age
            prev['pilot_age_days'] = pilot_age
            prev['followup_status'] = followup_status
            prev['last_evaluated_at'] = today
            if missing_days > self.max_missing_days:
                followup_status = 'failed_cancel_missing_signal'
            if 'expired' in followup_status or 'failed_cancel' in followup_status:
                action = 'BLOCK_NEW_ORDER_AND_CANCEL_WATCH' if stage == 'PREPARE' else 'BLOCK_ADDON_AND_REVIEW_PILOT'
                plan = {'ticker': ticker, 'stage': stage, 'action': action, 'reason': followup_status, 'source': 'entry_tracking_service'}
                action_plan.append(plan)
                events.append({'date': today, 'ticker': ticker, 'from': stage, 'to': stage, 'followup_status': followup_status, 'event': 'rolled_forward_expiry'})
        return events, action_plan

    def build(self) -> tuple[Path, dict[str, Any]]:
        source, df = self._load_decisions()
        today = date.today().isoformat()
        state = self._load_state()
        positions: dict[str, Any] = dict(state.get('positions', {}))
        events: list[dict[str, Any]] = list(state.get('events', []))[-1500:]
        updates: list[dict[str, Any]] = []
        action_plan: list[dict[str, Any]] = []

        roll_events, roll_actions = self._roll_forward_missing(positions, today)
        events.extend(roll_events)
        action_plan.extend(roll_actions)

        if not df.empty:
            for _, row in df.iterrows():
                ticker = self._ticker(row)
                if not ticker:
                    continue
                stage = self._stage_from_row(row)
                if stage == 'WATCH':
                    continue
                prev = positions.get(ticker, {}) if isinstance(positions.get(ticker, {}), dict) else {}
                old_stage = prev.get('stage')
                first_seen = prev.get('first_seen_at') or today
                first_prepare = prev.get('first_prepare_at') or (today if stage == 'PREPARE' else None)
                first_pilot = prev.get('first_pilot_at') or (today if stage == 'PILOT_ENTRY' else None)
                first_full = prev.get('first_full_entry_at') or (today if stage == 'FULL_ENTRY' else None)
                if stage == 'PREPARE' and not first_prepare:
                    first_prepare = today
                if stage == 'PILOT_ENTRY' and not first_pilot:
                    first_pilot = today
                if stage == 'FULL_ENTRY' and not first_full:
                    first_full = today
                prepare_age = self._age(today, first_prepare)
                pilot_age = self._age(today, first_pilot)
                followup_status = 'tracking'
                if stage == 'PREPARE' and prepare_age > self.max_prepare_days:
                    followup_status = 'expired_cancel_prepare'
                elif stage == 'PILOT_ENTRY' and pilot_age > self.max_pilot_days:
                    followup_status = 'expired_cancel_pilot'
                elif stage == 'FULL_ENTRY':
                    followup_status = 'confirmed_entry'
                entry = {
                    'ticker': ticker,
                    'stage': stage,
                    'previous_stage': old_stage,
                    'first_seen_at': first_seen,
                    'first_prepare_at': first_prepare,
                    'first_pilot_at': first_pilot,
                    'first_full_entry_at': first_full,
                    'last_seen_at': today,
                    'last_evaluated_at': today,
                    'missing_days': 0,
                    'prepare_age_days': prepare_age,
                    'pilot_age_days': pilot_age,
                    'followup_status': followup_status,
                    'reference_price': row.get('Reference_Price', row.get('Close', None)),
                    'reason': str(row.get('Reason') or row.get('觸發條件明細') or row.get('Golden_Type') or row.get('Setup_Tag') or ''),
                }
                positions[ticker] = entry
                if 'expired' in followup_status or 'failed_cancel' in followup_status:
                    action_plan.append({'ticker': ticker, 'stage': stage, 'action': 'BLOCK_NEW_ORDER', 'reason': followup_status, 'source': 'entry_tracking_service'})
                if old_stage != stage or followup_status != prev.get('followup_status'):
                    ev = {'date': today, 'ticker': ticker, 'from': old_stage, 'to': stage, 'followup_status': followup_status}
                    events.append(ev)
                    updates.append(entry)
                    append_execution_journal_event('ENTRY_STAGE_UPDATE', source='entry_tracking_service', ticker=ticker, status=followup_status, reason=entry['reason'], stage=stage)

        status = 'entry_tracking_ready' if not df.empty or positions else 'waiting_for_decision_output'
        action_payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'status': 'entry_tracking_action_plan_ready',
            'actions': action_plan,
            'policy': {'block_expired_prepare_or_pilot': True, 'cap_pilot_entry_qty_to_one_third': True},
        }
        self.action_plan_path.write_text(json.dumps(action_payload, ensure_ascii=False, indent=2), encoding='utf-8')
        payload = {
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'module_version': self.MODULE_VERSION,
            'status': status,
            'source': str(source) if source else None,
            'action_plan_json': str(self.action_plan_path),
            'policy': {'max_prepare_days': self.max_prepare_days, 'max_pilot_days': self.max_pilot_days, 'max_missing_days': self.max_missing_days},
            'positions': positions,
            'events': events[-1500:],
            'summary': {
                'tracked_count': len(positions),
                'new_updates': len(updates),
                'action_plan_count': len(action_plan),
                'prepare_count': sum(1 for x in positions.values() if isinstance(x, dict) and x.get('stage') == 'PREPARE'),
                'pilot_count': sum(1 for x in positions.values() if isinstance(x, dict) and x.get('stage') == 'PILOT_ENTRY'),
                'full_entry_count': sum(1 for x in positions.values() if isinstance(x, dict) and x.get('stage') == 'FULL_ENTRY'),
                'expired_count': sum(1 for x in positions.values() if isinstance(x, dict) and 'expired' in str(x.get('followup_status'))),
            },
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = EntryTrackingService().build()
    print(f'🧭 進場追蹤完成：{path} | status={payload.get("status")} tracked={payload.get("summary",{}).get("tracked_count")}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
