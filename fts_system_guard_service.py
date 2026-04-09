# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS
from fts_utils import now_str, log, resolve_decision_csv, safe_float


class SystemGuardService:
    MODULE_VERSION = 'v83_system_guard_service'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'system_guard_service.json'

    def check_model_artifacts(self) -> dict[str, Any]:
        required = [
            PATHS.model_dir / 'selected_features.pkl',
            PATHS.model_dir / 'model_趨勢多頭.pkl',
            PATHS.model_dir / 'model_區間盤整.pkl',
            PATHS.model_dir / 'model_趨勢空頭.pkl',
        ]
        existing = [p.name for p in required if p.exists()]
        health = 'OK' if len(existing) >= 2 else ('WARN' if existing else 'FAIL')
        return {'health': health, 'existing': existing, 'required_count': len(required), 'existing_count': len(existing)}

    def check_decision_desk(self) -> dict[str, Any]:
        path = resolve_decision_csv()
        if not path.exists():
            return {'health': 'FAIL', 'rows': 0, 'message': '找不到決策桌', 'path': str(path)}
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(path)
        if df.empty:
            return {'health': 'FAIL', 'rows': 0, 'message': '決策桌為空', 'path': str(path)}
        avg_ai = safe_float(pd.to_numeric(df.get('AI_Proba', pd.Series([0.5]*len(df))), errors='coerce').mean(), 0.5)
        avg_ev = safe_float(pd.to_numeric(df.get('Realized_EV', pd.Series([0.0]*len(df))), errors='coerce').mean(), 0.0)
        avg_gap = safe_float(pd.to_numeric(df.get('Score_Gap', pd.Series([0.0]*len(df))), errors='coerce').mean(), 0.0)
        health = 'OK'
        message = '決策桌正常'
        if len(df) < 3 or avg_ai < 0.50 or avg_ev <= 0 or avg_gap <= 0:
            health = 'WARN'
            message = '決策桌品質偏弱，但未到阻斷'
        return {'health': health, 'rows': int(len(df)), 'avg_ai_proba': round(avg_ai, 4), 'avg_realized_ev': round(avg_ev, 4), 'avg_score_gap': round(avg_gap, 4), 'message': message, 'path': str(path)}

    def check_recent_trades(self, limit: int = 100) -> dict[str, Any]:
        candidates = [PATHS.base_dir / 'trade_stats.csv', PATHS.data_dir / 'trade_stats.csv']
        path = next((p for p in candidates if p.exists()), None)
        if path is None:
            return {'health': 'WARN', 'sample_size': 0, 'message': '近期無成交報表'}
        try:
            df = pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(path)
        if df.empty:
            return {'health': 'WARN', 'sample_size': 0, 'message': '近期成交表為空'}
        df = df.tail(limit)
        ret_col = '報酬率(%)' if '報酬率(%)' in df.columns else ('return_pct' if 'return_pct' in df.columns else None)
        if ret_col is None:
            return {'health': 'WARN', 'sample_size': int(len(df)), 'message': '近期成交表缺少報酬欄位'}
        ret = pd.to_numeric(df[ret_col], errors='coerce').dropna()
        if ret.empty:
            return {'health': 'WARN', 'sample_size': 0, 'message': '近期報酬缺失'}
        win_rate = float((ret > 0).mean())
        avg_return = float(ret.mean())
        health = 'OK' if win_rate >= 0.30 and avg_return >= -0.20 else 'WARN'
        return {'health': health, 'sample_size': int(len(ret)), 'win_rate': round(win_rate, 4), 'avg_return': round(avg_return, 4), 'path': str(path)}

    def evaluate_system_guard(self) -> dict[str, Any]:
        model_status = self.check_model_artifacts()
        desk_status = self.check_decision_desk()
        trade_status = self.check_recent_trades()
        alerts = []
        block_builds = False
        if model_status['health'] == 'FAIL':
            alerts.append('模型核心檔案不足，禁止建倉')
            block_builds = True
        if desk_status['health'] == 'FAIL':
            alerts.append('決策桌失敗或空白，禁止建倉')
            block_builds = True
        overall = 'BLOCK' if block_builds else ('WARN' if alerts or desk_status['health'] == 'WARN' or trade_status['health'] == 'WARN' else 'OK')
        return {
            'timestamp': now_str(),
            'overall': overall,
            'block_new_positions': block_builds,
            'alerts': alerts,
            'model_status': model_status,
            'decision_desk': desk_status,
            'recent_trades': trade_status,
        }

    def format_alert_message(self, payload: dict[str, Any]) -> str:
        lines = [
            '🚨【系統自我保護告警】',
            f"時間：{payload['timestamp']}",
            f"總體狀態：{payload['overall']}",
            f"是否阻止建倉：{'是' if payload['block_new_positions'] else '否'}",
            '-' * 20,
        ]
        if payload['alerts']:
            lines.extend([f'• {msg}' for msg in payload['alerts']])
        else:
            lines.append('• 無異常')
        return '\n'.join(lines)

    def build_summary(self) -> tuple[Path, dict[str, Any]]:
        payload = self.evaluate_system_guard()
        payload.update({'module_version': self.MODULE_VERSION, 'status': 'wave2_system_guard_ready'})
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🛡️ system guard ready: {self.runtime_path}')
        return self.runtime_path, payload
