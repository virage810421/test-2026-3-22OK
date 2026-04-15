# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_config import PATHS


class ModelLiveSignalGate:
    MODULE_VERSION = 'v94_model_live_signal_gate_builder'

    def __init__(self) -> None:
        self.path = PATHS.runtime_dir / 'model_live_signal_gate.json'
        self.backend_path = PATHS.runtime_dir / 'trainer_backend_report.json'

    @staticmethod
    def _read_json(path: Path) -> dict[str, Any]:
        if not path.exists():
            return {}
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _suggest_remediation(self, payload: dict[str, Any]) -> list[str]:
        out = []
        promotion = payload.get('promotion', {}) if isinstance(payload.get('promotion'), dict) else {}
        failures = list(promotion.get('failures', []) or [])
        if 'oot_profit_factor_below_floor' in failures:
            out.append('先重建乾淨訓練集，再提升 OOT profit factor。')
        if 'oot_hit_rate_below_floor' in failures:
            out.append('優先改善樣本外 hit rate，再考慮放行。')
        if 'walk_forward_return_below_floor' in failures:
            out.append('先降低過擬合，改善 walk-forward 平均報酬。')
        if not out and not payload.get('allow_live_signal', False):
            out.append('先檢查 promotion failures 與 trainer backend report。')
        return out

    def build(self) -> tuple[Path, dict[str, Any]]:
        existing = self._read_json(self.path)
        backend = self._read_json(self.backend_path)
        if existing:
            payload = dict(existing)
            payload.setdefault('generated_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            payload.setdefault('module_version', self.MODULE_VERSION)
            payload.setdefault('status', 'live_signal_allowed' if payload.get('allow_live_signal') else 'live_signal_blocked')
        else:
            promotion = backend.get('promotion', {}) if isinstance(backend.get('promotion'), dict) else {}
            promoted = str(promotion.get('status') or '') == 'promoted_best'
            promotion_ready = bool(backend.get('promotion_ready', False))
            blocked_reason = (
                backend.get('blocked_reason_category')
                or backend.get('reason')
                or promotion.get('reason')
                or ((promotion.get('failures') or ['trainer_backend_report_missing'])[0] if isinstance(promotion.get('failures'), list) else 'trainer_backend_report_missing')
                if backend else 'trainer_backend_report_missing'
            )
            payload = {
                'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'module_version': self.MODULE_VERSION,
                'allow_live_signal': bool(promotion_ready and promoted),
                'promotion_ready': bool(promotion_ready),
                'promoted_current_candidate': bool(promoted),
                'reason': str(blocked_reason),
                'promotion': promotion,
                'out_of_time': backend.get('out_of_time', {}),
                'walk_forward_summary': backend.get('walk_forward_summary', {}),
                'overall_score': float(backend.get('overall_score', 0.0) or 0.0),
                'status': 'live_signal_allowed' if bool(promotion_ready and promoted) else 'live_signal_blocked',
            }
        payload['remediation'] = self._suggest_remediation(payload)
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = ModelLiveSignalGate().build()
    print(f'🚦 model live signal gate：{path} | status={payload.get("status")}')
    return 0 if payload.get('status') == 'live_signal_allowed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
