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
                'status': 'live_signal_allowed' if bool(promotion_ready and promoted) else 'live_signal_blocked',
            }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.path, payload


def main() -> int:
    path, payload = ModelLiveSignalGate().build()
    print(f'🚦 model live signal gate：{path} | status={payload.get("status")}')
    return 0 if payload.get('status') == 'live_signal_allowed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
