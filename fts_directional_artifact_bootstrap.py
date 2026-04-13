
# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib

try:
    from fts_prelive_runtime import PATHS, now_str, write_json
except Exception:
    from pathlib import Path
    def now_str():
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    def write_json(path: Path, payload: Any) -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path
    class _Paths:
        model_dir = Path('models')
        runtime_dir = Path('runtime')
    PATHS = _Paths()

LANES = ['LONG', 'SHORT', 'RANGE']
REGIMES = ['趨勢多頭', '區間盤整', '趨勢空頭']
REPORT = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'directional_artifact_bootstrap.json'


def bootstrap_directional_artifacts(force: bool = False) -> tuple[str, dict[str, Any]]:
    model_dir = Path(getattr(PATHS, 'model_dir', Path('models')))
    model_dir.mkdir(parents=True, exist_ok=True)
    shared_feat = model_dir / 'selected_features.pkl'
    if not shared_feat.exists():
        payload = {'generated_at': now_str(), 'status': 'shared_selected_features_missing', 'lanes': {}}
        write_json(REPORT, payload)
        return str(REPORT), payload
    try:
        shared_features = [str(x) for x in joblib.load(shared_feat) if str(x).strip()]
    except Exception:
        shared_features = []
    payload = {'generated_at': now_str(), 'status': 'directional_artifacts_bootstrapped', 'lanes': {}}
    for lane in LANES:
        lane_info = {'features': False, 'models': []}
        lane_feat = model_dir / f'selected_features_{lane.lower()}.pkl'
        if force or not lane_feat.exists():
            joblib.dump(shared_features, lane_feat)
            lane_info['features'] = True
        for regime in REGIMES:
            shared_model = model_dir / f'model_{regime}.pkl'
            lane_model = model_dir / f'model_{lane.lower()}_{regime}.pkl'
            if shared_model.exists() and (force or not lane_model.exists()):
                try:
                    model = joblib.load(shared_model)
                    joblib.dump(model, lane_model)
                    lane_info['models'].append({'regime': regime, 'status': 'copied_from_shared'})
                except Exception as e:
                    lane_info['models'].append({'regime': regime, 'status': 'copy_failed', 'error': str(e)})
            elif lane_model.exists():
                lane_info['models'].append({'regime': regime, 'status': 'present'})
        payload['lanes'][lane] = lane_info
    write_json(REPORT, payload)
    return str(REPORT), payload


if __name__ == '__main__':
    print(bootstrap_directional_artifacts())
