# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
    PATHS = _Paths()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)

from fts_feature_service import FeatureService
from fts_feature_catalog import FEATURE_BUCKETS, PRIORITY_NEW_FEATURES_20, FEATURE_SPECS

try:
    from fts_cross_sectional_percentile_service import CrossSectionalPercentileService
except Exception:  # pragma: no cover
    CrossSectionalPercentileService = None

try:
    from fts_event_calendar_service import EventCalendarService
except Exception:  # pragma: no cover
    EventCalendarService = None


class FeatureStackAudit:
    MODULE_VERSION = 'v83_feature_stack_audit_hotfix_compat'

    def __init__(self):
        self.runtime_path = Path(PATHS.runtime_dir) / 'feature_stack_audit.json'
        Path(PATHS.runtime_dir).mkdir(parents=True, exist_ok=True)
        self.features = FeatureService()
        self.cross = CrossSectionalPercentileService() if CrossSectionalPercentileService else None
        self.events = EventCalendarService() if EventCalendarService else None

    @staticmethod
    def _is_percentile_backed(spec) -> bool:
        explicit = getattr(spec, 'percentile_backed', None)
        if explicit is not None:
            return bool(explicit)
        name = getattr(spec, 'name', '')
        source = str(getattr(spec, 'source', '')).lower()
        return (
            name.endswith('_Pctl')
            or name in {'Revenue_YoY_Rank', 'Chip_Total_Ratio_Rank'}
            or 'percentile' in source
        )

    @staticmethod
    def _is_precise_event(spec) -> bool:
        explicit = getattr(spec, 'event_calendar_precise', None)
        if explicit is not None:
            return bool(explicit)
        bucket = getattr(spec, 'bucket', '')
        source = str(getattr(spec, 'source', '')).lower()
        name = getattr(spec, 'name', '')
        return (
            bucket == 'events'
            or 'calendar' in source
            or name.startswith('Event_')
            or name.endswith('_Window_Flag')
            or '_Window_' in name
        )

    def build(self):
        selected = self.features.load_selected_features()
        cross_payload = {}
        event_payload = {}
        cross_path = ''
        event_path = ''
        if self.cross is not None:
            cross_path, cross_payload = self.cross.build_summary()
        if self.events is not None:
            event_path, event_payload = self.events.build_summary()
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'bucket_count': len(FEATURE_BUCKETS),
            'feature_buckets': {k: len(v) for k, v in FEATURE_BUCKETS.items()},
            'selected_features_present': bool(selected),
            'selected_feature_count': len(selected),
            'selected_features_sample': selected[:20],
            'priority_new_features_20': PRIORITY_NEW_FEATURES_20,
            'percentile_backed_feature_count': int(sum(1 for s in FEATURE_SPECS.values() if self._is_percentile_backed(s))),
            'precise_event_feature_count': int(sum(1 for s in FEATURE_SPECS.values() if self._is_precise_event(s))),
            'official_percentile_mode': bool(cross_payload.get('official_percentile_mode', False)) if cross_payload else False,
            'precise_event_calendar_mode': bool(event_payload.get('status')) if event_payload else False,
            'live_mount_path': str(getattr(self.features, 'live_mount_path', '')),
            'cross_sectional_runtime': str(cross_path),
            'event_calendar_runtime': str(event_path),
            'status': 'feature_stack_audit_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧪 feature stack audit ready: {self.runtime_path}')
        return self.runtime_path, payload
