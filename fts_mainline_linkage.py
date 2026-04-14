# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path

from fts_cross_sectional_percentile_service import CrossSectionalPercentileService
from fts_event_calendar_service import EventCalendarService
from fts_feature_stack_audit import FeatureStackAudit
from fts_project_quality_suite import ProjectCompletionAudit
from fts_operations_suite import TaskCompletionRegistry

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
    PATHS = _Paths()


class MainlineLinkage:
    def __init__(self):
        self.runtime_path = Path(PATHS.runtime_dir) / 'mainline_linkage.json'
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)

    def build(self):
        cp, _ = CrossSectionalPercentileService().build_summary()
        ep, _ = EventCalendarService().build_summary()
        fp, _ = FeatureStackAudit().build()
        tp, _ = TaskCompletionRegistry().build()
        ap, _ = ProjectCompletionAudit().build()
        payload = {
            'cross': str(cp), 'event': str(ep), 'feature_stack': str(fp),
            'task_registry': str(tp), 'project_audit': str(ap),
            'status': 'mainline_linkage_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload
