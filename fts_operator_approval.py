# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class OperatorApprovalRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'operator_approval_registry.json'

    def _load(self) -> dict[str, Any]:
        payload = load_json(self.path, {}) or {}
        if 'approvals' not in payload:
            payload = {'approvals': []}
        return payload

    def approve(self, stage: str, operator: str, approved: bool, note: str = '') -> tuple[str, dict[str, Any]]:
        payload = self._load()
        item = {
            'ts': now_str(),
            'stage': stage,
            'operator': operator,
            'approved': bool(approved),
            'note': note,
        }
        payload['approvals'].append(item)
        payload['last'] = item
        payload['status'] = 'approval_recorded'
        write_json(self.path, payload)
        return str(self.path), payload

    def latest_for(self, stage: str) -> dict[str, Any]:
        payload = self._load()
        for item in reversed(payload.get('approvals', [])):
            if item.get('stage') == stage:
                return item
        return {}
