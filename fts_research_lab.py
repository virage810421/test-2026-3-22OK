# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS
from fts_utils import now_str


class ResearchLab:
    """Research-only artifact store. Never writes directly to production config or production model files."""

    def __init__(self):
        self.root = PATHS.runtime_dir / 'research_lab'
        self.root.mkdir(parents=True, exist_ok=True)

    def area(self, name: str) -> Path:
        p = self.root / str(name)
        p.mkdir(parents=True, exist_ok=True)
        return p

    def registry_path(self, name: str) -> Path:
        return self.area(name) / 'registry.json'

    def load_registry(self, name: str) -> list[dict[str, Any]]:
        path = self.registry_path(name)
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding='utf-8'))
            return data if isinstance(data, list) else []
        except Exception:
            return []

    def append_registry(self, name: str, entry: dict[str, Any]) -> Path:
        rows = self.load_registry(name)
        rows.append(entry)
        path = self.registry_path(name)
        path.write_text(json.dumps(rows, ensure_ascii=False, indent=2), encoding='utf-8')
        return path

    def latest(self, name: str) -> dict[str, Any] | None:
        rows = self.load_registry(name)
        return rows[-1] if rows else None

    def write_json_artifact(self, area: str, filename: str, payload: dict[str, Any]) -> Path:
        path = self.area(area) / filename
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path

    def write_text_artifact(self, area: str, filename: str, body: str) -> Path:
        path = self.area(area) / filename
        path.write_text(body, encoding='utf-8')
        return path

    def write_csv_artifact(self, area: str, filename: str, df) -> Path:
        path = self.area(area) / filename
        df.to_csv(path, index=False, encoding='utf-8-sig')
        return path

    def summary(self) -> dict[str, Any]:
        areas = {}
        for child in sorted(self.root.iterdir()):
            if child.is_dir():
                reg = child / 'registry.json'
                count = 0
                if reg.exists():
                    try:
                        data = json.loads(reg.read_text(encoding='utf-8'))
                        count = len(data) if isinstance(data, list) else 0
                    except Exception:
                        count = 0
                areas[child.name] = {'path': str(child), 'registry_count': count}
        return {
            'generated_at': now_str(),
            'root': str(self.root),
            'areas': areas,
            'area_count': len(areas),
        }
