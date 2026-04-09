# -*- coding: utf-8 -*-
from __future__ import annotations

import ast
import importlib
import json
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class FundamentalsETLMainline:
    """v83 fundamentals ETL mainline.
    將 legacy `yahoo_csv_to_sql.py` 納入主線治理，但仍保留原檔作為相容入口。
    預設只做盤點與收編，不自動觸發大量網路下載。
    """

    MODULE_VERSION = "v83_fundamentals_etl_mainline"

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / "fundamentals_etl_mainline.json"
        self.compat_module_name = "yahoo_csv_to_sql"
        self.compat_source_path = PATHS.base_dir / "yahoo_csv_to_sql.py"

    def _import_legacy(self):
        try:
            module = importlib.import_module(self.compat_module_name)
            return module, None
        except Exception as exc:
            return None, str(exc)

    def _inspect_legacy_source(self) -> dict[str, Any]:
        if not self.compat_source_path.exists():
            return {"source_exists": False, "table_name": None, "csv_filename": None, "function_names": []}
        text = self.compat_source_path.read_text(encoding="utf-8", errors="ignore")
        try:
            tree = ast.parse(text)
        except Exception:
            return {"source_exists": True, "parse_ok": False, "table_name": None, "csv_filename": None, "function_names": []}

        table_name = None
        csv_filename = None
        functions = []
        for node in tree.body:
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name) and target.id == "TABLE_NAME":
                        if isinstance(node.value, ast.Constant):
                            table_name = node.value.value
                    if isinstance(target, ast.Name) and target.id == "CSV_FILENAME":
                        if isinstance(node.value, ast.Constant):
                            csv_filename = node.value.value
            elif isinstance(node, ast.FunctionDef):
                functions.append(node.name)
        return {
            "source_exists": True,
            "parse_ok": True,
            "table_name": table_name,
            "csv_filename": csv_filename,
            "function_names": functions,
        }

    def _find_existing_backups(self) -> list[dict[str, Any]]:
        candidates: list[Path] = []
        for folder in [PATHS.base_dir, PATHS.data_dir]:
            for pattern in ("market_financials_backup*.csv", "quarterly_fundamentals*.csv", "*financials*.csv"):
                candidates.extend(folder.glob(pattern))
        rows = []
        seen = set()
        for path in sorted(candidates):
            if path in seen:
                continue
            seen.add(path)
            info = {
                "path": str(path),
                "exists": path.exists(),
                "size_bytes": path.stat().st_size if path.exists() else 0,
                "rows": 0,
                "columns": [],
            }
            if path.exists():
                try:
                    df = pd.read_csv(path, nrows=5, encoding="utf-8-sig")
                except Exception:
                    try:
                        df = pd.read_csv(path, nrows=5)
                    except Exception:
                        df = None
                if df is not None:
                    info["rows"] = int(len(df))
                    info["columns"] = [str(c) for c in df.columns.tolist()]
            rows.append(info)
        return rows

    def build_summary(self, mode: str = "summary") -> tuple[Path, dict[str, Any]]:
        legacy, import_error = self._import_legacy()
        source_meta = self._inspect_legacy_source()
        backups = self._find_existing_backups()
        callable_names = []
        if legacy is not None:
            for name in ["main", "ensure_sql_table", "get_official_stock_list", "download_single_stock_financials", "import_csv_to_sql"]:
                if hasattr(legacy, name):
                    callable_names.append(name)
        else:
            callable_names = source_meta.get("function_names", [])

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "system_name": CONFIG.system_name,
            "legacy_module": self.compat_module_name,
            "legacy_import_ok": legacy is not None,
            "legacy_import_error": import_error,
            "legacy_source_path": str(self.compat_source_path),
            "legacy_source_inspection": source_meta,
            "legacy_table_name": (getattr(legacy, "TABLE_NAME", None) if legacy else None) or source_meta.get("table_name"),
            "legacy_csv_filename": (getattr(legacy, "CSV_FILENAME", None) if legacy else None) or source_meta.get("csv_filename"),
            "legacy_entrypoints": callable_names,
            "existing_backups": backups,
            "merge_status": "merged_into_mainline",
            "safe_default_mode": mode,
            "note": "v83 主線已吸收 yahoo_csv_to_sql.py 的 ETL 角色；原檔暫保留為相容腳本。",
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"📚 fundamentals ETL 主線盤點完成：{self.runtime_path}")
        return self.runtime_path, payload


def main() -> int:
    FundamentalsETLMainline().build_summary()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
