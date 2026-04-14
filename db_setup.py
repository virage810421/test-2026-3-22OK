# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from fts_db_migrations import MigrationRunner
from fts_db_schema import SCHEMA_VERSION

try:
    from fts_utils import log, now_str  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)


"""
db_setup.py 相容入口。

正式規則：
- schema 定義只在 fts_db_schema.py。
- schema 升級只由 fts_db_migrations.py / MigrationRunner 執行。
- 本檔不再維護第二套 CREATE TABLE，避免 schema 雙軌。
"""


def run_upgrade() -> tuple[Path, dict[str, Any]]:
    log('🧱 db_setup wrapper：委派 fts_db_migrations.py upgrade')
    return MigrationRunner().upgrade()


def run_reset_upgrade() -> tuple[Path, dict[str, Any]]:
    log('🧱 db_setup wrapper：委派 fts_db_migrations.py reset')
    return MigrationRunner().reset_then_upgrade()


def setup_database(mode: str = 'upgrade') -> tuple[Path, dict[str, Any]]:
    if mode in {'reset', 'rebuild'}:
        return run_reset_upgrade()
    return run_upgrade()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='DB setup 相容入口；正式 schema 主權已移至 fts_db_migrations.py')
    parser.add_argument('--mode', default='upgrade', choices=['upgrade', 'reset', 'rebuild', 'status'])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.mode == 'status':
        path = MigrationRunner().runtime_path
        payload = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {
            'generated_at': now_str(),
            'status': 'not_run',
            'schema_version': SCHEMA_VERSION,
            'schema_owner': 'fts_db_schema.py + fts_db_migrations.py',
        }
    else:
        path, payload = setup_database(args.mode)
    print(json.dumps({'path': str(path), **payload}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') in {'db_upgrade_ready', 'db_reset_upgrade_ready', 'not_run'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
