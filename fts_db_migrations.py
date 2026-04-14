# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from fts_db_engine import DBConfig, DatabaseSession
from fts_db_schema import SCHEMA_VERSION, iter_table_specs, TableSpec

try:
    from fts_runtime_diagnostics import record_issue
except Exception:  # pragma: no cover - runtime diagnostics import fallback; migration diagnostics must not block bootstrap
    def record_issue(*args, **kwargs):
        return {}


try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover - runtime diagnostics import fallback
    pyodbc = None

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover - runtime diagnostics import fallback
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover - runtime diagnostics import fallback
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)


EXECUTION_TICKER_COLUMN_TABLES = (
    'execution_orders',
    'execution_fills',
    'execution_positions_snapshot',
    'execution_position_lots',
    'execution_broker_callbacks',
)

# LEGACY_SYMBOL_MIGRATION_COMPAT_MARKER: Ticker SYMBOL references below are migration/backfill compatibility only.
# 舊持倉/交易紀錄表保留 [Ticker SYMBOL]，但補 ticker_symbol alias。
LEGACY_TICKER_ALIAS_TABLES = (
    'active_positions',
    'trade_history',
)


class MigrationRunner:
    """
    正式 schema 主權入口。

    主權規則：
    1. fts_db_schema.py 定義表結構。
    2. fts_db_migrations.py 負責 upgrade / reset / schema_migrations 記錄。
    3. db_setup.py 只保留為相容 wrapper，不再維護第二套 CREATE TABLE。
    """

    MODULE_VERSION = 'v20260414_single_schema_migration_mainline_v3_tax_lot_washsale'

    def __init__(self, config: DBConfig | None = None):
        self.config = config or DBConfig()
        self.runtime_path = Path(getattr(PATHS, 'runtime_dir', Path('runtime'))) / 'db_migration_status.json'

    @staticmethod
    def _create_table_sql(spec: TableSpec) -> str:
        cols = [c.ddl() for c in spec.columns]
        pks = spec.primary_keys
        if pks:
            cols.append('CONSTRAINT PK_' + spec.name + ' PRIMARY KEY (' + ', '.join(f'[{k}]' for k in pks) + ')')
        joined = ',\n            '.join(cols)
        return f"""
        CREATE TABLE dbo.{spec.name} (
            {joined}
        )
        """.strip()

    def _ensure_database_exists(self) -> bool:
        """
        讓 fts_db_migrations.py 可以獨立作為 bootstrap 第一站。
        若 pyodbc 不存在，交給 DatabaseSession 回報明確錯誤。
        """
        if pyodbc is None:
            return False
        try:
            master_conn = (
                f"DRIVER={{{self.config.driver}}};"
                f"SERVER={self.config.server};"
                f"DATABASE=master;"
                f"Trusted_Connection={self.config.trusted_connection};"
            )
            with pyodbc.connect(master_conn, autocommit=True) as conn:
                cur = conn.cursor()
                cur.execute(
                    "IF DB_ID(?) IS NULL EXEC('CREATE DATABASE ' + QUOTENAME(?))",
                    self.config.database,
                    self.config.database,
                )
            return True
        except Exception as exc:
            record_issue('db_migrations', 'ensure_database_exists_failed', exc, severity='WARNING', fail_mode='fail_open', context={'database': self.config.database})
            log(f'⚠️ ensure database skipped/failed: {exc}')
            return False

    def _ensure_migrations_table(self, db: DatabaseSession) -> None:
        db.execute("""
        IF OBJECT_ID(N'dbo.schema_migrations', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.schema_migrations (
                [version] NVARCHAR(128) NOT NULL PRIMARY KEY,
                [applied_at] DATETIME NOT NULL,
                [note] NVARCHAR(255) NULL
            )
        END
        """)

    def _record_version(self, db: DatabaseSession) -> None:
        db.execute("""
        MERGE dbo.schema_migrations AS tgt
        USING (SELECT ? AS [version]) AS src
        ON tgt.[version] = src.[version]
        WHEN MATCHED THEN UPDATE SET [applied_at]=GETDATE(), [note]=?
        WHEN NOT MATCHED THEN INSERT ([version],[applied_at],[note]) VALUES (?,GETDATE(),?);
        """, [SCHEMA_VERSION, 'single_schema_execution_ticker_symbol_tax_lot_washsale', SCHEMA_VERSION, 'single_schema_execution_ticker_symbol_tax_lot_washsale'])

    def _normalize_execution_ticker_columns(self, db: DatabaseSession) -> list[str]:
        """
        正規化 execution domain 欄位：正式欄位一律為 ticker_symbol。
        若舊表已存在 [Ticker SYMBOL]：
        - ticker_symbol 不存在：直接 sp_rename。
        - ticker_symbol 已存在：先補值，再保留舊欄位不強制 drop，避免破壞舊查詢。
        """
        actions: list[str] = []
        for table in EXECUTION_TICKER_COLUMN_TABLES:
            if not db.table_exists(table):
                continue
            has_old = db.column_exists(table, 'Ticker SYMBOL')
            has_new = db.column_exists(table, 'ticker_symbol')
            if has_old and not has_new:
                try:
                    db.execute(f"EXEC sp_rename 'dbo.{table}.[Ticker SYMBOL]', 'ticker_symbol', 'COLUMN'")
                    actions.append(f'{table}:renamed_Ticker_SYMBOL_to_ticker_symbol')
                except Exception as exc:
                    record_issue('db_migrations', 'rename_legacy_ticker_symbol_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'table': table})
                    actions.append(f'{table}:rename_failed:{type(exc).__name__}:{exc}')
            elif has_old and has_new:
                try:
                    db.execute(f"""
                    UPDATE dbo.{table}
                    SET ticker_symbol = COALESCE(ticker_symbol, [Ticker SYMBOL])
                    WHERE ticker_symbol IS NULL AND [Ticker SYMBOL] IS NOT NULL
                    """)
                    actions.append(f'{table}:copied_old_Ticker_SYMBOL_into_ticker_symbol')
                except Exception as exc:
                    record_issue('db_migrations', 'copy_legacy_ticker_symbol_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'table': table})
                    actions.append(f'{table}:copy_failed:{type(exc).__name__}:{exc}')
            elif has_new:
                actions.append(f'{table}:ticker_symbol_ready')
        return actions

    def _normalize_legacy_symbol_alias_columns(self, db: DatabaseSession) -> list[str]:
        """
        非破壞性升級舊表 active_positions / trade_history：
        - 保留 [Ticker SYMBOL]，避免破壞既有 SQL / 報表 / CSV 相容。
        - 新增 ticker_symbol alias，讓 execution runtime 可以用正式 contract。
        - 每次 migration 都把舊欄位值補進新欄位。
        """
        actions: list[str] = []
        for table in LEGACY_TICKER_ALIAS_TABLES:
            if not db.table_exists(table):
                continue
            try:
                if not db.column_exists(table, 'ticker_symbol'):
                    db.execute(f"ALTER TABLE dbo.{table} ADD [ticker_symbol] VARCHAR(20) NULL")
                    actions.append(f'{table}:added_ticker_symbol_alias')
                if db.column_exists(table, 'Ticker SYMBOL'):
                    db.execute(f"""
                    UPDATE dbo.{table}
                    SET [ticker_symbol] = COALESCE(NULLIF([ticker_symbol], ''), [Ticker SYMBOL])
                    WHERE [Ticker SYMBOL] IS NOT NULL
                      AND (ticker_symbol IS NULL OR ticker_symbol = '')
                    """)
                    actions.append(f'{table}:backfilled_ticker_symbol_from_Ticker_SYMBOL')
                else:
                    actions.append(f'{table}:legacy_Ticker_SYMBOL_missing')
            except Exception as exc:
                record_issue('db_migrations', 'legacy_symbol_alias_backfill_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'table': table})
                actions.append(f'{table}:legacy_alias_failed:{type(exc).__name__}:{exc}')
        return actions

    def upgrade(self) -> tuple[Path, dict[str, Any]]:
        self._ensure_database_exists()
        created: list[str] = []
        altered: list[str] = []
        normalized: list[str] = []
        legacy_aliases: list[str] = []
        with DatabaseSession(self.config) as db:
            self._ensure_migrations_table(db)
            normalized.extend(self._normalize_execution_ticker_columns(db))
            legacy_aliases.extend(self._normalize_legacy_symbol_alias_columns(db))
            for spec in iter_table_specs():
                if not db.table_exists(spec.name):
                    db.execute(self._create_table_sql(spec))
                    created.append(spec.name)
                for col in spec.columns:
                    if not db.column_exists(spec.name, col.name):
                        db.execute(f"ALTER TABLE dbo.{spec.name} ADD {col.ddl()}")
                        altered.append(f'{spec.name}.{col.name}')
            normalized.extend(self._normalize_execution_ticker_columns(db))
            legacy_aliases.extend(self._normalize_legacy_symbol_alias_columns(db))
            self._record_version(db)
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'schema_version': SCHEMA_VERSION,
            'schema_source_of_truth': 'fts_db_schema.py',
            'migration_entrypoint': 'fts_db_migrations.py',
            'execution_ticker_column': 'ticker_symbol',
            'created_tables': created,
            'altered_columns': altered,
            'normalized_columns': normalized,
            'legacy_symbol_alias_columns': legacy_aliases,
            'legacy_tables_keep_Ticker_SYMBOL': list(LEGACY_TICKER_ALIAS_TABLES),
            'status': 'db_upgrade_ready',
        }
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🗃️ db migration status：{self.runtime_path}')
        return self.runtime_path, payload

    def reset_then_upgrade(self) -> tuple[Path, dict[str, Any]]:
        self._ensure_database_exists()
        dropped: list[str] = []
        with DatabaseSession(self.config) as db:
            for spec in reversed(tuple(iter_table_specs())):
                if db.table_exists(spec.name):
                    db.execute(f"DROP TABLE dbo.{spec.name}")
                    dropped.append(spec.name)
        path, payload = self.upgrade()
        payload['dropped_tables'] = dropped
        payload['status'] = 'db_reset_upgrade_ready'
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path, payload


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='FTS 正式 schema migration 主線')
    parser.add_argument('command', nargs='?', default='upgrade', choices=['upgrade', 'reset', 'status'])
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    runner = MigrationRunner()
    if args.command == 'reset':
        path, payload = runner.reset_then_upgrade()
    elif args.command == 'status':
        path = runner.runtime_path
        payload = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {
            'status': 'not_run',
            'schema_version': SCHEMA_VERSION,
            'runtime_path': str(path),
        }
    else:
        path, payload = runner.upgrade()
    print(json.dumps({'path': str(path), **payload}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') in {'db_upgrade_ready', 'db_reset_upgrade_ready', 'not_run'} else 1


if __name__ == '__main__':
    raise SystemExit(main())
