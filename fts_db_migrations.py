# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

from fts_db_engine import DBConfig, DatabaseSession
from fts_db_schema import SCHEMA_VERSION, iter_table_specs, TableSpec
from fts_sql_chinese_column_views import ensure_chinese_column_views

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

# fundamentals_clean 是基本面正式主表。這些名稱是舊版或誤建常見別名，
# migration 會把資料安全收斂到 dbo.fundamentals_clean，避免 Table/View 雙軌。
CANONICAL_FUNDAMENTALS_TABLE = 'fundamentals_clean'
LEGACY_FUNDAMENTALS_ALIAS_TABLES = (
    'damendals_clean',
    'damental_clean',
    'fundamental_clean',
    'fundamental_data',
    'fundamentals_data',
)


class MigrationRunner:
    """
    正式 schema 主權入口。

    主權規則：
    1. fts_db_schema.py 定義表結構。
    2. fts_db_migrations.py 負責 upgrade / reset / schema_migrations 記錄。
    3. db_setup.py 只保留為相容 wrapper，不再維護第二套 CREATE TABLE。
    """

    MODULE_VERSION = 'v20260415_v5_autosync_canonical_fundamentals_views'

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
            database_name = str(self.config.database or '').strip()
            if not database_name:
                raise ValueError('database name is empty')
            # SQL Server identifiers cannot be passed as parameters.
            # Avoid QUOTENAME inside dynamic SQL here because some ODBC / SQL Server
            # combinations fail to prepare that expression. We safely bracket-escape
            # the identifier in Python and only use a Unicode literal for DB_ID().
            db_literal = "N'" + database_name.replace("'", "''") + "'"
            db_identifier = '[' + database_name.replace(']', ']]') + ']'
            create_sql = (
                f"IF DB_ID({db_literal}) IS NULL\n"
                "BEGIN\n"
                f"    EXEC(N'CREATE DATABASE {db_identifier}');\n"
                "END"
            )
            with pyodbc.connect(master_conn, autocommit=True) as conn:
                cur = conn.cursor()
                cur.execute(create_sql)
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

    def _safe_table_count(self, db: DatabaseSession, table_name: str) -> int | None:
        try:
            return int(db.scalar(f"SELECT COUNT(1) FROM dbo.{self._quote_ident(table_name)}") or 0)
        except Exception as exc:
            record_issue('db_migrations', 'table_count_failed', exc, severity='WARNING', fail_mode='fail_open', context={'table': table_name})
            return None

    def _table_columns(self, db: DatabaseSession, table_name: str) -> list[str]:
        try:
            rows = db.execute("""
                SELECT c.name
                FROM sys.columns AS c
                JOIN sys.tables AS t ON t.object_id = c.object_id
                JOIN sys.schemas AS s ON s.schema_id = t.schema_id
                WHERE s.name = N'dbo' AND t.name = ?
                ORDER BY c.column_id
            """, [table_name]).fetchall()
            return [str(r[0]) for r in rows if r and r[0]]
        except Exception as exc:
            record_issue('db_migrations', 'list_table_columns_failed', exc, severity='WARNING', fail_mode='fail_open', context={'table': table_name})
            return []

    @staticmethod
    def _quote_ident(name: str) -> str:
        return '[' + str(name).replace(']', ']]') + ']'

    def _copy_legacy_fundamentals_rows(self, db: DatabaseSession, alias_table: str) -> dict[str, Any]:
        """
        將誤建/舊名基本面表的資料收斂到 fundamentals_clean。
        - 只複製兩邊都存在的欄位。
        - 若有 [Ticker SYMBOL] + [資料年月日]，用這組鍵避免重複。
        - 若沒有足夠 key，只在 fundamentals_clean 空表時全量搬入，避免重複灌資料。
        """
        canonical = CANONICAL_FUNDAMENTALS_TABLE
        alias_cols = self._table_columns(db, alias_table)
        canonical_cols = self._table_columns(db, canonical)
        common_cols = [c for c in alias_cols if c in set(canonical_cols)]
        if not common_cols:
            return {'alias_table': alias_table, 'copied': False, 'reason': 'no_common_columns'}

        alias_count = self._safe_table_count(db, alias_table)
        canonical_count = self._safe_table_count(db, canonical)
        qcols = ', '.join(self._quote_ident(c) for c in common_cols)
        alias_q = self._quote_ident(alias_table)
        canonical_q = self._quote_ident(canonical)

        try:
            if {'Ticker SYMBOL', '資料年月日'}.issubset(set(common_cols)):
                sql = f"""
                INSERT INTO dbo.{canonical_q} ({qcols})
                SELECT {qcols}
                FROM dbo.{alias_q} AS src
                WHERE NOT EXISTS (
                    SELECT 1 FROM dbo.{canonical_q} AS tgt
                    WHERE ISNULL(tgt.[Ticker SYMBOL], '') = ISNULL(src.[Ticker SYMBOL], '')
                      AND ISNULL(CONVERT(VARCHAR(10), tgt.[資料年月日], 120), '') = ISNULL(CONVERT(VARCHAR(10), src.[資料年月日], 120), '')
                )
                """
                db.execute(sql)
                return {
                    'alias_table': alias_table,
                    'copied': True,
                    'mode': 'insert_missing_by_ticker_date',
                    'alias_row_count_before': alias_count,
                    'canonical_row_count_before': canonical_count,
                    'common_columns': common_cols,
                }
            if canonical_count == 0:
                db.execute(f"INSERT INTO dbo.{canonical_q} ({qcols}) SELECT {qcols} FROM dbo.{alias_q}")
                return {
                    'alias_table': alias_table,
                    'copied': True,
                    'mode': 'canonical_empty_full_copy_common_columns',
                    'alias_row_count_before': alias_count,
                    'canonical_row_count_before': canonical_count,
                    'common_columns': common_cols,
                }
            return {
                'alias_table': alias_table,
                'copied': False,
                'reason': 'missing_key_columns_and_canonical_not_empty',
                'alias_row_count_before': alias_count,
                'canonical_row_count_before': canonical_count,
                'common_columns': common_cols,
            }
        except Exception as exc:
            record_issue('db_migrations', 'copy_legacy_fundamentals_rows_failed', exc, severity='ERROR', fail_mode='fail_closed', context={'alias_table': alias_table})
            return {'alias_table': alias_table, 'copied': False, 'reason': f'{type(exc).__name__}: {exc}', 'common_columns': common_cols}

    def _quarantine_legacy_fundamentals_alias(self, db: DatabaseSession, alias_table: str) -> dict[str, Any]:
        """
        不 drop 誤建表；改名成 zzz_legacy_* 備份，讓正式主線只看 fundamentals_clean。
        """
        if not db.table_exists(alias_table):
            return {'alias_table': alias_table, 'quarantined': False, 'reason': 'alias_missing'}
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_name = f'zzz_legacy_{alias_table}_{ts}'[:120]
        try:
            db.execute(f"EXEC sp_rename 'dbo.{alias_table}', '{backup_name}', 'OBJECT'")
            return {'alias_table': alias_table, 'quarantined': True, 'backup_table': backup_name}
        except Exception as exc:
            record_issue('db_migrations', 'quarantine_legacy_fundamentals_alias_failed', exc, severity='WARNING', fail_mode='fail_open', context={'alias_table': alias_table, 'backup_table': backup_name})
            return {'alias_table': alias_table, 'quarantined': False, 'reason': f'{type(exc).__name__}: {exc}', 'backup_table': backup_name}

    def _normalize_canonical_fundamentals_table(self, db: DatabaseSession) -> list[dict[str, Any]]:
        """
        正式基本面主表只保留 dbo.fundamentals_clean 作為 active table。
        舊名/誤建表的資料會先安全搬到 fundamentals_clean，再改名成 zzz_legacy_* 備份。
        """
        actions: list[dict[str, Any]] = []
        if not db.table_exists(CANONICAL_FUNDAMENTALS_TABLE):
            for alias in LEGACY_FUNDAMENTALS_ALIAS_TABLES:
                if db.table_exists(alias):
                    try:
                        db.execute(f"EXEC sp_rename 'dbo.{alias}', '{CANONICAL_FUNDAMENTALS_TABLE}', 'OBJECT'")
                        actions.append({'alias_table': alias, 'action': 'renamed_to_canonical', 'canonical_table': CANONICAL_FUNDAMENTALS_TABLE})
                        return actions
                    except Exception as exc:
                        record_issue('db_migrations', 'rename_fundamentals_alias_to_canonical_failed', exc, severity='WARNING', fail_mode='fail_open', context={'alias_table': alias})
                        actions.append({'alias_table': alias, 'action': 'rename_to_canonical_failed', 'error': f'{type(exc).__name__}: {exc}'})
        for alias in LEGACY_FUNDAMENTALS_ALIAS_TABLES:
            if alias == CANONICAL_FUNDAMENTALS_TABLE or not db.table_exists(alias):
                continue
            copy_report = self._copy_legacy_fundamentals_rows(db, alias)
            quarantine_report = self._quarantine_legacy_fundamentals_alias(db, alias)
            actions.append({'alias_table': alias, 'action': 'consolidated_to_fundamentals_clean', 'copy': copy_report, 'quarantine': quarantine_report})
        if not actions:
            actions.append({'canonical_table': CANONICAL_FUNDAMENTALS_TABLE, 'action': 'canonical_ready_no_alias_found'})
        return actions

    def upgrade(self) -> tuple[Path, dict[str, Any]]:
        self._ensure_database_exists()
        created: list[str] = []
        altered: list[str] = []
        normalized: list[str] = []
        legacy_aliases: list[str] = []
        chinese_views: list[str] = []
        canonical_tables: list[dict[str, Any]] = []
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
            # fundamentals_clean 是正式基本面主表；舊名/誤建表先收斂再建中文 View。
            canonical_tables.extend(self._normalize_canonical_fundamentals_table(db))

            normalized.extend(self._normalize_execution_ticker_columns(db))

            legacy_aliases.extend(self._normalize_legacy_symbol_alias_columns(db))
            chinese_views.extend(ensure_chinese_column_views(db))
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
            'canonical_table_consolidation': canonical_tables,
            'canonical_fundamentals_table': CANONICAL_FUNDAMENTALS_TABLE,
            'legacy_fundamentals_alias_tables': list(LEGACY_FUNDAMENTALS_ALIAS_TABLES),
            'chinese_column_views': chinese_views,
            'sql_chinese_column_strategy': 'non_destructive_views_keep_canonical_tables',
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
