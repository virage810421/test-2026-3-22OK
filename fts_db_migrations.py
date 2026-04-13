# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from fts_db_engine import DBConfig, DatabaseSession
from fts_db_schema import SCHEMA_VERSION, iter_table_specs, TableSpec

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()

try:
    from fts_utils import now_str, log  # type: ignore
except Exception:  # pragma: no cover
    from datetime import datetime
    def now_str() -> str:
        return datetime.now().isoformat(timespec='seconds')
    def log(msg: str) -> None:
        print(msg)


class MigrationRunner:
    MODULE_VERSION = 'v20260413_db_migrations'

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

    def _ensure_migrations_table(self, db: DatabaseSession) -> None:
        db.execute("""
        IF OBJECT_ID(N'dbo.schema_migrations', N'U') IS NULL
        BEGIN
            CREATE TABLE dbo.schema_migrations (
                [version] NVARCHAR(64) NOT NULL PRIMARY KEY,
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
        """, [SCHEMA_VERSION, 'architecture_finish_upgrade', SCHEMA_VERSION, 'architecture_finish_upgrade'])

    def upgrade(self) -> tuple[Path, dict[str, Any]]:
        created = []
        altered = []
        with DatabaseSession(self.config) as db:
            self._ensure_migrations_table(db)
            for spec in iter_table_specs():
                if not db.table_exists(spec.name):
                    db.execute(self._create_table_sql(spec))
                    created.append(spec.name)
                for col in spec.columns:
                    if not db.column_exists(spec.name, col.name):
                        db.execute(f"ALTER TABLE dbo.{spec.name} ADD {col.ddl()}")
                        altered.append(f'{spec.name}.{col.name}')
            self._record_version(db)
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'schema_version': SCHEMA_VERSION,
            'created_tables': created,
            'altered_columns': altered,
            'status': 'db_upgrade_ready',
        }
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🗃️ db migration status：{self.runtime_path}')
        return self.runtime_path, payload

    def reset_then_upgrade(self) -> tuple[Path, dict[str, Any]]:
        dropped = []
        with DatabaseSession(self.config) as db:
            for spec in iter_table_specs():
                if db.table_exists(spec.name):
                    db.execute(f"DROP TABLE dbo.{spec.name}")
                    dropped.append(spec.name)
        path, payload = self.upgrade()
        payload['dropped_tables'] = dropped
        payload['status'] = 'db_reset_upgrade_ready'
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return path, payload
