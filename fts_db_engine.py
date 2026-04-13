# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Sequence

try:
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None

try:  # pragma: no cover
    from sqlalchemy import create_engine
    SQLALCHEMY_AVAILABLE = True
except Exception:  # pragma: no cover
    create_engine = None
    SQLALCHEMY_AVAILABLE = False


@dataclass
class DBConfig:
    server: str = 'localhost'
    database: str = '股票online'
    driver: str = 'ODBC Driver 17 for SQL Server'
    trusted_connection: str = 'yes'
    use_sqlalchemy_preferred: bool = True
    autocommit: bool = False

    @property
    def pyodbc_conn_str(self) -> str:
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.server};"
            f"DATABASE={self.database};"
            f"Trusted_Connection={self.trusted_connection};"
        )

    @property
    def sqlalchemy_url(self) -> str:
        from urllib.parse import quote_plus
        return 'mssql+pyodbc:///?odbc_connect=' + quote_plus(self.pyodbc_conn_str)


class DatabaseSession:
    def __init__(self, config: DBConfig | None = None):
        self.config = config or DBConfig()
        self.backend = None
        self._engine = None
        self._conn = None
        self._cursor = None

    def connect(self):
        if self.backend:
            return self
        if self.config.use_sqlalchemy_preferred and SQLALCHEMY_AVAILABLE:
            try:
                self._engine = create_engine(self.config.sqlalchemy_url, future=True)
                self._conn = self._engine.connect()
                self.backend = 'sqlalchemy'
                return self
            except Exception:
                self._engine = None
                self._conn = None
        if pyodbc is None:
            raise RuntimeError('pyodbc 未安裝，無法建立 SQL Server 連線。')
        self._conn = pyodbc.connect(self.config.pyodbc_conn_str, autocommit=self.config.autocommit)
        self._cursor = self._conn.cursor()
        self.backend = 'pyodbc'
        return self

    def execute(self, sql: str, params: Sequence[Any] | None = None):
        self.connect()
        params = list(params or [])
        if self.backend == 'sqlalchemy':
            return self._conn.exec_driver_sql(sql, tuple(params))
        return self._cursor.execute(sql, *params)

    def executemany(self, sql: str, rows: Iterable[Sequence[Any]]):
        self.connect()
        if self.backend == 'sqlalchemy':
            for row in rows:
                self._conn.exec_driver_sql(sql, tuple(row))
            return None
        return self._cursor.executemany(sql, list(rows))

    def scalar(self, sql: str, params: Sequence[Any] | None = None):
        self.connect()
        if self.backend == 'sqlalchemy':
            result = self._conn.exec_driver_sql(sql, tuple(params or []))
            row = result.fetchone()
            return None if row is None else row[0]
        row = self._cursor.execute(sql, *(params or [])).fetchone()
        return None if row is None else row[0]

    def commit(self):
        if self.backend == 'sqlalchemy' and self._conn is not None:
            self._conn.commit()
        elif self._conn is not None:
            self._conn.commit()

    def rollback(self):
        if self._conn is not None:
            self._conn.rollback()

    def close(self):
        try:
            if self._cursor is not None:
                self._cursor.close()
        except Exception:
            pass
        try:
            if self._conn is not None:
                self._conn.close()
        except Exception:
            pass
        try:
            if self._engine is not None:
                self._engine.dispose()
        except Exception:
            pass
        self.backend = None
        self._engine = None
        self._conn = None
        self._cursor = None

    def __enter__(self):
        return self.connect()

    def __exit__(self, exc_type, exc, tb):
        if exc_type:
            try:
                self.rollback()
            finally:
                self.close()
        else:
            try:
                self.commit()
            finally:
                self.close()

    def table_exists(self, table_name: str) -> bool:
        value = self.scalar("SELECT COUNT(1) FROM sys.tables WHERE name=?", [table_name])
        return bool(value)

    def column_exists(self, table_name: str, column_name: str) -> bool:
        value = self.scalar("SELECT COL_LENGTH(?, ?) ", [f'dbo.{table_name}', column_name])
        return value is not None
