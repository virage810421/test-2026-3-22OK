# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime
from typing import Any

from db_logger import SQLServerExecutionLogger
from fts_db_migrations import MigrationRunner
from fts_utils import log


def _as_dt(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', ''))
    except Exception:
        return None


def _to_dict(obj: Any) -> dict[str, Any]:
    if isinstance(obj, dict):
        return dict(obj)
    if hasattr(obj, '__dict__'):
        return dict(obj.__dict__)
    return {}


class SQLLogger:
    """
    舊門牌相容 logger。

    重要修正：
    - 本檔不再建立中文 execution_* 第二套 schema。
    - ensure_tables() 只委派 fts_db_migrations.py。
    - 真正 SQL 寫入統一委派 db_logger.SQLServerExecutionLogger。
    """

    def __init__(self, db_config):
        self.db_config = db_config
        self._logger: SQLServerExecutionLogger | None = None
        self.conn = None

    def connect(self) -> bool:
        try:
            self._logger = SQLServerExecutionLogger(
                server=getattr(self.db_config, 'server', 'localhost'),
                database=getattr(self.db_config, 'database', '股票online'),
                driver=getattr(self.db_config, 'driver', 'ODBC Driver 17 for SQL Server'),
                trusted_connection=getattr(self.db_config, 'trusted_connection', 'yes'),
                enabled=True,
            )
            self.conn = self._logger.conn
            log('✅ SQLLogger 相容入口已連線；schema 由 fts_db_migrations.py 管理。')
            return True
        except Exception as exc:
            log(f'⚠️ SQLLogger 相容入口停用：{exc}')
            self._logger = None
            self.conn = None
            return False

    def close(self):
        if self._logger:
            self._logger.close()
        self.conn = None

    def ensure_tables(self):
        # 不再維護第二套 CREATE TABLE；只委派正式 migration。
        try:
            MigrationRunner().upgrade()
            log('✅ schema migration 已由 fts_db_migrations.py 確認。')
        except Exception as exc:
            log(f'⚠️ schema migration 檢查失敗：{exc}')

    def insert_order(self, order):
        if not self._logger:
            return
        row = _to_dict(order)
        if 'ticker' in row and 'ticker_symbol' not in row:
            row['ticker_symbol'] = row.get('ticker')
        if 'side' in row and 'direction_bucket' not in row:
            side = row.get('side')
            row['direction_bucket'] = getattr(side, 'value', side)
        if 'status' in row:
            row['status'] = getattr(row.get('status'), 'value', row.get('status'))
        self._logger.insert_order(row)

    def update_order_status(self, order_id, status, updated_at, note="", filled_qty=None, remaining_qty=None, avg_fill_price=None, reject_reason=None):
        if not self._logger:
            return
        self._logger.insert_order({
            'order_id': order_id,
            'status': getattr(status, 'value', status),
            'updated_at': updated_at,
            'note': note,
            'filled_qty': filled_qty,
            'remaining_qty': remaining_qty,
            'avg_fill_price': avg_fill_price,
            'reject_reason': reject_reason,
        })

    def insert_fill(self, fill):
        if not self._logger:
            return
        row = _to_dict(fill)
        if 'ticker' in row and 'ticker_symbol' not in row:
            row['ticker_symbol'] = row.get('ticker')
        if 'side' in row and 'direction_bucket' not in row:
            side = row.get('side')
            row['direction_bucket'] = getattr(side, 'value', side)
        self._logger.insert_fill(row)

    def upsert_account_snapshot(self, snapshot, account_name: str = "預設帳戶", buying_power=None, unrealized_pnl=None, realized_pnl=None, day_pnl=None, currency="TWD", note=""):
        if not self._logger:
            return
        row = _to_dict(snapshot)
        row.update({
            'account_name': account_name,
            'buying_power': buying_power,
            'unrealized_pnl': unrealized_pnl,
            'realized_pnl': realized_pnl,
            'day_pnl': day_pnl,
            'currency': currency,
            'note': note,
        })
        self._logger.upsert_account_snapshot(row)

    def replace_positions_snapshot(self, positions, snapshot_time=None):
        if not self._logger:
            return
        rows: list[dict[str, Any]] = []
        for pos in positions or []:
            row = _to_dict(pos)
            if 'ticker' in row and 'ticker_symbol' not in row:
                row['ticker_symbol'] = row.get('ticker')
            rows.append(row)
        self._logger.replace_positions_snapshot(rows, snapshot_time=snapshot_time)
