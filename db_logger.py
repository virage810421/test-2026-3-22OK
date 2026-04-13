# db_logger.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from fts_db_engine import DBConfig, DatabaseSession
from fts_db_migrations import MigrationRunner


def _pick(row: dict[str, Any], *keys, default=None):
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return default


def _dt(value):
    if value in (None, ''):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', ''))
    except Exception:
        return None


class SQLServerExecutionLogger:
    def __init__(
        self,
        server: str = 'localhost',
        database: str = '股票online',
        driver: str = 'ODBC Driver 17 for SQL Server',
        trusted_connection: str = 'yes',
        enabled: bool = False,
    ):
        self.enabled = enabled
        self.db = None
        if not self.enabled:
            return
        cfg = DBConfig(server=server, database=database, driver=driver, trusted_connection=trusted_connection)
        self.db = DatabaseSession(cfg).connect()
        MigrationRunner(cfg).upgrade()

    def insert_order(self, row: dict) -> None:
        if not self.enabled or not self.db:
            return
        sql = """
        INSERT INTO dbo.execution_orders (
            [order_id], [client_order_id], [broker_order_id], [ticker_symbol], [direction_bucket], [strategy_bucket],
            [status], [qty], [filled_qty], [remaining_qty], [avg_fill_price], [order_type], [submitted_price], [ref_price],
            [reject_reason], [signal_id], [industry], [signal_score], [ai_confidence], [note], [created_at], [updated_at]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        side = _pick(row, '買賣方向', 'side', 'direction_bucket')
        self.db.execute(sql, [
            _pick(row, '委託單號', 'order_id'),
            _pick(row, '客戶委託編號', 'client_order_id'),
            _pick(row, 'broker_order_id'),
            _pick(row, '股票代號', 'symbol', 'ticker', 'ticker_symbol'),
            side,
            _pick(row, '策略名稱', 'strategy_name', 'strategy_bucket'),
            _pick(row, '委託狀態', 'status'),
            _pick(row, '委託股數', 'quantity', 'qty'),
            _pick(row, '已成交股數', 'filled_qty', default=0),
            _pick(row, '剩餘股數', 'remaining_qty'),
            _pick(row, '平均成交價', 'avg_fill_price'),
            _pick(row, '委託類型', 'order_type'),
            _pick(row, '委託價格', 'limit_price', 'submitted_price'),
            _pick(row, '參考價', 'ref_price'),
            _pick(row, '拒單原因', 'reject_reason'),
            _pick(row, '訊號編號', 'signal_id'),
            _pick(row, '產業名稱', 'industry'),
            _pick(row, '訊號分數', 'signal_score'),
            _pick(row, 'AI信心分數', 'ai_confidence'),
            _pick(row, '備註', 'note'),
            _dt(_pick(row, '建立時間', 'create_time', 'created_at')),
            _dt(_pick(row, '更新時間', 'update_time', 'updated_at')),
        ])
        self.db.commit()

    def insert_fill(self, row: dict) -> None:
        if not self.enabled or not self.db:
            return
        sql = """
        INSERT INTO dbo.execution_fills (
            [fill_id], [order_id], [ticker_symbol], [direction_bucket], [fill_qty], [fill_price], [fill_time],
            [commission], [tax], [slippage], [strategy_name], [signal_id], [note]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.db.execute(sql, [
            _pick(row, '成交編號', 'fill_id'),
            _pick(row, '委託單號', 'order_id'),
            _pick(row, '股票代號', 'symbol', 'ticker', 'ticker_symbol'),
            _pick(row, '買賣方向', 'side', 'direction_bucket'),
            _pick(row, '成交股數', 'fill_qty'),
            _pick(row, '成交價格', 'fill_price'),
            _dt(_pick(row, '成交時間', 'fill_time')),
            _pick(row, '手續費', 'commission'),
            _pick(row, '交易稅', 'tax'),
            _pick(row, '滑價', 'slippage'),
            _pick(row, '策略名稱', 'strategy_name'),
            _pick(row, '訊號編號', 'signal_id'),
            _pick(row, '備註', 'note'),
        ])
        self.db.commit()

    def upsert_account_snapshot(self, row: dict) -> None:
        if not self.enabled or not self.db:
            return
        snap_time = _dt(_pick(row, '快照時間', 'snapshot_time', 'update_time', 'updated_at')) or datetime.now()
        sql = """
        MERGE dbo.execution_account_snapshot AS tgt
        USING (SELECT ? AS [snapshot_time]) AS src
        ON tgt.[snapshot_time] = src.[snapshot_time]
        WHEN MATCHED THEN UPDATE SET
            [account_name]=?, [cash]=?, [market_value]=?, [equity]=?, [buying_power]=?,
            [unrealized_pnl]=?, [realized_pnl]=?, [day_pnl]=?, [exposure_ratio]=?, [currency]=?, [broker_type]=?, [note]=?
        WHEN NOT MATCHED THEN INSERT (
            [snapshot_time], [account_name], [cash], [market_value], [equity], [buying_power],
            [unrealized_pnl], [realized_pnl], [day_pnl], [exposure_ratio], [currency], [broker_type], [note]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        vals = [
            snap_time,
            _pick(row, '帳戶名稱', 'account_name', default='預設帳戶'),
            _pick(row, '可用現金', 'cash', 'cash_available'),
            _pick(row, '總市值', 'market_value'),
            _pick(row, '總權益', 'equity'),
            _pick(row, '買進力', 'buying_power'),
            _pick(row, '未實現損益', 'unrealized_pnl'),
            _pick(row, '已實現損益', 'realized_pnl'),
            _pick(row, '當日損益', 'day_pnl'),
            _pick(row, '曝險比率', 'exposure_ratio'),
            _pick(row, '幣別', 'currency', default='TWD'),
            _pick(row, 'broker_type', '券商類型', default='paper'),
            _pick(row, '備註', 'note'),
        ]
        self.db.execute(sql, vals + [snap_time] + vals[1:])
        self.db.commit()

    def replace_positions_snapshot(self, rows: list[dict], snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.db:
            return
        snap_time = _dt(snapshot_time) or datetime.now()
        self.db.execute('DELETE FROM dbo.execution_positions_snapshot WHERE [snapshot_time]=?', [snap_time])
        sql = """
        INSERT INTO dbo.execution_positions_snapshot (
            [snapshot_time], [ticker_symbol], [direction_bucket], [qty], [available_qty], [avg_cost], [market_price],
            [market_value], [unrealized_pnl], [realized_pnl], [strategy_name], [industry], [note]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for row in rows:
            qty = _pick(row, '持股數量', 'qty', 'quantity', default=0) or 0
            side = _pick(row, '持倉方向', 'side', 'direction_bucket', default=('LONG' if int(qty) >= 0 else 'SHORT'))
            self.db.execute(sql, [
                snap_time,
                _pick(row, '股票代號', 'ticker', 'symbol', 'ticker_symbol'),
                side,
                qty,
                _pick(row, '可用股數', 'available_qty', default=qty),
                _pick(row, '庫存均價', 'avg_cost'),
                _pick(row, '現價', 'market_price'),
                _pick(row, '市值', 'market_value'),
                _pick(row, '未實現損益', 'unrealized_pnl'),
                _pick(row, '已實現損益', 'realized_pnl'),
                _pick(row, '策略名稱', 'strategy_name'),
                _pick(row, '產業名稱', 'industry'),
                _pick(row, '備註', 'note', 'lifecycle_note'),
            ])
        self.db.commit()

    def close(self) -> None:
        if self.db:
            self.db.close()
