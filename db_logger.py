# db_logger.py
from __future__ import annotations

from datetime import datetime
from typing import Any, Optional
import pyodbc


def _pick(row: dict[str, Any], *keys, default=None):
    for key in keys:
        if key in row and row.get(key) is not None:
            return row.get(key)
    return default


def _dt(value):
    if value in (None, ""):
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
        server: str = "localhost",
        database: str = "股票online",
        driver: str = "ODBC Driver 17 for SQL Server",
        trusted_connection: str = "yes",
        enabled: bool = False,
    ):
        self.enabled = enabled
        self.conn = None
        self.cursor = None

        if not self.enabled:
            return

        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection={trusted_connection};"
        )
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def insert_order(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return

        sql = """
        INSERT INTO execution_orders (
            [委託單號], [股票代號], [買賣方向], [委託股數], [已成交股數], [剩餘股數], [平均成交價],
            [委託類型], [委託價格], [委託狀態], [建立時間], [更新時間], [拒單原因],
            [策略名稱], [訊號編號], [客戶委託編號], [產業名稱], [訊號分數], [AI信心分數], [備註]
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.execute(
            sql,
            _pick(row, "委託單號", "order_id"),
            _pick(row, "股票代號", "symbol", "ticker"),
            _pick(row, "買賣方向", "side"),
            _pick(row, "委託股數", "quantity", "qty"),
            _pick(row, "已成交股數", "filled_qty", default=0),
            _pick(row, "剩餘股數", "remaining_qty"),
            _pick(row, "平均成交價", "avg_fill_price"),
            _pick(row, "委託類型", "order_type"),
            _pick(row, "委託價格", "limit_price", "submitted_price"),
            _pick(row, "委託狀態", "status"),
            _dt(_pick(row, "建立時間", "create_time", "created_at")),
            _dt(_pick(row, "更新時間", "update_time", "updated_at")),
            _pick(row, "拒單原因", "reject_reason"),
            _pick(row, "策略名稱", "strategy_name"),
            _pick(row, "訊號編號", "signal_id"),
            _pick(row, "客戶委託編號", "client_order_id"),
            _pick(row, "產業名稱", "industry"),
            _pick(row, "訊號分數", "signal_score"),
            _pick(row, "AI信心分數", "ai_confidence"),
            _pick(row, "備註", "note"),
        )
        self.conn.commit()

    def insert_fill(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return

        sql = """
        INSERT INTO execution_fills (
            [成交編號], [委託單號], [股票代號], [買賣方向], [成交股數], [成交價格], [成交時間],
            [手續費], [交易稅], [滑價], [策略名稱], [訊號編號], [備註]
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.execute(
            sql,
            _pick(row, "成交編號", "fill_id"),
            _pick(row, "委託單號", "order_id"),
            _pick(row, "股票代號", "symbol", "ticker"),
            _pick(row, "買賣方向", "side"),
            _pick(row, "成交股數", "fill_qty"),
            _pick(row, "成交價格", "fill_price"),
            _dt(_pick(row, "成交時間", "fill_time")),
            _pick(row, "手續費", "commission"),
            _pick(row, "交易稅", "tax"),
            _pick(row, "滑價", "slippage"),
            _pick(row, "策略名稱", "strategy_name"),
            _pick(row, "訊號編號", "signal_id"),
            _pick(row, "備註", "note"),
        )
        self.conn.commit()

    def upsert_account_snapshot(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return

        snap_time = _dt(_pick(row, "快照時間", "snapshot_time", "update_time", "updated_at")) or datetime.now()
        sql = """
        MERGE execution_account_snapshot AS tgt
        USING (SELECT ? AS [快照時間]) AS src
        ON tgt.[快照時間] = src.[快照時間]
        WHEN MATCHED THEN UPDATE SET
            [帳戶名稱]=?, [可用現金]=?, [總市值]=?, [總權益]=?, [買進力]=?,
            [未實現損益]=?, [已實現損益]=?, [當日損益]=?, [曝險比率]=?, [幣別]=?, [備註]=?
        WHEN NOT MATCHED THEN INSERT (
            [快照時間], [帳戶名稱], [可用現金], [總市值], [總權益], [買進力],
            [未實現損益], [已實現損益], [當日損益], [曝險比率], [幣別], [備註]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        values = [
            snap_time,
            _pick(row, "帳戶名稱", "account_name", default="預設帳戶"),
            _pick(row, "可用現金", "cash", "cash_available"),
            _pick(row, "總市值", "market_value"),
            _pick(row, "總權益", "equity"),
            _pick(row, "買進力", "buying_power"),
            _pick(row, "未實現損益", "unrealized_pnl"),
            _pick(row, "已實現損益", "realized_pnl"),
            _pick(row, "當日損益", "day_pnl"),
            _pick(row, "曝險比率", "exposure_ratio"),
            _pick(row, "幣別", "currency", default="TWD"),
            _pick(row, "備註", "note"),
        ]
        self.cursor.execute(sql, *(values + [snap_time] + values[1:]))
        self.conn.commit()

    def replace_positions_snapshot(self, rows: list[dict], snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.cursor:
            return

        snap_time = _dt(snapshot_time) or datetime.now()
        self.cursor.execute("DELETE FROM execution_positions_snapshot WHERE [快照時間]=?", snap_time)
        sql = """
        INSERT INTO execution_positions_snapshot (
            [快照時間], [股票代號], [持倉方向], [持股數量], [可用股數], [庫存均價], [現價], [市值],
            [未實現損益], [已實現損益], [策略名稱], [產業名稱], [備註]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        for row in rows:
            qty = _pick(row, "持股數量", "qty", "quantity", default=0) or 0
            self.cursor.execute(
                sql,
                snap_time,
                _pick(row, "股票代號", "ticker", "symbol"),
                _pick(row, "持倉方向", "side", default=("多單" if int(qty) >= 0 else "空單")),
                qty,
                _pick(row, "可用股數", "available_qty", default=qty),
                _pick(row, "庫存均價", "avg_cost"),
                _pick(row, "現價", "market_price"),
                _pick(row, "市值", "market_value"),
                _pick(row, "未實現損益", "unrealized_pnl"),
                _pick(row, "已實現損益", "realized_pnl"),
                _pick(row, "策略名稱", "strategy_name"),
                _pick(row, "產業名稱", "industry"),
                _pick(row, "備註", "note", "lifecycle_note"),
            )
        self.conn.commit()

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
