# db_logger.py
from __future__ import annotations

from typing import Optional
import pyodbc


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
            order_id, symbol, side, quantity, filled_qty, remaining_qty, avg_fill_price,
            order_type, limit_price, status, create_time, update_time, reject_reason,
            strategy_name, signal_id, client_order_id, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.execute(
            sql,
            row.get("order_id"),
            row.get("symbol"),
            row.get("side"),
            row.get("quantity"),
            row.get("filled_qty"),
            row.get("remaining_qty"),
            row.get("avg_fill_price"),
            row.get("order_type"),
            row.get("limit_price"),
            row.get("status"),
            row.get("create_time"),
            row.get("update_time"),
            row.get("reject_reason"),
            row.get("strategy_name"),
            row.get("signal_id"),
            row.get("client_order_id"),
            row.get("note"),
        )
        self.conn.commit()

    def insert_fill(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return

        sql = """
        INSERT INTO execution_fills (
            order_id, symbol, side, fill_qty, fill_price, fill_time,
            commission, tax, slippage, strategy_name, signal_id, note
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.cursor.execute(
            sql,
            row.get("order_id"),
            row.get("symbol"),
            row.get("side"),
            row.get("fill_qty"),
            row.get("fill_price"),
            row.get("fill_time"),
            row.get("commission"),
            row.get("tax"),
            row.get("slippage"),
            row.get("strategy_name"),
            row.get("signal_id"),
            row.get("note"),
        )
        self.conn.commit()

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
