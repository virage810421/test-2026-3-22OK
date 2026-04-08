# -*- coding: utf-8 -*-
try:
    import pyodbc
except Exception:
    pyodbc = None
from fts_utils import log
from fts_models import Order, Fill

class SQLLogger:
    def __init__(self, db_config):
        self.db_config = db_config
        self.conn = None

    def connect(self) -> bool:
        if pyodbc is None:
            log("⚠️ pyodbc 未安裝，SQL Logger 停用。")
            return False
        try:
            conn_str = (
                f"DRIVER={{{self.db_config.driver}}};SERVER={self.db_config.server};"
                f"DATABASE={self.db_config.database};Trusted_Connection={self.db_config.trusted_connection};"
            )
            self.conn = pyodbc.connect(conn_str)
            log("✅ SQL Server 連線成功。")
            return True
        except Exception as e:
            log(f"❌ SQL Server 連線失敗：{e}")
            return False

    def close(self):
        try:
            if self.conn: self.conn.close()
        except Exception:
            pass

    def ensure_tables(self):
        if not self.conn: return
        ddl = r"""
        IF OBJECT_ID('dbo.execution_orders', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_orders (
                order_id NVARCHAR(64) PRIMARY KEY,
                ticker NVARCHAR(32), side NVARCHAR(16), qty INT,
                ref_price DECIMAL(18,4), submitted_price DECIMAL(18,4), status NVARCHAR(32),
                strategy_name NVARCHAR(128), signal_score DECIMAL(18,4), ai_confidence DECIMAL(18,4),
                industry NVARCHAR(64), note NVARCHAR(MAX), created_at DATETIME, updated_at DATETIME
            );
        END;
        IF OBJECT_ID('dbo.execution_fills', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_fills (
                fill_id NVARCHAR(64) PRIMARY KEY, order_id NVARCHAR(64), ticker NVARCHAR(32),
                side NVARCHAR(16), fill_qty INT, fill_price DECIMAL(18,4),
                commission DECIMAL(18,4), tax DECIMAL(18,4), fill_time DATETIME
            );
        END;
        """
        cur = self.conn.cursor(); cur.execute(ddl); self.conn.commit(); cur.close()
        log("✅ execution tables ready.")

    def insert_order(self, order: Order):
        if not self.conn: return
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO dbo.execution_orders
        (order_id,ticker,side,qty,ref_price,submitted_price,status,strategy_name,signal_score,ai_confidence,industry,note,created_at,updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        order.order_id, order.ticker, order.side.value, order.qty, order.ref_price, order.submitted_price,
        order.status.value, order.strategy_name, order.signal_score, order.ai_confidence,
        order.industry, order.note, order.created_at, order.updated_at)
        self.conn.commit(); cur.close()

    def update_order_status(self, order_id, status, updated_at, note=""):
        if not self.conn: return
        cur = self.conn.cursor()
        cur.execute("""UPDATE dbo.execution_orders
        SET status=?, updated_at=?, note = CASE WHEN ?='' THEN note ELSE ? END
        WHERE order_id=?""", status.value, updated_at, note, note, order_id)
        self.conn.commit(); cur.close()

    def insert_fill(self, fill: Fill):
        if not self.conn: return
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO dbo.execution_fills
        (fill_id,order_id,ticker,side,fill_qty,fill_price,commission,tax,fill_time)
        VALUES (?,?,?,?,?,?,?,?,?)""",
        fill.fill_id, fill.order_id, fill.ticker, fill.side.value, fill.fill_qty,
        fill.fill_price, fill.commission, fill.tax, fill.fill_time)
        self.conn.commit(); cur.close()
