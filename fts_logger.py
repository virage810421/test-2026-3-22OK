# -*- coding: utf-8 -*-
try:
    import pyodbc
except Exception:
    pyodbc = None
from datetime import datetime
from fts_utils import log
from fts_models import Order, Fill, Position, AccountSnapshot


def _as_dt(value):
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    try:
        return datetime.fromisoformat(str(value).replace('Z', ''))
    except Exception:
        return None


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
            if self.conn:
                self.conn.close()
        except Exception:
            pass

    def ensure_tables(self):
        if not self.conn:
            return
        ddl = r"""
        IF OBJECT_ID('dbo.execution_orders', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_orders (
                [委託單號] NVARCHAR(64) PRIMARY KEY,
                [股票代號] NVARCHAR(32), [買賣方向] NVARCHAR(16), [委託股數] INT,
                [已成交股數] INT, [剩餘股數] INT, [平均成交價] DECIMAL(18,4),
                [參考價] DECIMAL(18,4), [委託價格] DECIMAL(18,4), [委託類型] NVARCHAR(20),
                [委託狀態] NVARCHAR(32), [策略名稱] NVARCHAR(128), [訊號分數] DECIMAL(18,4),
                [AI信心分數] DECIMAL(18,4), [產業名稱] NVARCHAR(64), [訊號編號] NVARCHAR(100),
                [客戶委託編號] NVARCHAR(100), [拒單原因] NVARCHAR(255), [備註] NVARCHAR(MAX),
                [建立時間] DATETIME, [更新時間] DATETIME
            );
        END;
        IF OBJECT_ID('dbo.execution_fills', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_fills (
                [成交編號] NVARCHAR(64) PRIMARY KEY, [委託單號] NVARCHAR(64), [股票代號] NVARCHAR(32),
                [買賣方向] NVARCHAR(16), [成交股數] INT, [成交價格] DECIMAL(18,4), [成交時間] DATETIME,
                [手續費] DECIMAL(18,4), [交易稅] DECIMAL(18,4), [滑價] DECIMAL(18,6),
                [策略名稱] NVARCHAR(128), [訊號編號] NVARCHAR(100), [備註] NVARCHAR(MAX)
            );
        END;
        IF OBJECT_ID('dbo.execution_account_snapshot', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_account_snapshot (
                [快照時間] DATETIME PRIMARY KEY, [帳戶名稱] NVARCHAR(100), [可用現金] DECIMAL(18,4),
                [總市值] DECIMAL(18,4), [總權益] DECIMAL(18,4), [買進力] DECIMAL(18,4),
                [未實現損益] DECIMAL(18,4), [已實現損益] DECIMAL(18,4), [當日損益] DECIMAL(18,4),
                [曝險比率] DECIMAL(18,6), [幣別] NVARCHAR(20), [備註] NVARCHAR(MAX)
            );
        END;
        IF OBJECT_ID('dbo.execution_positions_snapshot', 'U') IS NULL
        BEGIN
            CREATE TABLE dbo.execution_positions_snapshot (
                [快照時間] DATETIME NOT NULL, [股票代號] NVARCHAR(32) NOT NULL, [持倉方向] NVARCHAR(16),
                [持股數量] INT, [可用股數] INT, [庫存均價] DECIMAL(18,4), [現價] DECIMAL(18,4),
                [市值] DECIMAL(18,4), [未實現損益] DECIMAL(18,4), [已實現損益] DECIMAL(18,4),
                [策略名稱] NVARCHAR(128), [產業名稱] NVARCHAR(64), [備註] NVARCHAR(MAX),
                CONSTRAINT PK_execution_positions_snapshot_中文 PRIMARY KEY ([快照時間], [股票代號])
            );
        END;
        """
        cur = self.conn.cursor()
        cur.execute(ddl)
        self.conn.commit()
        cur.close()
        log("✅ execution_orders / execution_fills / execution_account_snapshot / execution_positions_snapshot 中文表已就緒。")

    def insert_order(self, order: Order):
        if not self.conn:
            return
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO dbo.execution_orders
        ([委託單號],[股票代號],[買賣方向],[委託股數],[已成交股數],[剩餘股數],[平均成交價],[參考價],[委託價格],[委託類型],[委託狀態],[策略名稱],[訊號分數],[AI信心分數],[產業名稱],[訊號編號],[客戶委託編號],[拒單原因],[備註],[建立時間],[更新時間])
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        order.order_id, order.ticker, order.side.value, order.qty, 0, order.qty, None, order.ref_price,
        order.submitted_price, None, order.status.value, order.strategy_name, order.signal_score,
        order.ai_confidence, order.industry, None, None, None, order.note,
        _as_dt(order.created_at), _as_dt(order.updated_at))
        self.conn.commit()
        cur.close()

    def update_order_status(self, order_id, status, updated_at, note="", filled_qty=None, remaining_qty=None, avg_fill_price=None, reject_reason=None):
        if not self.conn:
            return
        cur = self.conn.cursor()
        cur.execute("""UPDATE dbo.execution_orders
        SET [委託狀態]=?, [更新時間]=?,
            [已成交股數]=COALESCE(?, [已成交股數]),
            [剩餘股數]=COALESCE(?, [剩餘股數]),
            [平均成交價]=COALESCE(?, [平均成交價]),
            [拒單原因]=COALESCE(?, [拒單原因]),
            [備註]=CASE WHEN ?='' THEN [備註] ELSE ? END
        WHERE [委託單號]=?""",
        status.value if hasattr(status, 'value') else str(status), _as_dt(updated_at),
        filled_qty, remaining_qty, avg_fill_price, reject_reason, note, note, order_id)
        self.conn.commit()
        cur.close()

    def insert_fill(self, fill: Fill):
        if not self.conn:
            return
        cur = self.conn.cursor()
        cur.execute("""INSERT INTO dbo.execution_fills
        ([成交編號],[委託單號],[股票代號],[買賣方向],[成交股數],[成交價格],[成交時間],[手續費],[交易稅],[滑價],[策略名稱],[訊號編號],[備註])
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        fill.fill_id, fill.order_id, fill.ticker, fill.side.value, fill.fill_qty,
        fill.fill_price, _as_dt(fill.fill_time), fill.commission, fill.tax,
        getattr(fill, 'slippage', None), getattr(fill, 'strategy_name', None), getattr(fill, 'signal_id', None), getattr(fill, 'note', None))
        self.conn.commit()
        cur.close()

    def upsert_account_snapshot(self, snapshot: AccountSnapshot | dict, account_name: str = "預設帳戶", buying_power=None, unrealized_pnl=None, realized_pnl=None, day_pnl=None, currency="TWD", note=""):
        if not self.conn:
            return
        data = snapshot if isinstance(snapshot, dict) else snapshot.__dict__
        snap_time = _as_dt(data.get('updated_at')) or datetime.now()
        cur = self.conn.cursor()
        cur.execute("""
        MERGE dbo.execution_account_snapshot AS tgt
        USING (SELECT ? AS [快照時間]) AS src
        ON tgt.[快照時間] = src.[快照時間]
        WHEN MATCHED THEN UPDATE SET
            [帳戶名稱]=?, [可用現金]=?, [總市值]=?, [總權益]=?, [買進力]=?, [未實現損益]=?, [已實現損益]=?, [當日損益]=?, [曝險比率]=?, [幣別]=?, [備註]=?
        WHEN NOT MATCHED THEN INSERT ([快照時間],[帳戶名稱],[可用現金],[總市值],[總權益],[買進力],[未實現損益],[已實現損益],[當日損益],[曝險比率],[幣別],[備註])
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?);
        """,
        snap_time,
        account_name, data.get('cash'), data.get('market_value'), data.get('equity'), buying_power, unrealized_pnl, realized_pnl, day_pnl, data.get('exposure_ratio'), currency, note,
        snap_time, account_name, data.get('cash'), data.get('market_value'), data.get('equity'), buying_power, unrealized_pnl, realized_pnl, day_pnl, data.get('exposure_ratio'), currency, note)
        self.conn.commit()
        cur.close()

    def replace_positions_snapshot(self, positions, snapshot_time=None):
        if not self.conn:
            return
        snap_time = _as_dt(snapshot_time) or datetime.now()
        cur = self.conn.cursor()
        cur.execute("DELETE FROM dbo.execution_positions_snapshot WHERE [快照時間]=?", snap_time)
        for pos in positions:
            data = pos if isinstance(pos, dict) else pos.__dict__
            qty = int(data.get('qty', 0) or 0)
            side = '多單' if qty >= 0 else '空單'
            cur.execute("""INSERT INTO dbo.execution_positions_snapshot
            ([快照時間],[股票代號],[持倉方向],[持股數量],[可用股數],[庫存均價],[現價],[市值],[未實現損益],[已實現損益],[策略名稱],[產業名稱],[備註])
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            snap_time, data.get('ticker') or data.get('symbol'), side, qty, qty,
            data.get('avg_cost'), data.get('market_price'), data.get('market_value'),
            data.get('unrealized_pnl'), data.get('realized_pnl'), data.get('strategy_name'), data.get('industry'), data.get('lifecycle_note') or data.get('note'))
        self.conn.commit()
        cur.close()
