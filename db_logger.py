# db_logger.py
from __future__ import annotations

from datetime import datetime
from fts_symbol_contract import ensure_execution_symbol
from typing import Any, Optional
try:
    import pyodbc
except Exception:  # allow non-SQL smoke tests without ODBC installed
    pyodbc = None


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
        if pyodbc is None:
            raise RuntimeError("pyodbc is required when SQLServerExecutionLogger(enabled=True)")
        self.conn = pyodbc.connect(conn_str)
        self.cursor = self.conn.cursor()

    def _execute(self, sql: str, params: list[Any] | tuple[Any, ...] = ()) -> None:
        if not self.cursor:
            return
        self.cursor.execute(sql, params)

    def insert_order(self, row: dict) -> None:
        row = ensure_execution_symbol(dict(row or {}), keep_legacy=True)
        if not self.enabled or not self.cursor:
            return
        order_id = _pick(row, 'order_id', '委託單號', 'broker_order_id', 'client_order_id')
        if not order_id:
            return
        qty = _pick(row, 'qty', 'quantity', '委託股數')
        filled_qty = _pick(row, 'filled_qty', '已成交股數', default=0)
        remaining_qty = _pick(row, 'remaining_qty', '剩餘股數', default=(None if qty is None else max(int(qty or 0) - int(filled_qty or 0), 0)))
        sql = """
        MERGE dbo.execution_orders AS tgt
        USING (SELECT ? AS [order_id]) AS src
        ON tgt.[order_id] = src.[order_id]
        WHEN MATCHED THEN UPDATE SET
            [client_order_id]=?,
            [broker_order_id]=?,
            ticker_symbol=?,
            [direction_bucket]=?,
            [strategy_bucket]=?,
            [status]=?,
            [qty]=?,
            [ref_price]=?,
            [created_at]=COALESCE(tgt.[created_at], ?),
            [updated_at]=?
        WHEN NOT MATCHED THEN INSERT (
            [order_id], [client_order_id], [broker_order_id], ticker_symbol, [direction_bucket], [strategy_bucket],
            [status], [qty], [ref_price], [created_at], [updated_at]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        created_at = _dt(_pick(row, 'created_at', 'create_time', '建立時間')) or datetime.now()
        updated_at = _dt(_pick(row, 'updated_at', 'update_time', '更新時間')) or created_at
        vals = [
            str(order_id),
            _pick(row, 'client_order_id', '客戶委託編號'),
            _pick(row, 'broker_order_id'),
            _pick(row, 'ticker_symbol', 'Ticker SYMBOL', 'symbol', 'ticker', '股票代號'),
            _pick(row, 'direction_bucket', 'side', '買賣方向'),
            _pick(row, 'strategy_bucket', 'strategy_name', '策略名稱'),
            _pick(row, 'status', '委託狀態'),
            qty,
            _pick(row, 'ref_price', 'Reference_Price', 'submitted_price', 'limit_price', 'stop_price', '委託價格'),
            created_at,
            updated_at,
        ]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def insert_fill(self, row: dict) -> None:
        row = ensure_execution_symbol(dict(row or {}), keep_legacy=True)
        if not self.enabled or not self.cursor:
            return
        fill_id = _pick(row, 'fill_id', '成交編號')
        if not fill_id:
            order_id = _pick(row, 'order_id', '委託單號', 'client_order_id', 'broker_order_id', default='FILL')
            fill_time = _pick(row, 'fill_time', '成交時間', 'updated_at') or datetime.now().isoformat(timespec='seconds')
            fill_id = f"{order_id}-{fill_time}"
        sql = """
        MERGE dbo.execution_fills AS tgt
        USING (SELECT ? AS [fill_id]) AS src
        ON tgt.[fill_id] = src.[fill_id]
        WHEN MATCHED THEN UPDATE SET
            [order_id]=?,
            ticker_symbol=?,
            [direction_bucket]=?,
            [fill_qty]=?,
            [fill_price]=?,
            [fill_time]=?
        WHEN NOT MATCHED THEN INSERT (
            [fill_id], [order_id], ticker_symbol, [direction_bucket], [fill_qty], [fill_price], [fill_time]
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """
        vals = [
            str(fill_id),
            _pick(row, 'order_id', '委託單號', 'client_order_id', 'broker_order_id'),
            _pick(row, 'ticker_symbol', 'Ticker SYMBOL', 'symbol', 'ticker', '股票代號'),
            _pick(row, 'direction_bucket', 'side', '買賣方向'),
            _pick(row, 'fill_qty', '成交股數'),
            _pick(row, 'fill_price', '成交價格'),
            _dt(_pick(row, 'fill_time', '成交時間')) or datetime.now(),
        ]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def upsert_account_snapshot(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return
        snap_time = _dt(_pick(row, 'snapshot_time', '快照時間', 'update_time', 'updated_at')) or datetime.now()
        sql = """
        MERGE dbo.execution_account_snapshot AS tgt
        USING (SELECT ? AS [snapshot_time]) AS src
        ON tgt.[snapshot_time] = src.[snapshot_time]
        WHEN MATCHED THEN UPDATE SET
            [cash]=?, [equity]=?, [broker_type]=?
        WHEN NOT MATCHED THEN INSERT ([snapshot_time], [cash], [equity], [broker_type]) VALUES (?, ?, ?, ?);
        """
        vals = [snap_time, _pick(row,'cash','cash_available','可用現金'), _pick(row,'equity','總權益'), _pick(row,'broker_type','券商類型', default='paper')]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def replace_positions_snapshot(self, rows: list[dict], snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.cursor:
            return
        snap_time = _dt(snapshot_time) or datetime.now()
        self.cursor.execute("DELETE FROM dbo.execution_positions_snapshot WHERE [snapshot_time]=?", snap_time)
        sql = """
        INSERT INTO dbo.execution_positions_snapshot ([snapshot_time], ticker_symbol, [direction_bucket], [qty], [avg_cost])
        VALUES (?, ?, ?, ?, ?)
        """
        for row in rows or []:
            qty = _pick(row, 'qty', 'quantity', '持股數量', default=0) or 0
            self.cursor.execute(sql, snap_time, _pick(row,'ticker_symbol','Ticker SYMBOL','ticker','symbol','股票代號'), _pick(row,'direction_bucket','side','持倉方向', default=('LONG' if int(qty) >=0 else 'SHORT')), qty, _pick(row,'avg_cost','庫存均價'))
        self.conn.commit()

    def sync_runtime_snapshot(self, account_row: dict, position_rows: list[dict], snapshot_time: Optional[str] = None, note: str = '') -> None:
        if not self.enabled or not self.cursor:
            return
        snap_time = _dt(snapshot_time) or datetime.now()
        acct = dict(account_row or {})
        acct['snapshot_time'] = snap_time
        if note and 'note' not in acct:
            acct['note'] = note
        self.upsert_account_snapshot(acct)
        self.replace_positions_snapshot(position_rows or [], snapshot_time=snap_time.isoformat(timespec='seconds'))

    def ingest_protective_stop_order(self, row: dict) -> None:
        if not self.enabled or not self.cursor:
            return
        payload = {
            'order_id': _pick(row, 'broker_order_id', 'order_id', 'client_order_id'),
            'client_order_id': _pick(row, 'client_order_id'),
            'broker_order_id': _pick(row, 'broker_order_id', 'order_id'),
            'ticker_symbol': _pick(row, 'ticker_symbol', 'Ticker SYMBOL', 'ticker', 'symbol'),
            'direction_bucket': _pick(row, 'direction_bucket', 'side', default='STOP'),
            'strategy_bucket': _pick(row, 'strategy_bucket', 'strategy_name', default='protective_stop'),
            'status': _pick(row, 'status', default='WORKING'),
            'qty': _pick(row, 'qty', 'quantity'),
            'ref_price': _pick(row, 'submitted_price', 'stop_price', 'ref_price'),
            'created_at': _dt(_pick(row, 'created_at', 'create_time')) or datetime.now(),
            'updated_at': _dt(_pick(row, 'updated_at', 'update_time')) or datetime.now(),
        }
        self.insert_order(payload)

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

# =============================================================================
# vNext lot-level / callback / reconciliation SQL extension
# =============================================================================
try:
    import json as _json
    import uuid as _uuid

    def _dblogger_json(value: Any) -> str:
        try:
            return _json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            return str(value)

    _DBL_ORIG_INIT = SQLServerExecutionLogger.__init__
    _DBL_ORIG_SYNC_RUNTIME = SQLServerExecutionLogger.sync_runtime_snapshot

    def _dbl_patched_init(self, *args, **kwargs):
        _DBL_ORIG_INIT(self, *args, **kwargs)
        # Schema is now owned by db_setup.py / fts_db_migrations.py.
        # db_logger is write-only and must not create runtime tables.

    def _dbl_ensure_lot_callback_tables(self) -> None:
        """Validate execution extension schema without creating tables.

        Formal schema ownership belongs to db_setup.py / fts_db_migrations.py.
        This logger is write-only; if schema is missing, tell the operator to run
        the database upgrade instead of silently creating runtime tables here.
        """
        if not self.enabled or not self.cursor:
            return
        required = (
            'execution_position_lots',
            'execution_broker_callbacks',
            'execution_reconciliation_report',
        )
        missing = []
        for table in required:
            self.cursor.execute("SELECT CASE WHEN OBJECT_ID(N'dbo." + table + "', N'U') IS NULL THEN 0 ELSE 1 END")
            row = self.cursor.fetchone()
            if not row or int(row[0]) != 1:
                missing.append(table)
        if missing:
            raise RuntimeError(
                'Missing execution schema tables: ' + ', '.join(missing) +
                '. Run: python db_setup.py --mode upgrade, or run fts_db_migrations.py before execution logging.'
            )

    def _dbl_upsert_position_lot(self, row: dict, snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.cursor:
            return
        lot_id = _pick(row, 'lot_id', 'Lot_ID')
        if not lot_id:
            return
        snap_time = _dt(snapshot_time) or _dt(_pick(row, 'snapshot_time')) or datetime.now()
        sql = """
        MERGE dbo.execution_position_lots AS tgt
        USING (SELECT ? AS lot_id) AS src
        ON tgt.lot_id = src.lot_id
        WHEN MATCHED THEN UPDATE SET
            snapshot_time=?, ticker_symbol=?, direction_bucket=?, status=?, open_qty=?, remaining_qty=?, avg_cost=?, entry_price=?,
            market_price=?, market_value=?, unrealized_pnl=?, realized_pnl=?, entry_time=?, close_time=?, entry_order_id=?, exit_order_id=?,
            strategy_name=?, updated_at=?, raw_json=?
        WHEN NOT MATCHED THEN INSERT (
            lot_id, snapshot_time, ticker_symbol, direction_bucket, status, open_qty, remaining_qty, avg_cost, entry_price,
            market_price, market_value, unrealized_pnl, realized_pnl, entry_time, close_time, entry_order_id, exit_order_id,
            strategy_name, updated_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        vals = [
            str(lot_id),
            snap_time,
            _pick(row, 'ticker_symbol', 'ticker', 'symbol', 'Ticker SYMBOL'),
            _pick(row, 'direction_bucket', 'side'),
            _pick(row, 'status', default='OPEN'),
            _pick(row, 'open_qty'),
            _pick(row, 'remaining_qty', 'qty'),
            _pick(row, 'avg_cost'),
            _pick(row, 'entry_price'),
            _pick(row, 'market_price'),
            _pick(row, 'market_value'),
            _pick(row, 'unrealized_pnl'),
            _pick(row, 'realized_pnl'),
            _dt(_pick(row, 'entry_time')),
            _dt(_pick(row, 'close_time')),
            _pick(row, 'entry_order_id'),
            _pick(row, 'exit_order_id'),
            _pick(row, 'strategy_name'),
            datetime.now(),
            _dblogger_json(row),
        ]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def _dbl_replace_position_lots(self, rows: list[dict], snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.cursor:
            return
        self.ensure_lot_callback_tables()
        snap_time = _dt(snapshot_time) or datetime.now()
        for row in rows or []:
            self.upsert_position_lot(row, snapshot_time=snap_time.isoformat(timespec='seconds'))

    def _dbl_ingest_broker_callback(self, event: dict) -> None:
        if not self.enabled or not self.cursor:
            return
        self.ensure_lot_callback_tables()
        broker_order_id = _pick(event, 'broker_order_id', 'order_id')
        event_type = _pick(event, 'event_type', 'type', default='UNKNOWN')
        ts = _dt(_pick(event, 'timestamp', 'callback_time', 'updated_at')) or datetime.now()
        callback_id = _pick(event, 'callback_id') or f"{broker_order_id or 'NOORDER'}-{event_type}-{ts.isoformat(timespec='seconds')}-{_pick(event, 'filled_qty', default=0)}"
        sql = """
        MERGE dbo.execution_broker_callbacks AS tgt
        USING (SELECT ? AS callback_id) AS src
        ON tgt.callback_id = src.callback_id
        WHEN MATCHED THEN UPDATE SET
            broker_order_id=?, client_order_id=?, event_type=?, status=?, ticker_symbol=?, filled_qty=?, remaining_qty=?, avg_fill_price=?, callback_time=?, raw_json=?
        WHEN NOT MATCHED THEN INSERT (
            callback_id, broker_order_id, client_order_id, event_type, status, ticker_symbol, filled_qty, remaining_qty, avg_fill_price, callback_time, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        vals = [
            str(callback_id),
            broker_order_id,
            _pick(event, 'client_order_id'),
            event_type,
            _pick(event, 'status'),
            _pick(event, 'ticker_symbol', 'symbol', 'ticker', 'Ticker SYMBOL'),
            _pick(event, 'filled_qty'),
            _pick(event, 'remaining_qty'),
            _pick(event, 'avg_fill_price', 'fill_price'),
            ts,
            _dblogger_json(event),
        ]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def _index_by(rows: list[dict], *keys: str) -> dict[str, dict]:
        out = {}
        for row in rows or []:
            val = None
            for k in keys:
                val = row.get(k)
                if val not in (None, ''):
                    break
            if val not in (None, ''):
                out[str(val)] = row
        return out

    def _dbl_reconcile_execution_state(self, local_orders=None, broker_orders=None, local_fills=None, broker_fills=None, local_positions=None, broker_positions=None, local_lots=None, broker_lots=None, local_cash=None, broker_cash=None, note: str = '') -> dict:
        local_orders = local_orders or []
        broker_orders = broker_orders or []
        local_fills = local_fills or []
        broker_fills = broker_fills or []
        local_positions = local_positions or []
        broker_positions = broker_positions or []
        local_lots = local_lots or []
        broker_lots = broker_lots or []
        order_diff = []
        lo = _index_by(local_orders, 'broker_order_id', 'order_id', 'client_order_id')
        bo = _index_by(broker_orders, 'broker_order_id', 'order_id', 'client_order_id')
        for oid in sorted(set(lo) | set(bo)):
            if oid not in lo or oid not in bo:
                order_diff.append({'id': oid, 'reason': 'missing_local' if oid not in lo else 'missing_broker'})
            elif str(lo[oid].get('status', '')).upper() != str(bo[oid].get('status', '')).upper():
                order_diff.append({'id': oid, 'reason': 'status_diff', 'local': lo[oid].get('status'), 'broker': bo[oid].get('status')})
        fill_diff = []
        lf = _index_by(local_fills, 'fill_id')
        bf = _index_by(broker_fills, 'fill_id')
        for fid in sorted(set(lf) | set(bf)):
            if fid not in lf or fid not in bf:
                fill_diff.append({'id': fid, 'reason': 'missing_local' if fid not in lf else 'missing_broker'})
        def pos_map(rows):
            out={}
            for r in rows or []:
                k=str(r.get('ticker_symbol', r.get('ticker', r.get('symbol', r.get('Ticker SYMBOL','')))))
                if k:
                    out[k]=int(r.get('qty', r.get('quantity', 0)) or 0)
            return out
        lp, bp = pos_map(local_positions), pos_map(broker_positions)
        pos_diff=[{'ticker':k,'local_qty':lp.get(k,0),'broker_qty':bp.get(k,0),'diff_qty':lp.get(k,0)-bp.get(k,0)} for k in sorted(set(lp)|set(bp)) if lp.get(k,0)!=bp.get(k,0)]
        ll = _index_by(local_lots, 'lot_id')
        bl = _index_by(broker_lots, 'lot_id')
        lot_diff=[]
        for lot_id in sorted(set(ll)|set(bl)):
            if lot_id not in ll or lot_id not in bl:
                lot_diff.append({'lot_id':lot_id,'reason':'missing_local' if lot_id not in ll else 'missing_broker'})
            elif int(ll[lot_id].get('remaining_qty',0) or 0)!=int(bl[lot_id].get('remaining_qty',0) or 0):
                lot_diff.append({'lot_id':lot_id,'reason':'qty_diff','local_qty':ll[lot_id].get('remaining_qty'),'broker_qty':bl[lot_id].get('remaining_qty')})
        cash_diff = None
        try:
            if local_cash is not None and broker_cash is not None:
                cash_diff = round(float(local_cash)-float(broker_cash), 4)
        except Exception:
            cash_diff = None
        status = 'OK' if not (order_diff or fill_diff or pos_diff or lot_diff or (cash_diff is not None and abs(cash_diff)>1.0)) else 'MISMATCH'
        summary = {'status':status,'order_mismatch_count':len(order_diff),'fill_mismatch_count':len(fill_diff),'position_mismatch_count':len(pos_diff),'lot_mismatch_count':len(lot_diff),'cash_diff':cash_diff,'orders':order_diff[:50],'fills':fill_diff[:50],'positions':pos_diff[:50],'lots':lot_diff[:50],'note':note}
        if self.enabled and self.cursor:
            self.ensure_lot_callback_tables()
            rid = f"REC-{datetime.now().strftime('%Y%m%d%H%M%S')}-{_uuid.uuid4().hex[:6].upper()}"
            sql = """
            MERGE dbo.execution_reconciliation_report AS tgt
            USING (SELECT ? AS reconcile_id) AS src
            ON tgt.reconcile_id = src.reconcile_id
            WHEN MATCHED THEN UPDATE SET
                reconcile_time=?, status=?, order_mismatch_count=?, fill_mismatch_count=?,
                position_mismatch_count=?, lot_mismatch_count=?, cash_diff=?, summary_json=?
            WHEN NOT MATCHED THEN INSERT (
                reconcile_id, reconcile_time, status, order_mismatch_count, fill_mismatch_count,
                position_mismatch_count, lot_mismatch_count, cash_diff, summary_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """
            vals = [rid, datetime.now(), status, len(order_diff), len(fill_diff), len(pos_diff), len(lot_diff), cash_diff, _dblogger_json(summary)]
            self.cursor.execute(sql, vals + vals)
            self.conn.commit()
            summary['reconcile_id'] = rid
        return summary

    def _dbl_patched_sync_runtime_snapshot(self, account_row: dict, position_rows: list[dict], snapshot_time: Optional[str] = None, note: str = '') -> None:
        _DBL_ORIG_SYNC_RUNTIME(self, account_row, position_rows, snapshot_time=snapshot_time, note=note)
        lots = []
        if isinstance(account_row, dict):
            lots = account_row.get('position_lots') or account_row.get('lots') or []
        if lots:
            self.replace_position_lots(lots, snapshot_time=snapshot_time)

    SQLServerExecutionLogger.__init__ = _dbl_patched_init
    SQLServerExecutionLogger.ensure_lot_callback_tables = _dbl_ensure_lot_callback_tables
    SQLServerExecutionLogger.upsert_position_lot = _dbl_upsert_position_lot
    SQLServerExecutionLogger.replace_position_lots = _dbl_replace_position_lots
    SQLServerExecutionLogger.ingest_broker_callback = _dbl_ingest_broker_callback
    SQLServerExecutionLogger.reconcile_execution_state = _dbl_reconcile_execution_state
    SQLServerExecutionLogger.sync_runtime_snapshot = _dbl_patched_sync_runtime_snapshot

except Exception:
    pass


# =============================================================================
# vNext institutional lot lifecycle extension
# =============================================================================
try:
    import uuid as _uuid2

    def _dbl_json_list(v):
        import json
        if v is None:
            return None
        if isinstance(v, str):
            return v
        try:
            return json.dumps(list(v), ensure_ascii=False)
        except Exception:
            try:
                return json.dumps(v, ensure_ascii=False)
            except Exception:
                return None

    def _dbl_upsert_position_lot_v2(self, row: dict, snapshot_time: Optional[str] = None) -> None:
        if not self.enabled or not self.cursor:
            return
        lot_id = _pick(row, 'lot_id', 'Lot_ID')
        if not lot_id:
            return
        snap_time = _dt(snapshot_time) or _dt(_pick(row, 'snapshot_time')) or datetime.now()
        sql = """
        MERGE dbo.execution_position_lots AS tgt
        USING (SELECT ? AS lot_id) AS src
        ON tgt.lot_id = src.lot_id
        WHEN MATCHED THEN UPDATE SET
            snapshot_time=?, ticker_symbol=?, direction_bucket=?, status=?, open_qty=?, remaining_qty=?, avg_cost=?, entry_price=?,
            market_price=?, market_value=?, unrealized_pnl=?, realized_pnl=?, entry_time=?, close_time=?, entry_order_id=?, exit_order_id=?,
            strategy_name=?, signal_id=?, client_order_id=?, position_key=?, strategy_bucket=?, cost_basis_method=?,
            entry_fill_qty=?, close_fill_qty=?, entry_fill_count=?, close_fill_count=?, entry_fill_ids_json=?, exit_fill_ids_json=?,
            open_commission=?, open_tax=?, close_commission=?, close_tax=?, stop_order_id=?, stop_price=?, stop_status=?, linked_stop_qty=?,
            last_fill_time=?, updated_at=?, raw_json=?
        WHEN NOT MATCHED THEN INSERT (
            lot_id, snapshot_time, ticker_symbol, direction_bucket, status, open_qty, remaining_qty, avg_cost, entry_price,
            market_price, market_value, unrealized_pnl, realized_pnl, entry_time, close_time, entry_order_id, exit_order_id,
            strategy_name, signal_id, client_order_id, position_key, strategy_bucket, cost_basis_method,
            entry_fill_qty, close_fill_qty, entry_fill_count, close_fill_count, entry_fill_ids_json, exit_fill_ids_json,
            open_commission, open_tax, close_commission, close_tax, stop_order_id, stop_price, stop_status, linked_stop_qty,
            last_fill_time, updated_at, raw_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        vals = [str(lot_id), snap_time, _pick(row, 'ticker_symbol', 'ticker', 'symbol', 'Ticker SYMBOL'), _pick(row, 'direction_bucket', 'side'), _pick(row, 'lifecycle_status', 'status', default='OPEN'), _pick(row, 'open_qty'), _pick(row, 'remaining_qty', 'qty'), _pick(row, 'avg_cost'), _pick(row, 'entry_price'), _pick(row, 'market_price'), _pick(row, 'market_value'), _pick(row, 'unrealized_pnl'), _pick(row, 'realized_pnl'), _dt(_pick(row, 'entry_time')), _dt(_pick(row, 'close_time')), _pick(row, 'entry_order_id'), _pick(row, 'exit_order_id'), _pick(row, 'strategy_name'), _pick(row, 'signal_id'), _pick(row, 'client_order_id'), _pick(row, 'position_key'), _pick(row, 'strategy_bucket'), _pick(row, 'cost_basis_method'), _pick(row, 'entry_fill_qty'), _pick(row, 'close_fill_qty'), _pick(row, 'entry_fill_count'), _pick(row, 'close_fill_count'), _dbl_json_list(_pick(row, 'entry_fill_ids_json', 'entry_fill_ids')), _dbl_json_list(_pick(row, 'exit_fill_ids_json', 'exit_fill_ids')), _pick(row, 'open_commission'), _pick(row, 'open_tax'), _pick(row, 'close_commission'), _pick(row, 'close_tax'), _pick(row, 'stop_order_id'), _pick(row, 'stop_price'), _pick(row, 'stop_status'), _pick(row, 'linked_stop_qty'), _dt(_pick(row, 'last_fill_time')), datetime.now(), _dblogger_json(row)]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    def _dbl_ingest_broker_callback_v2(self, event: dict) -> None:
        if not self.enabled or not self.cursor:
            return
        self.ensure_lot_callback_tables()
        broker_order_id = _pick(event, 'broker_order_id', 'order_id')
        event_type = _pick(event, 'event_type', 'type', default='UNKNOWN')
        ts = _dt(_pick(event, 'timestamp', 'callback_time', 'updated_at', 'fill_time')) or datetime.now()
        callback_id = _pick(event, 'callback_id') or f"{broker_order_id or 'NOORDER'}-{event_type}-{ts.isoformat(timespec='seconds')}-{_uuid2.uuid4().hex[:6]}"
        fill_qty = int(_pick(event, 'filled_qty', 'fill_qty', default=0) or 0)
        avg_fill_price = _pick(event, 'avg_fill_price', 'fill_price')
        fill_notional = None
        try:
            fill_notional = round(float(avg_fill_price or 0.0) * fill_qty, 4)
        except Exception:
            fill_notional = None
        sql = """
        MERGE dbo.execution_broker_callbacks AS tgt
        USING (SELECT ? AS callback_id) AS src
        ON tgt.callback_id = src.callback_id
        WHEN MATCHED THEN UPDATE SET broker_order_id=?, client_order_id=?, event_type=?, status=?, ticker_symbol=?, filled_qty=?, remaining_qty=?, avg_fill_price=?, lot_id=?, position_key=?, strategy_name=?, signal_id=?, fill_notional=?, callback_time=?, raw_json=?
        WHEN NOT MATCHED THEN INSERT (callback_id, broker_order_id, client_order_id, event_type, status, ticker_symbol, filled_qty, remaining_qty, avg_fill_price, lot_id, position_key, strategy_name, signal_id, fill_notional, callback_time, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        vals = [str(callback_id), broker_order_id, _pick(event, 'client_order_id'), event_type, _pick(event, 'status'), _pick(event, 'ticker_symbol', 'symbol', 'ticker', 'Ticker SYMBOL'), fill_qty, _pick(event, 'remaining_qty'), avg_fill_price, _pick(event, 'lot_id'), _pick(event, 'position_key'), _pick(event, 'strategy_name'), _pick(event, 'signal_id'), fill_notional, ts, _dblogger_json(event)]
        self.cursor.execute(sql, vals + vals)
        self.conn.commit()

    SQLServerExecutionLogger.upsert_position_lot = _dbl_upsert_position_lot_v2
    SQLServerExecutionLogger.ingest_broker_callback = _dbl_ingest_broker_callback_v2
except Exception:
    pass
