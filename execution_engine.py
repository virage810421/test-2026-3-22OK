from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

import pandas as pd

from broker_base import OrderRequest, OrderSide, OrderType, OrderStatus
try:
    from paper_broker import PaperBroker
except Exception:
    from fts_broker_paper import PaperBroker
from risk_gateway import RiskGateway
from db_logger import SQLServerExecutionLogger

LOG_DIR = "execution_logs"
ORDER_BLOTTER_PATH = os.path.join(LOG_DIR, "order_blotter.csv")
FILL_BLOTTER_PATH = os.path.join(LOG_DIR, "fill_blotter.csv")
STATE_PATH = os.path.join(LOG_DIR, "execution_state.json")

class ExecutionEngine:
    def __init__(self, broker: PaperBroker, risk_gateway: RiskGateway, db_logger: Optional[SQLServerExecutionLogger] = None, max_consecutive_rejects: int = 3, max_consecutive_exceptions: int = 2):
        self.broker = broker
        self.risk_gateway = risk_gateway
        self.db_logger = db_logger
        self.max_consecutive_rejects = int(max_consecutive_rejects)
        self.max_consecutive_exceptions = int(max_consecutive_exceptions)
        os.makedirs(LOG_DIR, exist_ok=True)
        self._load_state()

    def run_from_csv(self, decision_csv_path: str) -> None:
        print("========================================================")
        print("🚀 啟動 Execution Engine：決策桌 → 風控 → 券商 → blotter")
        print("========================================================")
        if not os.path.exists(decision_csv_path):
            print(f"❌ 找不到檔案：{decision_csv_path}")
            return
        df = pd.read_csv(decision_csv_path)
        if df.empty:
            print("⚠️ 決策桌為空")
            return
        required_cols = {"Ticker SYMBOL", "Action", "Target_Qty", "Reference_Price"}
        missing = required_cols - set(df.columns)
        if missing:
            print(f"❌ 缺少必要欄位：{missing}")
            return
        self._push_market_prices(df)
        submit_count = 0
        skip_count = 0
        consecutive_rejects = 0
        consecutive_exceptions = 0
        for _, row in df.iterrows():
            order_req = self._row_to_order_request(row)
            if order_req is None:
                skip_count += 1
                continue
            if order_req.side in (OrderSide.SHORT, OrderSide.COVER) and not getattr(self.broker, 'supports_short', False):
                print(f"⛔ broker 不支援 {order_req.side.value} | {order_req.symbol}")
                skip_count += 1
                continue
            try:
                ref_price = self._resolve_ref_price(order_req)
                risk_result = self.risk_gateway.validate(order_req, ref_price)
                if not risk_result.approved:
                    print(f"⛔ 風控拒單 | {order_req.symbol} | {risk_result.reason}")
                    skip_count += 1
                    continue
                record = self.broker.place_order(order_req)
                order_row = self._order_record_to_row(record)
                self._append_csv_row(ORDER_BLOTTER_PATH, order_row)
                if self.db_logger:
                    self.db_logger.insert_order(order_row)
                print(f"📨 {record.order_id} | {record.symbol} | {record.side.value} | Qty={record.quantity} | Status={record.status.value}")
                if order_req.signal_id:
                    self.risk_gateway.register_signal(order_req.signal_id)
                submit_count += 1
                consecutive_exceptions = 0
                if record.status == OrderStatus.REJECTED:
                    consecutive_rejects += 1
                else:
                    consecutive_rejects = 0
                if consecutive_rejects >= self.max_consecutive_rejects:
                    print(f"🛑 連續 {consecutive_rejects} 筆券商拒單，觸發 execution circuit breaker，中止後續送單")
                    break
            except Exception as e:
                consecutive_exceptions += 1
                print(f"❌ execution exception | {order_req.symbol} | {e}")
                if consecutive_exceptions >= self.max_consecutive_exceptions:
                    print(f"🛑 連續 {consecutive_exceptions} 次 execution 例外，觸發 circuit breaker")
                    break
        try:
            fills = self.broker.poll_fills()
        except Exception as e:
            fills = []
            print(f"⚠️ poll_fills 失敗：{e}")
        for fill in fills:
            fill_row = self._fill_event_to_row(fill)
            self._append_csv_row(FILL_BLOTTER_PATH, fill_row)
            if self.db_logger:
                self.db_logger.insert_fill(fill_row)
        self._save_state()
        print("--------------------------------------------------------")
        print(f"✅ 本輪完成 | 送單: {submit_count} | 跳過: {skip_count}")
        print(f"💰 Cash: {self.broker.get_cash():,.2f}")
        print(f"📦 Positions: {self.broker.get_positions()}")
        print("========================================================")

    def _push_market_prices(self, df: pd.DataFrame) -> None:
        price_map = {}
        for _, row in df.iterrows():
            symbol = str(row.get("Ticker SYMBOL", "")).strip()
            ref_price = self._safe_float(row.get("Reference_Price", 0))
            if symbol and ref_price > 0:
                price_map[symbol] = ref_price
        self.broker.update_market_prices(price_map)

    def _row_to_order_request(self, row: pd.Series) -> Optional[OrderRequest]:
        symbol = str(row.get("Ticker SYMBOL", "")).strip()
        action_raw = str(row.get("Action", "")).strip().upper()
        qty = int(self._safe_float(row.get("Target_Qty", 0)))
        ref_price = self._safe_float(row.get("Reference_Price", 0))
        order_type_raw = str(row.get("Order_Type", "MARKET")).strip().upper()
        strategy_name = str(row.get("Strategy_Name", "")).strip()
        signal_id = str(row.get("Signal_ID", "")).strip()
        note = str(row.get("Note", "")).strip()
        if not symbol or qty <= 0:
            return None
        side_map = {"BUY": OrderSide.BUY, "SELL": OrderSide.SELL, "SHORT": OrderSide.SHORT, "COVER": OrderSide.COVER}
        if action_raw not in side_map:
            print(f"⚠️ 不支援 Action: {action_raw} | {symbol}")
            return None
        order_type = OrderType.MARKET if order_type_raw == "MARKET" else OrderType.LIMIT
        limit_price = ref_price if order_type == OrderType.LIMIT else None
        return OrderRequest(symbol=symbol, side=side_map[action_raw], quantity=qty, order_type=order_type, limit_price=limit_price, strategy_name=strategy_name, signal_id=signal_id, client_order_id=signal_id, note=note)

    def _resolve_ref_price(self, order_req: OrderRequest) -> float:
        if order_req.order_type == OrderType.LIMIT and order_req.limit_price:
            return float(order_req.limit_price)
        return float(getattr(self.broker, 'last_prices', {}).get(order_req.symbol, 0.0))

    def _save_state(self) -> None:
        state = {"save_time": datetime.now().isoformat(timespec="seconds"), "cash": self.broker.get_cash(), "positions": self.broker.get_positions(), "used_signal_ids": sorted(list(self.risk_gateway.used_signal_ids)), "open_orders": [self._order_record_to_row(o) for o in self.broker.get_open_orders()]}
        with open(STATE_PATH, "w", encoding="utf-8") as f:
            json.dump(state, f, ensure_ascii=False, indent=2)

    def _load_state(self) -> None:
        if not os.path.exists(STATE_PATH):
            return
        try:
            with open(STATE_PATH, "r", encoding="utf-8") as f:
                state = json.load(f)
            for sid in state.get("used_signal_ids", []):
                self.risk_gateway.used_signal_ids.add(sid)
            positions = state.get("positions", {})
            if isinstance(positions, dict):
                self.broker.positions = {k: int(v) for k, v in positions.items()}
            self.broker.cash = float(state.get("cash", self.broker.cash))
        except Exception as e:
            print(f"⚠️ 載入 state 失敗：{e}")

    @staticmethod
    def _append_csv_row(path: str, row: dict) -> None:
        df = pd.DataFrame([row])
        file_exists = os.path.exists(path)
        df.to_csv(path, mode="a", index=False, header=not file_exists, encoding="utf-8-sig")

    @staticmethod
    def _order_record_to_row(record) -> dict:
        return {"order_id": record.order_id, "symbol": record.symbol, "side": record.side.value, "quantity": record.quantity, "filled_qty": record.filled_qty, "remaining_qty": record.remaining_qty, "avg_fill_price": record.avg_fill_price, "order_type": record.order_type.value, "limit_price": record.limit_price, "status": record.status.value, "create_time": record.create_time, "update_time": record.update_time, "reject_reason": record.reject_reason, "strategy_name": record.strategy_name, "signal_id": record.signal_id, "client_order_id": record.client_order_id, "note": record.note}

    @staticmethod
    def _fill_event_to_row(fill) -> dict:
        return {"order_id": fill.order_id, "symbol": fill.symbol, "side": fill.side.value, "fill_qty": fill.fill_qty, "fill_price": fill.fill_price, "fill_time": fill.fill_time, "commission": fill.commission, "tax": fill.tax, "slippage": fill.slippage, "strategy_name": fill.strategy_name, "signal_id": fill.signal_id, "note": fill.note}

    @staticmethod
    def _safe_float(x, default: float = 0.0) -> float:
        try:
            if pd.isna(x):
                return default
            return float(x)
        except Exception:
            return default
