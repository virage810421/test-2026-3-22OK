# execution_engine.py
from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from typing import Optional

import pandas as pd

from .broker_base import OrderRequest, OrderSide, OrderType
from .paper_broker import PaperBroker
from .risk_gateway import RiskGateway
from .db_logger import SQLServerExecutionLogger


LOG_DIR = "execution_logs"
ORDER_BLOTTER_PATH = os.path.join(LOG_DIR, "order_blotter.csv")
FILL_BLOTTER_PATH = os.path.join(LOG_DIR, "fill_blotter.csv")
STATE_PATH = os.path.join(LOG_DIR, "execution_state.json")


class ExecutionEngine:
    def __init__(
        self,
        broker: PaperBroker,
        risk_gateway: RiskGateway,
        db_logger: Optional[SQLServerExecutionLogger] = None,
    ):
        self.broker = broker
        self.risk_gateway = risk_gateway
        self.db_logger = db_logger

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

        for _, row in df.iterrows():
            order_req = self._row_to_order_request(row)
            if order_req is None:
                skip_count += 1
                continue

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

            print(
                f"📨 {record.order_id} | {record.symbol} | {record.side.value} | "
                f"Qty={record.quantity} | Status={record.status.value}"
            )

            if order_req.signal_id:
                self.risk_gateway.register_signal(order_req.signal_id)

            submit_count += 1

        fills = self.broker.poll_fills()
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
        print(f"🧾 order_blotter: {ORDER_BLOTTER_PATH}")
        print(f"🧾 fill_blotter: {FILL_BLOTTER_PATH}")
        print(f"🧠 state: {STATE_PATH}")
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

        side_map = {
            "BUY": OrderSide.BUY,
            "SELL": OrderSide.SELL,
            "SHORT": OrderSide.SHORT,
            "COVER": OrderSide.COVER,
        }
        if action_raw not in side_map:
            print(f"⚠️ 不支援 Action: {action_raw} | {symbol}")
            return None

        order_type = OrderType.MARKET if order_type_raw == "MARKET" else OrderType.LIMIT
        limit_price = ref_price if order_type == OrderType.LIMIT else None

        return OrderRequest(
            symbol=symbol,
            side=side_map[action_raw],
            quantity=qty,
            order_type=order_type,
            limit_price=limit_price,
            strategy_name=strategy_name,
            signal_id=signal_id,
            client_order_id=signal_id,
            note=note,
        )

    def _resolve_ref_price(self, order_req: OrderRequest) -> float:
        if order_req.order_type == OrderType.LIMIT and order_req.limit_price:
            return float(order_req.limit_price)
        return float(self.broker.last_prices.get(order_req.symbol, 0.0))

    def _save_state(self) -> None:
        state = {
            "save_time": datetime.now().isoformat(timespec="seconds"),
            "cash": self.broker.get_cash(),
            "positions": self.broker.get_positions(),
            "used_signal_ids": sorted(list(self.risk_gateway.used_signal_ids)),
            "open_orders": [self._order_record_to_row(o) for o in self.broker.get_open_orders()],
        }
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

            cash = state.get("cash", self.broker.cash)
            self.broker.cash = float(cash)

        except Exception as e:
            print(f"⚠️ 載入 state 失敗：{e}")

    @staticmethod
    def _append_csv_row(path: str, row: dict) -> None:
        df = pd.DataFrame([row])
        file_exists = os.path.exists(path)
        df.to_csv(
            path,
            mode="a",
            index=False,
            header=not file_exists,
            encoding="utf-8-sig",
        )

    @staticmethod
    def _order_record_to_row(record) -> dict:
        return {
            "order_id": record.order_id,
            "symbol": record.symbol,
            "side": record.side.value,
            "quantity": record.quantity,
            "filled_qty": record.filled_qty,
            "remaining_qty": record.remaining_qty,
            "avg_fill_price": record.avg_fill_price,
            "order_type": record.order_type.value,
            "limit_price": record.limit_price,
            "status": record.status.value,
            "create_time": record.create_time,
            "update_time": record.update_time,
            "reject_reason": record.reject_reason,
            "strategy_name": record.strategy_name,
            "signal_id": record.signal_id,
            "client_order_id": record.client_order_id,
            "note": record.note,
        }

    @staticmethod
    def _fill_event_to_row(fill) -> dict:
        return {
            "order_id": fill.order_id,
            "symbol": fill.symbol,
            "side": fill.side.value,
            "fill_qty": fill.fill_qty,
            "fill_price": fill.fill_price,
            "fill_time": fill.fill_time,
            "commission": fill.commission,
            "tax": fill.tax,
            "slippage": fill.slippage,
            "strategy_name": fill.strategy_name,
            "signal_id": fill.signal_id,
            "note": fill.note,
        }

    @staticmethod
    def _safe_float(x, default: float = 0.0) -> float:
        try:
            if pd.isna(x):
                return default
            return float(x)
        except Exception:
            return default


def create_sample_decision_csv(path: str = "daily_decision_desk.csv") -> None:
    sample = pd.DataFrame([
        {
            "Ticker SYMBOL": "2330.TW",
            "Action": "BUY",
            "Target_Qty": 1000,
            "Reference_Price": 820,
            "Order_Type": "MARKET",
            "Strategy_Name": "Regime_Bull_Breakout",
            "Signal_ID": "SIG-2330-20260407-01",
            "Note": "突破買進",
        },
        {
            "Ticker SYMBOL": "2454.TW",
            "Action": "BUY",
            "Target_Qty": 1500,
            "Reference_Price": 1120,
            "Order_Type": "LIMIT",
            "Strategy_Name": "Pullback_Reentry",
            "Signal_ID": "SIG-2454-20260407-01",
            "Note": "拉回承接",
        },
        {
            "Ticker SYMBOL": "2603.TW",
            "Action": "SELL",
            "Target_Qty": 500,
            "Reference_Price": 210,
            "Order_Type": "MARKET",
            "Strategy_Name": "TakeProfit",
            "Signal_ID": "SIG-2603-20260407-01",
            "Note": "多單停利",
        },
        {
            "Ticker SYMBOL": "2881.TW",
            "Action": "COVER",
            "Target_Qty": 500,
            "Reference_Price": 75,
            "Order_Type": "MARKET",
            "Strategy_Name": "CoverShort",
            "Signal_ID": "SIG-2881-20260407-01",
            "Note": "空單回補",
        },
        {
            "Ticker SYMBOL": "2002.TW",
            "Action": "SHORT",
            "Target_Qty": 1000,
            "Reference_Price": 38,
            "Order_Type": "MARKET",
            "Strategy_Name": "BearTrend",
            "Signal_ID": "SIG-2002-20260407-01",
            "Note": "空方測試",
        },
    ])
    sample.to_csv(path, index=False, encoding="utf-8-sig")
    print(f"✅ 已建立範例決策桌：{path}")


if __name__ == "__main__":
    decision_path = "daily_decision_desk.csv"
    if not os.path.exists(decision_path):
        create_sample_decision_csv(decision_path)

    broker = PaperBroker(
        initial_cash=5_000_000,
        commission_rate=0.001425,
        tax_rate_sell=0.003,
        default_slippage_bps=5.0,
        partial_fill_threshold_value=1_500_000,
        partial_fill_ratio=0.5,
    )

    # 預設部位，方便直接測 SELL / COVER
    broker.positions["2603.TW"] = 2000
    broker.positions["2881.TW"] = -1500

    risk_gateway = RiskGateway(
        broker=broker,
        max_single_order_value=1_000_000,
        max_symbol_abs_position=5000,
        allow_short=True,
        block_duplicate_signal_id=True,
        min_cash_buffer=50_000,
    )

    # enabled=True 才會寫 SQL Server
    db_logger = SQLServerExecutionLogger(
        server="localhost",
        database="股票online",
        driver="ODBC Driver 17 for SQL Server",
        trusted_connection="yes",
        enabled=False,
    )

    engine = ExecutionEngine(
        broker=broker,
        risk_gateway=risk_gateway,
        db_logger=db_logger,
    )

    try:
        engine.run_from_csv(decision_path)
    finally:
        db_logger.close()
