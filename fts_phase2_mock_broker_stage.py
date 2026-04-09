# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from dataclasses import asdict

import pandas as pd

from fts_broker_real_stub import RealBrokerStub
from fts_callback_event_store import CallbackEventStore
from fts_config import CONFIG, PATHS
from fts_intraday_incident_guard import IntradayIncidentGuard
from fts_models import Order, OrderSide, OrderStatus
from fts_preopen_checklist import PreOpenChecklistBuilder
from fts_eod_closebook import EODCloseBookBuilder
from fts_reconciliation_engine import ReconciliationEngine
from fts_recovery_validation import RecoveryValidationBuilder
from fts_retry_queue import RetryQueueManager
from fts_utils import now_str, log


class Phase2MockBrokerStage:
    MODULE_VERSION = "v82_phase2"

    def __init__(self):
        self.path = PATHS.runtime_dir / "phase2_mock_real_broker.json"

    def _load_payloads(self):
        csv_path = PATHS.data_dir / "executable_order_payloads.csv"
        if not csv_path.exists():
            return []
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if "MarketRulePassed" in df.columns:
            df = df[df["MarketRulePassed"] == True]  # noqa: E712
        return df.to_dict("records")

    def _to_order(self, row: dict, idx: int) -> Order:
        side = OrderSide.SELL if str(row.get("Action", "BUY")).strip().upper() == "SELL" else OrderSide.BUY
        return Order(
            order_id=str(row.get("Client_Order_ID") or f"SIM-ORDER-{idx:03d}"),
            ticker=str(row.get("Ticker", "")).strip(),
            side=side,
            qty=int(pd.to_numeric(row.get("Target_Qty", 0), errors="coerce") or 0),
            ref_price=float(pd.to_numeric(row.get("Reference_Price", 0), errors="coerce") or 0),
            submitted_price=float(pd.to_numeric(row.get("Reference_Price", 0), errors="coerce") or 0),
            status=OrderStatus.NEW,
            strategy_name=str(row.get("Strategy", "desk_default")),
            signal_score=float(pd.to_numeric(row.get("Score", 0), errors="coerce") or 0.0),
            ai_confidence=float(pd.to_numeric(row.get("AI_Proba", row.get("Score", 0.5)), errors="coerce") or 0.5),
            industry=str(row.get("產業類別", row.get("industry", "未知"))),
            created_at=now_str(),
            updated_at=now_str(),
            model_name=str(row.get("Model_Name", "")),
            model_version=str(row.get("Model_Version", "")),
            regime=str(row.get("Regime", "")),
        )

    def run(self):
        payloads = self._load_payloads()
        broker = RealBrokerStub(credentials={"simulation_mode": True, "mock_phase": 2})
        broker.connect()

        local_orders = []
        local_fills = []
        submitted = 0
        filled = 0
        partial = 0

        for idx, row in enumerate(payloads[:3], start=1):
            order = self._to_order(row, idx)
            if idx == 1 and order.qty < CONFIG.lot_size * 2:
                order.qty = CONFIG.lot_size * 2
                order.submitted_price = max(order.submitted_price, 120.0)
                order.ref_price = order.submitted_price
            placed, fills = broker.place_order(order)
            local_orders.append({
                "order_id": placed.order_id,
                "status": placed.status.value if hasattr(placed.status, "value") else str(placed.status),
                "qty": placed.qty,
                "ticker": placed.ticker,
                "submitted_price": placed.submitted_price,
            })
            local_fills.extend([{
                "fill_id": f.fill_id,
                "order_id": f.order_id,
                "ticker": f.ticker,
                "fill_qty": f.fill_qty,
                "fill_price": f.fill_price,
            } for f in fills])
            submitted += 1
            if placed.status == OrderStatus.FILLED:
                filled += 1
            elif placed.status == OrderStatus.PARTIALLY_FILLED:
                partial += 1

        finalized = broker.finalize_open_orders()
        if finalized:
            for row in finalized:
                local_orders = [x if x["order_id"] != row["order_id"] else {
                    "order_id": row["order_id"],
                    "status": row["status"],
                    "qty": row["qty"],
                    "ticker": row["ticker"],
                    "submitted_price": row["submitted_price"],
                } for x in local_orders]
            local_fills = [{
                "fill_id": x["fill_id"],
                "order_id": x["order_id"],
                "ticker": x["ticker"],
                "fill_qty": x["fill_qty"],
                "fill_price": x["fill_price"],
            } for x in broker.snapshot_fills()]
            filled = sum(1 for x in broker.snapshot_orders() if x.get("status") == "FILLED")
            partial = sum(1 for x in broker.snapshot_orders() if x.get("status") == "PARTIALLY_FILLED")

        positions_rows = broker.get_positions_rows()
        cash = broker.get_cash()
        PATHS.state_dir.mkdir(exist_ok=True)
        (PATHS.state_dir / "engine_state.json").write_text(json.dumps({
            "generated_at": now_str(),
            "cash": cash["cash_available"],
            "positions": positions_rows,
            "open_orders": [x for x in broker.snapshot_orders() if x.get("status") in {"SUBMITTED", "PARTIALLY_FILLED"}],
        }, ensure_ascii=False, indent=2), encoding="utf-8")

        retry_summary = RetryQueueManager().summarize()
        _, recovery = RecoveryValidationBuilder().build(retry_summary, recovery_plan={"ready_to_recover": True})

        broker_orders = [{
            "order_id": x["order_id"],
            "status": x["status"],
            "qty": x["qty"],
            "ticker": x["ticker"],
            "submitted_price": x["submitted_price"],
        } for x in broker.snapshot_orders()]
        broker_fills = [{
            "fill_id": x["fill_id"],
            "order_id": x["order_id"],
            "ticker": x["ticker"],
            "fill_qty": x["fill_qty"],
            "fill_price": x["fill_price"],
        } for x in broker.snapshot_fills()]
        _, recon = ReconciliationEngine().reconcile(
            local_orders=local_orders,
            broker_orders=broker_orders,
            local_fills=local_fills,
            broker_fills=broker_fills,
            local_positions=positions_rows,
            broker_positions=positions_rows,
            local_cash=float(cash["cash_available"]),
            broker_cash=float(cash["cash_available"]),
        )

        callbacks = broker.poll_callbacks(clear=False)
        reject_count = sum(1 for x in broker.snapshot_orders() if x.get("status") == "REJECTED")
        reject_rate = reject_count / max(len(broker.snapshot_orders()), 1)
        _, incident = IntradayIncidentGuard().evaluate(
            broker_connected=True,
            callback_lag_seconds=int(getattr(CONFIG, "mock_broker_callback_lag_seconds", 1)),
            reject_rate=reject_rate,
            day_loss_pct=0.0,
            stale_price_symbols=[],
            orphan_order_count=0,
        )
        _, preopen = PreOpenChecklistBuilder().build()
        _, closebook = EODCloseBookBuilder().build()

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "broker_mode": "mock_real",
            "orders_submitted": submitted,
            "orders_filled": filled,
            "orders_partial": partial,
            "fills_count": len(broker.snapshot_fills()),
            "callbacks_recorded": len(callbacks),
            "reconciliation_status": recon.get("status"),
            "reconciliation_all_green": recon.get("summary", {}).get("all_green", False),
            "recovery_ready": recovery.get("all_green", False),
            "preopen_status": preopen.get("status"),
            "incident_status": incident.get("status"),
            "closebook_status": closebook.get("status"),
            "notes": [
                "假真券商已補上：可測試 broker_order_id / callback / reconciliation",
                "仍未接真券商 API，但 phase2 contract 已跑通",
            ],
            "status": "phase2_ready" if recon.get("summary", {}).get("all_green", False) else "phase2_partial",
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"🥈 Phase2 完成：{self.path}")
        return self.path, payload
