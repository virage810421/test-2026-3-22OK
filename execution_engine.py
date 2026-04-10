from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional, Any

import pandas as pd

from broker_base import OrderRequest, OrderSide, OrderType, OrderStatus
try:
    from paper_broker import PaperBroker
except Exception:
    from fts_broker_paper import PaperBroker
from risk_gateway import RiskGateway
from db_logger import SQLServerExecutionLogger
from fts_level3_runtime_loader import build_level3_services

LOG_DIR = "execution_logs"
ORDER_BLOTTER_PATH = os.path.join(LOG_DIR, "order_blotter.csv")
FILL_BLOTTER_PATH = os.path.join(LOG_DIR, "fill_blotter.csv")
STATE_PATH = os.path.join(LOG_DIR, "execution_state.json")
LEVEL3_RUNTIME_PATH = os.path.join('runtime', 'level3_execution_runtime.json')


class ExecutionEngine:
    def __init__(self, broker: PaperBroker, risk_gateway: RiskGateway, db_logger: Optional[SQLServerExecutionLogger] = None, max_consecutive_rejects: int = 3, max_consecutive_exceptions: int = 2):
        self.broker = broker
        self.risk_gateway = risk_gateway
        self.db_logger = db_logger
        self.max_consecutive_rejects = int(max_consecutive_rejects)
        self.max_consecutive_exceptions = int(max_consecutive_exceptions)
        os.makedirs(LOG_DIR, exist_ok=True)
        os.makedirs('runtime', exist_ok=True)
        self.level3_services, self.level3_meta = build_level3_services()
        self._load_state()

    def run_from_csv(self, decision_csv_path: str) -> None:
        print("========================================================")
        print("🚀 啟動 Execution Engine：決策桌 → 風控 → 券商 → blotter")
        print("========================================================")
        df, source_meta = self._load_or_build_orders(decision_csv_path)
        if df is None or df.empty:
            print("⚠️ 決策桌為空")
            return
        required_cols = {"Ticker SYMBOL", "Action", "Target_Qty", "Reference_Price"}
        missing = required_cols - set(df.columns)
        if missing:
            print(f"❌ 缺少必要欄位：{missing}")
            return
        self._emit_level3_preflight(df, source_meta)
        self._push_market_prices(df)
        submit_count = 0
        skip_count = 0
        consecutive_rejects = 0
        consecutive_exceptions = 0
        order_rows_this_run: list[dict[str, Any]] = []
        fill_rows_this_run: list[dict[str, Any]] = []
        kill_switch = self.level3_services.get('KillSwitchManager')
        blocked, reasons = self._check_kill_switch(kill_switch, None, None)
        if blocked:
            print(f"🛑 kill switch blocking execution: {' | '.join(reasons)}")
            self._write_level3_runtime(df, source_meta, order_rows_this_run, fill_rows_this_run, status='blocked_by_kill_switch', extra={'reasons': reasons})
            return
        for _, row in df.iterrows():
            order_req = self._row_to_order_request(row)
            if order_req is None:
                skip_count += 1
                continue
            blocked, reasons = self._check_kill_switch(kill_switch, order_req.symbol, order_req.strategy_name)
            if blocked:
                print(f"🛑 kill switch | {order_req.symbol} | {' | '.join(reasons)}")
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
                order_rows_this_run.append(order_row)
                self._append_csv_row(ORDER_BLOTTER_PATH, order_row)
                if self.db_logger:
                    self.db_logger.insert_order(order_row)
                print(f"📨 {record.order_id} | {record.symbol} | {record.side.value} | Qty={record.quantity} | Status={record.status.value}")
                if order_req.signal_id:
                    self.risk_gateway.register_signal(order_req.signal_id)
                submit_count += 1
                consecutive_exceptions = 0
                transition = self._record_state_transition('NEW', record.status.value)
                if transition and not transition.get('allowed', True):
                    print(f"⚠️ order_state_machine detected illegal transition: {transition}")
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
            fill_rows_this_run.append(fill_row)
            self._append_csv_row(FILL_BLOTTER_PATH, fill_row)
            if self.db_logger:
                self.db_logger.insert_fill(fill_row)
            transition = self._record_state_transition('SUBMITTED', 'FILLED')
            if transition and not transition.get('allowed', True):
                print(f"⚠️ order_state_machine detected illegal fill transition: {transition}")
        self._save_state()
        self._write_level3_runtime(df, source_meta, order_rows_this_run, fill_rows_this_run, status='completed')
        print("--------------------------------------------------------")
        print(f"✅ 本輪完成 | 送單: {submit_count} | 跳過: {skip_count}")
        print(f"💰 Cash: {self.broker.get_cash():,.2f}")
        print(f"📦 Positions: {self.broker.get_positions()}")
        print("========================================================")

    def _load_or_build_orders(self, decision_csv_path: str) -> tuple[pd.DataFrame, dict[str, Any]]:
        if os.path.exists(decision_csv_path):
            try:
                df = pd.read_csv(decision_csv_path)
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        if not df.empty:
            return self._normalize_execution_df(df), {'source': decision_csv_path, 'bridge_used': False}
        bridge = self.level3_services.get('DecisionExecutionBridge')
        if bridge is None:
            return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': False, 'reason': 'missing_bridge'}
        try:
            out_path, payload = bridge.build()
            out_path = str(out_path)
            if os.path.exists(out_path):
                built_df = pd.read_csv(out_path, encoding='utf-8-sig')
                return self._normalize_execution_df(built_df), {'source': out_path, 'bridge_used': True, 'payload_status': payload.get('status', '')}
        except Exception as e:
            return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': True, 'error': repr(e)}
        return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': True, 'reason': 'bridge_output_missing'}

    @staticmethod
    def _normalize_execution_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        rename_map = {}
        if 'Ticker' in df.columns and 'Ticker SYMBOL' not in df.columns:
            rename_map['Ticker'] = 'Ticker SYMBOL'
        if rename_map:
            df = df.rename(columns=rename_map)
        if 'Strategy' in df.columns and 'Strategy_Name' not in df.columns:
            df['Strategy_Name'] = df['Strategy']
        if 'Client_Order_ID' in df.columns and 'Signal_ID' not in df.columns:
            df['Signal_ID'] = df['Client_Order_ID']
        if 'Order_Type' not in df.columns and 'order_type' in df.columns:
            df['Order_Type'] = df['order_type']
        return df

    def _emit_level3_preflight(self, df: pd.DataFrame, source_meta: dict[str, Any]) -> None:
        gate = self.level3_services.get('LiveReadinessGate')
        if gate is not None:
            try:
                gate.evaluate(df.rename(columns={'Ticker SYMBOL': 'Ticker'}))
            except Exception:
                pass
        recovery = self.level3_services.get('RecoveryEngine')
        if recovery is not None:
            try:
                kill_state = self._kill_switch_state()
                recovery.create_snapshot(
                    cash=self.broker.get_cash(),
                    positions=self._broker_positions_for_runtime(),
                    open_orders=self._broker_orders_for_runtime(),
                    recent_fills=self._broker_fills_for_runtime(),
                    kill_switch_state=kill_state,
                    meta={'phase': 'pre_execution', 'source_meta': source_meta},
                )
            except Exception:
                pass
        machine = self.level3_services.get('OrderStateMachine')
        if machine is not None:
            try:
                machine.build_definition()
            except Exception:
                pass

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
        strategy_name = str(row.get("Strategy_Name", row.get('Strategy', ""))).strip()
        signal_id = str(row.get("Signal_ID", row.get('Client_Order_ID', ""))).strip()
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
                if hasattr(self.broker, 'restore_state'):
                    self.broker.restore_state(state.get('cash', self.broker.get_cash()), positions, getattr(self.broker, 'last_prices', {}))
                else:
                    self.broker.positions = {k: int(v) for k, v in positions.items()}
                    self.broker.cash = float(state.get("cash", self.broker.cash))
        except Exception as e:
            print(f"⚠️ 載入 state 失敗：{e}")

    def _check_kill_switch(self, manager, symbol: str | None, strategy: str | None) -> tuple[bool, list[str]]:
        if manager is None:
            return False, []
        try:
            return manager.is_blocked(symbol=symbol, strategy=strategy)
        except Exception:
            return False, []

    def _kill_switch_state(self) -> dict[str, Any]:
        manager = self.level3_services.get('KillSwitchManager')
        if manager is None:
            return {}
        try:
            return manager._load()
        except Exception:
            return {}

    def _record_state_transition(self, current: str, target: str) -> dict[str, Any] | None:
        machine = self.level3_services.get('OrderStateMachine')
        if machine is None:
            return None
        try:
            return machine.transition(current, target)
        except Exception:
            return None

    def _broker_orders_for_runtime(self) -> list[dict[str, Any]]:
        if hasattr(self.broker, 'get_open_orders_dicts'):
            try:
                return self.broker.get_open_orders_dicts()
            except Exception:
                pass
        return [self._order_record_to_row(x) for x in self.broker.get_open_orders()]

    def _broker_fills_for_runtime(self) -> list[dict[str, Any]]:
        if hasattr(self.broker, 'get_fill_history_dicts'):
            try:
                return self.broker.get_fill_history_dicts()
            except Exception:
                pass
        return []

    def _broker_positions_for_runtime(self) -> list[dict[str, Any]]:
        if hasattr(self.broker, 'get_positions_detailed'):
            try:
                return self.broker.get_positions_detailed()
            except Exception:
                pass
        return [{'ticker': k, 'qty': int(v), 'avg_cost': float(getattr(self.broker, 'last_prices', {}).get(k, 0.0) or 0.0)} for k, v in self.broker.get_positions().items()]

    def _build_position_state(self) -> dict[str, Any]:
        service = self.level3_services.get('PositionStateService')
        if service is None:
            return {'total_alloc': 0.0, 'sector_alloc': {}, 'sector_count': {}, 'direction_alloc': {'LONG': 0.0, 'SHORT': 0.0}}
        try:
            snapshot = pd.DataFrame([
                {'Ticker SYMBOL': row.get('ticker', ''), '投入資金': row.get('market_value', 0.0), '方向': '做多(Long)' if int(row.get('qty', 0)) >= 0 else '做空(Short)'}
                for row in self._broker_positions_for_runtime()
            ])
            account_snapshot = self.broker.get_account_snapshot() if hasattr(self.broker, 'get_account_snapshot') else {'equity': self.broker.get_cash()}
            return service.current_portfolio_state(snapshot, float(account_snapshot.get('equity', self.broker.get_cash()) or 0.0))
        except Exception:
            return {'total_alloc': 0.0, 'sector_alloc': {}, 'sector_count': {}, 'direction_alloc': {'LONG': 0.0, 'SHORT': 0.0}}

    def _write_level3_runtime(self, df: pd.DataFrame, source_meta: dict[str, Any], order_rows: list[dict[str, Any]], fill_rows: list[dict[str, Any]], status: str, extra: dict[str, Any] | None = None) -> None:
        payload: dict[str, Any] = {
            'generated_at': datetime.now().isoformat(timespec='seconds'),
            'level3_meta': self.level3_meta,
            'status': status,
            'source_meta': source_meta,
            'rows_input': int(len(df.index)),
            'orders_submitted_this_run': len(order_rows),
            'fills_this_run': len(fill_rows),
            'broker_snapshot': self.broker.export_runtime_snapshot() if hasattr(self.broker, 'export_runtime_snapshot') else {},
            'risk_gateway': self.risk_gateway.build_runtime_summary() if hasattr(self.risk_gateway, 'build_runtime_summary') else {},
            'position_state': self._build_position_state(),
        }
        if extra:
            payload.update(extra)
        reconciliation = self.level3_services.get('ReconciliationEngine')
        if reconciliation is not None:
            try:
                _, rec = reconciliation.reconcile(
                    local_orders=order_rows,
                    broker_orders=self._broker_orders_for_runtime(),
                    local_fills=fill_rows,
                    broker_fills=self._broker_fills_for_runtime(),
                    local_positions=self._broker_positions_for_runtime(),
                    broker_positions=self._broker_positions_for_runtime(),
                    local_cash=float(self.broker.get_cash()),
                    broker_cash=float(self.broker.get_cash()),
                )
                payload['reconciliation'] = rec.get('summary', rec)
            except Exception as e:
                payload['reconciliation_error'] = repr(e)
        recovery = self.level3_services.get('RecoveryEngine')
        if recovery is not None:
            try:
                recovery.create_snapshot(
                    cash=self.broker.get_cash(),
                    positions=self._broker_positions_for_runtime(),
                    open_orders=self._broker_orders_for_runtime(),
                    recent_fills=self._broker_fills_for_runtime(),
                    kill_switch_state=self._kill_switch_state(),
                    meta={'phase': 'post_execution', 'status': status},
                )
                _, plan = recovery.build_recovery_plan(broker_snapshot=self.broker.export_runtime_snapshot() if hasattr(self.broker, 'export_runtime_snapshot') else {}, retry_queue_summary={'total': 0})
                payload['recovery'] = {'status': plan.get('status'), 'ready_to_recover': plan.get('ready_to_recover')}
            except Exception as e:
                payload['recovery_error'] = repr(e)
        with open(LEVEL3_RUNTIME_PATH, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    @staticmethod
    def _append_csv_row(path: str, row: dict) -> None:
        df = pd.DataFrame([row])
        file_exists = os.path.exists(path)
        df.to_csv(path, mode="a", index=False, header=not file_exists, encoding="utf-8-sig")

    @staticmethod
    def _order_record_to_row(record) -> dict:
        return {"order_id": record.order_id, "symbol": record.symbol, "side": record.side.value, "quantity": record.quantity, "qty": record.quantity, "filled_qty": record.filled_qty, "remaining_qty": record.remaining_qty, "avg_fill_price": record.avg_fill_price, "order_type": record.order_type.value, "limit_price": record.limit_price, "status": record.status.value, "create_time": record.create_time, "update_time": record.update_time, "updated_at": record.update_time, "reject_reason": record.reject_reason, "strategy_name": record.strategy_name, "signal_id": record.signal_id, "client_order_id": record.client_order_id, "note": record.note}

    @staticmethod
    def _fill_event_to_row(fill) -> dict:
        return {"fill_id": f"{fill.order_id}-{fill.fill_time}", "order_id": fill.order_id, "symbol": fill.symbol, "side": fill.side.value, "fill_qty": fill.fill_qty, "fill_price": fill.fill_price, "fill_time": fill.fill_time, "updated_at": fill.fill_time, "commission": fill.commission, "tax": fill.tax, "slippage": fill.slippage, "strategy_name": fill.strategy_name, "signal_id": fill.signal_id, "note": fill.note}

    @staticmethod
    def _safe_float(x, default: float = 0.0) -> float:
        try:
            if pd.isna(x):
                return default
            return float(x)
        except Exception:
            return default
