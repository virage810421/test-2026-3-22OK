from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional, Any

import pandas as pd

try:
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None

from broker_base import OrderRequest, OrderSide, OrderType, OrderStatus
try:
    from paper_broker import PaperBroker
except Exception:
    from fts_broker_core import PaperBroker
from risk_gateway import RiskGateway
from db_logger import SQLServerExecutionLogger
from fts_level_runtime import build_level3_services
from fts_data_quality_guard import validate_order_contract_dict, append_order_quality_report

LOG_DIR = "execution_logs"
ORDER_BLOTTER_PATH = os.path.join(LOG_DIR, "order_blotter.csv")
FILL_BLOTTER_PATH = os.path.join(LOG_DIR, "fill_blotter.csv")
STOP_REPLACE_BLOTTER_PATH = os.path.join(LOG_DIR, "stop_replace_blotter.csv")
STOP_TRIGGER_BLOTTER_PATH = os.path.join(LOG_DIR, "stop_trigger_blotter.csv")
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

    @staticmethod
    def _symbol_from_row(row: Any) -> str:
        """Execution-domain symbol accessor.

        Official execution field is ticker_symbol.  Ticker SYMBOL is accepted
        only as an inbound legacy decision-desk alias and normalized before use.
        """
        if hasattr(row, 'get'):
            return str(row.get('ticker_symbol', row.get('Ticker SYMBOL', row.get('Ticker', ''))) or '').strip()
        return ''

    def run_from_csv(self, decision_csv_path: str) -> None:
        print("========================================================")
        print("🚀 啟動 Execution Engine：決策桌 → 風控 → 券商 → blotter")
        print("========================================================")
        df, source_meta, stop_replace_df = self._load_or_build_orders(decision_csv_path)
        if df is None or df.empty:
            print("⚠️ 決策桌為空")
            return
        required_cols = {"Action", "Target_Qty", "Reference_Price"}
        missing = required_cols - set(df.columns)
        if 'ticker_symbol' not in df.columns and 'Ticker SYMBOL' not in df.columns and 'Ticker' not in df.columns:
            missing.add('ticker_symbol')
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
            self._sync_execution_sql_runtime(status='blocked_by_kill_switch', note='kill_switch_blocked')
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
                contract_report = validate_order_contract_dict({
                    'ticker': order_req.symbol,
                    'side': order_req.side.value if hasattr(order_req.side, 'value') else str(order_req.side),
                    'qty': order_req.quantity,
                    'ref_price': ref_price,
                    'Kelly_Pos': getattr(row, 'get', lambda *a, **k: None)('Kelly_Pos') if hasattr(row, 'get') else None,
                    'AI_Proba': getattr(row, 'get', lambda *a, **k: None)('AI_Proba') if hasattr(row, 'get') else None,
                })
                if not contract_report.get('passed', False):
                    append_order_quality_report(contract_report)
                    print(f"⛔ 訂單契約拒單 | {order_req.symbol} | {'/'.join(contract_report.get('failures', []))}")
                    skip_count += 1
                    continue
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
        stop_replace_rows = self._apply_stop_replace_workflow(stop_replace_df)
        stop_trigger_rows = self._apply_protective_stop_trigger_workflow()
        try:
            late_fills = self.broker.poll_fills()
        except Exception as e:
            late_fills = []
            print(f"⚠️ protective stop 後 poll_fills 失敗：{e}")
        for fill in late_fills:
            fill_row = self._fill_event_to_row(fill)
            fill_rows_this_run.append(fill_row)
            self._append_csv_row(FILL_BLOTTER_PATH, fill_row)
            if self.db_logger:
                self.db_logger.insert_fill(fill_row)
        self._sync_execution_sql_runtime(status='completed', note='execution_engine_run')
        self._save_state()
        self._write_level3_runtime(df, source_meta, order_rows_this_run, fill_rows_this_run, status='completed', extra={'stop_replace_rows': int(len(stop_replace_rows)), 'stop_trigger_rows': int(len(stop_trigger_rows))})
        print("--------------------------------------------------------")
        print(f"✅ 本輪完成 | 送單: {submit_count} | 跳過: {skip_count}")
        print(f"💰 Cash: {self.broker.get_cash():,.2f}")
        print(f"📦 Positions: {self.broker.get_positions()}")
        print("========================================================")

    def _load_or_build_orders(self, decision_csv_path: str) -> tuple[pd.DataFrame, dict[str, Any], pd.DataFrame]:
        if os.path.exists(decision_csv_path):
            try:
                df = pd.read_csv(decision_csv_path)
            except Exception:
                df = pd.DataFrame()
        else:
            df = pd.DataFrame()
        if not df.empty:
            stop_df = self._load_stop_replace_df(None)
            return self._normalize_execution_df(df), {'source': decision_csv_path, 'bridge_used': False}, stop_df
        bridge = self.level3_services.get('DecisionExecutionBridge')
        if bridge is None:
            return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': False, 'reason': 'missing_bridge'}, pd.DataFrame()
        try:
            out_path, payload = bridge.build()
            out_path = str(out_path)
            if os.path.exists(out_path):
                built_df = pd.read_csv(out_path, encoding='utf-8-sig')
                stop_df = self._load_stop_replace_df(payload.get('stop_replace_output_path'))
                return self._normalize_execution_df(built_df), {'source': out_path, 'bridge_used': True, 'payload_status': payload.get('status', ''), 'stop_replace_output_path': payload.get('stop_replace_output_path', '')}, stop_df
        except Exception as e:
            return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': True, 'error': repr(e)}, pd.DataFrame()
        return pd.DataFrame(), {'source': decision_csv_path, 'bridge_used': True, 'reason': 'bridge_output_missing'}, pd.DataFrame()


    def _load_stop_replace_df(self, explicit_path: str | None) -> pd.DataFrame:
        candidates = []
        if explicit_path:
            candidates.append(explicit_path)
        candidates.extend([os.path.join('data', 'stop_replace_payloads.csv'), os.path.join('runtime', 'stop_replace_payloads.csv')])
        for path in candidates:
            if path and os.path.exists(path):
                try:
                    return pd.read_csv(path, encoding='utf-8-sig')
                except Exception:
                    try:
                        return pd.read_csv(path)
                    except Exception:
                        continue
        return pd.DataFrame()

    def _apply_stop_replace_workflow(self, stop_df: pd.DataFrame) -> list[dict[str, Any]]:
        if stop_df is None or stop_df.empty:
            return []
        rows: list[dict[str, Any]] = []
        for _, row in stop_df.iterrows():
            try:
                should_replace = bool(row.get('Should_Replace_Stop', False))
                if not should_replace:
                    continue
                symbol = str(row.get('ticker_symbol', row.get('Ticker', row.get('Ticker SYMBOL', ''))) or '').strip()
                qty = int(float(row.get('Current_Position_Qty', 0) or 0))
                stop_price = float(row.get('Desired_Stop_Price', 0) or 0)
                mode = str(row.get('Stop_Workflow_Mode', 'PLAN_ONLY') or 'PLAN_ONLY').strip().upper()
                broker_order_id = str(row.get('Broker_Stop_Order_ID', '') or '').strip()
                note = str(row.get('Note', '') or '')
                result = {'ok': False, 'status': 'skipped', 'symbol': symbol, 'mode': mode, 'broker_order_id': broker_order_id, 'stop_price': stop_price, 'qty': qty, 'note': note}
                if symbol and qty > 0 and stop_price > 0:
                    if broker_order_id and hasattr(self.broker, 'replace_order'):
                        result = self.broker.replace_order(broker_order_id, {'stop_price': stop_price, 'qty': qty, 'note': note}) or result
                    elif mode == 'UPSERT_PROTECTIVE_STOP' and hasattr(self.broker, 'upsert_protective_stop'):
                        side = 'BUY' if str(row.get('Position_Side', 'LONG')).upper() == 'SHORT' else 'SELL'
                        result = self.broker.upsert_protective_stop(symbol=symbol, quantity=qty, stop_price=stop_price, side=side, client_order_id=str(row.get('Client_Order_ID', '') or ''), note=note) or result
                    else:
                        result['status'] = 'plan_only'
                blotter_row = {
                    'run_time': datetime.now().isoformat(timespec='seconds'),
                    'ticker_symbol': symbol,
                    'Broker_Stop_Order_ID': broker_order_id,
                    'Stop_Workflow_Mode': mode,
                    'Desired_Stop_Price': stop_price,
                    'Current_Position_Qty': qty,
                    'Result_Status': result.get('status', ''),
                    'Result_OK': bool(result.get('ok', False)),
                    'Client_Order_ID': str(row.get('Client_Order_ID', '') or ''),
                    'Note': note,
                }
                rows.append(blotter_row)
                self._append_csv_row(STOP_REPLACE_BLOTTER_PATH, blotter_row)
                if self.db_logger:
                    protective_record = result.get('record', {}) if isinstance(result, dict) else {}
                    payload = {
                        'order_id': broker_order_id or str(protective_record.get('order_id', '') or protective_record.get('broker_order_id', '') or row.get('Client_Order_ID', '') or ''),
                        'client_order_id': str(row.get('Client_Order_ID', '') or protective_record.get('client_order_id', '') or ''),
                        'broker_order_id': broker_order_id or str(protective_record.get('order_id', '') or protective_record.get('broker_order_id', '') or ''),
                        'ticker_symbol': symbol,
                        'direction_bucket': 'STOP_' + str(row.get('Position_Side', 'LONG')).upper(),
                        'strategy_bucket': 'protective_stop',
                        'status': str(result.get('status', protective_record.get('status', mode)) or mode),
                        'qty': qty,
                        'filled_qty': int(protective_record.get('filled_qty', 0) or 0),
                        'remaining_qty': int(protective_record.get('quantity', protective_record.get('qty', qty)) or qty),
                        'avg_fill_price': float(protective_record.get('trigger_fill_price', 0) or 0),
                        'order_type': 'STOP',
                        'submitted_price': stop_price,
                        'ref_price': float(row.get('Reference_Price', 0) or 0),
                        'signal_id': str(row.get('Client_Order_ID', '') or ''),
                        'note': note or 'protective_stop_workflow',
                        'created_at': protective_record.get('create_time', protective_record.get('created_at', datetime.now().isoformat(timespec='seconds'))),
                        'updated_at': protective_record.get('update_time', protective_record.get('updated_at', datetime.now().isoformat(timespec='seconds'))),
                    }
                    self.db_logger.ingest_protective_stop_order(payload)
            except Exception as e:
                rows.append({'run_time': datetime.now().isoformat(timespec='seconds'), 'ticker_symbol': self._symbol_from_row(row), 'Result_Status': f'exception:{e}', 'Result_OK': False})
        return rows

    def _apply_protective_stop_trigger_workflow(self) -> list[dict[str, Any]]:
        if not bool(getattr(self.broker, 'process_protective_stops', None)):
            return []
        try:
            triggered = self.broker.process_protective_stops() or []
        except Exception as e:
            print(f"⚠️ protective stop trigger workflow 失敗：{e}")
            return []
        rows: list[dict[str, Any]] = []
        for item in triggered:
            row = {
                'run_time': datetime.now().isoformat(timespec='seconds'),
                'ticker_symbol': str(item.get('symbol', '') or ''),
                'Broker_Stop_Order_ID': str(item.get('order_id', '') or ''),
                'Stop_Price': float(item.get('stop_price', 0) or 0),
                'Trigger_Price': float(item.get('trigger_price', 0) or 0),
                'Fill_Price': float(item.get('fill_price', 0) or 0),
                'Fill_Qty': int(item.get('qty', 0) or 0),
                'Side': str(item.get('side', '') or ''),
                'Result_Status': str(item.get('status', '') or ''),
            }
            rows.append(row)
            self._append_csv_row(STOP_TRIGGER_BLOTTER_PATH, row)
            if self.db_logger:
                self.db_logger.ingest_protective_stop_order({
                    'order_id': str(item.get('order_id', '') or ''),
                    'broker_order_id': str(item.get('order_id', '') or ''),
                    'ticker_symbol': str(item.get('symbol', '') or ''),
                    'direction_bucket': 'STOP_TRIGGER',
                    'strategy_bucket': 'protective_stop',
                    'status': str(item.get('status', 'TRIGGERED_FILLED') or 'TRIGGERED_FILLED'),
                    'qty': int(item.get('qty', 0) or 0),
                    'filled_qty': int(item.get('qty', 0) or 0),
                    'remaining_qty': 0,
                    'avg_fill_price': float(item.get('fill_price', 0) or 0),
                    'order_type': 'STOP',
                    'submitted_price': float(item.get('stop_price', 0) or 0),
                    'ref_price': float(item.get('trigger_price', 0) or 0),
                    'note': 'protective_stop_triggered',
                    'updated_at': str(item.get('time', datetime.now().isoformat(timespec='seconds'))),
                })
        return rows

    @staticmethod
    def _normalize_execution_df(df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if 'ticker_symbol' not in df.columns:
            if 'Ticker SYMBOL' in df.columns:
                df['ticker_symbol'] = df['Ticker SYMBOL']
            elif 'Ticker' in df.columns:
                df['ticker_symbol'] = df['Ticker']
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
                gate.evaluate(df.assign(Ticker=df.get('ticker_symbol', df.get('Ticker SYMBOL', ''))))
            except Exception as exc:
                record_issue('execution_engine', 'live_readiness_preflight_failed', exc, severity='ERROR', fail_mode='fail_closed')
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
            except Exception as exc:
                record_issue('execution_engine', 'recovery_snapshot_failed', exc, severity='ERROR', fail_mode='fail_closed')
        machine = self.level3_services.get('OrderStateMachine')
        if machine is not None:
            try:
                machine.build_definition()
            except Exception as exc:
                record_issue('execution_engine', 'order_state_machine_definition_failed', exc, severity='ERROR', fail_mode='fail_closed')

    def _push_market_prices(self, df: pd.DataFrame) -> None:
        price_map = {}
        for _, row in df.iterrows():
            symbol = self._symbol_from_row(row)
            ref_price = self._safe_float(row.get("Reference_Price", 0))
            if symbol and ref_price > 0:
                price_map[symbol] = ref_price
        self.broker.update_market_prices(price_map)

    def _row_to_order_request(self, row: pd.Series) -> Optional[OrderRequest]:
        symbol = self._symbol_from_row(row)
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
            except Exception as exc:
                record_issue('execution_engine', 'runtime_side_effect_failed', exc, severity='WARNING', fail_mode='fail_open')
        return [self._order_record_to_row(x) for x in self.broker.get_open_orders()]

    def _broker_fills_for_runtime(self) -> list[dict[str, Any]]:
        if hasattr(self.broker, 'get_fill_history_dicts'):
            try:
                return self.broker.get_fill_history_dicts()
            except Exception as exc:
                record_issue('execution_engine', 'runtime_side_effect_failed', exc, severity='WARNING', fail_mode='fail_open')
        return []

    def _broker_positions_for_runtime(self) -> list[dict[str, Any]]:
        if hasattr(self.broker, 'get_positions_detailed'):
            try:
                return self.broker.get_positions_detailed()
            except Exception as exc:
                record_issue('execution_engine', 'runtime_side_effect_failed', exc, severity='WARNING', fail_mode='fail_open')
        rows = []
        price_map = getattr(self.broker, 'last_prices', {}) or {}
        for k, v in self.broker.get_positions().items():
            qty = int(v)
            market_px = float(price_map.get(k, 0.0) or 0.0)
            avg_cost = market_px
            market_value = abs(qty) * market_px
            rows.append({'ticker': k, 'qty': qty, 'available_qty': abs(qty), 'avg_cost': avg_cost, 'market_price': market_px, 'market_value': market_value, 'unrealized_pnl': 0.0, 'realized_pnl': 0.0, 'direction_bucket': 'LONG' if qty >= 0 else 'SHORT'})
        return rows

    def _broker_protective_stops_for_runtime(self) -> list[dict[str, Any]]:
        getter = getattr(self.broker, 'get_protective_stops', None)
        if not callable(getter):
            return []
        try:
            return getter() or []
        except Exception:
            return []

    def _sync_execution_sql_runtime(self, status: str, note: str = '') -> None:
        if not self.db_logger:
            return
        try:
            for stop_row in self._broker_protective_stops_for_runtime():
                self.db_logger.ingest_protective_stop_order(stop_row)
            cash_snapshot = self.broker.get_account_snapshot() if hasattr(self.broker, 'get_account_snapshot') else {}
            if not isinstance(cash_snapshot, dict):
                cash_snapshot = {'cash': float(self.broker.get_cash() or 0.0), 'equity': float(self.broker.get_cash() or 0.0), 'market_value': 0.0, 'buying_power': float(self.broker.get_cash() or 0.0)}
            cash_snapshot = dict(cash_snapshot)
            cash_snapshot.update({'snapshot_time': datetime.now().isoformat(timespec='seconds'), 'broker_type': getattr(self.broker, '__class__', type('X',(object,),{})).__name__, 'note': f'{status}|{note}' if note else status})
            self.db_logger.sync_runtime_snapshot(cash_snapshot, self._broker_positions_for_runtime(), snapshot_time=cash_snapshot['snapshot_time'], note=cash_snapshot.get('note', ''))
        except Exception as e:
            print(f"⚠️ execution SQL runtime sync 失敗：{e}")

    def _build_position_state(self) -> dict[str, Any]:
        service = self.level3_services.get('PositionStateService')
        if service is None:
            return {'total_alloc': 0.0, 'sector_alloc': {}, 'sector_count': {}, 'direction_alloc': {'LONG': 0.0, 'SHORT': 0.0}}
        try:
            snapshot = pd.DataFrame([
                {'ticker_symbol': row.get('ticker', ''), '投入資金': row.get('market_value', 0.0), '方向': '做多(Long)' if int(row.get('qty', 0)) >= 0 else '做空(Short)'}
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
        if isinstance(record, dict):
            side = record.get('side', '')
            order_type = record.get('order_type', record.get('type', ''))
            status = record.get('status', '')
            return {
                'order_id': record.get('order_id', record.get('client_order_id', record.get('broker_order_id', ''))),
                'symbol': record.get('symbol', record.get('ticker', '')),
                'side': getattr(side, 'value', side),
                'quantity': int(record.get('quantity', record.get('qty', 0)) or 0),
                'qty': int(record.get('qty', record.get('quantity', 0)) or 0),
                'filled_qty': int(record.get('filled_qty', 0) or 0),
                'remaining_qty': int(record.get('remaining_qty', 0) or 0),
                'avg_fill_price': float(record.get('avg_fill_price', 0) or 0),
                'order_type': getattr(order_type, 'value', order_type),
                'limit_price': record.get('limit_price', record.get('submitted_price')),
                'status': getattr(status, 'value', status),
                'create_time': record.get('create_time', record.get('created_at', '')),
                'update_time': record.get('update_time', record.get('updated_at', '')),
                'updated_at': record.get('updated_at', record.get('update_time', '')),
                'reject_reason': record.get('reject_reason', ''),
                'strategy_name': record.get('strategy_name', ''),
                'signal_id': record.get('signal_id', record.get('client_order_id', '')),
                'client_order_id': record.get('client_order_id', ''),
                'note': record.get('note', ''),
            }
        return {"order_id": record.order_id, "symbol": record.symbol, "side": record.side.value, "quantity": record.quantity, "qty": record.quantity, "filled_qty": record.filled_qty, "remaining_qty": record.remaining_qty, "avg_fill_price": record.avg_fill_price, "order_type": record.order_type.value, "limit_price": record.limit_price, "status": record.status.value, "create_time": record.create_time, "update_time": record.update_time, "updated_at": record.update_time, "reject_reason": record.reject_reason, "strategy_name": record.strategy_name, "signal_id": record.signal_id, "client_order_id": record.client_order_id, "note": record.note}

    @staticmethod
    def _fill_event_to_row(fill) -> dict:
        if isinstance(fill, dict):
            side = fill.get('side', '')
            order_id = fill.get('order_id', fill.get('client_order_id', fill.get('broker_order_id', '')))
            fill_time = fill.get('fill_time', fill.get('updated_at', datetime.now().isoformat(timespec='seconds')))
            return {
                'fill_id': fill.get('fill_id', f'{order_id}-{fill_time}'),
                'order_id': order_id,
                'symbol': fill.get('symbol', fill.get('ticker', '')),
                'side': getattr(side, 'value', side),
                'fill_qty': int(fill.get('fill_qty', fill.get('qty', 0)) or 0),
                'fill_price': float(fill.get('fill_price', 0) or 0),
                'fill_time': fill_time,
                'updated_at': fill_time,
                'commission': float(fill.get('commission', 0) or 0),
                'tax': float(fill.get('tax', 0) or 0),
                'slippage': float(fill.get('slippage', 0) or 0),
                'strategy_name': fill.get('strategy_name', ''),
                'signal_id': fill.get('signal_id', fill.get('client_order_id', '')),
                'note': fill.get('note', ''),
            }
        return {"fill_id": f"{fill.order_id}-{fill.fill_time}", "order_id": fill.order_id, "symbol": fill.symbol, "side": fill.side.value, "fill_qty": fill.fill_qty, "fill_price": fill.fill_price, "fill_time": fill.fill_time, "updated_at": fill.fill_time, "commission": fill.commission, "tax": fill.tax, "slippage": fill.slippage, "strategy_name": fill.strategy_name, "signal_id": fill.signal_id, "note": fill.note}

    @staticmethod
    def _safe_float(x, default: float = 0.0) -> float:
        try:
            if pd.isna(x):
                return default
            return float(x)
        except Exception:
            return default

# =============================================================================
# vNext lot-level / broker callback / reconciliation extension
# =============================================================================
try:
    _EE_ORIG_RUN_FROM_CSV = ExecutionEngine.run_from_csv
    _EE_ORIG_SYNC_SQL_RUNTIME = ExecutionEngine._sync_execution_sql_runtime
    _EE_ORIG_WRITE_LEVEL3 = ExecutionEngine._write_level3_runtime

    CALLBACK_BLOTTER_PATH = os.path.join(LOG_DIR, 'broker_callback_blotter.csv')
    RECONCILIATION_BLOTTER_PATH = os.path.join(LOG_DIR, 'execution_reconciliation_blotter.csv')
    LOT_SNAPSHOT_PATH = os.path.join(LOG_DIR, 'position_lot_snapshot.csv')

    def _ee_broker_lots_for_runtime(self) -> list[dict[str, Any]]:
        getter = getattr(self.broker, 'get_position_lots', None)
        if callable(getter):
            try:
                return getter(include_closed=True) or []
            except TypeError:
                try:
                    return getter() or []
                except Exception as exc:
                    record_issue('execution_engine', 'broker_getter_fallback_failed', exc, severity='WARNING', fail_mode='fail_open')
                    return []
            except Exception as exc:
                record_issue('execution_engine', 'broker_runtime_getter_failed', exc, severity='WARNING', fail_mode='fail_open')
                return []
        snapper = getattr(self.broker, 'export_runtime_snapshot', None)
        if callable(snapper):
            try:
                snap = snapper() or {}
                return list(snap.get('position_lots', []) or [])
            except Exception as exc:
                record_issue('execution_engine', 'broker_runtime_snapshot_failed', exc, severity='WARNING', fail_mode='fail_open')
                return []
        return []

    def _ee_broker_callbacks_for_runtime(self, clear: bool = True) -> list[dict[str, Any]]:
        # Prefer cursor-style drain to avoid re-ingesting all historical callbacks every run.
        drain = getattr(self.broker, 'drain_new_callbacks', None)
        if callable(drain):
            try:
                return drain() or []
            except Exception as exc:
                record_issue('execution_engine', 'broker_callback_drain_failed', exc, severity='ERROR', fail_mode='fail_closed')
        poll = getattr(self.broker, 'poll_callbacks', None)
        if callable(poll):
            try:
                return poll(clear=clear) or []
            except TypeError:
                try:
                    return poll() or []
                except Exception as exc:
                    record_issue('execution_engine', 'broker_callback_poll_legacy_failed', exc, severity='ERROR', fail_mode='fail_closed')
                    return []
            except Exception as exc:
                record_issue('execution_engine', 'broker_callback_poll_failed', exc, severity='ERROR', fail_mode='fail_closed')
                return []
        return []

    def _ee_ingest_broker_callbacks(self, callbacks: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        callbacks = callbacks if callbacks is not None else self._broker_callbacks_for_runtime(clear=True)
        rows: list[dict[str, Any]] = []
        for event in callbacks or []:
            row = dict(event)
            row.setdefault('ingested_at', datetime.now().isoformat(timespec='seconds'))
            rows.append(row)
            try:
                self._append_csv_row(CALLBACK_BLOTTER_PATH, row)
            except Exception as exc:
                record_issue('execution_engine', 'callback_blotter_write_failed', exc, severity='WARNING', fail_mode='fail_open')
            if self.db_logger:
                try:
                    self.db_logger.ingest_broker_callback(row)
                except Exception as exc:
                    print(f"⚠️ broker callback SQL ingest 失敗：{exc}")
        return rows

    def _ee_append_lot_snapshot(self, lots: list[dict[str, Any]]) -> None:
        for lot in lots or []:
            row = dict(lot)
            row.setdefault('snapshot_time', datetime.now().isoformat(timespec='seconds'))
            try:
                self._append_csv_row(LOT_SNAPSHOT_PATH, row)
            except Exception as exc:
                record_issue('execution_engine', 'lot_snapshot_blotter_write_failed', exc, severity='WARNING', fail_mode='fail_open')

    def _ee_sync_lot_snapshot(self, note: str = '') -> list[dict[str, Any]]:
        lots = self._broker_lots_for_runtime()
        self._append_lot_snapshot(lots)
        if self.db_logger:
            try:
                self.db_logger.replace_position_lots(lots, snapshot_time=datetime.now().isoformat(timespec='seconds'))
            except Exception as exc:
                print(f"⚠️ lot-level SQL sync 失敗：{exc}")
        return lots

    def _ee_reconcile_execution_state(self, callbacks: list[dict[str, Any]] | None = None, note: str = '') -> dict[str, Any]:
        local_orders = self._broker_orders_for_runtime()
        broker_orders = self._broker_orders_for_runtime()
        local_fills = self._broker_fills_for_runtime()
        broker_fills = self._broker_fills_for_runtime()
        local_positions = self._broker_positions_for_runtime()
        broker_positions = self._broker_positions_for_runtime()
        lots = self._broker_lots_for_runtime()
        cash = None
        try:
            cash = float(self.broker.get_cash())
        except Exception as exc:
            record_issue('execution_engine', 'broker_cash_read_failed', exc, severity='ERROR', fail_mode='fail_closed')
        if self.db_logger and hasattr(self.db_logger, 'reconcile_execution_state'):
            try:
                summary = self.db_logger.reconcile_execution_state(
                    local_orders=local_orders,
                    broker_orders=broker_orders,
                    local_fills=local_fills,
                    broker_fills=broker_fills,
                    local_positions=local_positions,
                    broker_positions=broker_positions,
                    local_lots=lots,
                    broker_lots=lots,
                    local_cash=cash,
                    broker_cash=cash,
                    note=note or 'execution_engine_runtime_reconcile',
                )
            except Exception as exc:
                summary = {'status': 'ERROR', 'error': repr(exc)}
        else:
            summary = {'status': 'SKIPPED', 'reason': 'db_logger_missing'}
        try:
            self._append_csv_row(RECONCILIATION_BLOTTER_PATH, {'run_time': datetime.now().isoformat(timespec='seconds'), **summary})
        except Exception as exc:
            record_issue('execution_engine', 'reconciliation_blotter_write_failed', exc, severity='WARNING', fail_mode='fail_open')
        return summary

    def _ee_patched_sync_sql_runtime(self, status: str, note: str = '') -> None:
        # Keep original account/orders/positions behavior.
        _EE_ORIG_SYNC_SQL_RUNTIME(self, status=status, note=note)
        # Then sync lot-level detail and broker callback queue.
        try:
            callbacks = self._ee_ingest_broker_callbacks()
            lots = self._ee_sync_lot_snapshot(note=note)
            if self.db_logger:
                # Also sync lots via account_row path so old callers still carry lot payload.
                snap_time = datetime.now().isoformat(timespec='seconds')
                acct = {'snapshot_time': snap_time, 'cash': float(self.broker.get_cash() or 0.0), 'equity': float(self.broker.get_cash() or 0.0), 'broker_type': self.broker.__class__.__name__, 'note': f'{status}|{note}', 'position_lots': lots}
                try:
                    self.db_logger.sync_runtime_snapshot(acct, self._broker_positions_for_runtime(), snapshot_time=snap_time, note=acct['note'])
                except Exception as exc:
                    record_issue('execution_engine', 'sync_runtime_snapshot_failed', exc, severity='ERROR', fail_mode='fail_closed')
            self._ee_reconcile_execution_state(callbacks=callbacks, note=f'{status}|{note}')
        except Exception as exc:
            print(f"⚠️ callback/lot/reconcile runtime sync 失敗：{exc}")

    def _ee_patched_write_level3_runtime(self, df, source_meta, order_rows, fill_rows, status, extra=None):
        extra = dict(extra or {})
        try:
            extra['position_lots'] = self._broker_lots_for_runtime()
            extra['lot_reconciliation'] = self.broker.reconcile_lots_to_positions() if hasattr(self.broker, 'reconcile_lots_to_positions') else {}
            extra['broker_callbacks_pending'] = len(getattr(self.broker, '_callbacks', []) or [])
        except Exception as exc:
            extra['lot_runtime_error'] = repr(exc)
        return _EE_ORIG_WRITE_LEVEL3(self, df, source_meta, order_rows, fill_rows, status, extra=extra)

    def _ee_patched_run_from_csv(self, decision_csv_path: str) -> None:
        _EE_ORIG_RUN_FROM_CSV(self, decision_csv_path)
        # Final safety pass: callbacks generated after original final sync are ingested here.
        try:
            callbacks = self._ee_ingest_broker_callbacks()
            self._ee_sync_lot_snapshot(note='post_run_final_safety_pass')
            self._ee_reconcile_execution_state(callbacks=callbacks, note='post_run_final_safety_pass')
        except Exception as exc:
            print(f"⚠️ post-run callback/lot/reconcile pass 失敗：{exc}")

    ExecutionEngine._broker_lots_for_runtime = _ee_broker_lots_for_runtime
    ExecutionEngine._broker_callbacks_for_runtime = _ee_broker_callbacks_for_runtime
    ExecutionEngine._ee_ingest_broker_callbacks = _ee_ingest_broker_callbacks
    ExecutionEngine._ee_append_lot_snapshot = _ee_append_lot_snapshot
    ExecutionEngine._ee_sync_lot_snapshot = _ee_sync_lot_snapshot
    ExecutionEngine._ee_reconcile_execution_state = _ee_reconcile_execution_state
    ExecutionEngine._sync_execution_sql_runtime = _ee_patched_sync_sql_runtime
    ExecutionEngine._write_level3_runtime = _ee_patched_write_level3_runtime
    ExecutionEngine.run_from_csv = _ee_patched_run_from_csv

except Exception as exc:
    record_issue('execution_engine', 'lot_callback_reconcile_patch_install_failed', exc, severity='CRITICAL', fail_mode='fail_closed')
