# -*- coding: utf-8 -*-
from __future__ import annotations

from collections import defaultdict
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, safe_float, safe_int, write_json
from fts_kill_switch import KillSwitchManager

try:  # pragma: no cover
    from fts_market_rules_tw import validate_order_payload  # type: ignore
except Exception:  # pragma: no cover
    validate_order_payload = None


class LiveSafetyGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'live_safety_gate.json'
        self.kill_switch = KillSwitchManager()

    def evaluate(self, readiness: dict[str, Any], launch_gate: dict[str, Any], orders: list[dict[str, Any]] | None = None, account_snapshot: dict[str, Any] | None = None, risk_snapshot: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any]]:
        orders = orders or []
        account_snapshot = account_snapshot or {}
        risk_snapshot = risk_snapshot or {}
        failures = []
        warnings = []
        blocked_orders = []
        mode = getattr(CONFIG, 'mode', 'PAPER')
        broker_type = getattr(CONFIG, 'broker_type', 'paper')
        day_loss_pct = safe_float(risk_snapshot.get('day_loss_pct', 0.0), 0.0)
        industry_usage = defaultdict(float)

        kill_blocked, kill_reasons = self.kill_switch.is_blocked()
        if kill_blocked:
            failures.append({'type': 'kill_switch_armed', 'message': 'kill switch 已啟動', 'reasons': kill_reasons})

        if mode.upper() != 'PAPER':
            warnings.append({'type': 'non_paper_mode', 'message': f'目前 mode={mode}，已不是純 paper 模式'})
        if broker_type.lower() != 'paper':
            warnings.append({'type': 'non_paper_broker', 'message': f'目前 broker_type={broker_type}，請確認不是誤接真券商'})
        if not launch_gate.get('go_for_execution', launch_gate.get('live_ready', False)):
            failures.append({'type': 'launch_gate_blocked', 'message': '發車前驗證閘門未通過'})
        if readiness.get('total_signals', 0) == 0 and orders:
            warnings.append({'type': 'zero_signal', 'message': '本輪有效訊號為 0'})
        if day_loss_pct > float(getattr(CONFIG, 'daily_loss_limit_pct', 0.03)):
            failures.append({'type': 'daily_loss_limit_breach', 'message': f'day_loss_pct={day_loss_pct:.2%} 已超過日損上限'})

        total_equity = safe_float(account_snapshot.get('equity', account_snapshot.get('cash', 0.0)), 0.0)
        max_single_position_pct = float(getattr(CONFIG, 'max_single_position_pct', 0.10))
        max_order_notional = float(getattr(CONFIG, 'max_order_notional', 500000))
        max_industry_exposure_pct = float(getattr(CONFIG, 'max_industry_exposure_pct', 0.25))

        for order in orders:
            symbol = str(order.get('ticker') or order.get('Ticker') or '').strip()
            strategy = str(order.get('strategy_name') or order.get('Strategy') or '').strip()
            blocked, reasons = self.kill_switch.is_blocked(symbol=symbol, strategy=strategy)
            qty = safe_int(order.get('qty', order.get('Target_Qty', 0)), 0)
            ref_price = safe_float(order.get('ref_price', order.get('Reference_Price', 0.0)), 0.0)
            notional = qty * ref_price
            industry = str(order.get('industry', '未知') or '未知')
            if blocked:
                reasons = list(reasons)
            if total_equity > 0 and notional / total_equity > max_single_position_pct:
                reasons.append('single_position_limit_breach')
            if notional > max_order_notional:
                reasons.append('order_notional_limit_breach')
            industry_usage[industry] += notional
            if total_equity > 0 and industry_usage[industry] / total_equity > max_industry_exposure_pct:
                reasons.append('industry_exposure_limit_breach')
            if validate_order_payload is not None:
                result = validate_order_payload(symbol, ref_price, qty, int(getattr(CONFIG, 'lot_size', 1000)), bool(getattr(CONFIG, 'allow_odd_lot_in_paper', True)))
                if not result.passed:
                    reasons.append(f'market_rule_fail:{result.reason}')
            if reasons:
                blocked_orders.append({'ticker': symbol, 'strategy_name': strategy, 'reasons': reasons, 'notional': round(notional, 2)})

        if blocked_orders:
            failures.append({'type': 'blocked_orders_present', 'message': f'共有 {len(blocked_orders)} 筆訂單被 safety gate 擋下'})

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'mode': mode,
            'broker_type': broker_type,
            'paper_live_safe': len(failures) == 0,
            'failures': failures,
            'warnings': warnings,
            'blocked_orders': blocked_orders[:100],
            'limits': {
                'daily_loss_limit_pct': getattr(CONFIG, 'daily_loss_limit_pct', 0.03),
                'max_single_position_pct': max_single_position_pct,
                'max_order_notional': max_order_notional,
                'max_industry_exposure_pct': max_industry_exposure_pct,
            },
        }
        write_json(self.path, payload)
        log(f"🛡️ Live Safety Gate | paper_live_safe={payload['paper_live_safe']} | failures={len(failures)} | warnings={len(warnings)} | blocked_orders={len(blocked_orders)}")
        return self.path, payload
