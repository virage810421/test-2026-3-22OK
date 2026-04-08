# -*- coding: utf-8 -*-
from fts_config import CONFIG
from fts_models import TradeSignal
from fts_utils import safe_float, safe_int, round_price, log

class SignalLoader:
    API_LEVEL = "v20.1"
    MODULE_VERSION = "v20.1"

    def _normalize_action(self, raw_action: str) -> str:
        action = str(raw_action).strip().upper()
        mapping = {
            "LONG": "BUY", "OPEN_LONG": "BUY", "BUY_SIGNAL": "BUY", "BUY": "BUY",
            "多": "BUY", "做多": "BUY", "做多(Long)": "BUY", "多方進場": "BUY", "1": "BUY",
            "EXIT": "SELL", "CLOSE_LONG": "SELL", "SELL_SIGNAL": "SELL", "SELL": "SELL",
            "SHORT": "SELL", "做空(SHORT)": "SELL", "做空(Short)": "SELL", "空方進場": "SELL", "空": "SELL", "做空": "SELL", "-1": "SELL",
            "ADD": "BUY", "ADD_ON": "BUY",
        }
        return mapping.get(action, action)

    def load_from_normalized_df(self, df):
        signals = []
        skipped_no_ticker = 0
        skipped_bad_action = 0
        skipped_no_price = 0
        skipped_zero_qty_after_calc = 0
        action_buckets = {}

        for _, row in df.iterrows():
            ticker = str(row.get("Ticker", "")).strip()
            raw_action = row.get("Action", "")
            action = self._normalize_action(raw_action)
            action_buckets[action] = action_buckets.get(action, 0) + 1

            if not ticker or ticker.lower() == "nan":
                skipped_no_ticker += 1
                continue
            if action not in ("BUY", "SELL"):
                skipped_bad_action += 1
                continue

            ref_price = safe_float(row.get("Reference_Price", 0.0), 0.0)
            if ref_price <= 0:
                skipped_no_price += 1
                continue

            target_qty = safe_int(row.get("Target_Qty", 0), 0)
            kelly_fraction = safe_float(row.get("Kelly_Pos", 0.0), 0.0)

            if target_qty <= 0:
                if kelly_fraction > 0:
                    trade_amount = CONFIG.starting_cash * min(kelly_fraction, CONFIG.max_single_position_pct)
                    target_qty = int(trade_amount // (ref_price * CONFIG.lot_size)) * CONFIG.lot_size
                if target_qty <= 0:
                    max_trade_amount = CONFIG.starting_cash * CONFIG.max_single_position_pct
                    target_qty = int(max_trade_amount // (ref_price * CONFIG.lot_size)) * CONFIG.lot_size
                target_qty = max(target_qty, CONFIG.lot_size)

            if target_qty <= 0:
                skipped_zero_qty_after_calc += 1
                continue

            signals.append(
                TradeSignal(
                    ticker=ticker,
                    action=action,
                    reference_price=round_price(ref_price),
                    target_qty=target_qty,
                    score=safe_float(row.get("Score", 70.0), 70.0),
                    ai_confidence=safe_float(row.get("AI_Proba", 0.60), 0.60),
                    industry="未知",
                    strategy_name=str(row.get("Structure", "未命名策略")),
                    reason="",
                    model_name="",
                    model_version="",
                    regime=str(row.get("Regime", "")),
                    expected_return=safe_float(row.get("Heuristic_EV", 0.0), 0.0),
                    kelly_fraction=kelly_fraction,
                    raw=row.to_dict(),
                )
            )

        log(
            f"🧪 SignalLoader 診斷 | valid={len(signals)} | "
            f"skip_no_ticker={skipped_no_ticker} | "
            f"skip_bad_action={skipped_bad_action} | "
            f"skip_no_price={skipped_no_price} | "
            f"skip_zero_qty={skipped_zero_qty_after_calc} | "
            f"actions={action_buckets}"
        )
        return signals

class ExecutionReadinessChecker:
    def check(self, signals):
        total = len(signals)
        return {
            "total_signals": total,
            "buy_count": sum(1 for s in signals if s.action == "BUY"),
            "sell_count": sum(1 for s in signals if s.action == "SELL"),
            "with_regime": sum(1 for s in signals if s.regime),
            "with_expected_return": sum(1 for s in signals if s.expected_return != 0),
            "execution_ready": total > 0,
        }
