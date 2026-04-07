# -*- coding: utf-8 -*-
from fts_config import CONFIG
class RiskGateway:
    def __init__(self, broker):
        self.broker = broker
        self.seen_buy_tickers = set()
        self.current_bar = CONFIG.current_bar_index

    def _industry_ratio_after_trade(self, accepted, new_signal, equity):
        amounts = {}
        for p in self.broker.get_positions().values():
            amounts[p.industry] = amounts.get(p.industry, 0.0) + (p.avg_cost * p.qty)
        for s in accepted + [new_signal]:
            if s.action == "BUY":
                trade_amount = s.reference_price * s.target_qty
                amounts[s.industry] = amounts.get(s.industry, 0.0) + trade_amount
        return (amounts.get(new_signal.industry, 0.0) / equity) if equity > 0 else 0.0

    def filter_signals(self, signals):
        account = self.broker.get_account_snapshot(); cash = account.cash; equity = max(account.equity, 1.0)
        positions = self.broker.get_positions()
        accepted, rejected = [], []; pending_buy_amount = 0.0
        for s in signals:
            if s.action not in ("BUY", "SELL"): rejected.append((s, f"不支援 action={s.action}")); continue
            if s.score < CONFIG.min_score_to_trade: rejected.append((s, f"分數不足：{s.score:.2f}")); continue
            if s.ai_confidence < CONFIG.min_ai_confidence: rejected.append((s, f"AI 信心不足：{s.ai_confidence:.2f}")); continue
            if s.expected_return < CONFIG.min_expected_return: rejected.append((s, f"預期報酬過低：{s.expected_return:.4f}")); continue
            if s.kelly_fraction < CONFIG.min_kelly_fraction: rejected.append((s, f"Kelly 倉位比過低：{s.kelly_fraction:.4f}")); continue
            if s.target_qty <= 0: rejected.append((s, "target_qty <= 0")); continue

            if s.action == "BUY":
                if CONFIG.block_duplicate_buy_same_run and s.ticker in self.seen_buy_tickers:
                    rejected.append((s, "同輪重複買進阻擋")); continue
                pos = positions.get(s.ticker)
                if pos:
                    if not CONFIG.allow_add_on_signal:
                        rejected.append((s, "已有持倉，禁止加碼")); continue
                    if pos.cooldown_until > self.current_bar:
                        rejected.append((s, "持倉冷卻中")); continue
                trade_amount = s.reference_price * s.target_qty
                if trade_amount > equity * CONFIG.max_single_position_pct: rejected.append((s, "超過單筆上限")); continue
                if account.market_value + pending_buy_amount + trade_amount > equity * CONFIG.max_total_exposure_pct: rejected.append((s, "超過總曝險上限")); continue
                if cash - pending_buy_amount - trade_amount < equity * CONFIG.cash_buffer_pct: rejected.append((s, "現金緩衝不足")); continue
                if self._industry_ratio_after_trade(accepted, s, equity) > CONFIG.max_industry_exposure_pct: rejected.append((s, "產業曝險過高")); continue
                accepted.append(s); pending_buy_amount += trade_amount; self.seen_buy_tickers.add(s.ticker)
            else:
                pos = positions.get(s.ticker)
                if not pos: rejected.append((s, "無持倉可賣")); continue
                if s.target_qty > pos.qty: s.target_qty = pos.qty
                accepted.append(s)
        return accepted[: CONFIG.max_orders_per_run], rejected
