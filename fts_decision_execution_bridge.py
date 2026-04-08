# -*- coding: utf-8 -*-
import json
import pandas as pd
from fts_config import PATHS, CONFIG
from fts_market_rules_tw import validate_order_payload
from fts_price_gap_bridge import PriceGapBridge
from fts_utils import now_str, log


class DecisionExecutionBridge:
    MODULE_VERSION = 'v79'

    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'decision_execution_bridge.json'
        self.output_path = PATHS.data_dir / 'executable_order_payloads.csv'
        self.watchlist_output_path = PATHS.data_dir / 'paper_execution_watchlist.csv'
        self.price_gap_bridge = PriceGapBridge()

    def _load_normalized(self) -> pd.DataFrame:
        for p in [PATHS.data_dir / 'normalized_decision_output_enriched.csv', PATHS.data_dir / 'normalized_decision_output.csv', PATHS.base_dir / 'daily_decision_desk.csv']:
            if p.exists():
                df = pd.read_csv(p, encoding='utf-8-sig')
                if 'Action' not in df.columns and 'Direction' in df.columns:
                    mp = {'做多(Long)': 'BUY', '多方進場': 'BUY', 'BUY': 'BUY', '做空(Short)': 'SELL', '空方進場': 'SELL', 'SELL': 'SELL'}
                    df['Action'] = df['Direction'].map(lambda x: mp.get(str(x).strip(), str(x).strip().upper()))
                return df
        return pd.DataFrame()

    def _merge_prices(self, df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
        _, gap = self.price_gap_bridge.build(df['Ticker'].tolist())
        auto_path = PATHS.data_dir / 'auto_price_snapshot_candidates.csv'
        if auto_path.exists():
            px = pd.read_csv(auto_path, encoding='utf-8-sig')
            if not px.empty:
                df = df.merge(px[['Ticker', 'Reference_Price', 'Source']], on='Ticker', how='left', suffixes=('', '_auto'))
                mask = pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) <= 0
                df.loc[mask, 'Reference_Price'] = pd.to_numeric(df.loc[mask, 'Reference_Price_auto'], errors='coerce').fillna(0)
                df['PriceSource'] = df.get('Source_auto')
                for c in ('Reference_Price_auto', 'Source_auto'):
                    if c in df.columns:
                        df = df.drop(columns=[c])
        else:
            df['PriceSource'] = ''
        return df, gap

    def build(self):
        df = self._load_normalized()
        if df.empty:
            payload = {'generated_at': now_str(), 'module_version': self.MODULE_VERSION, 'status': 'missing_normalized_decision', 'output_path': str(self.output_path)}
            self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
            return self.output_path, payload
        df['Ticker'] = df['Ticker'].astype(str).str.strip()
        if 'Reference_Price' not in df.columns:
            existing_price_col = next((c for c in ('Close', 'Latest_Close', '收盤價', '最新收盤價') if c in df.columns), None)
            df['Reference_Price'] = pd.to_numeric(df[existing_price_col], errors='coerce').fillna(0) if existing_price_col else 0.0
        if 'Target_Qty' not in df.columns:
            df['Target_Qty'] = 0
        if '風險金額' not in df.columns:
            df['風險金額'] = 0.0
        if '預期停損(%)' not in df.columns:
            df['預期停損(%)'] = CONFIG.default_stop_loss_pct
        if 'Kelly_Pos' not in df.columns:
            alt = next((c for c in ('Kelly', 'Kelly_Position', 'KellyPct') if c in df.columns), None)
            df['Kelly_Pos'] = pd.to_numeric(df[alt], errors='coerce').fillna(0) if alt else 0.0

        df, gap = self._merge_prices(df)

        sizing_reasons = []
        checks = []
        for i, row in df.iterrows():
            price = float(pd.to_numeric(row.get('Reference_Price', 0), errors='coerce') or 0)
            qty = int(pd.to_numeric(row.get('Target_Qty', 0), errors='coerce') or 0)
            risk_budget = float(pd.to_numeric(row.get('風險金額', 0), errors='coerce') or 0)
            stop_pct = max(float(pd.to_numeric(row.get('預期停損(%)', CONFIG.default_stop_loss_pct), errors='coerce') or CONFIG.default_stop_loss_pct), 0.005)
            kelly = max(float(pd.to_numeric(row.get('Kelly_Pos', 0), errors='coerce') or 0), 0.0)
            reason = 'preexisting_qty'
            allow_odd_lot = False
            if price > 0 and qty <= 0:
                capital_cap = CONFIG.starting_cash * max(min(kelly, CONFIG.max_single_position_pct), 0.01)
                qty_by_cap = int(capital_cap // (price * CONFIG.lot_size)) * CONFIG.lot_size
                qty_by_risk = int(risk_budget // (price * stop_pct)) if risk_budget > 0 else 0
                qty_by_risk = (qty_by_risk // CONFIG.lot_size) * CONFIG.lot_size
                positive = [q for q in [qty_by_cap, qty_by_risk] if q > 0]
                qty = min(positive) if positive else 0
                if qty > 0:
                    reason = 'strict_min_of_risk_cap'
                elif CONFIG.mode.upper() == 'PAPER' and CONFIG.allow_odd_lot_in_paper:
                    share_cap = int(capital_cap // price) if price > 0 else 0
                    share_risk = int(risk_budget // (price * stop_pct)) if risk_budget > 0 and price > 0 else 0
                    positive_share = [q for q in [share_cap, share_risk] if q > 0]
                    odd_qty = min(positive_share) if positive_share else 0
                    odd_qty = max(odd_qty, CONFIG.paper_min_qty) if odd_qty > 0 else 0
                    if odd_qty > 0:
                        qty = odd_qty
                        allow_odd_lot = True
                        reason = 'paper_odd_lot_min_of_risk_cap'
                    else:
                        reason = 'missing_risk_budget_or_kelly'
                elif price * CONFIG.lot_size > CONFIG.starting_cash * CONFIG.max_single_position_pct:
                    reason = 'insufficient_capital_for_board_lot'
                else:
                    reason = 'missing_risk_budget_or_kelly'
                df.at[i, 'Target_Qty'] = int(qty)
            else:
                allow_odd_lot = (CONFIG.mode.upper() == 'PAPER' and CONFIG.allow_odd_lot_in_paper and qty > 0 and qty % CONFIG.lot_size != 0)
            sizing_reasons.append(reason)
            checks.append(validate_order_payload(str(row.get('Ticker', '')), float(df.at[i, 'Reference_Price'] or 0), int(df.at[i, 'Target_Qty'] or 0), int(CONFIG.lot_size), allow_odd_lot=allow_odd_lot).to_dict())
        df['Target_Qty'] = pd.to_numeric(df['Target_Qty'], errors='coerce').fillna(0).astype(int)
        df['MarketRulePassed'] = [bool(x.get('passed')) for x in checks]
        df['MarketRuleReason'] = [x.get('reason', '') for x in checks]
        df['LotMode'] = [x.get('lot_mode', 'board') for x in checks]
        df['SizingReason'] = sizing_reasons
        df.to_csv(self.output_path, index=False, encoding='utf-8-sig')
        watchlist = df[['Ticker', 'Action', 'Reference_Price', 'Target_Qty', 'MarketRuleReason', 'SizingReason']].copy()
        watchlist.to_csv(self.watchlist_output_path, index=False, encoding='utf-8-sig')

        rows_total = int(len(df))
        rows_with_price = int((pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) > 0).sum())
        rows_with_qty = int((df['Target_Qty'] > 0).sum())
        rows_passed = int(df['MarketRulePassed'].sum())
        rows_watchlist_ready = int(((pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) > 0) | (df['Ticker'].astype(str).str.len() > 0)).sum())
        price_ratio = (rows_with_price / rows_total) if rows_total else 0
        qty_ratio = (rows_with_qty / rows_total) if rows_total else 0
        pass_ratio = (rows_passed / rows_total) if rows_total else 0
        watch_ratio = (rows_watchlist_ready / rows_total) if rows_total else 0
        execution_readiness_pct = int(round(price_ratio * 35 + qty_ratio * 25 + pass_ratio * 25 + watch_ratio * 15, 0))
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'rows_total': rows_total,
            'rows_with_price': rows_with_price,
            'rows_with_qty': rows_with_qty,
            'rows_market_rule_passed': rows_passed,
            'rows_market_rule_failed': int((~df['MarketRulePassed']).sum()),
            'rows_watchlist_ready': rows_watchlist_ready,
            'price_source_counts': df.get('PriceSource', pd.Series(dtype='object')).fillna('unknown').value_counts(dropna=False).to_dict(),
            'sizing_reason_counts': pd.Series(sizing_reasons).value_counts(dropna=False).to_dict(),
            'failed_tickers_preview': [x.get('ticker') for x in checks if not x.get('passed')][:20],
            'missing_price_tickers': df.loc[pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) <= 0, 'Ticker'].astype(str).tolist(),
            'rows_board_lot_ready': int(((df['LotMode'] == 'board') & (pd.to_numeric(df['Reference_Price'], errors='coerce').fillna(0) > 0)).sum()),
            'rows_odd_lot_ready_for_paper': int(((df['LotMode'] == 'odd') & (df['MarketRulePassed'])).sum()),
            'execution_readiness_pct': execution_readiness_pct,
            'output_path': str(self.output_path),
            'watchlist_output_path': str(self.watchlist_output_path),
            'price_gap_bridge_status': gap.get('status'),
            'status': 'execution_payload_ready' if rows_passed > 0 else ('partial_execution_ready' if rows_with_price > 0 or rows_with_qty > 0 else 'waiting_for_price_or_qty'),
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧩 已輸出 execution bridge：{self.report_path}')
        return self.output_path, payload
