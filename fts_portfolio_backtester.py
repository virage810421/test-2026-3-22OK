# -*- coding: utf-8 -*-
from __future__ import annotations

"""Portfolio-level backtest report.

Builds on event_backtester trade simulation and adds portfolio aggregation:
capital curve, max drawdown, exposure buckets, fees/slippage already embedded in
net_return_pct, and per-lane diagnostics.
"""

import json
from pathlib import Path
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str
from fts_exception_policy import record_diagnostic


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


class PortfolioBacktester:
    def __init__(self):
        self.report_path = PATHS.runtime_dir / 'portfolio_backtest_report.json'
        self.trades_path = PATHS.runtime_dir / 'portfolio_backtest_trades.csv'

    def run(self, tickers: list[str] | None = None, period: str = '3y') -> tuple[str, dict[str, Any]]:
        try:
            import pandas as pd  # type: ignore
            from config import WATCH_LIST  # type: ignore
            from event_backtester import backtest_single_ticker, summarize_backtest  # type: ignore
        except Exception as exc:
            record_diagnostic('portfolio_backtester', 'imports_failed', exc, severity='error', fail_closed=True)
            payload = {'generated_at': now_str(), 'status': 'portfolio_backtest_blocked_import_failed', 'error': repr(exc)}
            return self._write(payload)
        tickers = tickers or list(WATCH_LIST or [])
        all_trades = []
        errors = []
        for ticker in tickers:
            try:
                tdf = backtest_single_ticker(ticker, period=period)
                if tdf is not None and not tdf.empty:
                    all_trades.append(tdf)
            except Exception as exc:
                record_diagnostic('portfolio_backtester', f'backtest_failed_{ticker}', exc, severity='warning', fail_closed=False)
                errors.append({'ticker': ticker, 'error': repr(exc)})
        if not all_trades:
            payload = {'generated_at': now_str(), 'status': 'portfolio_backtest_no_trades', 'tickers': tickers, 'errors': errors}
            return self._write(payload)
        df = pd.concat(all_trades, ignore_index=True)
        df['entry_date'] = pd.to_datetime(df.get('entry_date'), errors='coerce')
        df = df.sort_values(['entry_date', 'Ticker']).reset_index(drop=True)
        returns = pd.to_numeric(df.get('net_return_pct'), errors='coerce').fillna(0.0) / 100.0
        starting_cash = float(getattr(CONFIG, 'starting_cash', 3_000_000) or 3_000_000)
        max_single_position_pct = float(getattr(CONFIG, 'max_single_position_pct', 0.10) or 0.10)
        trade_notional = starting_cash * max_single_position_pct
        pnl = returns * trade_notional
        equity = starting_cash + pnl.cumsum()
        peak = equity.cummax()
        dd = (equity - peak) / peak.replace(0, 1)
        df['portfolio_trade_notional'] = round(trade_notional, 2)
        df['portfolio_pnl'] = pnl.round(2)
        df['portfolio_equity'] = equity.round(2)
        df['portfolio_drawdown_pct'] = (dd * 100).round(4)
        self.trades_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.trades_path, index=False, encoding='utf-8-sig')
        base_summary = summarize_backtest(df)
        payload = {
            'generated_at': now_str(),
            'status': 'portfolio_backtest_ready',
            'period': period,
            'tickers': tickers,
            'trades_path': str(self.trades_path),
            'trade_count': int(len(df)),
            'starting_cash': starting_cash,
            'trade_notional': trade_notional,
            'ending_equity': round(float(equity.iloc[-1]), 2),
            'total_pnl': round(float(pnl.sum()), 2),
            'total_return_pct': round(float((equity.iloc[-1] / starting_cash - 1) * 100), 4),
            'max_drawdown_pct': round(float(dd.min() * 100), 4),
            'summary': base_summary,
            'by_ticker': self._group(df, 'Ticker'),
            'by_strategy_bucket': self._group(df, 'Strategy_Bucket') if 'Strategy_Bucket' in df.columns else {},
            'by_direction_bucket': self._group(df, 'Direction_Bucket') if 'Direction_Bucket' in df.columns else {},
            'errors': errors,
        }
        return self._write(payload)

    def _group(self, df: Any, col: str) -> dict[str, Any]:
        out: dict[str, Any] = {}
        if col not in df.columns:
            return out
        for k, g in df.groupby(col):
            rets = g['net_return_pct'].astype(float)
            out[str(k)] = {
                'trades': int(len(g)),
                'win_rate': round(float((rets > 0).mean()) * 100, 2),
                'avg_return_pct': round(float(rets.mean()), 4),
                'total_pnl': round(float(g['portfolio_pnl'].sum()), 2),
            }
        return out

    def _write(self, payload: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        return str(self.report_path), payload


def main(argv: list[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description='Portfolio-level pre-live backtest')
    parser.add_argument('--period', default='3y')
    parser.add_argument('--tickers', default='', help='Comma-separated ticker list. Default uses config.WATCH_LIST.')
    args = parser.parse_args(list(argv or []))
    tickers = [x.strip() for x in args.tickers.split(',') if x.strip()] or None
    path, payload = PortfolioBacktester().run(tickers=tickers, period=args.period)
    print(json.dumps({'status': payload.get('status'), 'path': path, 'trade_count': payload.get('trade_count', 0)}, ensure_ascii=False, indent=2))
    return 0 if payload.get('status') == 'portfolio_backtest_ready' else 1


if __name__ == '__main__':
    raise SystemExit(main())
