# -*- coding: utf-8 -*-
"""Ticker symbol contract bridge.

Execution domain canonical field: ticker_symbol.
Legacy research/ETL/old SQL tables may still expose: Ticker SYMBOL.
This bridge keeps legacy compatibility while making execution payloads explicit.
"""
from __future__ import annotations

from typing import Any, Mapping

CANONICAL_EXECUTION_TICKER = 'ticker_symbol'
LEGACY_TICKER_SYMBOL = 'Ticker SYMBOL'
DISPLAY_TICKER = 'Ticker'
TICKER_ALIASES = (CANONICAL_EXECUTION_TICKER, DISPLAY_TICKER, LEGACY_TICKER_SYMBOL, 'symbol', '股票代號')


def normalize_ticker_value(value: Any) -> str:
    s = str(value or '').strip()
    if not s or s.lower() in {'nan', 'none', 'null'}:
        return ''
    return s.upper()


def get_ticker_symbol(row: Mapping[str, Any] | Any, default: str = '') -> str:
    if row is None:
        return normalize_ticker_value(default)
    getter = row.get if hasattr(row, 'get') else None
    if getter is None:
        return normalize_ticker_value(default)
    for col in TICKER_ALIASES:
        val = getter(col, None)
        out = normalize_ticker_value(val)
        if out:
            return out
    return normalize_ticker_value(default)


def ensure_execution_symbol(payload: dict[str, Any], *, keep_legacy: bool = True) -> dict[str, Any]:
    ticker = get_ticker_symbol(payload)
    if ticker:
        payload[CANONICAL_EXECUTION_TICKER] = ticker
        if keep_legacy:
            payload.setdefault(LEGACY_TICKER_SYMBOL, ticker)
            payload.setdefault(DISPLAY_TICKER, ticker)
    return payload


def ensure_dataframe_symbol_contract(df):
    """Return df with both canonical execution and legacy display symbol aliases when possible."""
    if df is None:
        return df
    try:
        import pandas as pd  # noqa: F401
        if getattr(df, 'empty', False):
            return df
        source = next((c for c in TICKER_ALIASES if c in df.columns), None)
        if source is None:
            return df
        vals = df[source].map(normalize_ticker_value)
        if CANONICAL_EXECUTION_TICKER not in df.columns:
            df[CANONICAL_EXECUTION_TICKER] = vals
        else:
            canon_vals = df[CANONICAL_EXECUTION_TICKER].map(normalize_ticker_value)
            df[CANONICAL_EXECUTION_TICKER] = canon_vals.where(canon_vals.astype(bool), vals)
        if DISPLAY_TICKER not in df.columns:
            df[DISPLAY_TICKER] = df[CANONICAL_EXECUTION_TICKER]
        if LEGACY_TICKER_SYMBOL not in df.columns:
            df[LEGACY_TICKER_SYMBOL] = df[CANONICAL_EXECUTION_TICKER]
    except Exception:
        return df
    return df


def legacy_symbol_alias_sql() -> str:
    """SQL expression for legacy tables that may have both symbol columns."""
    return "COALESCE(NULLIF([ticker_symbol], ''), [Ticker SYMBOL])"


def execution_symbol_contract_report(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    ticker = get_ticker_symbol(payload or {})
    return {
        'canonical_field': CANONICAL_EXECUTION_TICKER,
        'legacy_field': LEGACY_TICKER_SYMBOL,
        'ticker_symbol': ticker,
        'ready': bool(ticker),
        'policy': 'execution_uses_ticker_symbol__legacy_tables_may_keep_Ticker_SYMBOL',
    }
