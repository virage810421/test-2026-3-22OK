# -*- coding: utf-8 -*-
import pandas as pd

try:
    from fts_runtime_diagnostics import record_issue, write_summary as write_runtime_diagnostics_summary
except Exception:  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}
    def write_runtime_diagnostics_summary(*args, **kwargs):
        return None
from fts_config import PATHS, CONFIG
from fts_utils import log, safe_float

_ACTION_MAP = {
    '做多(LONG)': 'BUY', '做多': 'BUY', '多方進場': 'BUY', 'LONG': 'BUY', 'BUY': 'BUY',
    '做空(SHORT)': 'SELL', '做空': 'SELL', '空方進場': 'SELL', 'SHORT': 'SELL', 'SELL': 'SELL',
    'RANGE': 'BUY', '區間': 'BUY', 'HOLD': 'BUY',
}
_HEALTH_BLOCKERS = {'KILL', 'BLOCKED', 'FALLBACK_BUILD'}


def normalize_action_value(value):
    raw = str(value or '').strip()
    if not raw:
        return ''
    key = raw.upper()
    if key in _ACTION_MAP:
        return _ACTION_MAP[key]
    return _ACTION_MAP.get(raw, key)


def _as_bool_series(series, default=False):
    if series is None:
        return pd.Series(dtype=bool)
    mapped = series.map(lambda x: str(x).strip().lower() in {'1', 'true', 'yes', 'y'}) if hasattr(series, 'map') else series
    try:
        return mapped.fillna(default).astype(bool)
    except Exception:
        return pd.Series([default] * len(series), index=series.index, dtype=bool)


def apply_decision_integrity_flags(df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    out = df.copy() if df is not None else pd.DataFrame()
    if out.empty:
        return out, {
            'row_count': 0,
            'usable_rows': 0,
            'execution_eligible_rows': 0,
            'blocked_rows': 0,
            'incomplete_rows': 0,
            'status': 'empty',
        }

    if 'Ticker' not in out.columns:
        out['Ticker'] = ''
    if 'Action' not in out.columns:
        out['Action'] = ''
    out['Action'] = out['Action'].map(normalize_action_value)

    if 'Reference_Price' not in out.columns:
        out['Reference_Price'] = 0.0
    out['Reference_Price'] = out['Reference_Price'].map(lambda x: safe_float(x, 0.0))

    if 'Target_Qty' not in out.columns:
        out['Target_Qty'] = 0
    out['Target_Qty'] = pd.to_numeric(out['Target_Qty'], errors='coerce').fillna(0).astype(int)

    if 'Kelly_Pos' not in out.columns:
        alt = next((c for c in ('Kelly', 'Kelly_Position', 'KellyPct', 'Kelly_Fraction', 'kelly_fraction') if c in out.columns), None)
        out['Kelly_Pos'] = pd.to_numeric(out[alt], errors='coerce').fillna(0.0) if alt else 0.0
    else:
        out['Kelly_Pos'] = pd.to_numeric(out['Kelly_Pos'], errors='coerce').fillna(0.0)

    risk_col = next((c for c in ('風險金額', 'Risk_Budget', 'risk_budget', 'RiskBudget') if c in out.columns), None)
    risk_budget = pd.to_numeric(out[risk_col], errors='coerce').fillna(0.0) if risk_col else pd.Series([0.0] * len(out), index=out.index)

    if 'FallbackBuild' not in out.columns:
        out['FallbackBuild'] = False
    out['FallbackBuild'] = _as_bool_series(out['FallbackBuild'], default=False)

    if 'Health' not in out.columns:
        out['Health'] = 'UNKNOWN'
    out['Health'] = out['Health'].astype(str).fillna('UNKNOWN').str.upper()

    out['_has_ticker'] = out['Ticker'].astype(str).str.strip().ne('')
    out['_has_action'] = out['Action'].astype(str).str.strip().ne('')
    out['_action_valid'] = out['Action'].isin(['BUY', 'SELL'])
    out['_has_price'] = pd.to_numeric(out['Reference_Price'], errors='coerce').fillna(0).gt(0)
    out['_has_qty'] = out['Target_Qty'].gt(0)
    out['_has_kelly'] = pd.to_numeric(out['Kelly_Pos'], errors='coerce').fillna(0).gt(0)
    out['_has_risk_budget'] = risk_budget.gt(0)
    out['_can_size_from_budget'] = out['_has_price'] & out['_has_kelly']
    out['_health_blocked'] = out['Health'].isin(_HEALTH_BLOCKERS)

    integrity = []
    review_severity = []
    desk_usable = []
    execution_eligible = []
    reasons_list = []
    for _, row in out.iterrows():
        reasons = []
        if bool(row.get('FallbackBuild', False)):
            reasons.append('fallback_build')
        if not bool(row.get('_has_ticker', False)):
            reasons.append('missing_ticker')
        if not bool(row.get('_has_action', False)):
            reasons.append('missing_action')
        elif not bool(row.get('_action_valid', False)):
            reasons.append('invalid_action')
        if not bool(row.get('_has_price', False)):
            reasons.append('missing_reference_price')
        if bool(row.get('_health_blocked', False)):
            reasons.append('health_blocked')
        if not bool(row.get('_has_qty', False)) and not bool(row.get('_can_size_from_budget', False)):
            reasons.append('missing_executable_qty_or_sizing_inputs')

        usable = not any(x in reasons for x in ['fallback_build', 'missing_ticker', 'missing_action', 'invalid_action', 'missing_reference_price', 'health_blocked'])
        eligible = usable and not any(x in reasons for x in ['missing_executable_qty_or_sizing_inputs'])
        if not usable:
            state = 'fallback_blocked' if 'fallback_build' in reasons else 'csv_incomplete'
            severity = 'HARD_BLOCK'
        elif eligible and bool(row.get('_has_qty', False)):
            state = 'ready_for_execution'
            severity = ''
        elif eligible:
            state = 'ready_for_bridge_sizing'
            severity = ''
        else:
            state = 'needs_completion'
            severity = 'SOFT_BLOCK'
        integrity.append(state)
        review_severity.append(severity)
        desk_usable.append(bool(usable))
        execution_eligible.append(bool(eligible))
        reasons_list.append('|'.join(reasons))

    out['DeskIntegrity'] = integrity
    out['ReviewSeverity'] = review_severity
    out['IntegrityReasons'] = reasons_list
    out['DeskUsable'] = desk_usable
    out['ExecutionEligible'] = execution_eligible
    out['CanAutoSubmit'] = out.get('CanAutoSubmit', False)
    out['CanAutoSubmit'] = _as_bool_series(out['CanAutoSubmit'], default=False) | out['ExecutionEligible']

    diag = {
        'row_count': int(len(out)),
        'usable_rows': int(pd.Series(desk_usable).sum()),
        'execution_eligible_rows': int(pd.Series(execution_eligible).sum()),
        'blocked_rows': int((~pd.Series(desk_usable)).sum()),
        'incomplete_rows': int(((pd.Series(desk_usable)) & (~pd.Series(execution_eligible))).sum()),
        'integrity_counts': {str(k): int(v) for k, v in pd.Series(integrity).value_counts(dropna=False).to_dict().items()},
        'status': 'integrity_applied',
    }
    return out, diag


class DecisionCompatibilityLayer:
    MODULE_VERSION = "v19_integrity_guard"

    def _price_lookup(self, ticker):
        import pandas as pd
        candidates = [
            PATHS.base_dir / "last_price_snapshot.csv",
            PATHS.base_dir / "daily_price_snapshot.csv",
            PATHS.data_dir / "last_price_snapshot.csv",
        ]
        for p in candidates:
            if p.exists():
                try:
                    snap = pd.read_csv(p, encoding="utf-8-sig")
                    tcol = next((c for c in snap.columns if c.lower() in ("ticker", "ticker symbol", "symbol")), None)
                    pcol = next((c for c in snap.columns if c.lower() in ("close", "price", "reference_price") or "收盤" in c), None)
                    if tcol and pcol:
                        row = snap[snap[tcol].astype(str).str.strip() == str(ticker).strip()]
                        if not row.empty:
                            return safe_float(row.iloc[-1][pcol], 0.0)
                except Exception as exc:
                    record_issue('compat', 'numeric_integrity_parse_failed', exc, severity='WARNING', fail_mode='fail_open')
        try:
            import yfinance as yf
            hist = yf.Ticker(str(ticker)).history(period="5d", interval="1d", auto_adjust=False)
            if hist is not None and not hist.empty and "Close" in hist.columns:
                return safe_float(hist["Close"].dropna().iloc[-1], 0.0)
        except Exception as exc:
            record_issue('compat', 'decision_row_integrity_failed', exc, severity='ERROR', fail_mode='fail_closed')
        return 0.0

    def normalize(self, csv_path):
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
        if df.empty:
            return df, {"normalized": False, "reason": "empty"}

        out = pd.DataFrame()

        def pick(*names):
            for n in names:
                if n in df.columns:
                    return df[n]
            return None

        out["Ticker"] = pick("Ticker", "Ticker SYMBOL", "ticker", "symbol")
        direction = pick("Action", "action", "Direction", "direction", "Signal", "signal")
        out["Action"] = direction.astype(str) if direction is not None else ""
        out["Action"] = out["Action"].map(normalize_action_value)
        out["Regime"] = pick("Regime", "regime")
        out["Structure"] = pick("Structure", "Strategy_Name", "Strategy", "strategy_name")
        out["AI_Proba"] = pick("AI_Proba", "ai_proba", "AI_Confidence", "confidence")
        out["Score"] = pick("Score", "Final_Score", "score", "final_score")
        out["Kelly_Pos"] = pick("Kelly_Pos", "Kelly_Fraction", "kelly_fraction")
        out["Heuristic_EV"] = pick("Heuristic_EV", "expected_return", "Expected_Return", "Realized_EV")
        out["Reference_Price"] = pick("Reference_Price", "Close", "close", "收盤價", "價格")
        out["Target_Qty"] = pick("Target_Qty", "Qty", "qty", "建議股數", "建議張數")
        out["FallbackBuild"] = pick("FallbackBuild")
        out["DeskUsable"] = pick("DeskUsable")
        out["ExecutionEligible"] = pick("ExecutionEligible", "CanAutoSubmit")
        out["Health"] = pick("Health")
        risk_budget = pick("風險金額", "Risk_Budget", "risk_budget", "RiskBudget")
        if risk_budget is not None:
            out['風險金額'] = risk_budget

        if out["Ticker"] is None:
            out["Ticker"] = ""
        if out["Action"] is None:
            out["Action"] = ""
        if out["Reference_Price"] is None:
            out["Reference_Price"] = 0.0
        if out["Target_Qty"] is None:
            out["Target_Qty"] = 0
        if out["FallbackBuild"] is None:
            out["FallbackBuild"] = False
        if out["DeskUsable"] is None:
            out["DeskUsable"] = False
        if out["ExecutionEligible"] is None:
            out["ExecutionEligible"] = False
        if out["Health"] is None:
            out["Health"] = "UNKNOWN"

        out["Reference_Price"] = out["Reference_Price"].apply(lambda x: safe_float(x, 0.0))
        miss_price = out["Reference_Price"].astype(float).le(0)
        if miss_price.any():
            out.loc[miss_price, "Reference_Price"] = out.loc[miss_price, "Ticker"].apply(self._price_lookup)
        out["Target_Qty"] = pd.to_numeric(out["Target_Qty"], errors='coerce').fillna(0).astype(int)

        out, integrity_diag = apply_decision_integrity_flags(out)
        diag = {
            "normalized": True,
            "source_columns": list(df.columns),
            "normalized_columns": list(out.columns),
            "row_count": int(len(out)),
            "rows_with_ticker": int(out["_has_ticker"].sum()),
            "rows_with_action": int(out["_has_action"].sum()),
            "rows_with_price": int(out["_has_price"].sum()),
            "usable_rows": integrity_diag.get('usable_rows', 0),
            "execution_eligible_rows": integrity_diag.get('execution_eligible_rows', 0),
            "integrity_counts": integrity_diag.get('integrity_counts', {}),
        }

        if CONFIG.auto_export_normalized_decision_csv:
            norm_path = PATHS.data_dir / "normalized_decision_output.csv"
            out.to_csv(norm_path, index=False, encoding="utf-8-sig")
            log(f"🧩 已輸出 normalized decision csv：{norm_path}")
            preview_path = PATHS.log_dir / "normalized_decision_preview.csv"
            out.head(20).to_csv(preview_path, index=False, encoding="utf-8-sig")
            log(f"🔎 已輸出 normalized 預覽：{preview_path}")

        log(
            f"🧪 Compatibility 診斷 | rows={diag['row_count']} | "
            f"ticker={diag['rows_with_ticker']} | action={diag['rows_with_action']} | "
            f"price={diag['rows_with_price']} | usable={diag['usable_rows']} | executable={diag['execution_eligible_rows']}"
        )
        return out, diag
