# -*- coding: utf-8 -*-
"""Tax-lot accounting, jurisdiction routing, wash-sale adjustments and report export.

Engineering note
----------------
This module is a configurable accounting engine.  It is NOT legal or tax advice.
Rates, report formats and jurisdiction treatment are intentionally represented as
rule profiles so they can be reviewed and adjusted by an accountant/broker before
production filing.
"""
from __future__ import annotations

LEGACY_SYMBOL_MIGRATION_COMPAT_MARKER = True  # Ticker SYMBOL is accepted only as legacy alias/backfill input; canonical output is ticker_symbol.

from dataclasses import dataclass, asdict
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Any, Iterable
import csv
import json
import re

try:
    from fts_runtime_diagnostics import record_issue
except Exception:  # pragma: no cover
    def record_issue(*args, **kwargs):
        return {}


def _cfg(name: str, default: Any = None) -> Any:
    try:
        import config  # type: ignore
        if hasattr(config, name):
            return getattr(config, name)
        if hasattr(config, "PARAMS") and isinstance(config.PARAMS, dict) and name in config.PARAMS:
            return config.PARAMS.get(name, default)
    except Exception:
        pass
    try:
        from fts_config import CONFIG  # type: ignore
        return getattr(CONFIG, name.lower(), getattr(CONFIG, name, default))
    except Exception:
        return default


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def parse_dt(value: Any) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "").replace("/", "-"))
    except Exception as exc:
        record_issue("tax_lot_accounting", "datetime_parse_failed", exc, severity="WARNING", fail_mode="fail_open", context={"value": str(value)[:120]})
        return None


def money(value: Any, ndigits: int = 4) -> float:
    try:
        return round(float(value or 0.0), ndigits)
    except Exception:
        return 0.0


def qty_int(value: Any) -> int:
    try:
        return int(float(value or 0))
    except Exception:
        return 0


def _json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except Exception:
        return "{}"


def _upper(value: Any) -> str:
    return str(value or "").strip().upper()


@dataclass(frozen=True)
class TaxRule:
    jurisdiction: str
    asset_class: str
    tax_regime: str
    tax_treatment: str
    report_type: str
    currency: str
    capital_gain_taxable: bool = True
    ordinary_income: bool = False
    wash_sale_applicable: bool = False
    wash_sale_window_days: int = 30
    section1256_6040: bool = False
    long_term_days: int = 365
    sell_transaction_tax_rate: float = 0.0
    open_transaction_tax_rate: float = 0.0
    close_fee_deductible: bool = True
    note: str = ""


DEFAULT_RULES: dict[str, TaxRule] = {
    "TW_EQUITY": TaxRule(
        jurisdiction="TW", asset_class="EQUITY", tax_regime="TW_LISTED_STOCK",
        tax_treatment="TW_SECURITIES_TRANSACTION_TAX", report_type="TW_SECURITIES_GAIN_SUMMARY",
        currency="TWD", capital_gain_taxable=False, ordinary_income=False,
        wash_sale_applicable=False, sell_transaction_tax_rate=0.003,
        note="Taiwan listed stock profile: realized gain summary plus securities transaction tax; configurable.",
    ),
    "US_EQUITY": TaxRule(
        jurisdiction="US", asset_class="EQUITY", tax_regime="US_EQUITY_CAPITAL_GAIN",
        tax_treatment="US_CAPITAL_GAIN_LOSS", report_type="US_FORM_8949_LIKE",
        currency="USD", capital_gain_taxable=True, ordinary_income=False,
        wash_sale_applicable=True, wash_sale_window_days=30, long_term_days=365,
        note="US equity profile with wash-sale adjustment support; Form 8949-like export.",
    ),
    "FX": TaxRule(
        jurisdiction="GLOBAL", asset_class="FX", tax_regime="FX_ORDINARY",
        tax_treatment="FX_ORDINARY_INCOME", report_type="FX_ORDINARY_REPORT",
        currency="USD", capital_gain_taxable=True, ordinary_income=True,
        wash_sale_applicable=False,
        note="Generic FX ordinary income report profile; configure per jurisdiction.",
    ),
    "FUTURES": TaxRule(
        jurisdiction="GLOBAL", asset_class="FUTURES", tax_regime="FUTURES_6040_OR_TRANSACTION_TAX",
        tax_treatment="FUTURES_MARK_TO_MARKET_OR_6040", report_type="FUTURES_1256_LIKE",
        currency="USD", capital_gain_taxable=True, ordinary_income=False,
        wash_sale_applicable=False, section1256_6040=True,
        note="Generic futures profile with 60/40 fields where applicable; configure for local law.",
    ),
    "OTHER": TaxRule(
        jurisdiction="GLOBAL", asset_class="OTHER", tax_regime="GENERIC_CAPITAL_GAIN",
        tax_treatment="GENERIC_CAPITAL_GAIN", report_type="CONSOLIDATED_REALIZED_GAIN",
        currency="USD", capital_gain_taxable=True, ordinary_income=False,
        wash_sale_applicable=False,
        note="Fallback generic capital-gain profile.",
    ),
}



def _tax_rule_json_path() -> Path:
    raw = _cfg("TAX_RULES_JSON_PATH", "config/tax_rules.json") or "config/tax_rules.json"
    path = Path(str(raw))
    if not path.is_absolute():
        path = Path.cwd() / path
    return path

def _coerce_tax_rule(key: str, payload: dict[str, Any]) -> TaxRule:
    base = asdict(DEFAULT_RULES.get(key, DEFAULT_RULES["OTHER"]))
    base.update({k: v for k, v in dict(payload or {}).items() if k in base})
    return TaxRule(**base)

def load_tax_rules(path: str | Path | None = None) -> dict[str, TaxRule]:
    """Load external JSON tax rules, falling back to DEFAULT_RULES.

    Expected JSON shape:
      {"version": "...", "rules": {"US_EQUITY": {...}}}
    """
    rules = dict(DEFAULT_RULES)
    if not bool(_cfg("TAX_RULES_EXTERNAL_JSON_ENABLED", True)):
        return rules
    rule_path = Path(path) if path else _tax_rule_json_path()
    try:
        if rule_path.exists():
            payload = json.loads(rule_path.read_text(encoding="utf-8"))
            raw_rules = payload.get("rules", payload) if isinstance(payload, dict) else {}
            for key, value in dict(raw_rules or {}).items():
                if isinstance(value, dict):
                    rules[str(key).upper()] = _coerce_tax_rule(str(key).upper(), value)
    except Exception as exc:
        record_issue("tax_lot_accounting", "external_tax_rules_load_failed", exc, severity="ERROR", fail_mode="fail_closed", context={"path": str(rule_path)})
    return rules

def get_tax_rule(rule_key: str) -> TaxRule:
    rules = load_tax_rules()
    return rules.get(str(rule_key).upper(), rules.get("OTHER", DEFAULT_RULES["OTHER"]))

def classify_instrument(ticker: str, *, asset_class: str | None = None, jurisdiction: str | None = None) -> dict[str, Any]:
    """Auto-route Taiwan / US / FX / Futures tax treatment.

    The output is deterministic and configurable.  It should be validated before
    tax filing because brokers/jurisdictions may differ.
    """
    sym = _upper(ticker)
    ac = _upper(asset_class)
    j = _upper(jurisdiction)

    if ac in {"FX", "FOREX", "CURRENCY"} or re.match(r"^[A-Z]{3}[/-]?[A-Z]{3}$", sym) or sym.endswith("=X"):
        rule_key = "FX"
    elif ac in {"FUT", "FUTURE", "FUTURES"} or sym.startswith(("TXF", "MTX", "ES", "NQ", "YM", "RTY", "CL", "GC")) or "FUT" in sym:
        rule_key = "FUTURES"
    elif sym.endswith((".TW", ".TWO", ".TPEX")) or j == "TW":
        rule_key = "TW_EQUITY"
    elif j == "US" or re.match(r"^[A-Z][A-Z0-9.]{0,9}$", sym):
        rule_key = "US_EQUITY"
    else:
        rule_key = "OTHER"

    rule = get_tax_rule(rule_key)
    currency_override = _cfg(f"TAX_RULE_{rule_key}_CURRENCY", None)
    d = asdict(rule)
    d["rule_key"] = rule_key
    d["ticker_symbol"] = sym
    if currency_override:
        d["currency"] = str(currency_override).upper()
    return d


def tax_lot_id(ticker: str, side: str, entry_order_id: str = "", fill_id: str = "", index: int = 0) -> str:
    raw = f"TL-{_upper(ticker)}-{_upper(side)}-{entry_order_id or 'NOORDER'}-{fill_id or 'NOFILL'}-{index}"
    return "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in raw)[:120]


def position_key(ticker: str, side: str, strategy_name: str = "", signal_id: str = "") -> str:
    return "|".join([_upper(ticker), _upper(side), str(strategy_name or "CORE"), str(signal_id or "NO_SIGNAL")])


def holding_days(acq: Any, disp: Any | None = None) -> int:
    a = parse_dt(acq)
    b = parse_dt(disp) or datetime.now()
    if not a:
        return 0
    return max(0, int((b.date() - a.date()).days))


def holding_bucket(days: int, rule: dict[str, Any] | None = None) -> str:
    long_days = int((rule or {}).get("long_term_days") or _cfg("TAX_LOT_LONG_TERM_DAYS", 365) or 365)
    return "LONG_TERM" if int(days or 0) >= long_days else "SHORT_TERM"


def decorate_open_lot(lot: dict[str, Any], *, asset_class: str | None = None, jurisdiction: str | None = None) -> dict[str, Any]:
    row = dict(lot or {})
    ticker = row.get("ticker_symbol") or row.get("ticker") or row.get("symbol") or row.get("Ticker SYMBOL") or ""  # legacy alias compat
    side = row.get("side") or row.get("direction_bucket") or "LONG"
    rule = classify_instrument(str(ticker), asset_class=asset_class or row.get("asset_class"), jurisdiction=jurisdiction or row.get("jurisdiction"))
    open_qty = abs(qty_int(row.get("open_qty") or row.get("entry_fill_qty") or row.get("qty") or row.get("remaining_qty")))
    rem_qty = abs(qty_int(row.get("remaining_qty", open_qty)))
    entry_price = money(row.get("entry_price") or row.get("avg_cost") or row.get("fill_price"))
    open_commission = money(row.get("open_commission") or row.get("commission"))
    open_tax = money(row.get("open_tax") or row.get("tax"))
    gross_cost = money(row.get("gross_cost") or entry_price * open_qty)
    net_cost_basis = money(row.get("net_cost_basis") or gross_cost + open_commission + open_tax)
    remaining_basis = money(row.get("remaining_cost_basis") or (net_cost_basis * rem_qty / open_qty if open_qty else 0.0))
    lot_id = row.get("lot_id") or tax_lot_id(str(ticker), str(side), str(row.get("entry_order_id", "")), str(row.get("fill_id", "")))
    row.update({
        "lot_id": lot_id,
        "tax_lot_id": row.get("tax_lot_id") or lot_id,
        "ticker_symbol": _upper(ticker),
        "side": _upper(side),
        "direction_bucket": _upper(side),
        "asset_class": rule["asset_class"],
        "jurisdiction": rule["jurisdiction"],
        "tax_regime": rule["tax_regime"],
        "tax_treatment": rule["tax_treatment"],
        "report_type": rule["report_type"],
        "capital_gain_taxable": int(bool(rule["capital_gain_taxable"])),
        "ordinary_income": int(bool(rule["ordinary_income"])),
        "section1256_6040": int(bool(rule["section1256_6040"])),
        "wash_sale_applicable": int(bool(rule["wash_sale_applicable"])),
        "open_qty": open_qty,
        "remaining_qty": rem_qty,
        "gross_cost": gross_cost,
        "net_cost_basis": net_cost_basis,
        "remaining_cost_basis": remaining_basis,
        "allocated_open_fees": money(row.get("allocated_open_fees") or open_commission),
        "allocated_open_taxes": money(row.get("allocated_open_taxes") or open_tax),
        "currency": str(row.get("currency") or rule["currency"]),
        "cost_basis_method": _upper(row.get("cost_basis_method") or _cfg("TAX_LOT_METHOD", _cfg("LOT_ACCOUNTING_METHOD", "FIFO")) or "FIFO"),
        "acquisition_date": row.get("acquisition_date") or row.get("entry_time") or now_iso(),
        "position_key": row.get("position_key") or position_key(str(ticker), str(side), row.get("strategy_name", ""), row.get("signal_id", "")),
        "wash_sale_adjustment": money(row.get("wash_sale_adjustment")),
        "wash_sale_deferred_loss": money(row.get("wash_sale_deferred_loss")),
        "specific_id_tag": str(row.get("specific_id_tag") or ""),
    })
    return row


def enrich_open_lot(lot: dict[str, Any], market_price: float | None = None) -> dict[str, Any]:
    row = decorate_open_lot(lot)
    rem = abs(qty_int(row.get("remaining_qty")))
    side = _upper(row.get("side", "LONG"))
    if market_price is None:
        market_price = row.get("market_price") or row.get("last_price") or row.get("entry_price") or row.get("avg_cost") or 0.0
    market_price = money(market_price)
    remaining_basis = money(row.get("remaining_cost_basis"))
    avg_cost = money(remaining_basis / rem) if rem else money(row.get("avg_cost") or row.get("entry_price"))
    market_value = money(rem * market_price)
    gross_unreal = money((market_price - avg_cost) * rem if side == "LONG" else (avg_cost - market_price) * rem)
    row.update({
        "avg_cost": avg_cost,
        "market_price": market_price,
        "market_value": market_value,
        "unrealized_gross_pnl": gross_unreal,
        "unrealized_net_pnl": gross_unreal,
        "unrealized_pnl": gross_unreal,
        "holding_period_days": holding_days(row.get("acquisition_date") or row.get("entry_time")),
    })
    row["holding_period_bucket"] = holding_bucket(row["holding_period_days"], row)
    row["realized_unrealized_total"] = money(row.get("realized_net_pnl") or row.get("realized_pnl")) + gross_unreal
    return row


def closure_event(*, lot: dict[str, Any], qty: int, close_price: float, close_time: Any = None, exit_order_id: str = "", fill_id: str = "", commission: float = 0.0, tax: float = 0.0, method: str | None = None, note: str = "") -> dict[str, Any]:
    lot = decorate_open_lot(lot)
    rule = classify_instrument(str(lot.get("ticker_symbol", "")), asset_class=lot.get("asset_class"), jurisdiction=lot.get("jurisdiction"))
    close_ts = parse_dt(close_time) or datetime.now()
    q = abs(qty_int(qty))
    open_qty = max(1, abs(qty_int(lot.get("open_qty") or lot.get("entry_fill_qty") or lot.get("remaining_qty") or q)))
    remaining_basis = money(lot.get("remaining_cost_basis") or lot.get("net_cost_basis"))
    remaining_qty = max(1, abs(qty_int(lot.get("remaining_qty") or open_qty)))
    basis_per_share = money(remaining_basis / remaining_qty) if remaining_qty else money(lot.get("avg_cost") or lot.get("entry_price"))
    allocated_basis = money(basis_per_share * q)
    close_px = money(close_price)
    gross_proceeds = money(close_px * q)
    if tax in (None, "", 0) and float(rule.get("sell_transaction_tax_rate") or 0.0) > 0:
        tax = money(gross_proceeds * float(rule.get("sell_transaction_tax_rate") or 0.0))
    close_comm = money(commission)
    close_tax = money(tax)
    net_proceeds = money(gross_proceeds - close_comm - close_tax)
    side = _upper(lot.get("side", "LONG"))
    gross_pnl = money((close_px - basis_per_share) * q if side == "LONG" else (basis_per_share - close_px) * q)
    net_pnl = money(gross_pnl - close_comm - close_tax)
    hp = holding_days(lot.get("acquisition_date") or lot.get("entry_time"), close_ts)
    event_id = f"TE-{lot.get('tax_lot_id')}-{fill_id or exit_order_id or close_ts.isoformat(timespec='seconds')}-{q}"[:140]
    long_amt = money(net_pnl * 0.60) if rule.get("section1256_6040") else 0.0
    short_amt = money(net_pnl * 0.40) if rule.get("section1256_6040") else 0.0
    return {
        "tax_event_id": event_id,
        "tax_lot_id": lot.get("tax_lot_id"),
        "lot_id": lot.get("lot_id"),
        "ticker_symbol": lot.get("ticker_symbol"),
        "asset_class": rule["asset_class"],
        "jurisdiction": rule["jurisdiction"],
        "tax_regime": rule["tax_regime"],
        "tax_treatment": rule["tax_treatment"],
        "report_type": rule["report_type"],
        "direction_bucket": side,
        "position_key": lot.get("position_key"),
        "strategy_name": lot.get("strategy_name", ""),
        "signal_id": lot.get("signal_id", ""),
        "exit_order_id": exit_order_id,
        "exit_fill_id": fill_id,
        "closed_qty": q,
        "acquisition_date": lot.get("acquisition_date") or lot.get("entry_time"),
        "disposal_date": close_ts.isoformat(timespec="seconds"),
        "entry_price": money(lot.get("entry_price") or lot.get("avg_cost")),
        "cost_basis_price": basis_per_share,
        "close_price": close_px,
        "gross_proceeds": gross_proceeds,
        "allocated_cost_basis": allocated_basis,
        "close_commission": close_comm,
        "close_tax": close_tax,
        "net_proceeds": net_proceeds,
        "realized_gross_pnl": gross_pnl,
        "realized_net_pnl": net_pnl,
        "taxable_gain_loss": net_pnl if rule.get("capital_gain_taxable") or rule.get("ordinary_income") else 0.0,
        "ordinary_income_amount": net_pnl if rule.get("ordinary_income") else 0.0,
        "section1256_60pct_amount": long_amt,
        "section1256_40pct_amount": short_amt,
        "holding_period_days": hp,
        "holding_period_bucket": holding_bucket(hp, rule),
        "tax_year": int(close_ts.year),
        "cost_basis_method": _upper(method or lot.get("cost_basis_method") or _cfg("TAX_LOT_METHOD", "FIFO")),
        "currency": lot.get("currency") or rule["currency"],
        "wash_sale_applicable": int(bool(rule.get("wash_sale_applicable"))),
        "wash_sale_applied": 0,
        "wash_sale_adjustment": 0.0,
        "wash_sale_disallowed_loss": 0.0,
        "wash_sale_replacement_lot_ids": "[]",
        "wash_sale_window_start": (close_ts - timedelta(days=int(rule.get("wash_sale_window_days") or 30))).date().isoformat(),
        "wash_sale_window_end": (close_ts + timedelta(days=int(rule.get("wash_sale_window_days") or 30))).date().isoformat(),
        "specific_id_tag": lot.get("specific_id_tag", ""),
        "note": note,
        "raw_json": _json({"lot": lot, "rule": rule}),
    }


def update_lot_after_close(lot: dict[str, Any], event: dict[str, Any]) -> dict[str, Any]:
    row = decorate_open_lot(lot)
    q = abs(qty_int(event.get("closed_qty")))
    old_rem = abs(qty_int(row.get("remaining_qty")))
    new_rem = max(0, old_rem - q)
    row["remaining_qty"] = new_rem
    row["close_fill_qty"] = qty_int(row.get("close_fill_qty")) + q
    row["close_fill_count"] = qty_int(row.get("close_fill_count")) + 1
    row["exit_fill_ids_json"] = _append_list(row.get("exit_fill_ids_json"), event.get("exit_fill_id"))
    row["close_commission"] = money(row.get("close_commission")) + money(event.get("close_commission"))
    row["close_tax"] = money(row.get("close_tax")) + money(event.get("close_tax"))
    row["realized_gross_pnl"] = money(row.get("realized_gross_pnl")) + money(event.get("realized_gross_pnl"))
    row["realized_net_pnl"] = money(row.get("realized_net_pnl") or row.get("realized_pnl")) + money(event.get("realized_net_pnl"))
    row["realized_pnl"] = row["realized_net_pnl"]
    row["remaining_cost_basis"] = max(0.0, money(row.get("remaining_cost_basis")) - money(event.get("allocated_cost_basis")))
    row["close_time"] = event.get("disposal_date")
    row["disposal_date"] = event.get("disposal_date") if new_rem <= 0 else ""
    row["exit_order_id"] = event.get("exit_order_id")
    row["last_fill_time"] = event.get("disposal_date")
    row["holding_period_days"] = event.get("holding_period_days")
    row["holding_period_bucket"] = event.get("holding_period_bucket")
    row["tax_year"] = event.get("tax_year")
    row["status"] = "CLOSED" if new_rem <= 0 else "PARTIAL_EXIT"
    row["lifecycle_status"] = row["status"]
    row["updated_at"] = now_iso()
    return row


def _append_list(raw: Any, value: Any) -> str:
    try:
        arr = json.loads(raw) if isinstance(raw, str) and raw else (list(raw) if isinstance(raw, (list, tuple)) else [])
    except Exception:
        arr = []
    if value not in (None, ""):
        arr.append(str(value))
    return json.dumps(arr, ensure_ascii=False)


def apply_wash_sale_adjustments(closures: list[dict[str, Any]], open_lots: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    if not bool(_cfg("TAX_LOT_WASH_SALE_RULE_ENABLED", True)):
        return closures, open_lots
    lots = [decorate_open_lot(x) for x in (open_lots or [])]
    out_events: list[dict[str, Any]] = []
    for ev0 in closures or []:
        ev = dict(ev0)
        loss = money(ev.get("realized_net_pnl"))
        if not ev.get("wash_sale_applicable") or loss >= 0:
            out_events.append(ev)
            continue
        disp = parse_dt(ev.get("disposal_date")) or datetime.now()
        window = int(_cfg("TAX_LOT_WASH_SALE_WINDOW_DAYS", ev.get("wash_sale_window_days", 30)) or 30)
        start, end = disp - timedelta(days=window), disp + timedelta(days=window)
        candidates = []
        for lot in lots:
            if lot.get("ticker_symbol") != ev.get("ticker_symbol"):
                continue
            acq = parse_dt(lot.get("acquisition_date") or lot.get("entry_time"))
            if not acq:
                continue
            if start.date() <= acq.date() <= end.date() and qty_int(lot.get("remaining_qty")) > 0:
                candidates.append(lot)
        if not candidates:
            ev["wash_sale_pending"] = 1
            ev["wash_sale_pending_until"] = end.date().isoformat()
            out_events.append(ev)
            continue
        disallowed = abs(loss)
        total_qty = sum(max(0, qty_int(x.get("remaining_qty"))) for x in candidates) or 1
        replacement_ids = []
        for lot in candidates:
            q = max(0, qty_int(lot.get("remaining_qty")))
            adj = money(disallowed * q / total_qty)
            lot["wash_sale_adjustment"] = money(lot.get("wash_sale_adjustment")) + adj
            lot["wash_sale_deferred_loss"] = money(lot.get("wash_sale_deferred_loss")) + adj
            lot["remaining_cost_basis"] = money(lot.get("remaining_cost_basis")) + adj
            lot["net_cost_basis"] = money(lot.get("net_cost_basis")) + adj
            if q:
                lot["avg_cost"] = money(lot["remaining_cost_basis"] / q)
            lot["wash_sale_source_event_id"] = ev.get("tax_event_id")
            replacement_ids.append(lot.get("lot_id"))
        ev["wash_sale_applied"] = 1
        ev["wash_sale_adjustment"] = money(disallowed)
        ev["wash_sale_disallowed_loss"] = money(disallowed)
        ev["taxable_gain_loss"] = 0.0
        ev["realized_net_pnl_after_wash"] = 0.0
        ev["wash_sale_replacement_lot_ids"] = json.dumps(replacement_ids, ensure_ascii=False)
        out_events.append(ev)
    return out_events, lots


def summarize_tax_lots(closures: Iterable[dict[str, Any]], open_lots: Iterable[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
    buckets: dict[tuple, dict[str, Any]] = {}
    for ev in closures or []:
        key = (int(ev.get("tax_year") or datetime.now().year), ev.get("jurisdiction"), ev.get("asset_class"), ev.get("report_type"), ev.get("holding_period_bucket"), ev.get("currency"))
        row = buckets.setdefault(key, {
            "summary_id": f"TS-{key[0]}-{key[1]}-{key[2]}-{key[3]}-{key[4]}-{key[5]}"[:140],
            "tax_year": key[0], "jurisdiction": key[1], "asset_class": key[2], "report_type": key[3], "holding_period_bucket": key[4], "currency": key[5],
            "closed_qty": 0, "gross_proceeds": 0.0, "allocated_cost_basis": 0.0, "close_commission": 0.0, "close_tax": 0.0,
            "realized_gross_pnl": 0.0, "realized_net_pnl": 0.0, "taxable_gain_loss": 0.0, "ordinary_income_amount": 0.0,
            "section1256_60pct_amount": 0.0, "section1256_40pct_amount": 0.0, "wash_sale_adjustment": 0.0,
            "open_lot_count": 0, "unrealized_net_pnl": 0.0,
        })
        row["closed_qty"] += qty_int(ev.get("closed_qty"))
        for k in ("gross_proceeds", "allocated_cost_basis", "close_commission", "close_tax", "realized_gross_pnl", "realized_net_pnl", "taxable_gain_loss", "ordinary_income_amount", "section1256_60pct_amount", "section1256_40pct_amount", "wash_sale_adjustment"):
            row[k] = money(row[k]) + money(ev.get(k))
    for lot0 in open_lots or []:
        lot = enrich_open_lot(lot0)
        key = (int(datetime.now().year), lot.get("jurisdiction"), lot.get("asset_class"), lot.get("report_type"), "OPEN", lot.get("currency"))
        row = buckets.setdefault(key, {
            "summary_id": f"TS-{key[0]}-{key[1]}-{key[2]}-{key[3]}-OPEN-{key[5]}"[:140],
            "tax_year": key[0], "jurisdiction": key[1], "asset_class": key[2], "report_type": key[3], "holding_period_bucket": key[4], "currency": key[5],
            "closed_qty": 0, "gross_proceeds": 0.0, "allocated_cost_basis": 0.0, "close_commission": 0.0, "close_tax": 0.0,
            "realized_gross_pnl": 0.0, "realized_net_pnl": 0.0, "taxable_gain_loss": 0.0, "ordinary_income_amount": 0.0,
            "section1256_60pct_amount": 0.0, "section1256_40pct_amount": 0.0, "wash_sale_adjustment": 0.0,
            "open_lot_count": 0, "unrealized_net_pnl": 0.0,
        })
        row["open_lot_count"] += 1
        row["unrealized_net_pnl"] = money(row["unrealized_net_pnl"]) + money(lot.get("unrealized_net_pnl"))
    return list(buckets.values())


def export_tax_reports(closures: list[dict[str, Any]], open_lots: list[dict[str, Any]] | None = None, *, output_dir: str | Path | None = None, tax_year: int | None = None) -> dict[str, str]:
    base = Path(output_dir or _cfg("TAX_REPORT_OUTPUT_DIR", "runtime/tax_reports"))
    year = int(tax_year or datetime.now().year)
    outdir = base / str(year)
    outdir.mkdir(parents=True, exist_ok=True)
    closures2 = [dict(x) for x in closures if not tax_year or int(x.get("tax_year") or 0) == year]
    open_lots2 = [enrich_open_lot(x) for x in (open_lots or [])]
    summary = summarize_tax_lots(closures2, open_lots2)
    exports: dict[str, str] = {}

    def write_csv(name: str, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
        path = outdir / name
        if fields is None:
            keys = []
            for r in rows:
                for k in r.keys():
                    if k not in keys:
                        keys.append(k)
            fields = keys or ["empty"]
        with path.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            for r in rows:
                w.writerow(r)
        exports[name] = str(path)

    write_csv("consolidated_tax_lot_closures.csv", closures2)
    write_csv("consolidated_tax_lot_summary.csv", summary)
    write_csv("wash_sale_adjustments.csv", [x for x in closures2 if x.get("wash_sale_applied") or x.get("wash_sale_pending")])
    write_csv("tw_securities_report.csv", [x for x in closures2 if x.get("jurisdiction") == "TW"])
    write_csv("us_form8949_like.csv", [x for x in closures2 if x.get("report_type") == "US_FORM_8949_LIKE"])
    write_csv("us_1099b_like.csv", [x for x in closures2 if x.get("jurisdiction") == "US"])
    write_csv("fx_ordinary_income_report.csv", [x for x in closures2 if x.get("asset_class") == "FX"])
    write_csv("futures_1256_like_report.csv", [x for x in closures2 if x.get("asset_class") == "FUTURES"])
    manifest = {
        "generated_at": now_iso(), "tax_year": year, "exports": exports,
        "disclaimer": "Engineering export only; validate with broker/accountant before filing.",
    }
    (outdir / "tax_report_manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    exports["manifest"] = str(outdir / "tax_report_manifest.json")
    return exports
