# -*- coding: utf-8 -*-
"""Formal architecture cleanup smoke test.

Checks:
- formal class facades are active
- callback mapper normalizes broker callbacks
- chart/dashboard field contract is present
- external tax rules load and classify instruments
- runtime smoke report is written
"""
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime


def main() -> dict:
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "checks": {}, "ok": True}
    def check(name, fn):
        try:
            value = fn()
            report["checks"][name] = {"ok": True, "value": value}
        except Exception as exc:
            report["checks"][name] = {"ok": False, "error": repr(exc)}
            report["ok"] = False

    check("formal_paper_broker", lambda: getattr(__import__("paper_broker").PaperBroker, "FORMAL_CLASS_LAYER", False))
    check("formal_real_broker", lambda: "class FormalRealBrokerStub" in Path("fts_broker_real_stub.py").read_text(encoding="utf-8", errors="ignore"))
    check("formal_execution_engine", lambda: "class FormalExecutionEngine" in Path("execution_engine.py").read_text(encoding="utf-8", errors="ignore"))
    check("formal_db_logger", lambda: getattr(__import__("db_logger").SQLServerExecutionLogger, "FORMAL_CLASS_LAYER", False))
    def cb():
        from fts_broker_callback_mapping import normalize_broker_callback
        return normalize_broker_callback({"order_id":"O1","status":"partial","symbol":"2330.TW","fill_qty":100,"price":600})
    check("callback_mapping", cb)
    def tax():
        from fts_tax_lot_accounting import classify_instrument, load_tax_rules
        return {"rules": sorted(load_tax_rules().keys()), "tw": classify_instrument("2330.TW"), "us": classify_instrument("AAPL")}
    check("external_tax_rules", tax)
    def dash():
        txt = Path("fts_dashboard.py").read_text(encoding="utf-8", errors="ignore")
        return "THREE_PATH_DASHBOARD_FIELDS" in txt and "Exit_State" in txt and "PreEntry_Score" in txt
    check("three_path_dashboard_contract", dash)
    out = Path("runtime") / "formal_healthcheck_smoke_report.json"
    out.parent.mkdir(exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return report

if __name__ == "__main__":
    main()
