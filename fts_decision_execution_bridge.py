# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Tuple, List, Dict

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_market_rules_tw import validate_order_payload
from fts_utils import now_str, log, safe_float


class DecisionExecutionBridge:
    MODULE_VERSION = "v68"

    def __init__(self):
        self.report_path = PATHS.runtime_dir / "decision_execution_bridge.json"
        self.output_path = PATHS.data_dir / "executable_order_payloads.csv"
        self.price_template_path = PATHS.data_dir / "manual_price_snapshot_template.csv"
        self.auto_price_candidates_path = PATHS.data_dir / "auto_price_snapshot_candidates.csv"

    def _load_normalized(self) -> pd.DataFrame:
        candidates = [
            PATHS.data_dir / "normalized_decision_output_enriched.csv",
            PATHS.data_dir / "normalized_decision_output.csv",
            PATHS.base_dir / "daily_decision_desk.csv",
        ]
        for p in candidates:
            if p.exists():
                df = pd.read_csv(p, encoding="utf-8-sig")
                if "Action" not in df.columns and "Direction" in df.columns:
                    mapper = {
                        "做多(Long)": "BUY", "多方進場": "BUY", "BUY": "BUY",
                        "做空(Short)": "SELL", "空方進場": "SELL", "SELL": "SELL",
                    }
                    df["Action"] = df["Direction"].map(lambda x: mapper.get(str(x).strip(), str(x).strip().upper()))
                return df
        return pd.DataFrame()

    def _normalize_price_file(self, p: Path) -> pd.DataFrame:
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            try:
                df = pd.read_csv(p)
            except Exception:
                return pd.DataFrame(columns=["Ticker", "Reference_Price", "Source_File"])

        cols = {str(c).strip(): str(c).strip() for c in df.columns}
        ticker_candidates = [c for c in cols if c.lower() in ("ticker", "ticker symbol", "symbol", "stock_id") or "代號" in c]
        price_candidates = [
            c for c in cols
            if c.lower() in ("reference_price", "close", "adj close", "price", "last", "last_price")
            or "收盤" in c or "成交" in c or "價格" in c
        ]
        if not ticker_candidates or not price_candidates:
            return pd.DataFrame(columns=["Ticker", "Reference_Price", "Source_File"])

        tcol = ticker_candidates[0]
        pcol = price_candidates[0]
        out = df[[tcol, pcol]].copy()
        out.columns = ["Ticker", "Reference_Price"]
        out["Ticker"] = out["Ticker"].astype(str).str.strip()
        out["Reference_Price"] = pd.to_numeric(out["Reference_Price"], errors="coerce")
        out = out[(out["Ticker"] != "") & out["Reference_Price"].notna() & (out["Reference_Price"] > 0)]
        if out.empty:
            return pd.DataFrame(columns=["Ticker", "Reference_Price", "Source_File"])
        out["Source_File"] = str(p)
        return out.drop_duplicates(subset=["Ticker"], keep="last")

    def _scan_auto_price_sources(self) -> Tuple[pd.DataFrame, List[str]]:
        roots = [PATHS.base_dir, PATHS.data_dir]
        skip_names = {
            self.output_path.name,
            self.price_template_path.name,
            "normalized_decision_output.csv",
            "normalized_decision_output_enriched.csv",
            "daily_decision_desk.csv",
        }
        found: List[pd.DataFrame] = []
        source_files: List[str] = []
        seen = set()
        for root in roots:
            for p in root.rglob("*.csv"):
                if p.name in skip_names:
                    continue
                sp = str(p.resolve())
                if sp in seen:
                    continue
                seen.add(sp)
                norm = self._normalize_price_file(p)
                if not norm.empty:
                    found.append(norm)
                    source_files.append(str(p))
        if not found:
            if self.auto_price_candidates_path.exists():
                self.auto_price_candidates_path.unlink()
            return pd.DataFrame(columns=["Ticker", "Reference_Price"]), []
        merged = pd.concat(found, ignore_index=True)
        merged = merged.sort_values(["Ticker", "Source_File"]).drop_duplicates(subset=["Ticker"], keep="last")
        merged.to_csv(self.auto_price_candidates_path, index=False, encoding="utf-8-sig")
        return merged[["Ticker", "Reference_Price"]].copy(), source_files

    def _load_price_snapshot(self) -> Tuple[pd.DataFrame, str, List[str]]:
        candidates = [
            PATHS.base_dir / "last_price_snapshot.csv",
            PATHS.data_dir / "last_price_snapshot.csv",
            PATHS.base_dir / "daily_price_snapshot.csv",
            PATHS.data_dir / "manual_price_snapshot_template.csv",
        ]
        for p in candidates:
            if p.exists():
                df = self._normalize_price_file(p)
                if not df.empty:
                    return df[["Ticker", "Reference_Price"]].copy(), str(p), [str(p)]
        auto_df, source_files = self._scan_auto_price_sources()
        if not auto_df.empty:
            return auto_df, "auto_scan", source_files
        return pd.DataFrame(columns=["Ticker", "Reference_Price"]), "", []

    def _load_risk_source(self) -> pd.DataFrame:
        p = PATHS.base_dir / "daily_decision_desk.csv"
        if not p.exists():
            return pd.DataFrame(columns=["Ticker", "風險金額", "預期停損(%)", "預期停利(%)"])
        df = pd.read_csv(p, encoding="utf-8-sig")
        keep = [c for c in ["Ticker", "風險金額", "預期停損(%)", "預期停利(%)", "Kelly_Pos", "Heuristic_EV", "Score"] if c in df.columns]
        if not keep:
            return pd.DataFrame(columns=["Ticker"])
        out = df[keep].copy()
        out["Ticker"] = out["Ticker"].astype(str).str.strip()
        return out

    def _write_price_template(self, df: pd.DataFrame) -> int:
        missing = df[df["Reference_Price"] <= 0].copy()
        if missing.empty:
            if self.price_template_path.exists():
                self.price_template_path.unlink()
            return 0
        cols = [c for c in ["Ticker", "Action", "Regime", "風險金額", "預期停損(%)", "Target_Qty"] if c in missing.columns]
        templ = missing[cols].copy()
        insert_at = 1 if len(templ.columns) >= 1 else 0
        templ.insert(insert_at, "Reference_Price", 0.0)
        templ["Lot_Size"] = int(CONFIG.lot_size)
        templ["Capital_Cap"] = float(CONFIG.starting_cash * CONFIG.max_single_position_pct)
        templ["Suggested_Stop_Price"] = templ.apply(
            lambda r: round(float(r["Reference_Price"] or 0) * (1 - float(r.get("預期停損(%)", CONFIG.default_stop_loss_pct) or CONFIG.default_stop_loss_pct)), 2), axis=1
        )
        templ = templ.drop_duplicates(subset=["Ticker"]).sort_values(["Ticker"])
        templ.to_csv(self.price_template_path, index=False, encoding="utf-8-sig")
        return int(len(templ))

    def build(self):
        df = self._load_normalized()
        if df.empty:
            payload = {
                "generated_at": now_str(),
                "module_version": self.MODULE_VERSION,
                "status": "missing_normalized_decision",
                "output_path": str(self.output_path),
            }
            self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return self.output_path, payload

        df["Ticker"] = df["Ticker"].astype(str).str.strip()
        if "Reference_Price" not in df.columns:
            df["Reference_Price"] = 0.0
        if "Target_Qty" not in df.columns:
            df["Target_Qty"] = 0

        price_df, price_src, price_scan_files = self._load_price_snapshot()
        if not price_df.empty:
            df = df.merge(price_df, on="Ticker", how="left", suffixes=("", "_snap"))
            mask = pd.to_numeric(df["Reference_Price"], errors="coerce").fillna(0) <= 0
            df.loc[mask, "Reference_Price"] = pd.to_numeric(df.loc[mask, "Reference_Price_snap"], errors="coerce").fillna(0)
            if "Reference_Price_snap" in df.columns:
                df = df.drop(columns=["Reference_Price_snap"])

        risk_src = self._load_risk_source()
        if not risk_src.empty:
            df = df.merge(risk_src, on="Ticker", how="left", suffixes=("", "_risk"))

        df["Reference_Price"] = pd.to_numeric(df["Reference_Price"], errors="coerce").fillna(0.0)
        df["Kelly_Pos"] = pd.to_numeric(df.get("Kelly_Pos", 0.0), errors="coerce").fillna(0.0)
        df["風險金額"] = pd.to_numeric(df.get("風險金額", 0.0), errors="coerce").fillna(0.0)
        df["預期停損(%)"] = pd.to_numeric(df.get("預期停損(%)", CONFIG.default_stop_loss_pct), errors="coerce").fillna(CONFIG.default_stop_loss_pct)

        market_checks: List[Dict] = []
        valid_count = 0
        for idx, row in df.iterrows():
            price = float(row.get("Reference_Price", 0.0) or 0.0)
            qty = int(float(row.get("Target_Qty", 0) or 0))
            if price > 0 and qty <= 0:
                risk_budget = float(row.get("風險金額", 0.0) or 0.0)
                stop_pct = max(float(row.get("預期停損(%)", CONFIG.default_stop_loss_pct) or CONFIG.default_stop_loss_pct), 0.005)
                kelly = max(float(row.get("Kelly_Pos", 0.0) or 0.0), 0.0)
                capital_cap = CONFIG.starting_cash * max(min(kelly, CONFIG.max_single_position_pct), 0.01)
                qty_by_cap = int(capital_cap // (price * CONFIG.lot_size)) * CONFIG.lot_size
                qty_by_risk = 0
                if risk_budget > 0:
                    qty_by_risk = int(risk_budget // (price * stop_pct))
                    qty_by_risk = (qty_by_risk // CONFIG.lot_size) * CONFIG.lot_size
                qty = max(qty_by_risk, qty_by_cap, 0)
                if qty <= 0 and price * CONFIG.lot_size <= CONFIG.starting_cash * CONFIG.max_single_position_pct:
                    qty = CONFIG.lot_size
                df.at[idx, "Target_Qty"] = int(qty)
            check = validate_order_payload(str(row.get("Ticker", "")), float(df.at[idx, "Reference_Price"] or 0), int(df.at[idx, "Target_Qty"] or 0), int(CONFIG.lot_size)).to_dict()
            market_checks.append(check)
            valid_count += 1 if check.get("passed") else 0

        df["Target_Qty"] = pd.to_numeric(df["Target_Qty"], errors="coerce").fillna(0).astype(int)
        df["MarketRulePassed"] = [bool(x.get("passed")) for x in market_checks]
        df["MarketRuleReason"] = [x.get("reason", "") for x in market_checks]
        df.to_csv(self.output_path, index=False, encoding="utf-8-sig")
        template_rows = self._write_price_template(df)

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "price_snapshot_source": price_src,
            "price_scan_files": price_scan_files[:100],
            "auto_price_candidates_path": str(self.auto_price_candidates_path),
            "manual_price_template_path": str(self.price_template_path),
            "manual_price_template_rows": template_rows,
            "rows_total": int(len(df)),
            "rows_with_price": int((df["Reference_Price"] > 0).sum()),
            "rows_with_qty": int((df["Target_Qty"] > 0).sum()),
            "rows_market_rule_passed": int(valid_count),
            "rows_market_rule_failed": int(len(df) - valid_count),
            "failed_tickers_preview": [x.get("ticker") for x in market_checks if not x.get("passed")][:20],
            "output_path": str(self.output_path),
            "status": "execution_payload_ready" if valid_count > 0 else "waiting_for_price_or_qty",
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"🧩 已輸出 execution bridge：{self.report_path}")
        return self.output_path, payload
