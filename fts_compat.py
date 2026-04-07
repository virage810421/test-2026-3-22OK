# -*- coding: utf-8 -*-
import pandas as pd
from fts_config import PATHS, CONFIG
from fts_utils import log, safe_float

class DecisionCompatibilityLayer:
    MODULE_VERSION = "v17"

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
        out["Action"] = direction.astype(str) if direction is not None else "BUY"
        out["Regime"] = pick("Regime", "regime")
        out["Structure"] = pick("Structure", "Strategy_Name", "Strategy", "strategy_name")
        out["AI_Proba"] = pick("AI_Proba", "ai_proba", "AI_Confidence", "confidence")
        out["Score"] = pick("Score", "Final_Score", "score", "final_score")
        out["Kelly_Pos"] = pick("Kelly_Pos", "Kelly_Fraction", "kelly_fraction")
        out["Heuristic_EV"] = pick("Heuristic_EV", "expected_return", "Expected_Return", "Realized_EV")
        out["Reference_Price"] = pick("Reference_Price", "Close", "close", "收盤價", "價格")
        out["Target_Qty"] = pick("Target_Qty", "Qty", "qty", "建議股數", "建議張數")

        # 保底欄位
        if out["Ticker"] is None:
            out["Ticker"] = ""
        if out["Action"] is None:
            out["Action"] = "BUY"
        if out["Reference_Price"] is None:
            out["Reference_Price"] = 0.0
        if out["Target_Qty"] is None:
            out["Target_Qty"] = 0

        # 嘗試把字串價格轉成 float，空值補 0
        out["Reference_Price"] = out["Reference_Price"].apply(lambda x: safe_float(x, 0.0))
        out["Target_Qty"] = out["Target_Qty"].fillna(0)

        # 額外診斷欄位
        out["_has_ticker"] = out["Ticker"].astype(str).str.strip().ne("")
        out["_has_action"] = out["Action"].astype(str).str.strip().ne("")
        out["_has_price"] = out["Reference_Price"].astype(float).gt(0)
        out["_raw_action"] = out["Action"].astype(str)

        diag = {
            "normalized": True,
            "source_columns": list(df.columns),
            "normalized_columns": list(out.columns),
            "row_count": int(len(out)),
            "rows_with_ticker": int(out["_has_ticker"].sum()),
            "rows_with_action": int(out["_has_action"].sum()),
            "rows_with_price": int(out["_has_price"].sum()),
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
            f"ticker={diag['rows_with_ticker']} | "
            f"action={diag['rows_with_action']} | "
            f"price={diag['rows_with_price']}"
        )
        return out, diag
