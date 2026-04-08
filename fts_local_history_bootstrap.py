# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Dict, Any, List, Tuple

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class LocalHistoryBootstrap:
    MODULE_VERSION = "v70"

    def __init__(self):
        self.report_path = PATHS.runtime_dir / "local_history_bootstrap.json"
        self.recipe_path = PATHS.runtime_dir / "history_backfill_recipe.json"
        self.request_csv_path = PATHS.data_dir / "kline_cache_request_list.csv"
        self.cache_dir = PATHS.data_dir / "kline_cache"
        self.cache_dir.mkdir(exist_ok=True)
        self.snapshot_path = PATHS.data_dir / "last_price_snapshot.csv"

    def _load_csv_safe(self, p: Path) -> pd.DataFrame:
        try:
            return pd.read_csv(p, encoding="utf-8-sig")
        except Exception:
            try:
                return pd.read_csv(p)
            except Exception:
                return pd.DataFrame()

    def _match_col(self, cols: List[str], names: List[str], fuzzy_tokens: List[str] = None) -> str:
        lower = {str(c).strip().lower(): str(c).strip() for c in cols}
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        if fuzzy_tokens:
            for c in cols:
                lc = str(c).strip().lower()
                if all(tok.lower() in lc for tok in fuzzy_tokens):
                    return str(c).strip()
        return ""

    def _normalize_history_file(self, p: Path) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        df = self._load_csv_safe(p)
        if df.empty:
            return pd.DataFrame(), {"file": str(p), "status": "empty_or_unreadable"}

        cols = [str(c).strip() for c in df.columns]
        ticker_col = self._match_col(cols, ["Ticker", "Ticker SYMBOL", "Symbol", "stock_id", "ticker"], ["ticker"])
        date_col = self._match_col(cols, ["Date", "日期", "資料日期", "trade_date"], ["date"])
        open_col = self._match_col(cols, ["Open", "開盤", "open"])
        high_col = self._match_col(cols, ["High", "最高", "high"])
        low_col = self._match_col(cols, ["Low", "最低", "low"])
        close_col = self._match_col(cols, ["Close", "Adj Close", "收盤", "close"], ["close"])
        volume_col = self._match_col(cols, ["Volume", "成交量", "volume"])

        required = [ticker_col, date_col, open_col, high_col, low_col, close_col]
        if not all(required):
            return pd.DataFrame(), {
                "file": str(p),
                "status": "missing_required_columns",
                "columns": cols[:40],
            }

        out = df[[ticker_col, date_col, open_col, high_col, low_col, close_col] + ([volume_col] if volume_col else [])].copy()
        rename_map = {
            ticker_col: "Ticker",
            date_col: "Date",
            open_col: "Open",
            high_col: "High",
            low_col: "Low",
            close_col: "Close",
        }
        if volume_col:
            rename_map[volume_col] = "Volume"
        out = out.rename(columns=rename_map)
        out["Ticker"] = out["Ticker"].astype(str).str.strip()
        out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
        for c in ["Open", "High", "Low", "Close"]:
            out[c] = pd.to_numeric(out[c], errors="coerce")
        if "Volume" in out.columns:
            out["Volume"] = pd.to_numeric(out["Volume"], errors="coerce").fillna(0)
        else:
            out["Volume"] = 0
        out = out.dropna(subset=["Ticker", "Date", "Open", "High", "Low", "Close"])
        out = out[(out["Ticker"] != "") & (out[["Open", "High", "Low", "Close"]] > 0).all(axis=1)]
        if out.empty:
            return pd.DataFrame(), {"file": str(p), "status": "normalized_empty"}
        out["Date"] = out["Date"].dt.strftime("%Y-%m-%d")
        out = out.sort_values(["Ticker", "Date"]).drop_duplicates(subset=["Ticker", "Date"], keep="last")
        return out, {"file": str(p), "status": "ok", "rows": int(len(out)), "tickers": int(out["Ticker"].nunique())}

    def _scan_sources(self) -> List[Path]:
        skip = {
            "manual_price_snapshot_template.csv",
            "auto_price_snapshot_candidates.csv",
            "executable_order_payloads.csv",
            "normalized_decision_output.csv",
            "normalized_decision_output_enriched.csv",
            "daily_decision_desk.csv",
            "training_bootstrap_universe.csv",
            "ml_training_data.csv",
        }
        candidates = []
        seen = set()
        for root in [PATHS.base_dir, PATHS.data_dir]:
            for p in root.rglob("*.csv"):
                if p.name in skip or self.cache_dir in p.parents:
                    continue
                rp = str(p.resolve())
                if rp in seen:
                    continue
                seen.add(rp)
                candidates.append(p)
        return candidates

    def _write_cache_files(self, history_df: pd.DataFrame) -> Dict[str, Any]:
        if history_df.empty:
            return {"written_files": 0, "tickers": 0}
        written = 0
        for ticker, sub in history_df.groupby("Ticker"):
            out_path = self.cache_dir / f"{ticker}_ohlcv.csv"
            sub = sub[["Date", "Open", "High", "Low", "Close", "Volume"]].copy().sort_values("Date")
            sub.to_csv(out_path, index=False, encoding="utf-8-sig")
            written += 1
        return {"written_files": int(written), "tickers": int(history_df["Ticker"].nunique())}

    def _load_bootstrap_universe(self) -> List[str]:
        p = PATHS.data_dir / "training_bootstrap_universe.csv"
        if not p.exists():
            return []
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            if "Ticker" not in df.columns:
                return []
            return sorted(df["Ticker"].astype(str).str.strip().replace("nan", "").loc[lambda s: s != ""].unique().tolist())
        except Exception:
            return []

    def _load_decision_tickers(self) -> List[str]:
        p = PATHS.base_dir / "daily_decision_desk.csv"
        if not p.exists():
            return []
        try:
            df = pd.read_csv(p, encoding="utf-8-sig")
            if "Ticker" not in df.columns:
                return []
            return sorted(df["Ticker"].astype(str).str.strip().replace("nan", "").loc[lambda s: s != ""].unique().tolist())
        except Exception:
            return []

    def _write_request_list(self, missing_tickers: List[str]) -> int:
        req = pd.DataFrame({
            "Ticker": missing_tickers,
            "Preferred_Period": getattr(CONFIG, "online_history_period", "3y"),
            "Preferred_Interval": getattr(CONFIG, "online_history_interval", "1d"),
            "Preferred_Provider": getattr(CONFIG, "online_history_provider", "yfinance"),
            "Target_File": [str(self.cache_dir / f"{t}_ohlcv.csv") for t in missing_tickers],
        })
        req.to_csv(self.request_csv_path, index=False, encoding="utf-8-sig")
        return int(len(req))

    def _try_online_backfill(self, missing_tickers: List[str]) -> Dict[str, Any]:
        enabled = bool(getattr(CONFIG, "allow_online_history_backfill", False))
        provider = str(getattr(CONFIG, "online_history_provider", "yfinance"))
        limit = int(getattr(CONFIG, "online_history_max_tickers_per_run", 30) or 30)
        result = {
            "enabled": enabled,
            "provider": provider,
            "requested": int(min(len(missing_tickers), limit)),
            "fetched": 0,
            "written_files": 0,
            "status": "disabled",
            "errors_preview": [],
        }
        if not enabled or not missing_tickers:
            return result
        if provider.lower() != "yfinance":
            result["status"] = "unsupported_provider"
            return result
        try:
            import yfinance as yf
        except Exception as e:
            result["status"] = "missing_provider_package"
            result["errors_preview"] = [str(e)]
            return result

        fetched = 0
        written = 0
        errors = []
        for ticker in missing_tickers[:limit]:
            try:
                symbol = ticker if "." in ticker else f"{ticker}.TW"
                df = yf.download(symbol, period=getattr(CONFIG, "online_history_period", "3y"), interval=getattr(CONFIG, "online_history_interval", "1d"), progress=False, auto_adjust=False)
                if df is None or df.empty:
                    alt_symbol = ticker if "." in ticker else f"{ticker}.TWO"
                    df = yf.download(alt_symbol, period=getattr(CONFIG, "online_history_period", "3y"), interval=getattr(CONFIG, "online_history_interval", "1d"), progress=False, auto_adjust=False)
                if df is None or df.empty:
                    errors.append(f"{ticker}:empty_download")
                    continue
                if hasattr(df.columns, 'nlevels') and df.columns.nlevels > 1:
                    df.columns = [c[0] for c in df.columns]
                cols = [c for c in ["Open", "High", "Low", "Close", "Volume"] if c in df.columns]
                if len(cols) < 5:
                    errors.append(f"{ticker}:missing_ohlcv_cols")
                    continue
                out = df[cols].reset_index().rename(columns={df.index.name or 'Date': 'Date'})
                if 'Date' not in out.columns:
                    out = out.rename(columns={out.columns[0]: 'Date'})
                out['Date'] = pd.to_datetime(out['Date'], errors='coerce').dt.strftime('%Y-%m-%d')
                out = out.dropna(subset=['Date', 'Open', 'High', 'Low', 'Close'])
                if out.empty:
                    errors.append(f"{ticker}:normalized_empty")
                    continue
                out.to_csv(self.cache_dir / f"{ticker}_ohlcv.csv", index=False, encoding='utf-8-sig')
                fetched += 1
                written += 1
            except Exception as e:
                errors.append(f"{ticker}:{e}")
        result.update({
            "fetched": int(fetched),
            "written_files": int(written),
            "status": "ok" if written > 0 else "no_downloaded_history",
            "errors_preview": errors[:15],
        })
        return result



    def _write_last_snapshot(self) -> int:
        rows = []
        for p in sorted(self.cache_dir.glob('*_ohlcv.csv')):
            try:
                df = pd.read_csv(p, encoding='utf-8-sig')
            except Exception:
                try:
                    df = pd.read_csv(p)
                except Exception:
                    continue
            if df.empty or 'Close' not in df.columns:
                continue
            date_col = 'Date' if 'Date' in df.columns else df.columns[0]
            close = pd.to_numeric(df['Close'], errors='coerce')
            valid = df.loc[close > 0].copy()
            if valid.empty:
                continue
            last = valid.iloc[-1]
            ticker = p.stem.replace('_ohlcv', '')
            rows.append({
                'Ticker': ticker,
                'Reference_Price': float(last['Close']),
                'Source': f'kline_cache:{p.name}',
                'Source_Date': str(last.get(date_col, '')),
            })
        out = pd.DataFrame(rows, columns=['Ticker','Reference_Price','Source','Source_Date'])
        out.to_csv(self.snapshot_path, index=False, encoding='utf-8-sig')
        return int(len(out))

    def _write_recipe(self, universe: List[str], missing_tickers: List[str], online_result: Dict[str, Any]) -> Dict[str, Any]:
        recipe = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "mode": "local_first_optional_online_backfill",
            "what_it_means": {
                "local_history_bootstrap": "先掃描專案內已存在的 OHLCV / K 線 CSV，能吃就轉成 data/kline_cache/*.csv。",
                "missing_request_list": "如果專案內找不到某些標的的可用 K 線，就把缺的 ticker 列成請求清單，而不是假裝已完成。",
                "online_backfill": "只有當 allow_online_history_backfill=True 時，才會嘗試用 yfinance 補抓缺的 K 線。預設不開。",
            },
            "bootstrap_universe_count": int(len(universe)),
            "missing_ticker_count": int(len(missing_tickers)),
            "request_list_path": str(self.request_csv_path),
            "online_backfill": online_result,
        }
        self.recipe_path.write_text(json.dumps(recipe, ensure_ascii=False, indent=2), encoding='utf-8')
        return recipe

    def build(self):
        candidates = self._scan_sources()
        normalized_frames = []
        scanned = []
        for p in candidates:
            norm, info = self._normalize_history_file(p)
            scanned.append(info)
            if not norm.empty:
                normalized_frames.append(norm)

        if normalized_frames:
            merged = pd.concat(normalized_frames, ignore_index=True)
            merged = merged.sort_values(["Ticker", "Date"]).drop_duplicates(subset=["Ticker", "Date"], keep="last")
        else:
            merged = pd.DataFrame(columns=["Ticker", "Date", "Open", "High", "Low", "Close", "Volume"])

        cache_write = self._write_cache_files(merged)
        universe = sorted(set(self._load_bootstrap_universe()) | set(self._load_decision_tickers()))
        cache_tickers_before = sorted({p.stem.replace("_ohlcv", "") for p in self.cache_dir.glob("*_ohlcv.csv")})
        missing_before = [t for t in universe if t not in set(cache_tickers_before)]
        request_rows = self._write_request_list(missing_before)
        online_result = self._try_online_backfill(missing_before)
        cache_tickers_after = sorted({p.stem.replace("_ohlcv", "") for p in self.cache_dir.glob("*_ohlcv.csv")})
        missing_after = [t for t in universe if t not in set(cache_tickers_after)]
        self._write_request_list(missing_after)
        snapshot_rows = self._write_last_snapshot()
        recipe = self._write_recipe(universe, missing_after, online_result)

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "mode": recipe.get("mode"),
            "source_files_scanned": int(len(candidates)),
            "history_like_files_found": int(sum(1 for x in scanned if x.get("status") == "ok")),
            "history_rows_normalized": int(len(merged)),
            "cache_files_written_from_local": int(cache_write.get("written_files", 0)),
            "cache_ticker_count": int(len(cache_tickers_after)),
            "cache_tickers_preview": cache_tickers_after[:20],
            "bootstrap_universe_count": int(len(universe)),
            "missing_cache_ticker_count_before_online": int(len(missing_before)),
            "missing_cache_ticker_count": int(len(missing_after)),
            "request_list_rows": int(len(missing_after)),
            "request_list_path": str(self.request_csv_path),
            "cache_dir": str(self.cache_dir),
            "online_backfill": online_result,
            "last_price_snapshot_rows": int(snapshot_rows),
            "last_price_snapshot_path": str(self.snapshot_path),
            "status": "history_cache_ready" if len(cache_tickers_after) >= 5 else "waiting_for_ohlcv_history",
            "scan_diagnostics_preview": scanned[:20],
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"📚 Local history bootstrap：{payload['status']} | cache_tickers={payload['cache_ticker_count']} | online={online_result.get('status')}")
        return self.report_path, payload



def main():
    LocalHistoryBootstrap().build()


if __name__ == '__main__':
    main()
