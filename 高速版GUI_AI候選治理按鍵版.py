# -*- coding: utf-8 -*-
from __future__ import annotations
"""FTS 視窗化指令中心（高速版＋AI候選治理單檔整合）
同一支檔案同時包含：
1. 圖形化指令中心 GUI
2. 零值 / 空值 掃描與補值工具（高速版）

放在專案根目錄後執行：python fts_command_center_gui_all_in_one.py
"""


"""
fts_zero_null_sql_csv_repair.py

用途：
1. 掃描本地 CSV / SQL 中由「網路抓回來」的資料是否有 0 / 空值 / 空字串。
2. 再向網路權威來源確認該值到底是：
   - 真正就是 0 / 空值
   - 還是抓取時漏抓 / 解析失敗
3. 若確認是漏抓，則把正確值補回：
   - 本地 CSV
   - SQL Server
4. 輸出 runtime 修補報告 JSON，讓你知道：
   - 哪些欄位被補
   - 哪些欄位被確認為「真的就是 0」
   - 哪些欄位目前無法驗證

支援資料集：
- fundamentals  -> data/market_financials_backup_fullspeed.csv / dbo.fundamentals_clean
- revenue       -> data/monthly_revenue_simple.csv / dbo.monthly_revenue_simple
- chip          -> data/daily_chip_data_backup.csv / dbo.daily_chip_data

注意：
- 本工具預設 dry-run，不會真的改檔、不會寫 SQL。
- 要真的套用，請加 --apply
- revenue 的網路驗證目前以「最新可用月份」為主；舊月份若 OpenAPI 不再提供，會被標記成 unverifiable。
"""


import argparse
import json
import math
import os
import re
import shutil
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from io import StringIO
from pathlib import Path
from typing import Any, Callable

try:
    import pandas as pd
except Exception as exc:  # pragma: no cover
    raise RuntimeError("本工具需要 pandas，請先安裝 pandas") from exc

try:
    import requests
except Exception as exc:  # pragma: no cover
    raise RuntimeError("本工具需要 requests，請先安裝 requests") from exc

try:
    import pyodbc
except Exception:  # pragma: no cover
    pyodbc = None

try:
    import yfinance as yf
except Exception:  # pragma: no cover
    yf = None


# =========================================
# 共用工具
# =========================================

def now_ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def safe_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        s = value.strip().replace(",", "")
        if s in {"", "None", "null", "nan", "NaN", "-"}:
            return None
        value = s
    try:
        val = float(value)
    except Exception:
        return None
    if math.isnan(val) or math.isinf(val):
        return None
    return val


def is_blank(value: Any) -> bool:
    if value is None:
        return True
    if pd.isna(value):
        return True
    if isinstance(value, str) and value.strip() in {"", "None", "null", "nan", "NaN", "-"}:
        return True
    return False


def normalize_text(value: Any) -> str | None:
    if is_blank(value):
        return None
    return str(value).strip()


def normalize_date_like(value: Any) -> str | None:
    """
    正規化成：
    - YYYY-MM-DD
    - 或 YYYY-MM（給月營收月別用）
    """
    if is_blank(value):
        return None
    s = str(value).strip()

    # 先處理 yyyy-mm
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s

    # 先處理 date/datetime 物件
    if isinstance(value, (datetime, date)):
        return pd.to_datetime(value).strftime("%Y-%m-%d")

    # pandas parse
    try:
        dt = pd.to_datetime(s, errors="raise")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        pass

    # 例如 202602 -> 2026-02
    if re.fullmatch(r"\d{6}", s):
        year = int(s[:4])
        month = int(s[4:])
        if 1900 <= year <= 2999 and 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    # 民國年月 11502 -> 2026-02
    if re.fullmatch(r"\d{5}", s):
        roc_year = int(s[:3])
        month = int(s[3:])
        year = roc_year + 1911
        if 1 <= month <= 12:
            return f"{year:04d}-{month:02d}"

    return s


def normalize_ticker(value: Any) -> str | None:
    if is_blank(value):
        return None
    return str(value).strip().upper()


def normalize_stock_id_from_ticker(ticker: str) -> str | None:
    if not ticker:
        return None
    m = re.match(r"^(\d{4})", ticker.strip().upper())
    return m.group(1) if m else None


def coerce_csv_value(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return ""
        return round(value, 6)
    return value


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "Mozilla/5.0"})
    return session


SESSION = build_session()


def http_get_text(url: str, timeout: int = 20, encoding: str | None = None) -> str:
    last_err = None
    for verify in (True, False):
        try:
            res = SESSION.get(url, timeout=timeout, verify=verify)
            res.raise_for_status()
            if encoding:
                res.encoding = encoding
            return res.text
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"GET text failed: {url} | {last_err}")


def http_get_json(url: str, timeout: int = 20) -> Any:
    last_err = None
    for verify in (True, False):
        try:
            res = SESSION.get(url, timeout=timeout, verify=verify)
            res.raise_for_status()
            return res.json()
        except Exception as exc:
            last_err = exc
    raise RuntimeError(f"GET json failed: {url} | {last_err}")


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path


def backup_file(src: Path, backup_dir: Path) -> Path | None:
    if not src.exists():
        return None
    ensure_dir(backup_dir)
    target = backup_dir / f"{src.stem}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{src.suffix}"
    shutil.copy2(src, target)
    return target


def dedup_keep_order(items: list[Any]) -> list[Any]:
    seen = set()
    out = []
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


# =========================================
# 效能設定
# =========================================
REMOTE_FUNDAMENTALS_MAX_WORKERS = 6
REMOTE_CHIP_MAX_WORKERS = 6
SQL_UPSERT_BATCH_SIZE = 200


def chunked(items: list[Any], size: int) -> list[list[Any]]:
    if size <= 0:
        return [items]
    return [items[i:i + size] for i in range(0, len(items), size)]


# =========================================
# SQL / 路徑
# =========================================

def load_optional_config(base_dir: Path):
    sys.path.insert(0, str(base_dir))
    try:
        import config  # type: ignore
        return config
    except Exception:
        return None
    finally:
        try:
            sys.path.pop(0)
        except Exception:
            pass


def build_conn_str(config_module=None) -> str:
    driver = getattr(config_module, "db_driver", "ODBC Driver 17 for SQL Server") if config_module else "ODBC Driver 17 for SQL Server"
    server = getattr(config_module, "db_server", "localhost") if config_module else "localhost"
    database = getattr(config_module, "db_database", "股票Online") if config_module else "股票Online"
    return (
        rf"DRIVER={{{driver}}};"
        rf"SERVER={server};"
        rf"DATABASE={database};"
        r"Trusted_Connection=yes;"
    )


def read_csv_flexible(path: Path) -> pd.DataFrame:
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    return pd.read_csv(path)


# =========================================
# Dataset 規格
# =========================================

@dataclass
class DatasetSpec:
    name: str
    table_name: str
    csv_candidates: list[str]
    key_cols: list[str]
    numeric_scan_cols: list[str] = field(default_factory=list)
    text_scan_cols: list[str] = field(default_factory=list)
    all_columns: list[str] = field(default_factory=list)
    normalize_key: Callable[[tuple[Any, ...]], tuple[str, ...]] | None = None


def revenue_key_normalizer(key: tuple[Any, ...]) -> tuple[str, ...]:
    ticker = normalize_ticker(key[0]) or ""
    ym = normalize_date_like(key[1]) or ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", ym):
        ym = ym[:7]
    return (ticker, ym)


def date_key_normalizer(key: tuple[Any, ...]) -> tuple[str, ...]:
    return tuple(normalize_ticker(v) if i == 1 and "TW" in str(v).upper() else normalize_date_like(v) if i == 0 else normalize_ticker(v) for i, v in enumerate(key))


DATASETS: dict[str, DatasetSpec] = {
    "fundamentals": DatasetSpec(
        name="fundamentals",
        table_name="dbo.fundamentals_clean",
        csv_candidates=[
            "data/market_financials_backup_fullspeed.csv",
            "market_financials_backup_fullspeed.csv",
            "seed_data/market_financials_backup_fullspeed.csv",
        ],
        key_cols=["Ticker SYMBOL", "資料年月日"],
        numeric_scan_cols=[
            "毛利率(%)",
            "營業利益率(%)",
            "單季EPS",
            "ROE(%)",
            "稅後淨利率(%)",
            "營業現金流",
            "預估殖利率(%)",
            "負債比率(%)",
            "本業獲利比(%)",
        ],
        all_columns=[
            "Ticker SYMBOL", "資料年月日",
            "毛利率(%)", "營業利益率(%)", "單季EPS", "ROE(%)",
            "稅後淨利率(%)", "營業現金流", "預估殖利率(%)",
            "負債比率(%)", "本業獲利比(%)"
        ],
        normalize_key=lambda key: (normalize_ticker(key[0]) or "", normalize_date_like(key[1]) or ""),
    ),
    "revenue": DatasetSpec(
        name="revenue",
        table_name="dbo.monthly_revenue_simple",
        csv_candidates=[
            "data/monthly_revenue_simple.csv",
            "monthly_revenue_simple.csv",
            "seed_data/monthly_revenue_simple.csv",
        ],
        key_cols=["Ticker SYMBOL", "資料年月日"],
        numeric_scan_cols=["單月營收年增率(%)"],
        text_scan_cols=["公司名稱", "產業類別", "產業類別名稱"],
        all_columns=[
            "Ticker SYMBOL", "公司名稱", "產業類別", "產業類別名稱", "資料年月日", "單月營收年增率(%)"
        ],
        normalize_key=revenue_key_normalizer,
    ),
    "chip": DatasetSpec(
        name="chip",
        table_name="dbo.daily_chip_data",
        csv_candidates=[
            "data/daily_chip_data_backup.csv",
            "daily_chip_data_backup.csv",
            "seed_data/daily_chip_data_backup.csv",
        ],
        key_cols=["日期", "Ticker SYMBOL"],
        numeric_scan_cols=["外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計"],
        all_columns=["日期", "Ticker SYMBOL", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計", "資料來源"],
        normalize_key=lambda key: (normalize_date_like(key[0]) or "", normalize_ticker(key[1]) or ""),
    ),
}


# =========================================
# 遠端抓取：fundamentals
# =========================================

FUNDAMENTALS_REMOTE_COLUMNS = [
    "Ticker SYMBOL", "資料年月日",
    "毛利率(%)", "營業利益率(%)", "單季EPS", "ROE(%)",
    "稅後淨利率(%)", "營業現金流", "預估殖利率(%)",
    "負債比率(%)", "本業獲利比(%)",
]


def yfin_value(df: pd.DataFrame | None, key: str, date_col: Any) -> Any:
    if df is None or df.empty:
        return None
    if key not in df.index:
        return None
    if date_col not in df.columns:
        return None
    val = df.loc[key, date_col]
    return val if pd.notna(val) else None




def fetch_fundamentals_single_ticker(ticker_symbol: str, reports_per_ticker: int = 8) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if yf is None:
        return result

    ticker_symbol = normalize_ticker(ticker_symbol) or ""
    if not ticker_symbol:
        return result

    try:
        tk = yf.Ticker(ticker_symbol)
        is_df = tk.quarterly_financials
        bs_df = tk.quarterly_balance_sheet
        cf_df = tk.quarterly_cashflow
        if is_df is None or is_df.empty:
            return result

        div_yield = None
        try:
            info = getattr(tk, "info", {}) or {}
            raw = info.get("dividendYield")
            div_yield = round(float(raw) * 100, 2) if raw is not None else None
        except Exception:
            div_yield = None

        for date_col in list(is_df.columns)[:reports_per_ticker]:
            try:
                report_date = pd.to_datetime(date_col).strftime("%Y-%m-%d")
            except Exception:
                continue

            revenue = yfin_value(is_df, "Total Revenue", date_col)
            gross_profit = yfin_value(is_df, "Gross Profit", date_col)
            op_income = yfin_value(is_df, "Operating Income", date_col)
            pre_tax = yfin_value(is_df, "Pretax Income", date_col)
            net_income = yfin_value(is_df, "Net Income", date_col)
            eps = yfin_value(is_df, "Diluted EPS", date_col)
            assets = yfin_value(bs_df, "Total Assets", date_col)
            liabilities = yfin_value(bs_df, "Total Liabilities Net Minority Interest", date_col)
            equity = yfin_value(bs_df, "Total Equity Gross Minority Interest", date_col)
            cash_flow = yfin_value(cf_df, "Operating Cash Flow", date_col)

            gross_margin = (gross_profit / revenue * 100) if revenue not in [None, 0] and gross_profit is not None else None
            op_margin = (op_income / revenue * 100) if revenue not in [None, 0] and op_income is not None else None
            net_margin = (net_income / revenue * 100) if revenue not in [None, 0] and net_income is not None else None
            roe = (net_income / equity * 100) if equity not in [None, 0] and net_income is not None else None
            debt_ratio = (liabilities / assets * 100) if assets not in [None, 0] and liabilities is not None else None
            core_profit_ratio = (op_income / pre_tax * 100) if pre_tax not in [None, 0] and op_income is not None else None

            result[(ticker_symbol, report_date)] = {
                "Ticker SYMBOL": ticker_symbol,
                "資料年月日": report_date,
                "毛利率(%)": safe_float(gross_margin),
                "營業利益率(%)": safe_float(op_margin),
                "單季EPS": safe_float(eps),
                "ROE(%)": safe_float(roe),
                "稅後淨利率(%)": safe_float(net_margin),
                "營業現金流": safe_float(cash_flow),
                "預估殖利率(%)": safe_float(div_yield),
                "負債比率(%)": safe_float(debt_ratio),
                "本業獲利比(%)": safe_float(core_profit_ratio),
            }
    except Exception:
        return result
    return result


def fetch_fundamentals_remote(
    tickers: list[str],
    reports_per_ticker: int = 8,
    max_workers: int = REMOTE_FUNDAMENTALS_MAX_WORKERS,
) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if yf is None:
        return result

    unique_tickers = dedup_keep_order([normalize_ticker(x) for x in tickers if normalize_ticker(x)])
    if not unique_tickers:
        return result

    workers = max(1, min(max_workers, len(unique_tickers)))
    if workers <= 1:
        for ticker_symbol in unique_tickers:
            result.update(fetch_fundamentals_single_ticker(ticker_symbol, reports_per_ticker=reports_per_ticker))
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(fetch_fundamentals_single_ticker, ticker_symbol, reports_per_ticker): ticker_symbol
            for ticker_symbol in unique_tickers
        }
        for future in as_completed(futures):
            try:
                result.update(future.result() or {})
            except Exception:
                continue
    return result


# =========================================
# 遠端抓取：revenue
# =========================================

REVENUE_STOCK_CSV_URL = "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"
REVENUE_API_URL = "https://openapi.twse.com.tw/v1/opendata/t187ap05_L"

REVENUE_INDUSTRY_MAP = {
    "01": "水泥工業", "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維",
    "05": "電機機械", "06": "電器電纜", "08": "玻璃陶瓷", "09": "造紙工業",
    "10": "鋼鐵工業", "11": "橡膠工業", "12": "汽車工業", "13": "電子工業",
    "14": "建材營造業", "15": "航運業", "16": "觀光餐旅", "17": "金融保險",
    "18": "貿易百貨", "19": "綜合", "20": "其他", "21": "化學工業",
    "22": "生技醫療業", "23": "油電燃氣業", "24": "半導體業", "25": "電腦及週邊設備業",
    "26": "光電業", "27": "通信網路業", "28": "電子零組件業", "29": "電子通路業",
    "30": "資訊服務業", "31": "其他電子業", "32": "文化創意業", "33": "農業科技業",
    "80": "管理股票", "91": "存託憑證",
}


def revenue_normalize_code(x: Any) -> str:
    s = str(x).strip().replace(".0", "")
    s = "".join(ch for ch in s if ch.isdigit())
    return s


def revenue_find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    cols = {str(c).strip(): c for c in df.columns}
    for cand in candidates:
        if cand in cols:
            return cols[cand]
    return None


def revenue_get_stock_master() -> pd.DataFrame:
    # 先抓 listed 主檔；這裡跟你現有月營收主線風格保持一致
    for enc in ("utf-8-sig", "utf-8", "cp950", "big5"):
        try:
            text = http_get_text(REVENUE_STOCK_CSV_URL, encoding=enc)
            df = pd.read_csv(StringIO(text))
            if not df.empty:
                df.columns = [str(c).strip() for c in df.columns]
                break
        except Exception:
            df = pd.DataFrame()
    if df.empty:
        return pd.DataFrame(columns=["Ticker SYMBOL", "公司名稱", "產業類別", "產業類別名稱"])

    code_col = revenue_find_col(df, ["公司代號", "代號"])
    name_col = revenue_find_col(df, ["公司名稱", "名稱", "公司簡稱"])
    industry_col = revenue_find_col(df, ["產業別"])

    if not code_col:
        return pd.DataFrame(columns=["Ticker SYMBOL", "公司名稱", "產業類別", "產業類別名稱"])

    out = pd.DataFrame()
    out["stock_id"] = df[code_col].apply(revenue_normalize_code)
    out = out[out["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
    out["Ticker SYMBOL"] = out["stock_id"] + ".TW"
    out["公司名稱"] = df.loc[out.index, name_col].astype(str).str.strip() if name_col else None
    out["產業類別"] = df.loc[out.index, industry_col].astype(str).str.strip() if industry_col else None
    out.loc[out["公司名稱"].isin(["", "nan", "None"]), "公司名稱"] = None
    out.loc[out["產業類別"].isin(["", "nan", "None"]), "產業類別"] = None
    out["產業類別名稱"] = out["產業類別"].apply(lambda x: REVENUE_INDUSTRY_MAP.get(str(x).strip(), None) if not is_blank(x) else None)
    return out[["stock_id", "Ticker SYMBOL", "公司名稱", "產業類別", "產業類別名稱"]].drop_duplicates(subset=["Ticker SYMBOL"]).reset_index(drop=True)


def revenue_parse_ym(x: Any) -> str | None:
    s = normalize_date_like(x)
    if s is None:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        return s[:7]
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    return None


def fetch_revenue_remote_latest() -> dict[tuple[str, str], dict[str, Any]]:
    raw = http_get_json(REVENUE_API_URL)
    df = pd.DataFrame(raw)
    if df.empty:
        return {}

    df.columns = [str(c).strip() for c in df.columns]
    code_col = revenue_find_col(df, ["公司代號", "代號"])
    ym_col = revenue_find_col(df, ["資料年月"])
    yoy_col = revenue_find_col(df, ["營業收入-去年同月增減(%)", "去年同月增減(%)", "與去年同期增減(%)"])
    rev_col = revenue_find_col(df, ["營業收入-當月營收", "當月營收"])
    prev_col = revenue_find_col(df, ["營業收入-去年當月營收", "去年當月營收"])

    if not code_col or not ym_col:
        return {}

    temp = df.copy()
    temp["stock_id"] = temp[code_col].apply(revenue_normalize_code)
    temp = temp[temp["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
    temp["ym"] = temp[ym_col].apply(revenue_parse_ym)

    yoy_values = []
    for _, row in temp.iterrows():
        yoy = None
        if yoy_col:
            yoy = safe_float(row.get(yoy_col))
        if yoy is None and rev_col and prev_col:
            rev = safe_float(row.get(rev_col))
            prev = safe_float(row.get(prev_col))
            if rev is not None and prev not in (None, 0):
                yoy = round(((rev - prev) / prev) * 100, 2)
        yoy_values.append(None if yoy is None else round(yoy, 2))
    temp["單月營收年增率(%)"] = yoy_values
    temp = temp[temp["ym"].notna()].copy()
    if temp.empty:
        return {}

    latest_ym = sorted(temp["ym"].dropna().unique())[-1]
    temp = temp[temp["ym"] == latest_ym].copy()

    stock_master = revenue_get_stock_master()
    temp = temp.merge(stock_master, on="stock_id", how="left")

    result: dict[tuple[str, str], dict[str, Any]] = {}
    for _, row in temp.iterrows():
        ticker = normalize_ticker(row.get("Ticker SYMBOL"))
        ym = normalize_text(row.get("ym"))
        if not ticker or not ym:
            continue
        result[(ticker, ym)] = {
            "Ticker SYMBOL": ticker,
            "公司名稱": normalize_text(row.get("公司名稱")),
            "產業類別": normalize_text(row.get("產業類別")),
            "產業類別名稱": normalize_text(row.get("產業類別名稱")),
            "資料年月日": ym,
            "單月營收年增率(%)": safe_float(row.get("單月營收年增率(%)")),
        }
    return result


# =========================================
# 遠端抓取：chip（官方 CSV 備援）
# =========================================

TW_HOLIDAYS = {
    "2026-01-01", "2026-02-16", "2026-02-17", "2026-02-18", "2026-02-19", "2026-02-20",
    "2026-02-27", "2026-04-03", "2026-04-06", "2026-05-01", "2026-06-19", "2026-09-25", "2026-10-09",
}


def is_tw_trading_day(day_str: str) -> bool:
    try:
        d = datetime.strptime(day_str, "%Y-%m-%d").date()
    except Exception:
        return False
    return d.weekday() < 5 and day_str not in TW_HOLIDAYS


def roc_year(ad_year: int) -> int:
    return ad_year - 1911


def try_parse_csv_text_to_df(text: str) -> pd.DataFrame:
    for skiprows in range(0, 6):
        try:
            df = pd.read_csv(StringIO(text), skiprows=skiprows)
            if not df.empty and len(df.columns) >= 2:
                df = df.dropna(how="all")
                return df
        except Exception:
            continue
    return pd.DataFrame()


def parse_twse_backup_csv(date_obj: date) -> pd.DataFrame:
    if not is_tw_trading_day(date_obj.strftime("%Y-%m-%d")):
        return pd.DataFrame()

    year = str(date_obj.year)
    month = str(date_obj.month).zfill(2)
    day = str(date_obj.day).zfill(2)

    url = f"https://www.twse.com.tw/rwd/zh/fund/T86?date={year}{month}{day}&selectType=ALLBUT0999&response=csv"
    try:
        text = http_get_text(url, timeout=20, encoding="utf-8")
        if "沒有符合條件的資料" in text:
            return pd.DataFrame()

        df = try_parse_csv_text_to_df(text)
        if df.empty:
            return pd.DataFrame()

        df.columns = [str(c).strip().replace(" ", "") for c in df.columns]

        stock_col = None
        for cand in ["證券代號", "代號", "股票代號"]:
            if cand in df.columns:
                stock_col = cand
                break
        if stock_col is None:
            return pd.DataFrame()

        col_foreign = None
        col_trust = None
        col_dealer = None

        for c in df.columns:
            s = str(c)
            if "外陸資買賣超股數(不含外資自營商)" in s or "外資及陸資買賣超股數(不含外資自營商)" in s:
                col_foreign = c
            elif "投信買賣超股數" in s:
                col_trust = c
            elif "自營商買賣超股數(自行買賣+避險)" in s or "自營商買賣超股數" in s:
                col_dealer = c

        if not all([col_foreign, col_trust, col_dealer]):
            return pd.DataFrame()

        result = df[[stock_col, col_foreign, col_trust, col_dealer]].copy()
        result.columns = ["stock_id", "外資買賣超", "投信買賣超", "自營商買賣超"]

        for col in ["外資買賣超", "投信買賣超", "自營商買賣超"]:
            result[col] = (
                result[col].astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
            )
            result[col] = pd.to_numeric(result[col], errors="coerce")

        result["stock_id"] = result["stock_id"].astype(str).str.strip()
        result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
        result["日期"] = date_obj.strftime("%Y-%m-%d")
        result["Ticker SYMBOL"] = result["stock_id"] + ".TW"
        result["三大法人合計"] = result[["外資買賣超", "投信買賣超", "自營商買賣超"]].sum(axis=1, min_count=1)
        result["資料來源"] = "OFFICIAL_TWSE"
        return result[["日期", "Ticker SYMBOL", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計", "資料來源"]]
    except Exception:
        return pd.DataFrame()


def parse_tpex_backup_csv(date_obj: date) -> pd.DataFrame:
    if not is_tw_trading_day(date_obj.strftime("%Y-%m-%d")):
        return pd.DataFrame()

    roc_y = roc_year(date_obj.year)
    roc_m = str(date_obj.month).zfill(2)
    roc_d = str(date_obj.day).zfill(2)

    urls = [
        f"https://www.tpex.org.tw/www/zh-tw/three-major-institutions/afterTrading/dailyTrading?date={roc_y}/{roc_m}/{roc_d}&id=&response=csv",
        f"https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php?l=zh-tw&o=csv&d={roc_y}/{roc_m}/{roc_d}&se=EW",
    ]

    for url in urls:
        try:
            text = http_get_text(url, timeout=20, encoding="utf-8")
            if "查無資料" in text or "沒有符合條件的資料" in text:
                continue

            df = try_parse_csv_text_to_df(text)
            if df.empty:
                continue

            df.columns = [str(c).strip().replace(" ", "") for c in df.columns]

            stock_col = None
            for cand in ["代號", "證券代號", "股票代號"]:
                if cand in df.columns:
                    stock_col = cand
                    break
            if stock_col is None:
                continue

            col_foreign = None
            col_trust = None
            col_dealer = None
            for c in df.columns:
                s = str(c)
                if "外資及陸資買賣超股數" in s or "外陸資買賣超股數" in s:
                    col_foreign = c
                elif "投信買賣超股數" in s:
                    col_trust = c
                elif "自營商買賣超股數" in s:
                    col_dealer = c

            if not all([col_foreign, col_trust, col_dealer]):
                continue

            result = df[[stock_col, col_foreign, col_trust, col_dealer]].copy()
            result.columns = ["stock_id", "外資買賣超", "投信買賣超", "自營商買賣超"]

            for col in ["外資買賣超", "投信買賣超", "自營商買賣超"]:
                result[col] = (
                    result[col].astype(str).str.replace(",", "", regex=False).str.replace(" ", "", regex=False)
                )
                result[col] = pd.to_numeric(result[col], errors="coerce")

            result["stock_id"] = result["stock_id"].astype(str).str.strip()
            result = result[result["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()
            result["日期"] = date_obj.strftime("%Y-%m-%d")
            result["Ticker SYMBOL"] = result["stock_id"] + ".TWO"
            result["三大法人合計"] = result[["外資買賣超", "投信買賣超", "自營商買賣超"]].sum(axis=1, min_count=1)
            result["資料來源"] = "OFFICIAL_TPEX"
            return result[["日期", "Ticker SYMBOL", "外資買賣超", "投信買賣超", "自營商買賣超", "三大法人合計", "資料來源"]]
        except Exception:
            continue

    return pd.DataFrame()




def fetch_chip_remote_one_date(ds: str) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    ds = normalize_date_like(ds) or ""
    if not ds or not is_tw_trading_day(ds):
        return result
    try:
        d = datetime.strptime(ds, "%Y-%m-%d").date()
    except Exception:
        return result

    try:
        df_all = pd.concat([parse_twse_backup_csv(d), parse_tpex_backup_csv(d)], ignore_index=True)
    except Exception:
        return result
    if df_all.empty:
        return result

    for _, row in df_all.iterrows():
        ticker = normalize_ticker(row.get("Ticker SYMBOL"))
        ymd = normalize_date_like(row.get("日期"))
        if not ticker or not ymd:
            continue
        result[(ymd, ticker)] = {
            "日期": ymd,
            "Ticker SYMBOL": ticker,
            "外資買賣超": safe_float(row.get("外資買賣超")),
            "投信買賣超": safe_float(row.get("投信買賣超")),
            "自營商買賣超": safe_float(row.get("自營商買賣超")),
            "三大法人合計": safe_float(row.get("三大法人合計")),
            "資料來源": normalize_text(row.get("資料來源")) or "OFFICIAL",
        }
    return result


def fetch_chip_remote_for_dates(
    date_list: list[str],
    max_workers: int = REMOTE_CHIP_MAX_WORKERS,
) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    unique_dates = dedup_keep_order(sorted([d for d in date_list if normalize_date_like(d)]))
    if not unique_dates:
        return result

    workers = max(1, min(max_workers, len(unique_dates)))
    if workers <= 1:
        for ds in unique_dates:
            result.update(fetch_chip_remote_one_date(ds))
        return result

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(fetch_chip_remote_one_date, ds): ds for ds in unique_dates}
        for future in as_completed(futures):
            try:
                result.update(future.result() or {})
            except Exception:
                continue
    return result


# =========================================
# 掃描 / 修補主體
# =========================================

class ZeroNullRepairTool:
    def __init__(self, base_dir: Path, apply_changes: bool, datasets: list[str]):
        self.base_dir = base_dir
        self.apply_changes = apply_changes
        self.datasets = datasets
        self.config = load_optional_config(base_dir)
        self.conn_str = build_conn_str(self.config)
        self.runtime_dir = ensure_dir(base_dir / "runtime")
        self.backup_dir = ensure_dir(self.runtime_dir / "zero_null_repair_backups")
        self.report_path = self.runtime_dir / "zero_null_repair_report.json"

    # ---------- path / load ----------
    def resolve_csv_path(self, spec: DatasetSpec) -> Path | None:
        for rel in spec.csv_candidates:
            path = self.base_dir / rel
            if path.exists():
                return path
        return None

    def load_csv_df(self, spec: DatasetSpec) -> pd.DataFrame:
        csv_path = self.resolve_csv_path(spec)
        if csv_path is None or not csv_path.exists():
            return pd.DataFrame(columns=spec.all_columns)
        df = read_csv_flexible(csv_path)
        for col in spec.all_columns:
            if col not in df.columns:
                df[col] = None
        return df[spec.all_columns].copy()

    def load_sql_df(self, spec: DatasetSpec) -> pd.DataFrame:
        if pyodbc is None:
            return pd.DataFrame(columns=spec.all_columns)
        try:
            with pyodbc.connect(self.conn_str) as conn:
                sql = "SELECT " + ", ".join(f"[{c}]" for c in spec.all_columns) + f" FROM {spec.table_name}"
                return pd.read_sql(sql, conn)
        except Exception:
            return pd.DataFrame(columns=spec.all_columns)

    # ---------- normalize key ----------
    def key_tuple(self, spec: DatasetSpec, row: pd.Series | dict[str, Any]) -> tuple[str, ...]:
        raw = tuple((row.get(col) if isinstance(row, dict) else row[col]) for col in spec.key_cols)
        if spec.normalize_key:
            return spec.normalize_key(raw)
        return tuple(normalize_text(v) or "" for v in raw)



    def build_normalized_key_tuples(self, spec: DatasetSpec, df: pd.DataFrame) -> list[tuple[str, ...]]:
        if df.empty:
            return []

        normalized_parts: list[pd.Series] = []
        for i, col in enumerate(spec.key_cols):
            s = df[col] if col in df.columns else pd.Series([None] * len(df), index=df.index)
            if spec.name == "fundamentals":
                norm = s.apply(normalize_ticker if i == 0 else normalize_date_like)
            elif spec.name == "revenue":
                if i == 0:
                    norm = s.apply(normalize_ticker)
                else:
                    norm = s.apply(normalize_date_like).apply(
                        lambda x: x[:7] if isinstance(x, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", x) else x
                    )
            elif spec.name == "chip":
                norm = s.apply(normalize_date_like if i == 0 else normalize_ticker)
            else:
                norm = s.apply(normalize_text)
            normalized_parts.append(norm.fillna(""))

        return list(zip(*[part.tolist() for part in normalized_parts]))


    def build_index_map(self, spec: DatasetSpec, df: pd.DataFrame) -> dict[tuple[str, ...], int]:
        if df.empty:
            return {}
        keys = self.build_normalized_key_tuples(spec, df)
        return {tuple(key): idx for idx, key in zip(df.index.tolist(), keys)}

    # ---------- suspicious scan ----------
    def is_suspicious_numeric(self, value: Any) -> bool:
        if is_blank(value):
            return True
        f = safe_float(value)
        if f is None:
            return True
        return abs(f) < 1e-12


    def is_suspicious_text(self, value: Any) -> bool:
        return is_blank(value)


    def scan_suspicious(self, spec: DatasetSpec, df: pd.DataFrame, source_name: str) -> dict[tuple[tuple[str, ...], str], dict[str, Any]]:
        out: dict[tuple[tuple[str, ...], str], dict[str, Any]] = {}
        if df.empty:
            return out

        key_tuples = self.build_normalized_key_tuples(spec, df)
        position_map = {idx: pos for pos, idx in enumerate(df.index.tolist())}
        blank_tokens = {"", "None", "null", "nan", "NaN", "-"}

        for col in spec.numeric_scan_cols:
            if col not in df.columns:
                continue
            raw = df[col]
            raw_str = raw.astype(str).str.strip()
            blank_mask = raw.isna() | raw_str.isin(blank_tokens)
            numeric = pd.to_numeric(raw_str.str.replace(",", "", regex=False), errors="coerce")
            suspicious_mask = blank_mask | numeric.isna() | numeric.abs().lt(1e-12)
            for idx in suspicious_mask[suspicious_mask].index.tolist():
                pos = position_map[idx]
                value = raw.loc[idx]
                out[(key_tuples[pos], col)] = {
                    "dataset": spec.name,
                    "key": list(key_tuples[pos]),
                    "column": col,
                    "source": source_name,
                    "local_value": None if blank_mask.loc[idx] else safe_float(value),
                    "reason": "blank_or_zero",
                }

        for col in spec.text_scan_cols:
            if col not in df.columns:
                continue
            raw = df[col]
            raw_str = raw.astype(str).str.strip()
            suspicious_mask = raw.isna() | raw_str.isin(blank_tokens)
            for idx in suspicious_mask[suspicious_mask].index.tolist():
                pos = position_map[idx]
                value = raw.loc[idx]
                out[(key_tuples[pos], col)] = {
                    "dataset": spec.name,
                    "key": list(key_tuples[pos]),
                    "column": col,
                    "source": source_name,
                    "local_value": None if is_blank(value) else normalize_text(value),
                    "reason": "blank_text",
                }
        return out

    # ---------- remote fetch per dataset ----------
    def fetch_remote_rows(self, spec: DatasetSpec, candidate_keys: list[tuple[str, ...]]) -> dict[tuple[str, ...], dict[str, Any]]:
        if spec.name == "fundamentals":
            tickers = [key[0] for key in candidate_keys]
            raw = fetch_fundamentals_remote(tickers=tickers, reports_per_ticker=8, max_workers=REMOTE_FUNDAMENTALS_MAX_WORKERS)
            return {tuple(k): v for k, v in raw.items()}
        if spec.name == "revenue":
            raw = fetch_revenue_remote_latest()
            return {tuple(k): v for k, v in raw.items()}
        if spec.name == "chip":
            dates = [key[0] for key in candidate_keys]
            raw = fetch_chip_remote_for_dates(dates, max_workers=REMOTE_CHIP_MAX_WORKERS)
            return {tuple(k): v for k, v in raw.items()}
        return {}

    # ---------- compare ----------
    def remote_value_state(self, value: Any) -> str:
        if is_blank(value):
            return "blank"
        f = safe_float(value)
        if f is not None:
            if abs(f) < 1e-12:
                return "zero"
            return "valid_numeric"
        return "valid_text"

    def compare_and_build_patches(
        self,
        spec: DatasetSpec,
        candidates: dict[tuple[tuple[str, ...], str], dict[str, Any]],
        remote_rows: dict[tuple[str, ...], dict[str, Any]],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
        patches: list[dict[str, Any]] = []
        true_zero_or_blank: list[dict[str, Any]] = []
        unverifiable: list[dict[str, Any]] = []

        for (key, col), item in candidates.items():
            remote_row = remote_rows.get(tuple(key))
            if not remote_row:
                unverifiable.append({
                    **item,
                    "status": "remote_row_not_found",
                })
                continue

            remote_value = remote_row.get(col)
            rv_state = self.remote_value_state(remote_value)
            if rv_state in {"blank", "zero"}:
                true_zero_or_blank.append({
                    **item,
                    "status": "confirmed_true_blank_or_zero",
                    "remote_value": remote_value,
                })
                continue

            patch = {
                **item,
                "status": "patchable",
                "remote_value": remote_value,
                "authoritative_row": remote_row,
            }
            patches.append(patch)

        return patches, true_zero_or_blank, unverifiable

    # ---------- patch memory df ----------


    def apply_patches_to_df(self, spec: DatasetSpec, df: pd.DataFrame, patches: list[dict[str, Any]]) -> pd.DataFrame:
        if df.empty:
            df = pd.DataFrame(columns=spec.all_columns)

        index_map = self.build_index_map(spec, df)
        append_rows: list[dict[str, Any]] = []

        for patch in patches:
            key = tuple(patch["key"])
            auth_row = patch["authoritative_row"]
            if key in index_map:
                idx = index_map[key]
                for col in spec.all_columns:
                    if col in auth_row and col in df.columns:
                        df.at[idx, col] = auth_row.get(col)
            else:
                append_rows.append({col: auth_row.get(col) for col in spec.all_columns})

        if append_rows:
            df = pd.concat([df, pd.DataFrame(append_rows)], ignore_index=True)
        return df

    # ---------- SQL upsert ----------


    def upsert_sql_rows(self, spec: DatasetSpec, authoritative_rows: list[dict[str, Any]]) -> int:
        if pyodbc is None or not authoritative_rows:
            return 0

        statements: list[tuple[Any, ...]] = []
        sql = ""

        if spec.name == "fundamentals":
            sql = f"""
            IF EXISTS (SELECT 1 FROM {spec.table_name} WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?)
            BEGIN
                UPDATE {spec.table_name}
                SET [毛利率(%)] = ?, [營業利益率(%)] = ?, [單季EPS] = ?, [ROE(%)] = ?,
                    [稅後淨利率(%)] = ?, [營業現金流] = ?, [預估殖利率(%)] = ?, [負債比率(%)] = ?,
                    [本業獲利比(%)] = ?, [更新時間] = GETDATE()
                WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?
            END
            ELSE
            BEGIN
                INSERT INTO {spec.table_name}
                ([Ticker SYMBOL], [資料年月日], [毛利率(%)], [營業利益率(%)], [單季EPS], [ROE(%)],
                 [稅後淨利率(%)], [營業現金流], [預估殖利率(%)], [負債比率(%)], [本業獲利比(%)], [更新時間])
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
            END
            """
            for row in authoritative_rows:
                ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                report_date = normalize_date_like(row.get("資料年月日"))
                gm = safe_float(row.get("毛利率(%)"))
                opm = safe_float(row.get("營業利益率(%)"))
                eps = safe_float(row.get("單季EPS"))
                roe = safe_float(row.get("ROE(%)"))
                nm = safe_float(row.get("稅後淨利率(%)"))
                ocf = safe_float(row.get("營業現金流"))
                dy = safe_float(row.get("預估殖利率(%)"))
                dr = safe_float(row.get("負債比率(%)"))
                cpr = safe_float(row.get("本業獲利比(%)"))
                statements.append((ticker, report_date, gm, opm, eps, roe, nm, ocf, dy, dr, cpr, ticker, report_date, ticker, report_date, gm, opm, eps, roe, nm, ocf, dy, dr, cpr))
        elif spec.name == "revenue":
            sql = f"""
            IF EXISTS (
                SELECT 1 FROM {spec.table_name}
                WHERE [Ticker SYMBOL] = ?
                  AND CONVERT(NVARCHAR(7), [資料年月日], 120) = ?
            )
            BEGIN
                UPDATE {spec.table_name}
                SET [公司名稱] = ?, [產業類別] = ?, [產業類別名稱] = ?, [單月營收年增率(%)] = ?, [更新時間] = GETDATE()
                WHERE [Ticker SYMBOL] = ?
                  AND CONVERT(NVARCHAR(7), [資料年月日], 120) = ?
            END
            ELSE
            BEGIN
                INSERT INTO {spec.table_name}
                ([Ticker SYMBOL], [公司名稱], [產業類別], [產業類別名稱], [資料年月日], [單月營收年增率(%)], [更新時間])
                VALUES (?, ?, ?, ?, ?, ?, GETDATE())
            END
            """
            for row in authoritative_rows:
                ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                ym = normalize_date_like(row.get("資料年月日")) or ""
                sql_date = ym + "-01" if re.fullmatch(r"\d{4}-\d{2}", ym) else ym
                company_name = normalize_text(row.get("公司名稱"))
                industry = normalize_text(row.get("產業類別"))
                industry_name = normalize_text(row.get("產業類別名稱"))
                yoy = safe_float(row.get("單月營收年增率(%)"))
                statements.append((ticker, ym, company_name, industry, industry_name, yoy, ticker, ym, ticker, company_name, industry, industry_name, sql_date, yoy))
        elif spec.name == "chip":
            sql = f"""
            IF EXISTS (
                SELECT 1 FROM {spec.table_name}
                WHERE [日期] = ? AND [Ticker SYMBOL] = ?
            )
            BEGIN
                UPDATE {spec.table_name}
                SET [外資買賣超] = ?, [投信買賣超] = ?, [自營商買賣超] = ?, [三大法人合計] = ?, [資料來源] = ?, [更新時間] = GETDATE()
                WHERE [日期] = ? AND [Ticker SYMBOL] = ?
            END
            ELSE
            BEGIN
                INSERT INTO {spec.table_name}
                ([日期], [Ticker SYMBOL], [外資買賣超], [投信買賣超], [自營商買賣超], [三大法人合計], [資料來源], [更新時間])
                VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE())
            END
            """
            for row in authoritative_rows:
                ds = normalize_date_like(row.get("日期"))
                ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                foreign = safe_float(row.get("外資買賣超"))
                trust = safe_float(row.get("投信買賣超"))
                dealer = safe_float(row.get("自營商買賣超"))
                total = safe_float(row.get("三大法人合計"))
                source = normalize_text(row.get("資料來源"))
                statements.append((ds, ticker, foreign, trust, dealer, total, source, ds, ticker, ds, ticker, foreign, trust, dealer, total, source))

        if not sql or not statements:
            return 0

        count = 0
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            try:
                cursor.fast_executemany = True
            except Exception:
                pass
            try:
                for batch in chunked(statements, SQL_UPSERT_BATCH_SIZE):
                    cursor.executemany(sql, batch)
                    count += len(batch)
                conn.commit()
                return count
            except Exception:
                conn.rollback()
                count = 0
                for params in statements:
                    cursor.execute(sql, params)
                    count += 1
                conn.commit()
                return count

    # ---------- main per dataset ----------


    def process_dataset(self, dataset_name: str) -> dict[str, Any]:
        started = time.monotonic()
        spec = DATASETS[dataset_name]
        csv_path = self.resolve_csv_path(spec)
        csv_df = self.load_csv_df(spec)
        sql_df = self.load_sql_df(spec)

        csv_candidates = self.scan_suspicious(spec, csv_df, "csv")
        sql_candidates = self.scan_suspicious(spec, sql_df, "sql")

        merged_candidates: dict[tuple[tuple[str, ...], str], dict[str, Any]] = {}
        for source_map in (csv_candidates, sql_candidates):
            for k, item in source_map.items():
                if k in merged_candidates:
                    existing = merged_candidates[k]
                    sources = set(str(existing.get("source", "")).split(",")) | {item["source"]}
                    existing["source"] = ",".join(sorted(s for s in sources if s))
                else:
                    merged_candidates[k] = item

        candidate_keys = dedup_keep_order([key for key, _ in merged_candidates.keys()])
        remote_rows = self.fetch_remote_rows(spec, candidate_keys)
        patches, true_zero_or_blank, unverifiable = self.compare_and_build_patches(spec, merged_candidates, remote_rows)

        authoritative_rows = []
        seen_rows = set()
        for p in patches:
            row_key = tuple(p["key"])
            if row_key in seen_rows:
                continue
            seen_rows.add(row_key)
            authoritative_rows.append(p["authoritative_row"])

        backup_path = None
        written_csv = None
        sql_upserted_rows = 0

        if self.apply_changes:
            if csv_path:
                backup_path = str(backup_file(csv_path, self.backup_dir)) if csv_path.exists() else None
            csv_df_patched = self.apply_patches_to_df(spec, csv_df, patches)
            if csv_path:
                csv_df_patched = csv_df_patched[spec.all_columns].copy()
                for col in csv_df_patched.columns:
                    csv_df_patched[col] = csv_df_patched[col].apply(coerce_csv_value)
                csv_df_patched.to_csv(csv_path, index=False, encoding="utf-8-sig")
                written_csv = str(csv_path)
            sql_upserted_rows = self.upsert_sql_rows(spec, authoritative_rows)

        summary = {
            "dataset": dataset_name,
            "csv_path": str(csv_path) if csv_path else None,
            "csv_suspicious_count": len(csv_candidates),
            "sql_suspicious_count": len(sql_candidates),
            "unique_candidate_keys": len(candidate_keys),
            "remote_rows_found": len(remote_rows),
            "patchable_cell_count": len(patches),
            "confirmed_true_blank_or_zero_count": len(true_zero_or_blank),
            "unverifiable_count": len(unverifiable),
            "rows_to_upsert": len(authoritative_rows),
            "applied": self.apply_changes,
            "csv_backup_path": backup_path,
            "written_csv": written_csv,
            "sql_upserted_rows": sql_upserted_rows,
            "speed_mode": "high_speed_v2",
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "patch_preview": patches[:50],
            "confirmed_preview": true_zero_or_blank[:50],
            "unverifiable_preview": unverifiable[:50],
        }
        return summary

    # ---------- run ----------
    def run(self) -> dict[str, Any]:
        overall = {
            "generated_at": now_ts(),
            "base_dir": str(self.base_dir),
            "apply_changes": self.apply_changes,
            "datasets": self.datasets,
            "sql_enabled": pyodbc is not None,
            "summary": [],
        }
        for dataset_name in self.datasets:
            overall["summary"].append(self.process_dataset(dataset_name))

        self.report_path.write_text(
            json.dumps(overall, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        overall["report_path"] = str(self.report_path)
        return overall


# =========================================
# CLI
# =========================================

def zero_null_repair_parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="掃描並修補 CSV / SQL 裡由網路抓回來的 0 / 空值")
    parser.add_argument(
        "--datasets",
        default="fundamentals,revenue,chip",
        help="要處理的資料集，逗號分隔：fundamentals,revenue,chip 或 all",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="真的寫回 CSV 與 SQL；不加時只做掃描與報告",
    )
    parser.add_argument(
        "--base-dir",
        default=".",
        help="專案根目錄，預設為目前資料夾",
    )
    return parser.parse_args(argv)


def zero_null_repair_cli_main(argv: list[str] | None = None) -> int:
    args = zero_null_repair_parse_args(argv)
    base_dir = Path(args.base_dir).resolve()

    datasets = [x.strip() for x in str(args.datasets).split(",") if x.strip()]
    if "all" in datasets:
        datasets = list(DATASETS.keys())

    invalid = [d for d in datasets if d not in DATASETS]
    if invalid:
        print(f"❌ 不支援的 datasets: {invalid}")
        return 2

    tool = ZeroNullRepairTool(base_dir=base_dir, apply_changes=bool(args.apply), datasets=datasets)
    report = tool.run()

    print("=" * 72)
    print("🩺 0 / 空值 掃描與補值完成")
    print(f"📄 報告：{report['report_path']}")
    for item in report["summary"]:
        print("-" * 72)
        print(f"📦 {item['dataset']}")
        print(f"  csv_suspicious_count              : {item['csv_suspicious_count']}")
        print(f"  sql_suspicious_count              : {item['sql_suspicious_count']}")
        print(f"  unique_candidate_keys             : {item['unique_candidate_keys']}")
        print(f"  patchable_cell_count              : {item['patchable_cell_count']}")
        print(f"  confirmed_true_blank_or_zero_count: {item['confirmed_true_blank_or_zero_count']}")
        print(f"  unverifiable_count                : {item['unverifiable_count']}")
        print(f"  rows_to_upsert                    : {item['rows_to_upsert']}")
        print(f"  applied                           : {item['applied']}")
        if item["csv_backup_path"]:
            print(f"  csv_backup_path                   : {item['csv_backup_path']}")
        if item["written_csv"]:
            print(f"  written_csv                       : {item['written_csv']}")
        print(f"  sql_upserted_rows                 : {item['sql_upserted_rows']}")
    print("=" * 72)
    print("建議先 dry-run：")
    print("  python fts_command_center_gui_all_in_one_highspeed.py --run-zero-null-repair --datasets fundamentals,revenue,chip")
    print("確認報告後再正式套用：")
    print("  python fts_command_center_gui_all_in_one_highspeed.py --run-zero-null-repair --datasets fundamentals,revenue,chip --apply")
    return 0



# ===== GUI =====

"""FTS 視窗化指令中心。
放在專案根目錄後執行：python fts_command_center_gui.py
"""

import locale
import os
import queue
import subprocess
import sys
import threading
import time
import tkinter as tk
from dataclasses import dataclass
from pathlib import Path
from tkinter import messagebox, ttk
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parent
PYTHON = sys.executable or "python"
HEARTBEAT_SECONDS = 5
BOOTSTRAP_DATABASE_NAME = "股票Online"
ZERO_NULL_REPAIR_SCRIPT = "__SELF_ZERO_NULL_REPAIR__"
ZERO_NULL_REPAIR_DATASETS = "fundamentals,revenue,chip"


# Windows 中文輸出常見是 CP950/Big5；子程序也可能被 PYTHONIOENCODING 強制成 UTF-8。
# GUI 這裡用二進位讀取，再自動嘗試多種解碼，避免中文變成亂碼。
def _decode_output(data: bytes) -> str:
    if not data:
        return ""
    encodings = [
        "utf-8-sig",
        "utf-8",
        locale.getpreferredencoding(False),
        "cp950",
        "big5",
        "mbcs",
    ]
    seen = set()
    for enc in encodings:
        if not enc or enc in seen:
            continue
        seen.add(enc)
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="replace")



def _format_elapsed(seconds: float) -> str:
    total = max(0, int(seconds))
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    if h:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{m:02d}:{s:02d}"


@dataclass(frozen=True)
class CommandItem:
    category: str
    name: str
    args: List[str]
    desc: str
    note: str = ""
    confirm: bool = False
    dangerous: bool = False
    long_running: bool = False


COMMANDS = [
    CommandItem("主程式", "初始化 / 第一次啟動", ["formal_trading_system_v83_official_main.py", "--bootstrap"], "新電腦、重新建資料庫、第一次啟動時使用。會先確保建立資料庫【股票Online】、再建置 runtime、補本地資料、檢查 SQL 與系統狀態。", "此流程可能需要數分鐘；若資料或網路較慢，中途可能短暫沒有新輸出。", True, False, True),
    CommandItem("主程式", "日常執行", ["formal_trading_system_v83_official_main.py"], "日常主流程。", "預設就是日常模式。", False, False, True),
    CommandItem("主程式", "訓練模式", ["formal_trading_system_v83_official_main.py", "--train"], "建立/更新訓練資料並訓練模型。", "需要資料與 labels 充足；第一次可能花較久。", True, False, True),
    CommandItem("資料庫", "資料庫升級 / 建立資料表", ["fts_db_migrations.py", "upgrade"], "建立或升級 SQL 資料表、欄位與中文欄名查詢 view。", "重新建資料庫後第一個要跑。", True, False, True),
    CommandItem("檢查", "深度健康檢查", ["fts_admin_cli.py", "healthcheck", "--deep"], "深度檢查專案：語法編譯、核心 import、三路流程與 exception policy。", "每次覆蓋更新檔後建議執行。", False, False, True),
    CommandItem("檢查", "舊欄位 / 可刪除性檢查（含資料庫）", ["fts_admin_cli.py", "drop-readiness", "--check-db"], "檢查舊欄位 / 可刪除性，並連 SQL Server 檢查欄位狀態。", "用來判斷是否可以做破壞式清理。", False, False, True),
    CommandItem("清理", "第二輪舊檔清理預覽", ["fts_admin_cli.py", "second-merge-cleanup"], "第二輪合併汰除預覽，只產生報告，不刪檔。", "先看 ready / blocked / missing。"),
    CommandItem("清理", "第二輪舊檔清理套用", ["fts_admin_cli.py", "second-merge-cleanup", "--apply"], "實際刪除已判定 ready 的舊檔案。", "危險：請先確認深度健康檢查通過。", True, True),
    CommandItem("實盤前 95%", "券商合約檢查", ["fts_admin_cli.py", "broker-contract-audit"], "檢查券商 adapter 是否具備 connect / place / cancel / replace / query / callback 等必要方法。"),
    CommandItem("實盤前 95%", "券商回報匯入", ["fts_admin_cli.py", "callback-ingest"], "匯入並標準化券商 callback，寫入 callback runtime。"),
    CommandItem("實盤前 95%", "對帳 runtime 檢查", ["fts_admin_cli.py", "reconciliation-runtime"], "執行對帳 runtime，檢查委託、成交、持倉、現金是否一致。"),
    CommandItem("實盤前 95%", "重啟恢復檢查", ["fts_admin_cli.py", "restart-recovery"], "檢查系統重啟後是否能恢復 working orders / positions，並產生 recovery plan。"),
    CommandItem("實盤前 95%", "出場 AI 模型產生", ["fts_admin_cli.py", "exit-artifact-bootstrap"], "產生出場 AI artifacts；沒有 exit labels 時不會產生假模型。", "需要 exit labels。", True, False, True),
    CommandItem("回測", "投組回測 3 年", ["fts_admin_cli.py", "portfolio-backtest", "--period", "3y"], "投組層級回測，輸出 equity curve、drawdown、分股票 / 分策略統計。", "回測可能需要一些時間。", False, False, True),
    CommandItem("實盤前 95%", "實盤前 95% 總檢", ["fts_admin_cli.py", "prebroker-95-audit", "--run-backtest", "--bootstrap-exit"], "實盤前閉環總檢：券商合約、callback、ledger、對帳、重啟恢復、出場模型、回測。", "最重要的總檢，可能需要數分鐘。", True, False, True),
    CommandItem("資料/特徵", "全市場百分位快照", ["fts_admin_cli.py", "full-market-percentile"], "建立全市場 percentile / ranking 快照，供選股與排序使用。", "資料量大時可能需要較久。", False, False, True),
    CommandItem("資料/特徵", "事件日曆建立", ["fts_admin_cli.py", "event-calendar-build"], "建立事件日曆，例如財報、月營收、特殊事件窗口。"),
    CommandItem("資料/特徵", "同步特徵快照到 SQL", ["fts_admin_cli.py", "sync-feature-snapshots"], "把 feature snapshots 同步寫入 SQL。", "資料多時可能需要較久。", False, False, True),
    CommandItem("稽核", "訓練壓力測試", ["fts_admin_cli.py", "training-stress-audit"], "執行訓練壓力測試與穩定性稽核。", "檢查訓練流程安全性，不是正式訓練。", False, False, True),
    CommandItem("稽核", "資料回補韌性檢查", ["fts_admin_cli.py", "backfill-resilience-audit"], "檢查資料回補、缺口修復、local-first 韌性。", "資料多時可能需要較久。", False, False, True),
    CommandItem("資料修補", "零值 / 空值掃描（dry-run）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS], "高速掃描本地 CSV / SQL 的 0 值與空值，並平行到網路確認是本來就為 0/空值，還是抓取遺漏。只產生報告，不寫回。", "建議先跑這個確認報告，再決定是否正式補值。會輸出 runtime/zero_null_repair_report.json。", False, False, True),
    CommandItem("資料修補", "零值 / 空值正式補值（apply）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS, "--apply"], "高速驗證後，正式把確認屬於遺漏的數值補回本地 CSV 與 SQL。", "危險：會改寫 CSV 與 SQL；腳本會先自動備份 CSV。建議先完成 dry-run。", True, True, True),
    CommandItem("資料修補", "只修基本面（apply）", [ZERO_NULL_REPAIR_SCRIPT, "--datasets", "fundamentals", "--apply"], "只針對 fundamentals_clean / 基本面 CSV 進行高速 0 值與空值回補。", "適合先小範圍驗證流程。", True, True, True),

    # ===== AI 候選治理 / 自動訓練參數（Stage 2～4）=====
    CommandItem("AI候選治理", "訓練參數自動搜尋（24組）", ["fts_admin_cli.py", "train-param-optimize", "--iterations", "24"], "自動搜尋 ML trainer 超參數，產生 trainer::default candidate 與排行榜。", "只產生 candidate / report，不直接改 config、不直接 live。", False, False, True),
    CommandItem("AI候選治理", "訓練參數 AI 判斷", ["fts_admin_cli.py", "param-ai-judge", "--scope", "trainer::default"], "對 trainer::default 最新 candidate 做 AI 分數與 hard gate 判斷。", "通過後最多進 approved_for_research，不會直接 live。", False, False, True),
    CommandItem("AI候選治理", "Label Policy 自動搜尋（24組）", ["fts_admin_cli.py", "label-policy-optimize", "--iterations", "24"], "搜尋 label policy 候選參數，例如 TP / SL / holding days。", "會影響訓練資料定義，通過後仍需重建 training data。", True, False, True),
    CommandItem("AI候選治理", "Label Policy AI 判斷", ["fts_admin_cli.py", "param-ai-judge", "--scope", "label_policy::default"], "對 label_policy::default candidate 做 AI 判斷。", "最多批准到 approved_for_rebuild_training_data，不會直接影響 daily / live。", False, False, True),
    CommandItem("AI候選治理", "策略訊號 AI 判斷", ["fts_admin_cli.py", "param-ai-judge", "--scope", "strategy_signal::default"], "對 strategy_signal::default candidate 做 AI 判斷。", "用於 RSI / MACD / ADX / TRIGGER_SCORE 等策略訊號候選。", False, False, True),
    CommandItem("AI候選治理", "執行政策自動搜尋（24組）", ["fts_admin_cli.py", "execution-policy-optimize", "--iterations", "24"], "搜尋 execution_policy::default 候選參數，例如 TWAP3、滑價、流動性、partial fill。", "只產生候選與報告，不會直接下單。", True, False, True),
    CommandItem("AI候選治理", "執行政策 AI 判斷", ["fts_admin_cli.py", "param-ai-judge", "--scope", "execution_policy::default"], "對 execution_policy::default candidate 做 AI 判斷。", "用於執行層候選參數審核。", False, False, True),
    CommandItem("AI候選治理", "四大 Scope AI 判斷總控", ["fts_admin_cli.py", "param-governance", "--all-scopes"], "一次對 trainer / label_policy / strategy_signal / execution_policy 執行候選治理總控。", "此流程只做治理判斷，不會自動真倉。", False, False, True),
    CommandItem("AI候選治理", "Paper / Shadow Evidence 回填", ["fts_admin_cli.py", "param-evidence-collect", "--all-scopes"], "收集 paper / shadow / execution runtime evidence，寫回 param_storage release 欄位。", "若 runtime evidence 不足，release gate 會 fail-closed。", False, False, True),
    CommandItem("AI候選治理", "Release Gate 全 Scope", ["fts_admin_cli.py", "param-governance", "--all-scopes", "--release-gate"], "執行 AI judge、evidence collect、release gate。", "approved 不等於 live；live 自動推進仍被安全鎖關閉。", False, False, True),
    CommandItem("AI候選治理", "完整治理＋Live前安全檢查", ["fts_admin_cli.py", "param-governance", "--all-scopes", "--release-gate", "--live-manifest", "--live-guard", "--rollback-plan"], "完整跑 AI judge、evidence、release gate、live manifest、live guard、rollback plan。", "這是目前最完整的 pre-live 參數治理流程；仍不會自動真倉。", True, False, True),
    CommandItem("AI候選治理", "Approved 掛載報告", ["fts_admin_cli.py", "approved-param-mount-report"], "檢查 approved params 目前可掛載狀態與 mount summary。", "用來確認 train / label / strategy / execution 是否會讀 approved params。", False, False, True),

    # ===== Live 發布 / 回滾安全檢查（不會自動真倉）=====
    CommandItem("參數發布/回滾", "Strategy Live Manifest", ["fts_admin_cli.py", "param-live-manifest", "--scope", "strategy_signal::default"], "產生 strategy_signal::default live 發布清單。", "只產生 manifest，不會改 config、不會真倉。", False, False, True),
    CommandItem("參數發布/回滾", "Strategy Live Guard", ["fts_admin_cli.py", "param-live-guard", "--scope", "strategy_signal::default"], "檢查 strategy_signal::default promoted_for_live 是否可掛載。", "會檢查 release gate、paper/shadow、rollback、kill switch、broker readiness。", False, False, True),
    CommandItem("參數發布/回滾", "Strategy Rollback Plan", ["fts_admin_cli.py", "param-rollback-plan", "--scope", "strategy_signal::default"], "產生 strategy_signal::default 回滾計畫。", "預設 dry-run，不會自動回滾。", False, False, True),
    CommandItem("參數發布/回滾", "Execution Live Manifest", ["fts_admin_cli.py", "param-live-manifest", "--scope", "execution_policy::default"], "產生 execution_policy::default live 發布清單。", "只產生 manifest，不會真倉下單。", False, False, True),
    CommandItem("參數發布/回滾", "Execution Live Guard", ["fts_admin_cli.py", "param-live-guard", "--scope", "execution_policy::default"], "檢查 execution_policy::default promoted_for_live 是否可掛載。", "執行層最接近下單，建議只做檢查，不要自動套用。", False, False, True),
    CommandItem("參數發布/回滾", "Execution Rollback Plan", ["fts_admin_cli.py", "param-rollback-plan", "--scope", "execution_policy::default"], "產生 execution_policy::default 回滾計畫。", "預設 dry-run，不會自動回滾。", False, False, True),
    CommandItem("參數發布/回滾", "Trainer Release Gate", ["fts_admin_cli.py", "param-release-gate", "--scope", "trainer::default"], "針對 trainer::default 執行 release gate。", "訓練參數通常只到 research/train，不直接 live。", False, False, True),
    CommandItem("參數發布/回滾", "Strategy Release Gate", ["fts_admin_cli.py", "param-release-gate", "--scope", "strategy_signal::default"], "針對 strategy_signal::default 執行 release gate。", "必須 paper / shadow evidence 足夠才可能通過。", False, False, True),
    CommandItem("參數發布/回滾", "Execution Release Gate", ["fts_admin_cli.py", "param-release-gate", "--scope", "execution_policy::default"], "針對 execution_policy::default 執行 release gate。", "執行層 release gate 建議最後才看。", False, False, True),
]

FLOWS = {
    "新電腦 / 重建資料庫標準流程": [
        ["fts_db_migrations.py", "upgrade"],
        ["fts_admin_cli.py", "healthcheck", "--deep"],
        ["fts_admin_cli.py", "drop-readiness", "--check-db"],
        ["formal_trading_system_v83_official_main.py", "--bootstrap"],
    ],
    "實盤前 95% 總檢流程": [
        ["fts_admin_cli.py", "healthcheck", "--deep"],
        ["fts_admin_cli.py", "prebroker-95-audit", "--run-backtest", "--bootstrap-exit"],
    ],
    "清理前安全檢查流程": [
        ["fts_admin_cli.py", "second-merge-cleanup"],
        ["fts_admin_cli.py", "healthcheck", "--deep"],
    ],
    "零值 / 空值修補標準流程": [
        [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS],
        [ZERO_NULL_REPAIR_SCRIPT, "--datasets", ZERO_NULL_REPAIR_DATASETS, "--apply"],
    ],
    "AI候選治理：訓練參數搜尋→AI判斷→正式訓練": [
        ["fts_admin_cli.py", "train-param-optimize", "--iterations", "24"],
        ["fts_admin_cli.py", "param-ai-judge", "--scope", "trainer::default"],
        ["fts_admin_cli.py", "approved-param-mount-report"],
        ["formal_trading_system_v83_official_main.py", "--train"],
    ],
    "AI候選治理：四大Scope完整預檢": [
        ["fts_admin_cli.py", "param-governance", "--all-scopes", "--release-gate", "--live-manifest", "--live-guard", "--rollback-plan"],
        ["fts_admin_cli.py", "approved-param-mount-report"],
    ],
    "Label Policy：搜尋→AI判斷": [
        ["fts_admin_cli.py", "label-policy-optimize", "--iterations", "24"],
        ["fts_admin_cli.py", "param-ai-judge", "--scope", "label_policy::default"],
        ["fts_admin_cli.py", "approved-param-mount-report"],
    ],
    "Execution Policy：搜尋→AI判斷→Release Gate": [
        ["fts_admin_cli.py", "execution-policy-optimize", "--iterations", "24"],
        ["fts_admin_cli.py", "param-ai-judge", "--scope", "execution_policy::default"],
        ["fts_admin_cli.py", "param-evidence-collect", "--all-scopes"],
        ["fts_admin_cli.py", "param-release-gate", "--scope", "execution_policy::default"],
    ],
    "Strategy 參數發布前安全檢查": [
        ["fts_admin_cli.py", "param-evidence-collect", "--all-scopes"],
        ["fts_admin_cli.py", "param-release-gate", "--scope", "strategy_signal::default"],
        ["fts_admin_cli.py", "param-live-manifest", "--scope", "strategy_signal::default"],
        ["fts_admin_cli.py", "param-live-guard", "--scope", "strategy_signal::default"],
        ["fts_admin_cli.py", "param-rollback-plan", "--scope", "strategy_signal::default"],
    ],
}


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("FTS 視窗化指令中心（高速版＋AI候選治理按鍵版）")
        self.geometry("1220x800")
        self.proc = None
        self.q = queue.Queue()
        self.selected = COMMANDS[0]
        self.started_at: float | None = None
        self.next_heartbeat_at: float | None = None
        self.current_title = ""
        self._build()
        self.after(100, self._drain)

    def _build(self):
        paned = ttk.PanedWindow(self, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True, padx=8, pady=8)
        left = ttk.Frame(paned, width=430)
        right = ttk.Frame(paned)
        paned.add(left, weight=1)
        paned.add(right, weight=2)

        ttk.Label(left, text="指令按鍵", font=("Microsoft JhengHei", 13, "bold")).pack(anchor="w")
        nb = ttk.Notebook(left)
        nb.pack(fill=tk.BOTH, expand=True, pady=6)
        cats = []
        for c in [x.category for x in COMMANDS]:
            if c not in cats:
                cats.append(c)
        for cat in cats:
            f = ttk.Frame(nb)
            nb.add(f, text=cat)
            for item in [x for x in COMMANDS if x.category == cat]:
                ttk.Button(f, text=item.name, command=lambda it=item: self.select(it)).pack(fill=tk.X, padx=6, pady=3)
        lf = ttk.LabelFrame(left, text="一鍵流程")
        lf.pack(fill=tk.X, pady=6)
        for name, flow in FLOWS.items():
            ttk.Button(lf, text=name, command=lambda n=name, fl=flow: self.run_flow(n, fl)).pack(fill=tk.X, padx=6, pady=3)

        ttk.Label(right, text="說明", font=("Microsoft JhengHei", 13, "bold")).pack(anchor="w")
        self.info = tk.Text(right, height=11, wrap="word")
        self.info.pack(fill=tk.X, pady=6)

        bar = ttk.Frame(right)
        bar.pack(fill=tk.X)
        ttk.Button(bar, text="執行選取指令", command=self.run_selected).pack(side=tk.LEFT, padx=3)
        ttk.Button(bar, text="停止目前程序", command=self.stop_proc).pack(side=tk.LEFT, padx=3)
        ttk.Button(bar, text="清空紀錄", command=lambda: self.log.delete("1.0", tk.END)).pack(side=tk.LEFT, padx=3)
        self.status = ttk.Label(bar, text="準備就緒")
        self.status.pack(side=tk.RIGHT, padx=(12, 0))
        self.elapsed_label = ttk.Label(bar, text="已執行：00:00")
        self.elapsed_label.pack(side=tk.RIGHT, padx=(12, 0))

        self.progress = ttk.Progressbar(right, mode="indeterminate")
        self.progress.pack(fill=tk.X, pady=(8, 0))

        ttk.Label(right, text="執行紀錄", font=("Microsoft JhengHei", 12, "bold")).pack(anchor="w", pady=(10, 0))
        self.log = tk.Text(right, wrap="word")
        self.log.pack(fill=tk.BOTH, expand=True)
        self.select(self.selected)

    def _is_bootstrap_args(self, args: List[str]) -> bool:
        return "formal_trading_system_v83_official_main.py" in args and "--bootstrap" in args

    def _is_zero_null_repair_args(self, args: List[str]) -> bool:
        return bool(args) and args[0] == ZERO_NULL_REPAIR_SCRIPT

    def _is_zero_null_apply_args(self, args: List[str]) -> bool:
        return self._is_zero_null_repair_args(args) and "--apply" in args

    def _bootstrap_flow(self):
        return [
            ["fts_db_migrations.py", "upgrade"],
            ["formal_trading_system_v83_official_main.py", "--bootstrap"],
        ]

    def select(self, item):
        self.selected = item
        if self._is_zero_null_repair_args(item.args):
            display_args = [Path(__file__).name, "--run-zero-null-repair"] + item.args[1:]
            cmd = " ".join([PYTHON, "-u"] + display_args)
        else:
            cmd = " ".join([PYTHON, "-u"] + item.args)
        self.info.delete("1.0", tk.END)
        extra = ""
        if self._is_bootstrap_args(item.args):
            extra = (
                f"\nBootstrap 提醒：這個流程會先確保建立資料庫【{BOOTSTRAP_DATABASE_NAME}】，再進行初始化。"
                "若正在讀本地 CSV、連 SQL、補 runtime 或檢查資料，畫面可能短暫沒有新輸出。"
                "APP 會每 5 秒顯示仍在執行。\n"
            )
        elif self._is_zero_null_repair_args(item.args):
            apply_text = "會正式寫回 CSV / SQL。" if self._is_zero_null_apply_args(item.args) else "只掃描與驗證，不會寫回 CSV / SQL。"
            extra = (
                f"\n資料修補提醒：這支會先掃描本地資料，再去網路確認 0 值與空值是真缺漏還是真實值。{apply_text}\n"
                "建議先跑 dry-run 看 runtime/zero_null_repair_report.json，再決定是否 apply。\n"
            )
        self.info.insert(
            tk.END,
            f"名稱：{item.name}\n"
            f"分類：{item.category}\n\n"
            f"用途：{item.desc}\n\n"
            f"備註：{item.note or '無'}\n"
            f"{extra}\n"
            f"實際指令：\n{cmd}\n",
        )

    def run_selected(self):
        item = self.selected
        if self._is_bootstrap_args(item.args):
            message = (
                f"要執行：{item.name}\n\n"
                f"此流程會先建立 / 升級資料庫【{BOOTSTRAP_DATABASE_NAME}】後，再執行 bootstrap。\n"
                "Bootstrap 可能需要數分鐘。\n"
                "執行期間 APP 會顯示計時器，並每 5 秒輸出『仍在執行』。\n\n"
                f"第一步：{PYTHON} -u fts_db_migrations.py upgrade\n"
                f"第二步：{PYTHON} -u formal_trading_system_v83_official_main.py --bootstrap"
            )
            if not messagebox.askyesno("確認執行 Bootstrap", message):
                return
            self.run_flow(f"{item.name}（含建立資料庫 {BOOTSTRAP_DATABASE_NAME}）", self._bootstrap_flow())
            return

        if self._is_zero_null_repair_args(item.args) and not self._is_zero_null_apply_args(item.args):
            cmd_text = " ".join([PYTHON, "-u"] + item.args)
            message = (
                f"要執行：{item.name}\n\n"
                "這是 dry-run，只會掃描本地 CSV / SQL 的 0 值與空值，並到網路確認是否為遺漏資料。\n"
                "不會改寫 CSV / SQL。\n\n"
                f"實際指令：{cmd_text}"
            )
            if not messagebox.askyesno("確認執行資料修補掃描", message):
                return
            self.run_cmd(item.args, title=item.name)
            return

        if self._is_zero_null_apply_args(item.args):
            cmd_text = " ".join([PYTHON, "-u"] + item.args)
            message = (
                f"要執行：{item.name}\n\n"
                "這會先比對網路來源，再把確認屬於遺漏的值寫回 CSV 與 SQL。\n"
                "建議你先跑 dry-run 確認 runtime/zero_null_repair_report.json。\n"
                "腳本會先備份 CSV，但這仍屬於有改寫風險的操作。\n\n"
                f"實際指令：{cmd_text}"
            )
            if not messagebox.askyesno("確認正式補值", message):
                return
            self.run_cmd(item.args, title=item.name)
            return

        if item.confirm or item.dangerous:
            if not messagebox.askyesno("確認執行", f"要執行：{item.name}\n\n{' '.join([PYTHON, '-u'] + item.args)}"):
                return
        self.run_cmd(item.args, title=item.name)

    def run_flow(self, name, flow):
        if not messagebox.askyesno("確認流程", f"要連續執行流程：{name}？\n\n長流程執行時會顯示計時器與每 5 秒心跳訊息。"):
            return

        def worker():
            for args in flow:
                ok = self._run_blocking(args, title="流程步驟")
                if not ok:
                    break

        threading.Thread(target=worker, daemon=True).start()

    def run_cmd(self, args, title=""):
        threading.Thread(target=lambda: self._run_blocking(args, title), daemon=True).start()

    def _run_blocking(self, args, title=""):
        if self.proc is not None:
            self.q.put("\n[圖形介面] 目前已有程序執行中，請等待結束或按『停止目前程序』。\n")
            return False

        if self._is_zero_null_repair_args(args):
            cmd = [PYTHON, "-u", Path(__file__).name, "--run-zero-null-repair"] + args[1:]
        else:
            cmd = [PYTHON, "-u"] + args
        self.started_at = time.monotonic()
        self.next_heartbeat_at = self.started_at + HEARTBEAT_SECONDS
        self.current_title = title or args[0]

        self.q.put(f"\n========== 執行：{self.current_title} ==========\n")
        self.q.put("$ " + " ".join(cmd) + "\n")
        if self._is_bootstrap_args(args):
            self.q.put("[圖形介面] Bootstrap 可能需要數分鐘。若中途沒有新輸出，請看右上角狀態與已執行時間。\n")
        elif self._is_zero_null_repair_args(args):
            if self._is_zero_null_apply_args(args):
                self.q.put("[圖形介面] 資料修補 apply 模式：會先驗證，再正式回寫 CSV / SQL。\n")
            else:
                self.q.put("[圖形介面] 資料修補 dry-run：只掃描與驗證，不會回寫 CSV / SQL。\n")
        self.q.put("[圖形介面] 已啟動程序，開始計時。\n")

        try:
            env = os.environ.copy()
            env.setdefault("PYTHONIOENCODING", "utf-8")
            env.setdefault("PYTHONUTF8", "1")
            env.setdefault("PYTHONUNBUFFERED", "1")
            self.proc = subprocess.Popen(
                cmd,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                bufsize=0,
                text=False,
                env=env,
            )
            assert self.proc.stdout is not None
            for raw in iter(self.proc.stdout.readline, b""):
                if raw:
                    self.q.put(_decode_output(raw))
            rc = self.proc.wait()
            elapsed = _format_elapsed(time.monotonic() - (self.started_at or time.monotonic()))
            self.q.put(f"\n[圖形介面] 結束代碼={rc}，總耗時={elapsed}\n")
            return rc == 0
        except Exception as e:
            self.q.put(f"\n[圖形介面錯誤] {e!r}\n")
            return False
        finally:
            self.proc = None
            self.started_at = None
            self.next_heartbeat_at = None
            self.current_title = ""

    def stop_proc(self):
        if self.proc is not None:
            self.proc.terminate()
            self.q.put("\n[圖形介面] 已送出停止訊號。\n")
        else:
            self.q.put("\n[圖形介面] 目前沒有正在執行的程序。\n")

    def _drain(self):
        try:
            while True:
                s = self.q.get_nowait()
                self.log.insert(tk.END, s)
                self.log.see(tk.END)
        except queue.Empty:
            pass

        now = time.monotonic()
        if self.proc is not None and self.started_at is not None:
            elapsed = _format_elapsed(now - self.started_at)
            self.status.config(text=f"執行中：{self.current_title}")
            self.elapsed_label.config(text=f"已執行：{elapsed}")
            if self.next_heartbeat_at is not None and now >= self.next_heartbeat_at:
                self.q.put(f"[圖形介面] 仍在執行：{self.current_title}，已執行 {elapsed}。\n")
                self.next_heartbeat_at = now + HEARTBEAT_SECONDS
            try:
                self.progress.start(10)
            except tk.TclError:
                pass
        else:
            self.status.config(text="準備就緒")
            self.elapsed_label.config(text="已執行：00:00")
            try:
                self.progress.stop()
            except tk.TclError:
                pass

        self.after(100, self._drain)


if __name__ == "__main__":
    if "--run-zero-null-repair" in sys.argv:
        argv = [x for x in sys.argv[1:] if x != "--run-zero-null-repair"]
        raise SystemExit(zero_null_repair_cli_main(argv))
    App().mainloop()
