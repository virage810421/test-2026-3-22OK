
# -*- coding: utf-8 -*-
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

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import sys
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


def fetch_fundamentals_remote(tickers: list[str], reports_per_ticker: int = 8) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    if yf is None:
        return result

    for ticker_symbol in dedup_keep_order([normalize_ticker(x) for x in tickers if normalize_ticker(x)]):
        try:
            tk = yf.Ticker(ticker_symbol)
            is_df = tk.quarterly_financials
            bs_df = tk.quarterly_balance_sheet
            cf_df = tk.quarterly_cashflow
            if is_df is None or is_df.empty:
                continue

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

                row = {
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
                result[(ticker_symbol, report_date)] = row
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


def fetch_chip_remote_for_dates(date_list: list[str]) -> dict[tuple[str, str], dict[str, Any]]:
    result: dict[tuple[str, str], dict[str, Any]] = {}
    for ds in dedup_keep_order(sorted([d for d in date_list if normalize_date_like(d)])):
        if not is_tw_trading_day(ds):
            continue
        try:
            d = datetime.strptime(ds, "%Y-%m-%d").date()
        except Exception:
            continue
        df_all = pd.concat([parse_twse_backup_csv(d), parse_tpex_backup_csv(d)], ignore_index=True)
        if df_all.empty:
            continue
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

    def build_index_map(self, spec: DatasetSpec, df: pd.DataFrame) -> dict[tuple[str, ...], int]:
        out: dict[tuple[str, ...], int] = {}
        if df.empty:
            return out
        for idx, row in df.iterrows():
            out[self.key_tuple(spec, row)] = idx
        return out

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
        for _, row in df.iterrows():
            key = self.key_tuple(spec, row)
            for col in spec.numeric_scan_cols:
                value = row.get(col)
                if self.is_suspicious_numeric(value):
                    out[(key, col)] = {
                        "dataset": spec.name,
                        "key": list(key),
                        "column": col,
                        "source": source_name,
                        "local_value": None if is_blank(value) else safe_float(value),
                        "reason": "blank_or_zero",
                    }
            for col in spec.text_scan_cols:
                value = row.get(col)
                if self.is_suspicious_text(value):
                    out[(key, col)] = {
                        "dataset": spec.name,
                        "key": list(key),
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
            raw = fetch_fundamentals_remote(tickers=tickers, reports_per_ticker=8)
            return {tuple(k): v for k, v in raw.items()}
        if spec.name == "revenue":
            raw = fetch_revenue_remote_latest()
            return {tuple(k): v for k, v in raw.items()}
        if spec.name == "chip":
            dates = [key[0] for key in candidate_keys]
            raw = fetch_chip_remote_for_dates(dates)
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

        for patch in patches:
            key = tuple(patch["key"])
            auth_row = patch["authoritative_row"]
            if key in index_map:
                idx = index_map[key]
                for col, value in auth_row.items():
                    if col in df.columns:
                        df.at[idx, col] = value
            else:
                new_row = {col: auth_row.get(col) for col in spec.all_columns}
                df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                index_map = self.build_index_map(spec, df)

        return df

    # ---------- SQL upsert ----------
    def upsert_sql_rows(self, spec: DatasetSpec, authoritative_rows: list[dict[str, Any]]) -> int:
        if pyodbc is None or not authoritative_rows:
            return 0

        count = 0
        with pyodbc.connect(self.conn_str) as conn:
            cursor = conn.cursor()
            for row in authoritative_rows:
                if spec.name == "fundamentals":
                    ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                    report_date = normalize_date_like(row.get("資料年月日"))
                    cursor.execute(
                        f"""
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
                        """,
                        ticker, report_date,
                        safe_float(row.get("毛利率(%)")), safe_float(row.get("營業利益率(%)")), safe_float(row.get("單季EPS")), safe_float(row.get("ROE(%)")),
                        safe_float(row.get("稅後淨利率(%)")), safe_float(row.get("營業現金流")), safe_float(row.get("預估殖利率(%)")), safe_float(row.get("負債比率(%)")),
                        safe_float(row.get("本業獲利比(%)")), ticker, report_date,
                        ticker, report_date,
                        safe_float(row.get("毛利率(%)")), safe_float(row.get("營業利益率(%)")), safe_float(row.get("單季EPS")), safe_float(row.get("ROE(%)")),
                        safe_float(row.get("稅後淨利率(%)")), safe_float(row.get("營業現金流")), safe_float(row.get("預估殖利率(%)")), safe_float(row.get("負債比率(%)")),
                        safe_float(row.get("本業獲利比(%)")),
                    )
                    count += 1

                elif spec.name == "revenue":
                    ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                    ym = normalize_date_like(row.get("資料年月日")) or ""
                    # SQL DATE 用月初日；比對時以 yyyy-mm 為準
                    sql_date = ym + "-01" if re.fullmatch(r"\d{4}-\d{2}", ym) else ym
                    cursor.execute(
                        f"""
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
                        """,
                        ticker, ym,
                        normalize_text(row.get("公司名稱")), normalize_text(row.get("產業類別")), normalize_text(row.get("產業類別名稱")), safe_float(row.get("單月營收年增率(%)")),
                        ticker, ym,
                        ticker, normalize_text(row.get("公司名稱")), normalize_text(row.get("產業類別")), normalize_text(row.get("產業類別名稱")), sql_date, safe_float(row.get("單月營收年增率(%)")),
                    )
                    count += 1

                elif spec.name == "chip":
                    ds = normalize_date_like(row.get("日期"))
                    ticker = normalize_ticker(row.get("Ticker SYMBOL"))
                    cursor.execute(
                        f"""
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
                        """,
                        ds, ticker,
                        safe_float(row.get("外資買賣超")), safe_float(row.get("投信買賣超")), safe_float(row.get("自營商買賣超")), safe_float(row.get("三大法人合計")), normalize_text(row.get("資料來源")),
                        ds, ticker,
                        ds, ticker,
                        safe_float(row.get("外資買賣超")), safe_float(row.get("投信買賣超")), safe_float(row.get("自營商買賣超")), safe_float(row.get("三大法人合計")), normalize_text(row.get("資料來源")),
                    )
                    count += 1
            conn.commit()
        return count

    # ---------- main per dataset ----------
    def process_dataset(self, dataset_name: str) -> dict[str, Any]:
        spec = DATASETS[dataset_name]
        csv_path = self.resolve_csv_path(spec)
        csv_df = self.load_csv_df(spec)
        sql_df = self.load_sql_df(spec)

        csv_candidates = self.scan_suspicious(spec, csv_df, "csv")
        sql_candidates = self.scan_suspicious(spec, sql_df, "sql")

        # 合併同一 key+column，保留來源資訊
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
            "patchable_cell_count": len(patches),
            "confirmed_true_blank_or_zero_count": len(true_zero_or_blank),
            "unverifiable_count": len(unverifiable),
            "rows_to_upsert": len(authoritative_rows),
            "applied": self.apply_changes,
            "csv_backup_path": backup_path,
            "written_csv": written_csv,
            "sql_upserted_rows": sql_upserted_rows,
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

def parse_args() -> argparse.Namespace:
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
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
    print("  python fts_zero_null_sql_csv_repair.py --datasets fundamentals,revenue,chip")
    print("確認報告後再正式套用：")
    print("  python fts_zero_null_sql_csv_repair.py --datasets fundamentals,revenue,chip --apply")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
