# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

try:  # optional dependency on user machine
    import yfinance as yf  # type: ignore
except Exception:  # pragma: no cover
    yf = None  # type: ignore

try:  # optional dependency on user machine
    import pyodbc  # type: ignore
except Exception:  # pragma: no cover
    pyodbc = None  # type: ignore


DATA_COLUMNS = [
    'Ticker SYMBOL',
    '資料年月日',
    '毛利率(%)',
    '營業利益率(%)',
    '單季EPS',
    'ROE(%)',
    '稅後淨利率(%)',
    '營業現金流',
    '預估殖利率(%)',
    '負債比率(%)',
    '本業獲利比(%)',
]


@dataclass
class FundamentalsPaths:
    csv_path: Path
    runtime_path: Path
    template_json_path: Path


class FundamentalsETLMainline:
    """v83+ fundamentals ETL mainline.

    目標：
    1. 不再依賴 legacy `yahoo_csv_to_sql.py` 才能運作。
    2. 預設先做「安全本地同步」：自動建立 CSV 模板、掃描既有 backup、產生 runtime JSON。
    3. 若使用者電腦已安裝 yfinance / pyodbc，則可切到 smart sync：抓 Yahoo 財報並回寫 SQL。
    """

    MODULE_VERSION = 'v83_fundamentals_etl_mainline_builtin'
    CSV_FILENAME = str(getattr(CONFIG, 'fundamentals_csv_filename', 'market_financials_backup_fullspeed.csv'))
    TABLE_NAME = str(getattr(CONFIG, 'fundamentals_table_name', 'fundamentals_clean'))
    TARGET_REPORTS_PER_STOCK = int(getattr(CONFIG, 'fundamentals_target_reports_per_stock', 2))

    def __init__(self):
        self.paths = FundamentalsPaths(
            csv_path=PATHS.data_dir / self.CSV_FILENAME,
            runtime_path=PATHS.runtime_dir / 'fundamentals_etl_mainline.json',
            template_json_path=PATHS.runtime_dir / 'fundamentals_etl_config.template.json',
        )

    # -----------------------------
    # template / filesystem helpers
    # -----------------------------
    def _ensure_csv_template(self) -> tuple[Path, bool]:
        created = False
        if not self.paths.csv_path.exists():
            pd.DataFrame(columns=DATA_COLUMNS).to_csv(self.paths.csv_path, index=False, encoding='utf-8-sig')
            created = True
        return self.paths.csv_path, created

    def _write_template_json(self) -> Path:
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'csv_path': str(self.paths.csv_path),
            'table_name': self.TABLE_NAME,
            'target_reports_per_stock': self.TARGET_REPORTS_PER_STOCK,
            'network_fetch_requires': ['yfinance'],
            'sql_import_requires': ['pyodbc', 'SQL Server reachable'],
            'safe_default_mode': 'local_sync_only',
            'modes': ['local_sync_only', 'smart_sync'],
        }
        self.paths.template_json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.paths.template_json_path

    def _search_existing_backups(self) -> list[Path]:
        patterns = [
            'market_financials_backup*.csv',
            'quarterly_fundamentals*.csv',
            '*financials*.csv',
            '*fundamentals*.csv',
        ]
        found: list[Path] = []
        for folder in [PATHS.base_dir, PATHS.data_dir]:
            for pattern in patterns:
                for path in folder.glob(pattern):
                    if path.exists() and path not in found:
                        found.append(path)
        if self.paths.csv_path.exists() and self.paths.csv_path not in found:
            found.insert(0, self.paths.csv_path)
        return found

    # -----------------------------
    # data normalization
    # -----------------------------
    @staticmethod
    def _safe_float(value: Any) -> float | None:
        try:
            if value is None or pd.isna(value):
                return None
            return round(float(value), 2)
        except Exception:
            return None

    @staticmethod
    def _safe_int(value: Any) -> int | None:
        try:
            if value is None or pd.isna(value):
                return None
            return int(float(value))
        except Exception:
            return None

    def normalize_dataframe(self, df: pd.DataFrame | None) -> pd.DataFrame:
        if df is None or df.empty:
            return pd.DataFrame(columns=DATA_COLUMNS)
        out = df.copy()
        for col in DATA_COLUMNS:
            if col not in out.columns:
                out[col] = None
        out = out[DATA_COLUMNS].copy()
        out = out.where(pd.notnull(out), None)
        if 'Ticker SYMBOL' in out.columns:
            out['Ticker SYMBOL'] = out['Ticker SYMBOL'].astype(str).str.strip().str.upper()
        if '資料年月日' in out.columns:
            out['資料年月日'] = pd.to_datetime(out['資料年月日'], errors='coerce').dt.strftime('%Y-%m-%d')
        out = out.dropna(subset=['Ticker SYMBOL', '資料年月日'], how='any')
        out = out.drop_duplicates(subset=['Ticker SYMBOL', '資料年月日']).reset_index(drop=True)
        return out

    def load_existing_csv(self, path: Path | None = None) -> pd.DataFrame:
        target = path or self.paths.csv_path
        if not target.exists():
            return self.normalize_dataframe(None)
        try:
            return self.normalize_dataframe(pd.read_csv(target, encoding='utf-8-sig'))
        except Exception:
            try:
                return self.normalize_dataframe(pd.read_csv(target))
            except Exception:
                return self.normalize_dataframe(None)

    def consolidate_existing_backups(self) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        frames: list[pd.DataFrame] = []
        meta: list[dict[str, Any]] = []
        for path in self._search_existing_backups():
            df = self.load_existing_csv(path)
            meta.append({
                'path': str(path),
                'rows': int(len(df)),
                'columns': [str(c) for c in df.columns.tolist()],
            })
            if not df.empty:
                frames.append(df)
        if frames:
            merged = self.normalize_dataframe(pd.concat(frames, ignore_index=True))
        else:
            merged = self.normalize_dataframe(None)
        return merged, meta

    def save_csv(self, df: pd.DataFrame) -> Path:
        normalized = self.normalize_dataframe(df)
        normalized.to_csv(self.paths.csv_path, index=False, encoding='utf-8-sig')
        return self.paths.csv_path

    # -----------------------------
    # Yahoo fetch and SQL import
    # -----------------------------
    def _to_ymd(self, value: Any) -> str | None:
        try:
            return pd.to_datetime(value).strftime('%Y-%m-%d')
        except Exception:
            return None

    def _get_safe_value(self, df: pd.DataFrame | None, key: str, date_col: Any) -> Any:
        if df is None or df.empty or key not in df.index or date_col not in df.columns:
            return None
        value = df.loc[key, date_col]
        return value if pd.notna(value) else None

    def fetch_single_stock(self, stock_id: str, existing_keys: set[tuple[str, str]]) -> list[dict[str, Any]]:
        if yf is None:
            return []
        possible_suffixes = ['.TW', '.TWO']
        for suffix in possible_suffixes:
            ticker_symbol = f'{stock_id}{suffix}'
            try:
                ticker = yf.Ticker(ticker_symbol)
                is_df = ticker.quarterly_financials
                bs_df = ticker.quarterly_balance_sheet
                cf_df = ticker.quarterly_cashflow
                if is_df is None or is_df.empty:
                    continue
                div_yield = None
                try:
                    info = getattr(ticker, 'info', {}) or {}
                    raw = info.get('dividendYield')
                    div_yield = round(float(raw) * 100, 2) if raw is not None else None
                except Exception:
                    div_yield = None

                rows: list[dict[str, Any]] = []
                for date_col in list(is_df.columns)[: self.TARGET_REPORTS_PER_STOCK]:
                    report_date = self._to_ymd(date_col)
                    key = (ticker_symbol, str(report_date))
                    if not report_date or key in existing_keys:
                        continue
                    revenue = self._get_safe_value(is_df, 'Total Revenue', date_col)
                    gross_profit = self._get_safe_value(is_df, 'Gross Profit', date_col)
                    op_income = self._get_safe_value(is_df, 'Operating Income', date_col)
                    pre_tax = self._get_safe_value(is_df, 'Pretax Income', date_col)
                    net_income = self._get_safe_value(is_df, 'Net Income', date_col)
                    eps = self._get_safe_value(is_df, 'Diluted EPS', date_col)
                    assets = self._get_safe_value(bs_df, 'Total Assets', date_col)
                    liabilities = self._get_safe_value(bs_df, 'Total Liabilities Net Minority Interest', date_col)
                    equity = self._get_safe_value(bs_df, 'Total Equity Gross Minority Interest', date_col)
                    cash_flow = self._get_safe_value(cf_df, 'Operating Cash Flow', date_col)

                    gross_margin = (gross_profit / revenue * 100) if revenue not in [None, 0] and gross_profit is not None else None
                    op_margin = (op_income / revenue * 100) if revenue not in [None, 0] and op_income is not None else None
                    net_margin = (net_income / revenue * 100) if revenue not in [None, 0] and net_income is not None else None
                    roe = (net_income / equity * 100) if equity not in [None, 0] and net_income is not None else None
                    debt_ratio = (liabilities / assets * 100) if assets not in [None, 0] and liabilities is not None else None
                    core_profit_ratio = (op_income / pre_tax * 100) if pre_tax not in [None, 0] and op_income is not None else None
                    rows.append({
                        'Ticker SYMBOL': ticker_symbol,
                        '資料年月日': report_date,
                        '毛利率(%)': self._safe_float(gross_margin),
                        '營業利益率(%)': self._safe_float(op_margin),
                        '單季EPS': self._safe_float(eps),
                        'ROE(%)': self._safe_float(roe),
                        '稅後淨利率(%)': self._safe_float(net_margin),
                        '營業現金流': self._safe_int(cash_flow),
                        '預估殖利率(%)': self._safe_float(div_yield),
                        '負債比率(%)': self._safe_float(debt_ratio),
                        '本業獲利比(%)': self._safe_float(core_profit_ratio),
                    })
                if rows:
                    return rows
            except Exception:
                continue
        return []

    def _ensure_sql_table(self, cursor) -> None:
        cursor.execute(f"""
        IF OBJECT_ID(N'{self.TABLE_NAME}', N'U') IS NULL
        BEGIN
            CREATE TABLE {self.TABLE_NAME} (
                [Ticker SYMBOL] NVARCHAR(20),
                [資料年月日] DATE,
                [毛利率(%)] DECIMAL(18,2),
                [營業利益率(%)] DECIMAL(18,2),
                [單季EPS] DECIMAL(18,2),
                [ROE(%)] DECIMAL(18,2),
                [稅後淨利率(%)] DECIMAL(18,2),
                [營業現金流] BIGINT,
                [預估殖利率(%)] DECIMAL(18,2),
                [負債比率(%)] DECIMAL(18,2),
                [本業獲利比(%)] DECIMAL(18,2),
                [更新時間] DATETIME
            )
        END
        """)

    def import_df_to_sql(self, df: pd.DataFrame) -> dict[str, Any]:
        normalized = self.normalize_dataframe(df)
        if normalized.empty:
            return {'sql_import_attempted': False, 'sql_imported_rows': 0, 'sql_error': None}
        if pyodbc is None:
            return {'sql_import_attempted': False, 'sql_imported_rows': 0, 'sql_error': 'pyodbc_not_installed'}

        conn_str = (
            rf'DRIVER={{{getattr(CONFIG, "db_driver", "ODBC Driver 17 for SQL Server")}}};'
            rf'SERVER={getattr(CONFIG, "db_server", "localhost")};'
            rf'DATABASE={getattr(CONFIG, "db_database", "股票online")};'
            r'Trusted_Connection=yes;'
        )
        count = 0
        try:
            with pyodbc.connect(conn_str) as conn:
                cursor = conn.cursor()
                self._ensure_sql_table(cursor)
                conn.commit()
                for _, row in normalized.iterrows():
                    cursor.execute(
                        f"""
                        IF EXISTS (
                            SELECT 1 FROM {self.TABLE_NAME}
                            WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?
                        )
                        BEGIN
                            UPDATE {self.TABLE_NAME}
                            SET [毛利率(%)] = ?, [營業利益率(%)] = ?, [單季EPS] = ?,
                                [ROE(%)] = ?, [稅後淨利率(%)] = ?, [營業現金流] = ?,
                                [預估殖利率(%)] = ?, [負債比率(%)] = ?, [本業獲利比(%)] = ?,
                                [更新時間] = GETDATE()
                            WHERE [Ticker SYMBOL] = ? AND [資料年月日] = ?
                        END
                        ELSE
                        BEGIN
                            INSERT INTO {self.TABLE_NAME}
                            ([Ticker SYMBOL], [資料年月日], [毛利率(%)], [營業利益率(%)], [單季EPS], [ROE(%)],
                             [稅後淨利率(%)], [營業現金流], [預估殖利率(%)], [負債比率(%)], [本業獲利比(%)], [更新時間])
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE())
                        END
                        """,
                        row['Ticker SYMBOL'], row['資料年月日'],
                        row['毛利率(%)'], row['營業利益率(%)'], row['單季EPS'], row['ROE(%)'], row['稅後淨利率(%)'], row['營業現金流'], row['預估殖利率(%)'], row['負債比率(%)'], row['本業獲利比(%)'],
                        row['Ticker SYMBOL'], row['資料年月日'],
                        row['Ticker SYMBOL'], row['資料年月日'],
                        row['毛利率(%)'], row['營業利益率(%)'], row['單季EPS'], row['ROE(%)'], row['稅後淨利率(%)'], row['營業現金流'], row['預估殖利率(%)'], row['負債比率(%)'], row['本業獲利比(%)'],
                    )
                    count += 1
                conn.commit()
            return {'sql_import_attempted': True, 'sql_imported_rows': count, 'sql_error': None}
        except Exception as exc:
            return {'sql_import_attempted': True, 'sql_imported_rows': count, 'sql_error': str(exc)}

    # -----------------------------
    # stock universe
    # -----------------------------
    def _stock_list_from_existing_csv(self, merged_df: pd.DataFrame) -> list[str]:
        if merged_df.empty:
            return []
        ids = []
        for ticker in merged_df['Ticker SYMBOL'].dropna().astype(str).tolist():
            base = ticker.split('.')[0].strip()
            if len(base) == 4 and base.isdigit() and base not in ids:
                ids.append(base)
        return ids

    # -----------------------------
    # public entrypoints
    # -----------------------------
    def build_summary(self, mode: str = 'local_sync_only') -> tuple[Path, dict[str, Any]]:
        csv_path, csv_created = self._ensure_csv_template()
        template_path = self._write_template_json()
        consolidated_df, discovered = self.consolidate_existing_backups()
        saved_csv = self.save_csv(consolidated_df if not consolidated_df.empty else self.normalize_dataframe(None))
        sql_status = self.import_df_to_sql(consolidated_df)

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'system_name': CONFIG.system_name,
            'mode': mode,
            'csv_path': str(csv_path),
            'csv_template_created': csv_created,
            'config_template_path': str(template_path),
            'discovered_backups': discovered,
            'merged_rows': int(len(consolidated_df)),
            'saved_csv_path': str(saved_csv),
            'sql_status': sql_status,
            'legacy_dependency_removed': True,
            'legacy_compat_script_present': (PATHS.base_dir / 'yahoo_csv_to_sql.py').exists(),
            'network_fetch_available': yf is not None,
            'network_fetch_enabled': False,
            'auto_outputs': [
                str(csv_path),
                str(template_path),
                str(self.paths.runtime_path),
            ],
            'status': 'fundamentals_etl_mainline_ready',
        }
        self.paths.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📚 fundamentals ETL 主線完成：{self.paths.runtime_path}')
        return self.paths.runtime_path, payload

    def smart_sync(self, target_stocks: list[str] | None = None, enable_network_fetch: bool = False, write_sql: bool = True) -> tuple[Path, dict[str, Any]]:
        csv_path, csv_created = self._ensure_csv_template()
        template_path = self._write_template_json()
        local_df = self.load_existing_csv(csv_path)
        consolidated_df, discovered = self.consolidate_existing_backups()
        if not consolidated_df.empty and len(consolidated_df) > len(local_df):
            local_df = consolidated_df
        existing_keys = set((str(r['Ticker SYMBOL']).strip(), str(r['資料年月日']).strip()) for _, r in local_df.iterrows())

        if not target_stocks:
            target_stocks = self._stock_list_from_existing_csv(local_df)
        if not target_stocks:
            target_stocks = ['2330', '2317', '2454']

        fetched_rows: list[dict[str, Any]] = []
        fetch_errors: list[str] = []
        if enable_network_fetch and yf is not None:
            for stock_id in target_stocks:
                try:
                    rows = self.fetch_single_stock(stock_id, existing_keys)
                    if rows:
                        fetched_rows.extend(rows)
                        for row in rows:
                            existing_keys.add((str(row['Ticker SYMBOL']), str(row['資料年月日'])))
                except Exception as exc:
                    fetch_errors.append(f'{stock_id}: {exc}')
        elif enable_network_fetch and yf is None:
            fetch_errors.append('yfinance_not_installed')

        fetched_df = self.normalize_dataframe(pd.DataFrame(fetched_rows, columns=DATA_COLUMNS))
        merged = self.normalize_dataframe(pd.concat([local_df, fetched_df], ignore_index=True))
        saved_csv = self.save_csv(merged)
        sql_status = self.import_df_to_sql(merged) if write_sql else {'sql_import_attempted': False, 'sql_imported_rows': 0, 'sql_error': None}

        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'system_name': CONFIG.system_name,
            'mode': 'smart_sync',
            'csv_path': str(csv_path),
            'csv_template_created': csv_created,
            'config_template_path': str(template_path),
            'network_fetch_enabled': bool(enable_network_fetch),
            'network_fetch_available': yf is not None,
            'target_stocks': target_stocks,
            'discovered_backups': discovered,
            'preexisting_rows': int(len(local_df)),
            'new_rows_fetched': int(len(fetched_df)),
            'final_rows': int(len(merged)),
            'fetch_errors': fetch_errors,
            'saved_csv_path': str(saved_csv),
            'sql_status': sql_status,
            'legacy_dependency_removed': True,
            'auto_outputs': [str(saved_csv), str(template_path), str(self.paths.runtime_path)],
            'status': 'fundamentals_etl_mainline_ready' if not fetch_errors else 'fundamentals_etl_mainline_partial',
        }
        self.paths.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'📚 fundamentals ETL smart sync 完成：{self.paths.runtime_path}')
        return self.paths.runtime_path, payload


def main() -> int:
    FundamentalsETLMainline().build_summary()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
