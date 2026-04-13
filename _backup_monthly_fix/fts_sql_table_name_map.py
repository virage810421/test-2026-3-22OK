# -*- coding: utf-8 -*-
from __future__ import annotations

"""中央 SQL table 名稱映射層。

用途：
1. 讓 Python 主線查表字串不要再直接硬寫英文表名。
2. 支援「英文邏輯名稱 -> 中文實體表名」的統一轉換。
3. 即使傳入已是中文表名，也會安全回傳可直接放進 SQL 的完整名稱。
"""

from typing import Dict

EN_TO_ZH_TABLE: Dict[str, str] = {
    'trade_history': '交易歷史',
    'active_positions': '持有部位',
    'daily_chip_data': '每日籌碼資料',
    'fundamentals_clean': '基本面清洗',
    # 月營收主線目前的實體表仍以英文表名為主；避免中文映射造成 ETL/SQL 寫入找不到物件。
    'monthly_revenue_simple': 'monthly_revenue_simple',
    'account_info': '帳戶資訊',
    'execution_orders': '執行委託單',
    'execution_fills': '執行成交回報',
    'execution_account_snapshot': '帳戶快照',
    'execution_positions_snapshot': '部位快照',
    'feature_cross_section_snapshot': '特徵截面快照',
    'feature_event_calendar': '特徵事件日曆',
    'live_feature_mount': '實戰特徵掛載',
    'training_feature_registry': '訓練特徵註冊表',
    'task_completion_registry': '任務完成註冊表',
    'integration_run_registry': '整合執行註冊表',
    'system_guard_log': '系統守護日誌',
    'risk_gateway_log': '風控閘道日誌',
    'stock_master': '股票主檔',
    'company_quality_snapshot': '公司體質快照',
    'revenue_momentum_snapshot': '營收動能快照',
    'price_liquidity_daily': '流動性日表',
    'chip_factors_daily': '籌碼因子日表',
    'training_universe_daily': '訓練母池每日名單',
    'training_ticker_scoreboard': '訓練個股成績單',
}


def qident(name: str) -> str:
    return '[' + str(name).replace(']', ']]') + ']'


def normalize_table_key(table_name: str) -> str:
    raw = str(table_name or '').strip()
    if not raw:
        return raw
    raw = raw.replace('[', '').replace(']', '')
    if '.' in raw:
        raw = raw.split('.')[-1]
    return EN_TO_ZH_TABLE.get(raw, raw)


def sql_table(table_name: str, schema: str | None = 'dbo') -> str:
    mapped = normalize_table_key(table_name)
    if schema:
        return f"{qident(schema)}.{qident(mapped)}"
    return qident(mapped)


def get_table_alias(table_name: str) -> str:
    return normalize_table_key(table_name)
