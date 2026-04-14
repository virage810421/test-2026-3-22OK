# -*- coding: utf-8 -*-
from __future__ import annotations

"""
SQL 中文欄名查詢層 / View 同步器。

設計原則：
1. 不破壞 Python 主線與 broker/execution contract。
2. 底層正式表仍維持英文/既有欄位，避免 logger / execution / reconciliation 壞掉。
3. 每一張 dbo user table 都建立一張「中文欄位 View」，View 會即時讀底層 Table，所以資料天然同步。
4. 每次 migration 都會先清掉本工具管理的中文欄位 View，再依目前實際 Table/Column 重建。
5. 這不是 sp_rename，不會把實體欄位改中文；要破壞式中文實體欄位需另開 schema_zh_v2。
"""

from typing import Any, Iterable

try:
    from fts_db_schema import iter_table_specs
except Exception:  # pragma: no cover
    def iter_table_specs() -> Iterable[Any]:
        return []

try:
    from fts_runtime_diagnostics import record_issue
except Exception:  # pragma: no cover - diagnostics must not block migration
    def record_issue(*args: Any, **kwargs: Any) -> dict[str, Any]:
        return {}

MANAGED_VIEW_MARKER = 'FTS_CHINESE_COLUMN_VIEW_MANAGED_V2'

# 常用 table -> 中文 view 名。沒有列在這裡的 table 仍會自動建立：<table>_中文欄位。
TABLE_VIEW_NAME_MAP: dict[str, str] = {
    # legacy / trading records
    'trade_history': '交易紀錄_中文欄位',
    'active_positions': '目前持倉_中文欄位',
    'account_info': '帳戶資訊_中文欄位',

    # ETL / research / features
    'daily_chip_data': '法人籌碼日資料_中文欄位',
    'monthly_revenue_simple': '月營收資料_中文欄位',
    'fundamentals_clean': '基本面清洗資料_中文欄位',
    'stock_master': '股票主檔_中文欄位',
    'feature_cross_section_snapshot': '全市場百分位快照_中文欄位',
    'feature_event_calendar': '事件日曆_中文欄位',
    'live_feature_mount': '即時特徵掛載_中文欄位',
    'training_feature_registry': '訓練特徵註冊表_中文欄位',
    'training_universe_daily': '每日訓練股票池_中文欄位',
    'company_quality_snapshot': '公司品質快照_中文欄位',
    'revenue_momentum_snapshot': '營收動能快照_中文欄位',
    'price_liquidity_daily': '價格流動性日資料_中文欄位',
    'chip_factors_daily': '籌碼因子日資料_中文欄位',

    # execution / broker / accounting
    'execution_orders': '執行委託單_中文欄位',
    'execution_fills': '執行成交明細_中文欄位',
    'execution_account_snapshot': '執行帳戶快照_中文欄位',
    'execution_positions_snapshot': '執行持倉快照_中文欄位',
    'execution_position_lots': '執行持倉批次_中文欄位',
    'execution_broker_callbacks': '券商回報事件_中文欄位',
    'execution_reconciliation_report': '執行對帳報告_中文欄位',
    'execution_tax_lot_closures': '稅務批次平倉明細_中文欄位',
    'execution_tax_lot_summary': '稅務批次年度彙總_中文欄位',

    # system / registry / logs
    'schema_migrations': '資料庫版本遷移紀錄_中文欄位',
    'integration_run_registry': '整合執行註冊表_中文欄位',
    'task_completion_registry': '任務完成註冊表_中文欄位',
    'system_guard_log': '系統防護紀錄_中文欄位',
    'risk_gateway_log': '風控閘門紀錄_中文欄位',
}

COLUMN_NAME_MAP: dict[str, str] = {
    # common
    'id': '編號',
    'version': '版本',
    'applied_at': '套用時間',
    'created_at': '建立時間',
    'updated_at': '更新時間',
    'generated_at': '產生時間',
    'date': '日期',
    'note': '備註',
    'status': '狀態',
    'source': '資料來源',
    'raw_json': '原始JSON',
    'summary_json': '摘要JSON',
    'path': '路徑',
    'run_id': '執行ID',
    'task_name': '任務名稱',
    'completed_at': '完成時間',
    'module_name': '模組名稱',
    'component': '元件',
    'severity': '嚴重度',
    'message': '訊息',
    'error': '錯誤',
    'reason': '原因',
    'category': '分類',
    'payload_json': '內容JSON',

    # ticker / contract
    'ticker_symbol': '股票代號',
    'Ticker SYMBOL': '股票代號',
    'symbol': '股票代號',
    'company_name': '公司名稱',
    'company_code': '公司代號',
    'industry': '產業類別',
    'industry_name': '產業類別名稱',
    'sector': '產業群組',

    # execution order/fill
    'order_id': '委託單ID',
    'client_order_id': '客戶委託ID',
    'broker_order_id': '券商委託ID',
    'direction_bucket': '方向分類',
    'strategy_bucket': '策略分類',
    'strategy_name': '策略名稱',
    'qty': '委託數量',
    'filled_qty': '已成交數量',
    'remaining_qty': '剩餘數量',
    'avg_fill_price': '平均成交價',
    'order_type': '委託類型',
    'submitted_price': '送出價格',
    'ref_price': '參考價格',
    'reject_reason': '拒單原因',
    'signal_id': '訊號ID',
    'signal_score': '訊號分數',
    'ai_confidence': 'AI信心分數',
    'fill_id': '成交ID',
    'fill_qty': '成交數量',
    'fill_price': '成交價格',
    'fill_time': '成交時間',
    'commission': '手續費',
    'tax': '交易稅',
    'slippage': '滑價',

    # account/position
    'snapshot_time': '快照時間',
    'account_name': '帳戶名稱',
    'cash': '現金',
    'market_value': '市值',
    'equity': '權益總值',
    'buying_power': '可用購買力',
    'unrealized_pnl': '未實現損益',
    'realized_pnl': '已實現損益',
    'day_pnl': '當日損益',
    'exposure_ratio': '曝險比例',
    'currency': '幣別',
    'broker_type': '券商類型',
    'available_qty': '可用數量',
    'avg_cost': '平均成本',
    'market_price': '市價',

    # lots / callbacks / reconciliation
    'lot_id': '持倉批次ID',
    'tax_lot_id': '稅務批次ID',
    'open_qty': '開倉數量',
    'entry_price': '進場價格',
    'entry_time': '進場時間',
    'close_time': '平倉時間',
    'entry_order_id': '進場委託ID',
    'exit_order_id': '出場委託ID',
    'position_key': '持倉鍵',
    'cost_basis_method': '成本基礎方法',
    'entry_fill_qty': '進場成交數量',
    'close_fill_qty': '平倉成交數量',
    'entry_fill_count': '進場成交筆數',
    'close_fill_count': '平倉成交筆數',
    'entry_fill_ids_json': '進場成交ID清單',
    'exit_fill_ids_json': '出場成交ID清單',
    'open_commission': '開倉手續費',
    'open_tax': '開倉交易稅',
    'close_commission': '平倉手續費',
    'close_tax': '平倉交易稅',
    'stop_order_id': '停損委託ID',
    'stop_price': '停損價格',
    'stop_status': '停損狀態',
    'linked_stop_qty': '連動停損數量',
    'last_fill_time': '最後成交時間',
    'callback_id': '回報ID',
    'event_type': '事件類型',
    'fill_notional': '成交金額',
    'callback_time': '回報時間',
    'ingested_at': '匯入時間',
    'reconcile_id': '對帳ID',
    'reconcile_time': '對帳時間',
    'order_mismatch_count': '委託不一致數',
    'fill_mismatch_count': '成交不一致數',
    'position_mismatch_count': '持倉不一致數',
    'lot_mismatch_count': '批次不一致數',
    'cash_diff': '現金差異',

    # tax lot accounting
    'tax_event_id': '稅務事件ID',
    'asset_class': '資產類別',
    'jurisdiction': '稅務管轄區',
    'tax_regime': '稅制分類',
    'tax_treatment': '稅務處理方式',
    'report_type': '報表類型',
    'exit_fill_id': '出場成交ID',
    'closed_qty': '平倉數量',
    'acquisition_date': '取得日期',
    'disposal_date': '處分日期',
    'cost_basis_price': '成本基礎價格',
    'close_price': '平倉價格',
    'gross_proceeds': '總處分收入',
    'allocated_cost_basis': '分攤成本基礎',
    'net_proceeds': '淨處分收入',
    'realized_gross_pnl': '已實現毛損益',
    'realized_net_pnl': '已實現淨損益',
    'taxable_gain_loss': '應稅損益',
    'ordinary_income_amount': '普通所得金額',
    'section1256_60pct_amount': '一二五六條款六成金額',
    'section1256_40pct_amount': '一二五六條款四成金額',
    'holding_period_days': '持有天數',
    'holding_period_bucket': '持有期間分類',
    'tax_year': '稅務年度',
    'wash_sale_applicable': '洗售規則適用',
    'wash_sale_applied': '洗售規則已套用',
    'wash_sale_adjustment': '洗售調整金額',
    'wash_sale_disallowed_loss': '洗售不得認列損失',
    'wash_sale_replacement_lot_ids': '洗售替代批次ID清單',
    'wash_sale_window_start': '洗售窗口開始日',
    'wash_sale_window_end': '洗售窗口結束日',
    'specific_id_tag': '指定批次標籤',
    'summary_id': '彙總ID',
    'open_lot_count': '未平倉批次數',

    # feature/research common
    'feature_name': '特徵名稱',
    'feature_value': '特徵值',
    'feature_date': '特徵日期',
    'score': '分數',
    'rank': '排名',
    'percentile': '百分位',
    'event_date': '事件日期',
    'event_name': '事件名稱',
    'event_type_name': '事件類型名稱',
    'is_trading_day': '是否交易日',
    'data_date': '資料日期',
    '資料年月日': '資料年月日',
}


def _quote_ident(name: str) -> str:
    return '[' + str(name).replace(']', ']]') + ']'


def _sql_n_literal(value: str) -> str:
    return "N'" + str(value).replace("'", "''") + "'"


def _fetch_rows(db: Any, sql: str, params: list[Any] | None = None) -> list[Any]:
    result = db.execute(sql, params or [])
    try:
        return list(result.fetchall())
    except Exception:
        return []


def _row_value(row: Any, idx: int) -> Any:
    try:
        return row[idx]
    except Exception:
        return None


def _has_chinese(value: str) -> bool:
    return any('\u4e00' <= ch <= '\u9fff' for ch in str(value))


def _compact_ascii(value: str) -> str:
    return ''.join(ch for ch in str(value).replace('_', '') if ch.isalnum()) or '未命名'


def table_chinese_view_name(table_name: str) -> str:
    """回傳 table 對應中文欄位 view 名。未知 table 也會建立 view，確保 Table/View 同步。"""
    if table_name in TABLE_VIEW_NAME_MAP:
        return TABLE_VIEW_NAME_MAP[table_name]
    if _has_chinese(table_name):
        return f'{table_name}_中文欄位'
    return f'資料表_{_compact_ascii(table_name)}_中文欄位'


def chinese_column_name(column_name: str) -> str:
    """回傳 SQL view 用中文欄名；原本已中文的欄位保持不變。"""
    if _has_chinese(column_name):
        return column_name
    return COLUMN_NAME_MAP.get(column_name, '欄位_' + _compact_ascii(column_name))


def _schema_spec_table_names() -> set[str]:
    names: set[str] = set()
    for table in iter_table_specs():
        name = getattr(table, 'name', None)
        if name:
            names.add(str(name))
    return names


def _list_user_tables(db: Any) -> list[str]:
    rows = _fetch_rows(db, """
        SELECT t.name
        FROM sys.tables AS t
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = N'dbo'
          AND t.is_ms_shipped = 0
        ORDER BY t.name
    """)
    return [str(_row_value(r, 0)) for r in rows if _row_value(r, 0)]


def _list_columns(db: Any, table_name: str) -> list[str]:
    rows = _fetch_rows(db, """
        SELECT c.name
        FROM sys.columns AS c
        JOIN sys.tables AS t ON t.object_id = c.object_id
        JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = N'dbo'
          AND t.name = ?
        ORDER BY c.column_id
    """, [table_name])
    return [str(_row_value(r, 0)) for r in rows if _row_value(r, 0)]


def _list_managed_or_zh_views(db: Any) -> list[str]:
    rows = _fetch_rows(db, """
        SELECT v.name
        FROM sys.views AS v
        JOIN sys.schemas AS s ON s.schema_id = v.schema_id
        WHERE s.name = N'dbo'
          AND (
                v.name LIKE N'%中文欄位'
                OR OBJECT_DEFINITION(v.object_id) LIKE N'%FTS_CHINESE_COLUMN_VIEW_MANAGED%'
          )
        ORDER BY v.name
    """)
    names = {str(_row_value(r, 0)) for r in rows if _row_value(r, 0)}
    # 加入目前 map 的已知名稱，確保舊版同名 view 也會被重建。
    names.update(TABLE_VIEW_NAME_MAP.values())
    return sorted(names)


def _select_items_for_columns(columns: list[str]) -> list[str]:
    select_items: list[str] = []
    used_aliases: set[str] = set()
    for col in columns:
        alias = chinese_column_name(col)
        if alias in used_aliases:
            base = alias
            i = 2
            while f'{base}_{i}' in used_aliases:
                i += 1
            alias = f'{base}_{i}'
        used_aliases.add(alias)
        select_items.append(f'    {_quote_ident(col)} AS {_quote_ident(alias)}')
    return select_items


def iter_chinese_view_specs(db: Any | None = None) -> list[dict[str, Any]]:
    """
    產生中文 view specs。
    - 有 db：依照 DB 目前所有 dbo user tables 動態同步。
    - 沒 db：退回 fts_db_schema.py 靜態 specs，方便離線列印。
    """
    specs: list[dict[str, Any]] = []
    if db is not None:
        for table_name in _list_user_tables(db):
            columns = _list_columns(db, table_name)
            if not columns:
                continue
            specs.append({
                'table_name': table_name,
                'view_name': table_chinese_view_name(table_name),
                'select_items': _select_items_for_columns(columns),
                'source': 'db_dynamic',
            })
        return specs

    for table in iter_table_specs():
        columns = [col.name for col in getattr(table, 'columns', [])]
        specs.append({
            'table_name': table.name,
            'view_name': table_chinese_view_name(table.name),
            'select_items': _select_items_for_columns(columns),
            'source': 'schema_static',
        })
    return specs


def ensure_chinese_column_views(db: Any) -> list[str]:
    """同步所有 dbo user table 的中文欄位 view。"""
    actions: list[str] = []

    # 先清掉本工具管理的中文欄位 views，避免舊版 view 名或欄位不同步。
    for view_name in _list_managed_or_zh_views(db):
        try:
            db.execute(
                f"IF OBJECT_ID({_sql_n_literal('dbo.' + view_name)}, N'V') IS NOT NULL DROP VIEW dbo.{_quote_ident(view_name)};"
            )
            actions.append(f'view:{view_name}:dropped_before_sync')
        except Exception as exc:
            record_issue(
                'sql_chinese_column_views',
                'drop_old_chinese_column_view_failed',
                exc,
                severity='WARNING',
                fail_mode='fail_open',
                context={'view': view_name},
            )
            actions.append(f'view:{view_name}:drop_failed:{type(exc).__name__}:{exc}')

    table_names = set(_list_user_tables(db))
    schema_tables = _schema_spec_table_names()
    specs = iter_chinese_view_specs(db)
    for spec in specs:
        table_name = spec['table_name']
        view_name = spec['view_name']
        if table_name not in table_names:
            actions.append(f'{table_name}:table_missing_skip_chinese_view')
            continue
        select_sql = ',\n'.join(spec['select_items'])
        create_view_sql = (
            f"CREATE VIEW dbo.{_quote_ident(view_name)} AS\n"
            f"/* {MANAGED_VIEW_MARKER}; source_table=dbo.{table_name}; schema_declared={str(table_name in schema_tables).lower()} */\n"
            f"SELECT\n{select_sql}\nFROM dbo.{_quote_ident(table_name)};"
        )
        try:
            db.execute(create_view_sql)
            actions.append(f'{table_name}:synced_view:{view_name}')
        except Exception as exc:
            record_issue(
                'sql_chinese_column_views',
                'create_chinese_column_view_failed',
                exc,
                severity='ERROR',
                fail_mode='fail_closed',
                context={'table': table_name, 'view': view_name},
            )
            actions.append(f'{table_name}:view_failed:{type(exc).__name__}:{exc}')
    actions.append(f'summary:tables={len(table_names)}:views_synced={len(specs)}:strategy=all_dbo_user_tables_dynamic_sync')
    return actions


if __name__ == '__main__':
    for item in iter_chinese_view_specs(None):
        print(f"{item['table_name']} -> {item['view_name']} ({len(item['select_items'])} columns)")
