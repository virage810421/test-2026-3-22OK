# -*- coding: utf-8 -*-
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

SCHEMA_VERSION = '20260414_schema_single_source_execution_ticker_symbol_v4_chinese_column_views'


@dataclass(frozen=True)
class ColumnSpec:
    name: str
    sql_type: str
    nullable: bool = True
    default_sql: str | None = None
    primary_key: bool = False

    def ddl(self) -> str:
        chunks = [f'[{self.name}]', self.sql_type]
        chunks.append('NULL' if self.nullable else 'NOT NULL')
        if self.default_sql is not None:
            chunks.append(f'DEFAULT {self.default_sql}')
        return ' '.join(chunks)


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[ColumnSpec, ...] = field(default_factory=tuple)

    @property
    def primary_keys(self) -> list[str]:
        return [c.name for c in self.columns if c.primary_key]


def _c(name, sql_type, nullable=True, default_sql=None, primary_key=False):
    return ColumnSpec(name=name, sql_type=sql_type, nullable=nullable, default_sql=default_sql, primary_key=primary_key)


# LEGACY_SCHEMA_COMPAT_MARKER: Ticker SYMBOL appears only in data/research/legacy table schema; execution_* tables use ticker_symbol.
CORE_TABLES: tuple[TableSpec, ...] = (
    TableSpec('trade_history', (
        _c('策略名稱', 'NVARCHAR(50)'), _c('Ticker SYMBOL', 'VARCHAR(20)', nullable=False, default_sql="''"), _c('ticker_symbol', 'VARCHAR(20)'), _c('方向', 'NVARCHAR(10)'),
        _c('進場時間', 'DATETIME'), _c('出場時間', 'DATETIME'), _c('進場價', 'FLOAT'), _c('出場價', 'FLOAT'),
        _c('報酬率(%)', 'DECIMAL(10,3)'), _c('淨損益金額', 'FLOAT'), _c('結餘本金', 'FLOAT'), _c('市場狀態', 'NVARCHAR(50)'),
        _c('進場陣型', 'NVARCHAR(50)'), _c('期望值', 'DECIMAL(10,3)'), _c('預期停損(%)', 'DECIMAL(10,3)'), _c('預期停利(%)', 'DECIMAL(10,3)'),
        _c('風報比(RR)', 'DECIMAL(10,3)'), _c('風險金額', 'FLOAT'),
    )),
    TableSpec('active_positions', (
        _c('Ticker SYMBOL', 'VARCHAR(20)', nullable=False, default_sql="''"), _c('ticker_symbol', 'VARCHAR(20)'), _c('方向', 'NVARCHAR(10)'), _c('進場時間', 'DATETIME'),
        _c('進場價', 'FLOAT'), _c('投入資金', 'FLOAT'), _c('停利階段', 'INT'), _c('進場股數', 'INT'), _c('市場狀態', 'NVARCHAR(50)'),
        _c('進場陣型', 'NVARCHAR(50)'), _c('期望值', 'DECIMAL(10,3)'), _c('預期停損(%)', 'DECIMAL(10,3)'), _c('預期停利(%)', 'DECIMAL(10,3)'),
        _c('風報比(RR)', 'DECIMAL(10,3)'), _c('風險金額', 'FLOAT'),
    )),
    TableSpec('daily_chip_data', (
        _c('日期', 'DATE', nullable=False, primary_key=True), _c('Ticker SYMBOL', 'NVARCHAR(20)', nullable=False, primary_key=True),
        _c('外資買賣超', 'FLOAT'), _c('投信買賣超', 'FLOAT'), _c('自營商買賣超', 'FLOAT'), _c('三大法人合計', 'FLOAT'),
        _c('資料來源', 'NVARCHAR(20)'), _c('更新時間', 'DATETIME'),
    )),
    TableSpec('execution_orders', (
        _c('order_id', 'NVARCHAR(64)', nullable=False, primary_key=True), _c('client_order_id', 'NVARCHAR(64)'), _c('broker_order_id', 'NVARCHAR(64)'),
        _c('ticker_symbol', 'NVARCHAR(32)'), _c('direction_bucket', 'NVARCHAR(16)'), _c('strategy_bucket', 'NVARCHAR(64)'), _c('status', 'NVARCHAR(32)'),
        _c('qty', 'INT'), _c('filled_qty', 'INT'), _c('remaining_qty', 'INT'), _c('avg_fill_price', 'DECIMAL(18,4)'), _c('order_type', 'NVARCHAR(20)'),
        _c('submitted_price', 'DECIMAL(18,4)'), _c('ref_price', 'DECIMAL(18,4)'), _c('reject_reason', 'NVARCHAR(255)'), _c('signal_id', 'NVARCHAR(100)'),
        _c('industry', 'NVARCHAR(64)'), _c('signal_score', 'DECIMAL(18,4)'), _c('ai_confidence', 'DECIMAL(18,4)'), _c('note', 'NVARCHAR(MAX)'),
        _c('created_at', 'DATETIME'), _c('updated_at', 'DATETIME'),
    )),
    TableSpec('execution_fills', (
        _c('fill_id', 'NVARCHAR(64)', nullable=False, primary_key=True), _c('order_id', 'NVARCHAR(64)'), _c('ticker_symbol', 'NVARCHAR(32)'),
        _c('direction_bucket', 'NVARCHAR(16)'), _c('fill_qty', 'INT'), _c('fill_price', 'DECIMAL(18,4)'), _c('fill_time', 'DATETIME'),
        _c('commission', 'DECIMAL(18,4)'), _c('tax', 'DECIMAL(18,4)'), _c('slippage', 'DECIMAL(18,6)'), _c('strategy_name', 'NVARCHAR(128)'),
        _c('signal_id', 'NVARCHAR(100)'), _c('note', 'NVARCHAR(MAX)'),
    )),
    TableSpec('execution_account_snapshot', (
        _c('snapshot_time', 'DATETIME', nullable=False, primary_key=True), _c('account_name', 'NVARCHAR(100)'), _c('cash', 'DECIMAL(18,4)'),
        _c('market_value', 'DECIMAL(18,4)'), _c('equity', 'DECIMAL(18,4)'), _c('buying_power', 'DECIMAL(18,4)'), _c('unrealized_pnl', 'DECIMAL(18,4)'),
        _c('realized_pnl', 'DECIMAL(18,4)'), _c('day_pnl', 'DECIMAL(18,4)'), _c('exposure_ratio', 'DECIMAL(18,6)'), _c('currency', 'NVARCHAR(20)'),
        _c('broker_type', 'NVARCHAR(32)'), _c('note', 'NVARCHAR(MAX)'),
    )),
    TableSpec('execution_positions_snapshot', (
        _c('snapshot_time', 'DATETIME', nullable=False, primary_key=True), _c('ticker_symbol', 'NVARCHAR(32)', nullable=False, primary_key=True),
        _c('direction_bucket', 'NVARCHAR(16)'), _c('qty', 'INT'), _c('available_qty', 'INT'), _c('avg_cost', 'DECIMAL(18,4)'), _c('market_price', 'DECIMAL(18,4)'),
        _c('market_value', 'DECIMAL(18,4)'), _c('unrealized_pnl', 'DECIMAL(18,4)'), _c('realized_pnl', 'DECIMAL(18,4)'), _c('strategy_name', 'NVARCHAR(128)'),
        _c('industry', 'NVARCHAR(64)'), _c('note', 'NVARCHAR(MAX)'),
    )),
    TableSpec('execution_position_lots', (
        _c('lot_id', 'NVARCHAR(120)', nullable=False, primary_key=True), _c('tax_lot_id', 'NVARCHAR(120)'), _c('snapshot_time', 'DATETIME2'), _c('ticker_symbol', 'NVARCHAR(32)'),
        _c('direction_bucket', 'NVARCHAR(20)'), _c('status', 'NVARCHAR(30)'), _c('open_qty', 'INT'), _c('remaining_qty', 'INT'),
        _c('avg_cost', 'FLOAT'), _c('entry_price', 'FLOAT'), _c('market_price', 'FLOAT'), _c('market_value', 'FLOAT'),
        _c('unrealized_pnl', 'FLOAT'), _c('realized_pnl', 'FLOAT'), _c('entry_time', 'DATETIME2'), _c('close_time', 'DATETIME2'),
        _c('entry_order_id', 'NVARCHAR(120)'), _c('exit_order_id', 'NVARCHAR(120)'), _c('strategy_name', 'NVARCHAR(120)'), _c('signal_id', 'NVARCHAR(120)'),
        _c('client_order_id', 'NVARCHAR(120)'), _c('position_key', 'NVARCHAR(180)'), _c('strategy_bucket', 'NVARCHAR(120)'), _c('cost_basis_method', 'NVARCHAR(32)'),
        _c('entry_fill_qty', 'INT'), _c('close_fill_qty', 'INT'), _c('entry_fill_count', 'INT'), _c('close_fill_count', 'INT'),
        _c('entry_fill_ids_json', 'NVARCHAR(MAX)'), _c('exit_fill_ids_json', 'NVARCHAR(MAX)'),
        _c('open_commission', 'FLOAT'), _c('open_tax', 'FLOAT'), _c('close_commission', 'FLOAT'), _c('close_tax', 'FLOAT'),
        _c('stop_order_id', 'NVARCHAR(120)'), _c('stop_price', 'FLOAT'), _c('stop_status', 'NVARCHAR(40)'), _c('linked_stop_qty', 'INT'),
        _c('last_fill_time', 'DATETIME2'), _c('updated_at', 'DATETIME2', nullable=False, default_sql='SYSUTCDATETIME()'), _c('raw_json', 'NVARCHAR(MAX)'),
    )),
    TableSpec('execution_broker_callbacks', (
        _c('callback_id', 'NVARCHAR(120)', nullable=False, primary_key=True), _c('broker_order_id', 'NVARCHAR(120)'), _c('client_order_id', 'NVARCHAR(120)'),
        _c('event_type', 'NVARCHAR(60)'), _c('status', 'NVARCHAR(60)'), _c('ticker_symbol', 'NVARCHAR(32)'), _c('filled_qty', 'INT'),
        _c('remaining_qty', 'INT'), _c('avg_fill_price', 'FLOAT'), _c('lot_id', 'NVARCHAR(120)'), _c('position_key', 'NVARCHAR(180)'),
        _c('strategy_name', 'NVARCHAR(120)'), _c('signal_id', 'NVARCHAR(120)'), _c('fill_notional', 'FLOAT'), _c('callback_time', 'DATETIME2'),
        _c('ingested_at', 'DATETIME2', nullable=False, default_sql='SYSUTCDATETIME()'), _c('raw_json', 'NVARCHAR(MAX)'),
    )),
    TableSpec('execution_reconciliation_report', (
        _c('reconcile_id', 'NVARCHAR(120)', nullable=False, primary_key=True), _c('reconcile_time', 'DATETIME2', nullable=False), _c('status', 'NVARCHAR(60)'),
        _c('order_mismatch_count', 'INT'), _c('fill_mismatch_count', 'INT'), _c('position_mismatch_count', 'INT'),
        _c('lot_mismatch_count', 'INT'), _c('cash_diff', 'FLOAT'), _c('summary_json', 'NVARCHAR(MAX)'),
        _c('created_at', 'DATETIME2', nullable=False, default_sql='SYSUTCDATETIME()'),
    )),
    TableSpec('execution_tax_lot_closures', (
        _c('tax_event_id', 'NVARCHAR(140)', nullable=False, primary_key=True), _c('tax_lot_id', 'NVARCHAR(140)'), _c('lot_id', 'NVARCHAR(120)'),
        _c('ticker_symbol', 'NVARCHAR(32)'), _c('asset_class', 'NVARCHAR(32)'), _c('jurisdiction', 'NVARCHAR(32)'), _c('tax_regime', 'NVARCHAR(80)'),
        _c('tax_treatment', 'NVARCHAR(80)'), _c('report_type', 'NVARCHAR(80)'), _c('direction_bucket', 'NVARCHAR(20)'), _c('position_key', 'NVARCHAR(180)'),
        _c('strategy_name', 'NVARCHAR(120)'), _c('signal_id', 'NVARCHAR(120)'), _c('exit_order_id', 'NVARCHAR(120)'), _c('exit_fill_id', 'NVARCHAR(120)'),
        _c('closed_qty', 'INT'), _c('acquisition_date', 'DATETIME2'), _c('disposal_date', 'DATETIME2'), _c('entry_price', 'FLOAT'),
        _c('cost_basis_price', 'FLOAT'), _c('close_price', 'FLOAT'), _c('gross_proceeds', 'FLOAT'), _c('allocated_cost_basis', 'FLOAT'),
        _c('close_commission', 'FLOAT'), _c('close_tax', 'FLOAT'), _c('net_proceeds', 'FLOAT'), _c('realized_gross_pnl', 'FLOAT'),
        _c('realized_net_pnl', 'FLOAT'), _c('taxable_gain_loss', 'FLOAT'), _c('ordinary_income_amount', 'FLOAT'),
        _c('section1256_60pct_amount', 'FLOAT'), _c('section1256_40pct_amount', 'FLOAT'), _c('holding_period_days', 'INT'),
        _c('holding_period_bucket', 'NVARCHAR(32)'), _c('tax_year', 'INT'), _c('cost_basis_method', 'NVARCHAR(32)'), _c('currency', 'NVARCHAR(20)'),
        _c('wash_sale_applicable', 'BIT'), _c('wash_sale_applied', 'BIT'), _c('wash_sale_adjustment', 'FLOAT'), _c('wash_sale_disallowed_loss', 'FLOAT'),
        _c('wash_sale_replacement_lot_ids', 'NVARCHAR(MAX)'), _c('wash_sale_window_start', 'DATE'), _c('wash_sale_window_end', 'DATE'),
        _c('specific_id_tag', 'NVARCHAR(120)'), _c('note', 'NVARCHAR(MAX)'), _c('raw_json', 'NVARCHAR(MAX)'),
        _c('created_at', 'DATETIME2', nullable=False, default_sql='SYSUTCDATETIME()'),
    )),
    TableSpec('execution_tax_lot_summary', (
        _c('summary_id', 'NVARCHAR(140)', nullable=False, primary_key=True), _c('tax_year', 'INT'), _c('jurisdiction', 'NVARCHAR(32)'),
        _c('asset_class', 'NVARCHAR(32)'), _c('report_type', 'NVARCHAR(80)'), _c('holding_period_bucket', 'NVARCHAR(32)'), _c('currency', 'NVARCHAR(20)'),
        _c('closed_qty', 'INT'), _c('gross_proceeds', 'FLOAT'), _c('allocated_cost_basis', 'FLOAT'), _c('close_commission', 'FLOAT'),
        _c('close_tax', 'FLOAT'), _c('realized_gross_pnl', 'FLOAT'), _c('realized_net_pnl', 'FLOAT'), _c('taxable_gain_loss', 'FLOAT'),
        _c('ordinary_income_amount', 'FLOAT'), _c('section1256_60pct_amount', 'FLOAT'), _c('section1256_40pct_amount', 'FLOAT'),
        _c('wash_sale_adjustment', 'FLOAT'), _c('open_lot_count', 'INT'), _c('unrealized_net_pnl', 'FLOAT'),
        _c('updated_at', 'DATETIME2'), _c('raw_json', 'NVARCHAR(MAX)'),
    )),
    TableSpec('fundamentals_clean', (
        _c('Ticker SYMBOL', 'VARCHAR(20)', nullable=False, default_sql="''"), _c('資料年月日', 'DATE'), _c('毛利率(%)', 'DECIMAL(10,3)'),
        _c('營業利益率(%)', 'DECIMAL(10,3)'), _c('單季EPS', 'DECIMAL(10,3)'), _c('ROE(%)', 'DECIMAL(10,3)'), _c('稅後淨利率(%)', 'DECIMAL(10,3)'),
        _c('營業現金流', 'FLOAT'), _c('預估殖利率(%)', 'DECIMAL(10,3)'), _c('負債比率(%)', 'DECIMAL(10,3)'), _c('本業獲利比(%)', 'DECIMAL(10,3)'), _c('更新時間', 'DATETIME'),
    )),
    TableSpec('monthly_revenue_simple', (
        _c('Ticker SYMBOL', 'NVARCHAR(20)', nullable=False, default_sql="''"), _c('公司名稱', 'NVARCHAR(100)'), _c('產業類別', 'NVARCHAR(20)'),
        _c('產業類別名稱', 'NVARCHAR(100)'), _c('資料年月日', 'DATE'), _c('單月營收年增率(%)', 'DECIMAL(18,3)'), _c('更新時間', 'DATETIME'),
    )),
    TableSpec('account_info', (
        _c('帳戶名稱', 'NVARCHAR(50)', nullable=False, default_sql="''"), _c('可用現金', 'FLOAT'), _c('最後更新時間', 'DATETIME'),
    )),
)


def iter_table_specs() -> Iterable[TableSpec]:
    return CORE_TABLES
