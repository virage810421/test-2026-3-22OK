# -*- coding: utf-8 -*-
"""Level-2 variant registry for advanced_chart(1).zip absorption."""
from __future__ import annotations

VARIANT_SOURCE = "advanced_chart(1).zip"
VARIANT_PACKAGE = "advanced_chart1_runtime_variants"
VARIANT_MODULES = {
    'advanced_chart': 'advanced_chart1_runtime_variants/advanced_chart.py',
    'alert_manager': 'advanced_chart1_runtime_variants/alert_manager.py',
    'broker_base': 'advanced_chart1_runtime_variants/broker_base.py',
    'config': 'advanced_chart1_runtime_variants/config.py',
    'daily_chip_etl': 'advanced_chart1_runtime_variants/daily_chip_etl.py',
    'db_logger': 'advanced_chart1_runtime_variants/db_logger.py',
    'db_setup': 'advanced_chart1_runtime_variants/db_setup.py',
    'event_backtester': 'advanced_chart1_runtime_variants/event_backtester.py',
    'execution_engine': 'advanced_chart1_runtime_variants/execution_engine.py',
    'fundamental_screener': 'advanced_chart1_runtime_variants/fundamental_screener.py',
    'launcher': 'advanced_chart1_runtime_variants/launcher.py',
    'live_paper_trading': 'advanced_chart1_runtime_variants/live_paper_trading.py',
    'master_pipeline': 'advanced_chart1_runtime_variants/master_pipeline.py',
    'ml_data_generator': 'advanced_chart1_runtime_variants/ml_data_generator.py',
    'ml_trainer': 'advanced_chart1_runtime_variants/ml_trainer.py',
    'model_governance': 'advanced_chart1_runtime_variants/model_governance.py',
    'monitor_center': 'advanced_chart1_runtime_variants/monitor_center.py',
    'monthly_revenue_simple': 'advanced_chart1_runtime_variants/monthly_revenue_simple.py',
    'paper_broker': 'advanced_chart1_runtime_variants/paper_broker.py',
    'performance': 'advanced_chart1_runtime_variants/performance.py',
    'portfolio_risk': 'advanced_chart1_runtime_variants/portfolio_risk.py',
    'risk_gateway': 'advanced_chart1_runtime_variants/risk_gateway.py',
    'screening': 'advanced_chart1_runtime_variants/screening.py',
    'sector_classifier': 'advanced_chart1_runtime_variants/sector_classifier.py',
    'strategies': 'advanced_chart1_runtime_variants/strategies.py',
    'system_guard': 'advanced_chart1_runtime_variants/system_guard.py',
    'yahoo_csv_to_sql': 'advanced_chart1_runtime_variants/yahoo_csv_to_sql.py',
}

HIGH_VALUE_MODULES = [
    'alert_manager', 'db_logger', 'db_setup', 'execution_engine', 'model_governance',
    'paper_broker', 'daily_chip_etl', 'monthly_revenue_simple', 'yahoo_csv_to_sql',
]

IDENTICAL_OR_LOW_RISK_MODULES = [
    'broker_base', 'config', 'event_backtester', 'fundamental_screener', 'launcher',
    'live_paper_trading', 'master_pipeline', 'monitor_center', 'performance',
    'portfolio_risk', 'risk_gateway', 'sector_classifier', 'strategies', 'system_guard',
]
