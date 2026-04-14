# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 3 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_level_runtime.py
# ==============================================================================
# -*- coding: utf-8 -*-
"""Level-2 variant registry for advanced_chart(1).zip absorption."""
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


# ==============================================================================
# Merged from: fts_level_runtime.py
# ==============================================================================
# -*- coding: utf-8 -*-
"""Load non-destructive level-2 runtime variants from advanced_chart(1).zip."""
import importlib
# merged-local import removed: from fts_level_runtime import VARIANT_PACKAGE, VARIANT_MODULES


def load_variant(module_name: str):
    if module_name not in VARIANT_MODULES:
        raise KeyError(f"unknown level-2 variant: {module_name}")
    return importlib.import_module(f"{VARIANT_PACKAGE}.{module_name}")


def available_variants():
    return sorted(VARIANT_MODULES)


# ==============================================================================
# Merged from: fts_level_runtime.py
# ==============================================================================
# -*- coding: utf-8 -*-
"""Level-3 safe runtime loader for execution / broker / risk integration."""
import importlib
from typing import Any

_SERVICE_MAP = {
    'DecisionExecutionBridge': ('fts_decision_execution_bridge', 'DecisionExecutionBridge'),
    'LiveReadinessGate': ('fts_live_readiness_gate', 'LiveReadinessGate'),
    'OrderStateMachine': ('fts_execution_models', 'OrderStateMachine'),
    'PositionStateService': ('fts_admin_suite', 'PositionStateService'),
    'ReconciliationEngine': ('fts_reconciliation_engine', 'ReconciliationEngine'),
    'KillSwitchManager': ('fts_kill_switch', 'KillSwitchManager'),
    'RecoveryEngine': ('fts_recovery_engine', 'RecoveryEngine'),
}


def _load_class(module_name: str, attr_name: str):
    module = importlib.import_module(module_name)
    return getattr(module, attr_name)


def get_level3_classes() -> dict[str, Any]:
    loaded = {}
    for public_name, (module_name, attr_name) in _SERVICE_MAP.items():
        try:
            loaded[public_name] = _load_class(module_name, attr_name)
        except Exception:
            loaded[public_name] = None
    return loaded


def build_level3_services() -> tuple[dict[str, Any], dict[str, Any]]:
    classes = get_level3_classes()
    services: dict[str, Any] = {}
    meta: dict[str, Any] = {'services': {}, 'status': 'level3_partial_ready'}
    ok_count = 0
    for name, cls in classes.items():
        if cls is None:
            meta['services'][name] = {'loaded': False}
            continue
        try:
            services[name] = cls()
            meta['services'][name] = {'loaded': True}
            ok_count += 1
        except Exception as e:
            meta['services'][name] = {'loaded': False, 'error': repr(e)}
    if ok_count == len(classes):
        meta['status'] = 'level3_ready'
    elif ok_count == 0:
        meta['status'] = 'level3_unavailable'
    return services, meta
