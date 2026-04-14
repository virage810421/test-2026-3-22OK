# -*- coding: utf-8 -*-
from __future__ import annotations

"""正式守門員入口（wrapper only）。

主線已統一到 :mod:`fts_system_guard_service`。
本檔只保留舊門牌相容性，避免 system_guard.py 與
fts_system_guard_service.py 兩套邏輯同時存在、互相漂移。
"""

from pathlib import Path
from typing import Any

import pandas as pd

from fts_system_guard_service import SystemGuardService


def _service() -> SystemGuardService:
    return SystemGuardService()


def check_model_artifacts() -> dict[str, Any]:
    return _service().check_model_artifacts()


def check_decision_desk() -> dict[str, Any]:
    return _service().check_decision_desk()


def check_recent_trades(limit: int = 100) -> dict[str, Any]:
    return _service().check_recent_trades(limit=limit)


def check_strategy_layer() -> pd.DataFrame:
    """Legacy API compatibility.

    新主線不再由 system_guard.py 呼叫 performance.py/SQL 舊策略健康表，
    避免守門員雙版本。若需要策略層細節，請改由新版 runtime/報表來源接入
    fts_system_guard_service.py。
    """
    return pd.DataFrame()


def evaluate_system_guard() -> tuple[dict[str, Any], pd.DataFrame]:
    payload = _service().evaluate_system_guard()
    return payload, check_strategy_layer()


def save_guard_report(payload: dict[str, Any], strategy_df: pd.DataFrame | None = None, output_dir: str = 'runtime') -> tuple[str, str]:
    svc = _service()
    path, saved_payload = svc.build_summary()
    strategy_path = Path(output_dir) / 'system_guard_strategy_snapshot.csv'
    strategy_path.parent.mkdir(parents=True, exist_ok=True)
    (strategy_df if strategy_df is not None else pd.DataFrame()).to_csv(strategy_path, index=False, encoding='utf-8-sig')
    return str(path), str(strategy_path)


def format_alert_message(payload: dict[str, Any]) -> str:
    return _service().format_alert_message(payload)


def run_system_guard() -> dict[str, Any]:
    path, payload = _service().build_summary()
    print(_service().format_alert_message(payload))
    print(f"📁 已輸出：{path}")
    return payload


if __name__ == '__main__':
    run_system_guard()
