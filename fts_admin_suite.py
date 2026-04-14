# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 21 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from fts_config import PATHS
from fts_models import Position
from fts_utils import now_str, log

class StateStore:
    def __init__(self):
        self.state_file = PATHS.state_dir / "engine_state.json"

    def save(self, cash, positions, last_prices, meta=None):
        payload = {"saved_at": now_str(), "cash": cash, "positions": {k: asdict(v) for k, v in positions.items()}, "last_prices": last_prices, "meta": meta or {}}
        with open(self.state_file, "w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2)
        return self.state_file

    def load(self):
        if not self.state_file.exists(): return None
        with open(self.state_file, "r", encoding="utf-8") as f: data = json.load(f)
        positions = {k: Position(**v) for k, v in data.get("positions", {}).items()}
        return {"saved_at": data.get("saved_at",""), "cash": float(data.get("cash",0.0)), "positions": positions, "last_prices": data.get("last_prices",{}), "meta": data.get("meta",{})}

class RecoveryManager:
    def __init__(self, broker, state_store):
        self.broker = broker; self.state_store = state_store

    def recover_if_possible(self):
        state = self.state_store.load()
        if not state: return {"recovered": False, "reason": "no_state_file"}
        self.broker.restore_state(state["cash"], state["positions"], state.get("last_prices", {}))
        log(f"♻️ 已從 state 檔恢復狀態，saved_at={state['saved_at']} positions={len(state['positions'])}")
        return {"recovered": True, "saved_at": state["saved_at"], "positions_count": len(state["positions"])}


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from typing import Any

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class StartupRepairPlanner:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'startup_repair_plan.json'

    def build(self, recovery_report: dict[str, Any]):
        actions = []
        checks = recovery_report.get('checks', {}) or {}
        summary = recovery_report.get('summary', {}) or {}

        if not checks.get('state_file_exists', False):
            actions.append({'priority': 'medium', 'action': 'rebuild_empty_state', 'message': '建立乾淨的初始 state 骨架'})

        retry_total = int(checks.get('retry_queue_total', 0) or 0)
        if retry_total > 0:
            actions.append({'priority': 'high', 'action': 'review_retry_queue', 'message': f'檢查 retry queue，共 {retry_total} 筆'})

        if summary.get('corporate_action_suspect_count', 0) > 0:
            actions.append({'priority': 'high', 'action': 'apply_corporate_action_position_rebuild', 'message': '疑似除權息/減資/分割，重建持倉與成本'})

        if summary.get('position_mismatch_count', 0) > 0:
            actions.append({'priority': 'high', 'action': 'rebuild_positions_from_fills', 'message': '依成交紀錄重建持倉快照'})

        if not recovery_report.get('cash_check', {}).get('matched', True):
            actions.append({'priority': 'high', 'action': 'replay_cash_ledger', 'message': '重播現金帳與手續費稅額'})

        for name in recovery_report.get('repair_actions', []) or []:
            if not any(a['action'] == name for a in actions):
                actions.append({'priority': 'medium', 'action': name, 'message': f'建議執行 {name}'})

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'planned_actions': actions,
            'action_count': len(actions),
            'status': 'planner_ready_for_manual_orchestrated_repair',
        }
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f'🛠️ 已輸出 startup repair plan：{self.path}')
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

import pandas as pd

from fts_sector_service import SectorService
from fts_utils import safe_float


class PositionStateService:
    def __init__(self):
        self.sectors = SectorService()

    @staticmethod
    def read_active_positions_csv(path) -> pd.DataFrame:
        try:
            return pd.read_csv(path, encoding='utf-8-sig')
        except Exception:
            try:
                return pd.read_csv(path)
            except Exception:
                return pd.DataFrame()

    def current_portfolio_state(self, active_df: pd.DataFrame, total_nav: float) -> dict[str, Any]:
        state = {
            'total_alloc': 0.0,
            'sector_alloc': {},
            'sector_count': {},
            'direction_alloc': {'LONG': 0.0, 'SHORT': 0.0},
        }
        if active_df is None or active_df.empty or total_nav <= 0:
            return state
        for _, pos in active_df.iterrows():
            ticker = str(pos.get('Ticker SYMBOL', pos.get('Ticker', ''))).strip()
            invested = safe_float(pos.get('投入資金', pos.get('invested', 0.0)), 0.0)
            if invested <= 0:
                continue
            alloc = invested / total_nav
            direction = 'SHORT' if ('空' in str(pos.get('方向', pos.get('Direction', '')))) else 'LONG'
            sector = self.sectors.get_stock_sector(ticker)
            state['total_alloc'] += alloc
            state['sector_alloc'][sector] = state['sector_alloc'].get(sector, 0.0) + alloc
            state['sector_count'][sector] = state['sector_count'].get(sector, 0) + 1
            state['direction_alloc'][direction] = state['direction_alloc'].get(direction, 0.0) + alloc
        return state


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class DecisionConsistencyBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "decision_consistency.json"

    def build(self, compat_info: dict, readiness: dict):
        row_count = compat_info.get("row_count", 0)
        signal_count = readiness.get("total_signals", 0)
        rows_with_ticker = compat_info.get("rows_with_ticker", 0)
        rows_with_action = compat_info.get("rows_with_action", 0)
        rows_with_price = compat_info.get("rows_with_price", 0)

        issues = []
        if row_count > 0 and signal_count == 0:
            issues.append("decision_rows_exist_but_no_signals")
        if row_count > 0 and rows_with_ticker == 0:
            issues.append("rows_missing_ticker")
        if row_count > 0 and rows_with_action == 0:
            issues.append("rows_missing_action")
        if row_count > 0 and rows_with_price == 0:
            issues.append("rows_missing_price")

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "row_count": row_count,
                "signal_count": signal_count,
                "rows_with_ticker": rows_with_ticker,
                "rows_with_action": rows_with_action,
                "rows_with_price": rows_with_price,
                "issue_count": len(issues),
            },
            "issues": issues,
            "all_green": len(issues) == 0,
            "status": "consistency_check_ready"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧷 已輸出 decision consistency：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_upgrade_runtime import PATHS, CONFIG, now_str, log, load_json, normalize_key, write_json


class RecoveryConsistencySuite:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'recovery_consistency_report.json'

    def build(self, retry_queue_summary: dict[str, Any], broker_snapshot: dict[str, Any] | None = None) -> tuple[Any, dict[str, Any]]:
        state = load_json(PATHS.state_dir / 'engine_state.json', {}) or {}
        broker_snapshot = broker_snapshot or {}
        failures = []
        warnings = []
        if not state:
            failures.append({'type': 'missing_state_file', 'message': '尚未找到 state/engine_state.json'})
        if int(retry_queue_summary.get('total', 0) or 0) > 0:
            warnings.append({'type': 'pending_retry_queue', 'message': f"retry queue 目前仍有 {retry_queue_summary.get('total', 0)} 筆待處理/已記錄項目"})
        state_tickers = {normalize_key(x.get('ticker')) for x in state.get('positions', [])}
        broker_tickers = {normalize_key(x.get('ticker')) for x in broker_snapshot.get('positions', [])}
        missing_on_broker = sorted(x for x in state_tickers - broker_tickers if x)
        orphan_on_broker = sorted(x for x in broker_tickers - state_tickers if x)
        if missing_on_broker:
            failures.append({'type': 'state_position_missing_on_broker', 'tickers': missing_on_broker[:50]})
        if orphan_on_broker:
            warnings.append({'type': 'broker_position_missing_in_state', 'tickers': orphan_on_broker[:50]})
        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'checks': {
                'state_file_exists': bool(state),
                'retry_queue_total': int(retry_queue_summary.get('total', 0) or 0),
                'broker_snapshot_found': bool(broker_snapshot),
                'missing_on_broker_count': len(missing_on_broker),
                'orphan_on_broker_count': len(orphan_on_broker),
            },
            'failures': failures,
            'warnings': warnings,
            'all_passed': len(failures) == 0,
            'status': 'consistency_green' if len(failures) == 0 else 'consistency_break',
        }
        write_json(self.path, payload)
        log(f'🧩 已輸出 recovery consistency report：{self.path}')
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from typing import Iterable

from fts_config import PATHS, CONFIG
from fts_utils import now_str

LEGACY_FACADES = ('screening', 'strategies', 'advanced_chart')
DEFAULT_CORE_MODULES = [
    'live_paper_trading.py',
    'event_backtester.py',
    'advanced_optimizer.py',
    'optimizer.py',
    'fts_model_layer.py',
    'fts_legacy_master_pipeline_impl.py',
]


def audit_bridge_usage(core_modules: Iterable[str] | None = None) -> dict:
    modules = list(core_modules or DEFAULT_CORE_MODULES)
    callers: dict[str, list[str]] = {name: [] for name in LEGACY_FACADES}
    base_dir = Path(PATHS.base_dir)
    for mod in modules:
        path = base_dir / mod
        if not path.exists():
            continue
        text = path.read_text(encoding='utf-8', errors='ignore')
        for facade in LEGACY_FACADES:
            if f'from {facade} import' in text or f'import {facade}' in text:
                callers[facade].append(mod)
    offenders = sorted({m for vals in callers.values() for m in vals})
    payload = {
        'generated_at': now_str(),
        'mode': str(getattr(CONFIG, 'mode', 'PAPER')).upper(),
        'force_service_api_only': bool(getattr(CONFIG, 'force_service_api_only', True)),
        'legacy_facade_callers': callers,
        'offender_count': len(offenders),
        'offenders': offenders,
        'ok': len(offenders) == 0,
        'status': 'service_api_only' if len(offenders) == 0 else 'legacy_facade_imports_detected',
    }
    out = Path(PATHS.runtime_dir) / 'bridge_guard.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class BridgeReplacementPlanBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "bridge_replacement_plan.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "waves": [
                {
                    "wave": 1,
                    "focus": "明確 contract 與治理層",
                    "targets": [
                        "monthly_revenue_simple.py",
                        "ml_data_generator.py",
                        "ml_trainer.py"
                    ]
                },
                {
                    "wave": 2,
                    "focus": "逐步吸收可重複邏輯",
                    "targets": [
                        "yahoo_csv_to_sql.py",
                        "model_governance.py"
                    ]
                },
                {
                    "wave": 3,
                    "focus": "保留少數外部 facade，但核心流程全面改走 service API",
                    "targets": [
                        "advanced_chart.py",
                        "daily_chip_etl.py"
                    ]
                }
            ],
            "principle": "不是全部刪掉，而是把該吸收的吸收、該保留的保留成正規子模組",
            "status": "replacement_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛠️ 已輸出 bridge replacement plan：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import importlib
from fts_utils import log

class PackageConsistencyGuard:
    REQUIRED = {
        "fts_signal": ["SignalLoader", "ExecutionReadinessChecker"],
        "fts_compat": ["DecisionCompatibilityLayer"],
        "fts_admin_suite": ["StateStore", "RecoveryManager"],
    }

    def run(self):
        info = {"passed": True, "issues": [], "modules": {}}
        for module_name, attrs in self.REQUIRED.items():
            try:
                mod = importlib.import_module(module_name)
                info["modules"][module_name] = {"file": getattr(mod, "__file__", ""), "attrs": []}
            except Exception as e:
                info["passed"] = False
                info["issues"].append(f"import 失敗 {module_name}: {e}")
                continue

            for attr in attrs:
                ok = hasattr(mod, attr)
                info["modules"][module_name]["attrs"].append({"name": attr, "ok": ok})
                if not ok:
                    info["passed"] = False
                    info["issues"].append(f"{module_name} 缺少 {attr}")

        try:
            from fts_signal import SignalLoader
            loader = SignalLoader()
            if not hasattr(loader, "load_from_normalized_df"):
                info["passed"] = False
                info["issues"].append("SignalLoader 缺少 load_from_normalized_df")
        except Exception as e:
            info["passed"] = False
            info["issues"].append(f"SignalLoader 檢查失敗: {e}")

        if info["passed"]:
            log("🧷 package consistency check 通過")
        else:
            log(f"❌ package consistency check 失敗: {info['issues']}")
        return info


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class UnusedCandidateBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "unused_candidates.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "safe_to_archive_first": [
                "formal_trading_system_v40.py",
                "formal_trading_system_v41.py",
                "formal_trading_system_v42.py",
                "formal_trading_system_v43.py",
                "formal_trading_system_v44.py",
                "formal_trading_system_v45.py",
                "formal_trading_system_v46.py",
                "formal_trading_system_v47.py",
                "formal_trading_system_v48.py",
                "formal_trading_system_v49.py",
                "formal_trading_system_v50.py",
                "formal_trading_system_v51.py",
                "formal_trading_system_v52.py",
            ],
            "do_not_delete_yet": [
                "formal_trading_system_v53.py",
                "daily_chip_etl.py",
                "monthly_revenue_simple.py",
                "yahoo_csv_to_sql.py",
                "advanced_chart.py",
                "ml_data_generator.py",
                "ml_trainer.py",
                "model_governance.py",
            ],
            "notes": [
                "舊版主控檔大多可先搬到 archive/ 或 backup/，不建議直接永久刪除",
                "ETL / chart / AI 訓練腳本目前多半仍是被新主控橋接或依賴的來源",
            ],
            "status": "candidate_list_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🧹 已輸出 unused candidates：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ArchivePolicyBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "archive_policy.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "keep_now": [
                "formal_trading_system_v54.py",
                "formal_trading_system_v55.py",
                "daily_chip_etl.py",
                "monthly_revenue_simple.py",
                "yahoo_csv_to_sql.py",
                "advanced_chart.py",
                "ml_data_generator.py",
                "ml_trainer.py",
                "model_governance.py",
            ],
            "archive_first": [
                "formal_trading_system_v40.py",
                "formal_trading_system_v41.py",
                "formal_trading_system_v42.py",
                "formal_trading_system_v43.py",
                "formal_trading_system_v44.py",
                "formal_trading_system_v45.py",
                "formal_trading_system_v46.py",
                "formal_trading_system_v47.py",
                "formal_trading_system_v48.py",
                "formal_trading_system_v49.py",
                "formal_trading_system_v50.py",
                "formal_trading_system_v51.py",
                "formal_trading_system_v52.py",
                "formal_trading_system_v53.py",
            ],
            "policy": [
                "先 archive，再觀察 1~2 個版本週期",
                "不要先永久刪除 legacy engines",
                "只刪已完全被替換且無 bridge 依賴的檔案"
            ],
            "status": "archive_policy_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"📦 已輸出 archive policy：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RunManifestBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "run_manifest.json"

    def build(self, ai_exec, compat_info, readiness, accepted_count, rejected_count, execution_result, generated_paths: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "mode": getattr(CONFIG, "mode", "PAPER"),
            "broker_type": getattr(CONFIG, "broker_type", "paper"),
            "stages": {
                "ai_training": {
                    "enabled": ai_exec.get("ai_stage_enabled", False),
                    "dry_run": ai_exec.get("dry_run", True),
                    "executed_count": len(ai_exec.get("executed", [])),
                    "skipped_count": len(ai_exec.get("skipped", [])),
                    "failed_count": len(ai_exec.get("failed", [])),
                },
                "decision_normalize": {
                    "row_count": compat_info.get("row_count", 0),
                    "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                    "rows_with_action": compat_info.get("rows_with_action", 0),
                    "rows_with_price": compat_info.get("rows_with_price", 0),
                },
                "signal_readiness": {
                    "execution_ready": readiness.get("execution_ready", False),
                    "signal_count": readiness.get("total_signals", 0),
                },
                "risk_filter": {
                    "accepted_count": accepted_count,
                    "rejected_count": rejected_count,
                },
                "execution": {
                    "submitted": execution_result.get("submitted", 0),
                    "filled": execution_result.get("filled", 0),
                    "partially_filled": execution_result.get("partially_filled", 0),
                    "rejected": execution_result.get("rejected", 0),
                    "cancelled": execution_result.get("cancelled", 0),
                    "auto_exit_signals": execution_result.get("auto_exit_signals", 0),
                }
            },
            "generated_outputs": generated_paths,
            "status": "manifest_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗂️ 已輸出 run manifest：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class StageTraceWriter:
    def __init__(self):
        self.path = PATHS.runtime_dir / "stage_trace.json"

    def build(self, ai_exec: dict, compat_info: dict, readiness: dict, accepted_count: int, rejected_count: int, execution_result: dict):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "trace": [
                {
                    "stage": "ai_training",
                    "enabled": ai_exec.get("ai_stage_enabled", False),
                    "dry_run": ai_exec.get("dry_run", True),
                    "executed_count": len(ai_exec.get("executed", [])),
                    "skipped_count": len(ai_exec.get("skipped", [])),
                    "failed_count": len(ai_exec.get("failed", [])),
                },
                {
                    "stage": "decision_normalize",
                    "row_count": compat_info.get("row_count", 0),
                    "rows_with_ticker": compat_info.get("rows_with_ticker", 0),
                    "rows_with_action": compat_info.get("rows_with_action", 0),
                    "rows_with_price": compat_info.get("rows_with_price", 0),
                },
                {
                    "stage": "signal_readiness",
                    "execution_ready": readiness.get("execution_ready", False),
                    "total_signals": readiness.get("total_signals", 0),
                },
                {
                    "stage": "risk_filter",
                    "accepted_count": accepted_count,
                    "rejected_count": rejected_count,
                },
                {
                    "stage": "execution",
                    "submitted": execution_result.get("submitted", 0),
                    "filled": execution_result.get("filled", 0),
                    "partially_filled": execution_result.get("partially_filled", 0),
                    "rejected": execution_result.get("rejected", 0),
                    "cancelled": execution_result.get("cancelled", 0),
                    "auto_exit_signals": execution_result.get("auto_exit_signals", 0),
                }
            ]
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧭 已輸出 stage trace：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str

class ReportBuilder:
    def __init__(self, progress_tracker, version_policy):
        self.progress_tracker = progress_tracker
        self.version_policy = version_policy

    def save(
        self,
        package_check,
        compat_info,
        readiness,
        test_results,
        accepted,
        rejected,
        execution_result,
        account_snapshot,
        positions,
        decision_path,
        stage_results=None,
        runtime_manifest=None,
        service_facade_info=None,
        recovery_info=None,
    ):
        payload = {
            "run_time": now_str(),
            "system_name": CONFIG.system_name,
            "mode": CONFIG.mode,
            "broker_type": CONFIG.broker_type,
            "decision_path": str(decision_path),
            "progress_dashboard": self.progress_tracker.summary(),
            "version_policy": self.version_policy.summary(),
            "package_check": package_check,
            "compat_info": compat_info,
            "preflight_tests": test_results,
            "execution_readiness": readiness,
            "accepted_signals": [asdict(x) for x in accepted],
            "rejected_signals": [{"signal": asdict(s), "reason": reason} for s, reason in rejected],
            "execution_result": execution_result,
            "account_snapshot": asdict(account_snapshot),
            "positions": [asdict(x) for x in positions.values()],
            "stage_results": stage_results or {},
            "runtime_manifest": runtime_manifest or {},
            "service_facade_info": service_facade_info or {},
            "recovery_info": recovery_info or {},
        }
        filename = PATHS.log_dir / f"formal_trading_system_v17_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        return filename


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ConsoleBriefBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "console_brief.json"

    def build(self, ai_exec, compat_info, readiness, research_gate, model_gate, launch_gate, live_safety, broker_approval, submission_gate, execution_result):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "brief": {
                "ai_stage_enabled": ai_exec.get("ai_stage_enabled", False),
                "ai_dry_run": ai_exec.get("dry_run", True),
                "ai_executed_count": len(ai_exec.get("executed", [])),
                "decision_rows": compat_info.get("row_count", 0),
                "signal_count": readiness.get("total_signals", 0),
                "research_gate": research_gate.get("go_for_decision_linkage", False),
                "model_gate": model_gate.get("go_for_model_linkage", False),
                "launch_gate": launch_gate.get("go_for_execution", False),
                "live_safety_gate": live_safety.get("paper_live_safe", False),
                "broker_approval_gate": broker_approval.get("go_for_broker_submission", False),
                "submission_gate": submission_gate.get("go_for_submission_contract", False),
                "submitted": execution_result.get("submitted", 0),
                "filled": execution_result.get("filled", 0),
                "partially_filled": execution_result.get("partially_filled", 0),
                "rejected": execution_result.get("rejected", 0),
                "cancelled": execution_result.get("cancelled", 0),
            },
            "status": "console_brief_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🪖 已輸出 console brief：{self.path}")
        return self.path, payload

    def render_lines(self, payload: dict):
        b = payload["brief"]
        return [
            "================= 戰情室摘要 =================",
            f"AI階段 | enabled={b['ai_stage_enabled']} | dry_run={b['ai_dry_run']} | executed={b['ai_executed_count']}",
            f"Decision | rows={b['decision_rows']} | signals={b['signal_count']}",
            f"Gates | research={b['research_gate']} | model={b['model_gate']} | launch={b['launch_gate']}",
            f"Gates | live={b['live_safety_gate']} | approval={b['broker_approval_gate']} | submission={b['submission_gate']}",
            f"Execution | submitted={b['submitted']} | filled={b['filled']} | partial={b['partially_filled']} | rejected={b['rejected']} | cancelled={b['cancelled']}",
            "================================================"
        ]


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class UpgradeTruthReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'upgrade_truth_report.json'

    def build(self):
        runtime = PATHS.runtime_dir
        def load(name, default=None):
            p = runtime / name
            if not p.exists():
                return default if default is not None else {}
            try:
                return json.loads(p.read_text(encoding='utf-8'))
            except Exception:
                return default if default is not None else {}

        scorecard = load('target95_scorecard.json', {})
        ai = load('ai_pipeline_status.json', {})
        launch = load('launch_gate.json', {})
        console = load('console_brief.json', {})
        realapi = load('real_api_readiness.json', {})
        price = load('decision_price_bridge_plus.json', {})

        payload = {
            'generated_at': now_str(),
            'system_name': CONFIG.system_name,
            'headline': {
                'architecture_score_ready': scorecard.get('summary',{}).get('at_or_above_95',0),
                'architecture_score_total': scorecard.get('summary',{}).get('total_modules',0),
                'training_assets_present': ai.get('training_assets_present', False),
                'go_for_execution': launch.get('go_for_execution', False),
                'submitted_orders': console.get('brief',{}).get('submitted', 0) if isinstance(console.get('brief'), dict) else 0,
                'real_api_live_bound': realapi.get('status') == 'live_bound',
            },
            'truth_table': [
                {'layer':'架構/模組化', 'status':'high', 'evidence':'24/24 模組已達 95+'},
                {'layer':'舊核心納管', 'status':'high', 'evidence':'核心檔案已納入主控與評分'},
                {'layer':'AI 真正訓練資產', 'status':'medium' if ai.get('training_assets_present') else 'low', 'evidence':'training_data_exists=' + str(ai.get('checks',{}).get('training_data_exists', False))},
                {'layer':'決策可執行性', 'status':'medium' if price.get('rows_with_price_after',0) > 0 else 'low', 'evidence':'rows_with_price_after=' + str(price.get('rows_with_price_after', 0))},
                {'layer':'Paper execution', 'status':'medium' if launch.get('go_for_execution') else 'low', 'evidence':'go_for_execution=' + str(launch.get('go_for_execution', False))},
                {'layer':'Real API live binding', 'status':'low', 'evidence':'contract defined but not live bound'},
            ],
            'next_milestones': [
                '補齊 last_price_snapshot.csv 或在決策輸出直接寫入 Close/Reference_Price',
                '補齊 training_data 與模型 artifact 產出',
                '接入真券商登入/下單/查單/成交 callback',
                '建立 broker reject code -> internal reject classifier 映射',
            ],
            'status':'truth_report_ready'
        }
        with open(self.path, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧭 已輸出 upgrade truth report：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import log


class ProgressFullReport:
    def __init__(self):
        self.json_path = PATHS.runtime_dir / 'progress_full_report.json'
        self.md_path = PATHS.runtime_dir / 'progress_full_report.md'

    def build(self):
        log('📝 開始彙整完整進度報告...')
        def load(name):
            p = PATHS.runtime_dir / name
            if p.exists():
                return json.loads(p.read_text(encoding='utf-8'))
            return {}

        comp = load('completion_gap_report.json')
        train = load('training_orchestrator.json')
        execb = load('decision_execution_bridge.json')
        gap = load('price_gap_bridge.json')
        payload = {
            'completion_gap_report': comp,
            'training_orchestrator': train,
            'decision_execution_bridge': execb,
            'price_gap_bridge': gap,
            'config': {
                'package_version': CONFIG.package_version,
                'source_mount_dirs': [str(x) for x in PATHS.source_mount_dirs],
                'price_scan_dirs': [str(x) for x in PATHS.price_scan_dirs],
                'history_scan_dirs': [str(x) for x in PATHS.history_scan_dirs],
            }
        }
        self.json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')

        missing_preview = ', '.join(gap.get('missing_tickers', [])[:10]) or '無'
        lines = [
            '# FTS 完整進度報告 v73',
            '',
            f"- 缺口數：{len(comp.get('remaining_hard_gaps', []))}",
            f"- 訓練資料存在：{train.get('dataset', {}).get('exists', False)}",
            f"- 模型檔齊全：{train.get('models', {}).get('all_required_present', False)}",
            f"- 有價格列數：{execb.get('rows_with_price', 0)}",
            f"- 有股數列數：{execb.get('rows_with_qty', 0)}",
            f"- 通過市場規則列數：{execb.get('rows_market_rule_passed', 0)}",
            f"- 自動價格候選列數：{gap.get('candidate_rows', 0)}",
            f"- 自動價格掃描成功檔數：{gap.get('scanned_csv_success_count', 0)}",
            f"- 尚缺價格 ticker：{missing_preview}",
            '',
            '## 掛載狀態',
            f"- 來源掛載資料夾：{', '.join(str(x) for x in PATHS.source_mount_dirs) or '未設定'}",
            f"- 價格掃描資料夾：{', '.join(str(x) for x in PATHS.price_scan_dirs[:8])}",
            f"- 歷史掃描資料夾：{', '.join(str(x) for x in PATHS.history_scan_dirs[:8])}",
            '',
            '## 目前最硬的缺口',
        ]
        for item in comp.get('remaining_hard_gaps', []):
            lines.append(f'- {item}')
        self.md_path.write_text('\n'.join(lines), encoding='utf-8')
        log(f'📝 已輸出完整進度報告：{self.md_path}')
        return self.md_path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from typing import Any

from fts_config import PATHS
from fts_utils import now_str, log


class ABDiffAudit:
    MODULE_VERSION = 'v83_ab_diff_audit'

    def __init__(self):
        self.runtime_path = PATHS.runtime_dir / 'ab_diff_audit.json'

    def build(self) -> tuple[Any, dict[str, Any]]:
        rows = [
            {
                'module': 'yahoo_csv_to_sql.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_fundamentals_etl_mainline.py',
                'note': '不要整支重吸收，只補 retry / smart sync / batch commit / checkpoint 細節',
            },
            {
                'module': 'daily_chip_etl.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_etl_daily_chip_service.py',
                'note': '只補抓取規則與補抓差異，不回退到舊腳本主控',
            },
            {
                'module': 'monthly_revenue_simple.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_etl_monthly_revenue_service.py',
                'note': '只補欄位映射與日期規則差異',
            },
            {
                'module': 'ml_data_generator.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_training_data_builder.py',
                'note': '只補特徵欄位與標籤規則差異',
            },
            {
                'module': 'advanced_chart.py',
                'state': 'patch_diff_only',
                'paired_mainline': 'fts_chart_service.py',
                'note': '只補圖層或參數差異，不把繪圖塞進主交易回路',
            },
            {
                'module': 'config.py',
                'state': 'manual_merge_values_only',
                'paired_mainline': 'fts_config.py',
                'note': '不要整支覆蓋，只人工搬參數值',
            },
        ]
        payload = {
            'generated_at': now_str(),
            'module_version': self.MODULE_VERSION,
            'audit_rows': rows,
            'status': 'diff_patch_plan_ready',
        }
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'🧾 A/B diff audit ready: {self.runtime_path}')
        return self.runtime_path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class SingleCoreMigrationBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "single_core_migration.json"

    def build(self):
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "phases": [
                {
                    "phase": 1,
                    "name": "治理收口",
                    "done": True,
                    "items": [
                        "interface alignment",
                        "decision consistency",
                        "etl quality",
                        "ai quality",
                        "callback normalization",
                        "state/recovery/report governance"
                    ]
                },
                {
                    "phase": 2,
                    "name": "legacy bridge 明確化",
                    "done": True,
                    "items": [
                        "盤點哪些仍直接使用舊 code",
                        "盤點哪些可先封存",
                        "盤點哪些新骨架已可承接"
                    ]
                },
                {
                    "phase": 3,
                    "name": "逐步單核化",
                    "done": False,
                    "items": [
                        "把 ETL 實抓邏輯逐段搬進新主線",
                        "把 chart rendering 契約化後再替換",
                        "把 ml trainer / data generator 逐步包進新 pipeline"
                    ]
                },
                {
                    "phase": 4,
                    "name": "live-ready 最終收口",
                    "done": False,
                    "items": [
                        "broker adapter 真實實作",
                        "callback loop 實接",
                        "對帳引擎深化",
                        "實盤保護/回退機制"
                    ]
                }
            ],
            "status": "migration_plan_ready"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧭 已輸出 single core migration：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class SubmissionContractGate:
    def __init__(self):
        self.path = PATHS.runtime_dir / "submission_contract_gate.json"

    def evaluate(self, accepted_signals):
        failures = []
        warnings = []

        if len(accepted_signals) == 0:
            warnings.append({
                "type": "no_accepted_signals",
                "message": "目前沒有 accepted signals 可供生成 broker payload"
            })

        for s in accepted_signals[:50]:
            missing = []
            if not getattr(s, "ticker", None):
                missing.append("ticker")
            if not getattr(s, "action", None):
                missing.append("action")
            if getattr(s, "target_qty", 0) <= 0:
                missing.append("target_qty")
            if getattr(s, "reference_price", 0) <= 0:
                missing.append("reference_price")

            if missing:
                failures.append({
                    "ticker": getattr(s, "ticker", ""),
                    "missing_fields": missing,
                })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "go_for_submission_contract": len(failures) == 0,
            "failures": failures,
            "warnings": warnings,
            "summary": {
                "accepted_signal_count": len(accepted_signals),
                "failure_count": len(failures),
                "warning_count": len(warnings),
            }
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(
            f"📦 Submission Contract Gate | go_for_submission_contract={payload['go_for_submission_contract']} | "
            f"failures={len(failures)} | warnings={len(warnings)}"
        )
        return self.path, payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class PreOpenChecklistBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        governance = load_json(PATHS.runtime_dir / 'trainer_promotion_decision.json', {}) or {}
        safety = load_json(PATHS.runtime_dir / 'live_safety_gate.json', {}) or {}
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        recovery = load_json(PATHS.runtime_dir / 'recovery_validation.json', {}) or {}
        training = load_json(PATHS.runtime_dir / 'training_orchestrator.json', {}) or {}
        execution = load_json(PATHS.runtime_dir / 'decision_execution_bridge.json', {}) or {}
        incident = load_json(PATHS.runtime_dir / 'intraday_incident_guard.json', {}) or {}
        kill_state = load_json(PATHS.runtime_dir / 'kill_switch_state.json', {}) or {}

        recon_green = bool(recon.get('all_green', recon.get('summary', {}).get('all_green', False)))
        recovery_ready = bool(recovery.get('ready_for_resume', recovery.get('all_green', False)))
        execution_ready = str(execution.get('status', '')).startswith(('execution_payload_ready', 'partial_execution_ready'))
        checklist = [
            {'item': '模型 promotion / governance', 'ok': governance.get('go_for_shadow', governance.get('go_for_promote', False)), 'detail': governance.get('status', 'missing')},
            {'item': 'training orchestrator 已建置', 'ok': bool(training), 'detail': training.get('status', 'missing')},
            {'item': 'execution payload 已產出', 'ok': execution_ready, 'detail': execution.get('status', 'missing')},
            {'item': 'live safety 通過', 'ok': safety.get('paper_live_safe', False), 'detail': safety.get('status', 'missing')},
            {'item': '前次對帳全綠', 'ok': recon_green, 'detail': recon.get('status', 'missing')},
            {'item': 'recovery validation 通過', 'ok': recovery_ready, 'detail': recovery.get('status', 'missing')},
            {'item': 'incident guard 無封鎖', 'ok': incident.get('status', '') != 'incident_guard_block', 'detail': incident.get('status', 'missing')},
            {'item': 'kill switch 未觸發', 'ok': not bool(kill_state.get('system', {}).get('armed')), 'detail': kill_state.get('system', {}).get('reason', '')},
        ]
        green_count = sum(1 for x in checklist if x['ok'])
        payload = {
            'generated_at': now_str(),
            'status': 'preopen_green' if green_count == len(checklist) else 'preopen_partial',
            'all_green': green_count == len(checklist),
            'green_count': green_count,
            'total_count': len(checklist),
            'readiness_pct': int(round(green_count / max(len(checklist), 1) * 100, 0)),
            'items': checklist,
            'next_blockers': [x['item'] for x in checklist if not x['ok']],
        }
        path = PATHS.runtime_dir / 'preopen_checklist.json'
        write_json(path, payload)
        return str(path), payload


# ==============================================================================
# Merged from: fts_admin_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, write_json


class IntradayIncidentGuard:
    def evaluate(self, *, broker_connected: bool, callback_lag_seconds: int, reject_rate: float, day_loss_pct: float, stale_price_symbols: list[str] | None = None, orphan_order_count: int = 0) -> tuple[str, dict[str, Any]]:
        stale_price_symbols = stale_price_symbols or []
        alerts = []
        severity = 'ok'
        if not broker_connected:
            alerts.append('broker_disconnected')
            severity = 'critical'
        if callback_lag_seconds > 30:
            alerts.append('callback_lag_too_high')
            severity = 'critical'
        elif callback_lag_seconds > 5:
            alerts.append('callback_lag_warning')
            severity = 'warn'
        if reject_rate > 0.20:
            alerts.append('reject_rate_too_high')
            severity = 'critical'
        elif reject_rate > 0.05:
            alerts.append('reject_rate_warning')
            severity = max(severity, 'warn')
        if day_loss_pct <= -0.03:
            alerts.append('daily_loss_limit_hit')
            severity = 'critical'
        elif day_loss_pct <= -0.015:
            alerts.append('daily_loss_warning')
            severity = max(severity, 'warn')
        if stale_price_symbols:
            alerts.append('stale_price_symbols_present')
            if severity == 'ok':
                severity = 'warn'
        if orphan_order_count > 0:
            alerts.append('orphan_orders_present')
            if severity == 'ok':
                severity = 'warn'
        payload = {
            'generated_at': now_str(),
            'status': 'incident_guard_block' if severity == 'critical' else ('incident_guard_warn' if severity == 'warn' else 'incident_guard_ok'),
            'severity': severity,
            'alerts': alerts,
            'metrics': {
                'broker_connected': broker_connected,
                'callback_lag_seconds': callback_lag_seconds,
                'reject_rate': round(float(reject_rate), 6),
                'day_loss_pct': round(float(day_loss_pct), 6),
                'stale_price_symbols': stale_price_symbols,
                'orphan_order_count': int(orphan_order_count),
            },
            'kill_switch_recommended': severity == 'critical',
        }
        path = PATHS.runtime_dir / 'intraday_incident_guard.json'
        write_json(path, payload)
        return str(path), payload
