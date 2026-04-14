# -*- coding: utf-8 -*-
from __future__ import annotations

"""Consolidated module generated from 12 smaller files.
Original public classes/functions are preserved in this module.
"""


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from dataclasses import asdict, dataclass
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

@dataclass
class ArchitectureMap:
    etl_entry: str = "daily_chip_etl.py / monthly_revenue_simple.py / yahoo_csv_to_sql.py"
    ai_entry: str = "ml_data_generator.py / ml_trainer.py / model_governance.py"
    research_entry: str = "你的 research / scoring / decision builder 主程式"
    decision_file: str = "daily_decision_desk.csv"
    execution_entry: str = "formal_trading_system_v19.py"
    state_store: str = "state/engine_state.json"
    audit_trail: str = "runtime/audit_events.jsonl"
    runtime_heartbeat: str = "runtime/heartbeat.json"
    notes: str = "v19 重點是把新主控明確掛回你的原始上游架構，而不是取代它。"

class ArchitectureMapWriter:
    def __init__(self):
        self.path = PATHS.runtime_dir / "architecture_map.json"

    def write(self):
        amap = ArchitectureMap()
        payload = asdict(amap)
        payload["generated_at"] = now_str()
        payload["system_name"] = CONFIG.system_name
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗺️ 已輸出 architecture map：{self.path}")
        return self.path


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class GateSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "gate_summary.json"

    def build(self, research_gate, model_gate, launch_gate, live_safety_gate, broker_approval_gate, submission_gate):
        summary = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "research_gate": research_gate.get("go_for_decision_linkage", False),
            "model_gate": model_gate.get("go_for_model_linkage", False),
            "launch_gate": launch_gate.get("go_for_execution", False),
            "live_safety_gate": live_safety_gate.get("paper_live_safe", False),
            "broker_approval_gate": broker_approval_gate.get("go_for_broker_submission", False),
            "submission_gate": submission_gate.get("go_for_submission_contract", False),
        }
        summary["all_green"] = all(v for k, v in summary.items() if isinstance(v, bool))

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        log(f"🚥 已輸出 gate summary：{self.path}")
        return self.path, summary


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ModeSwitchPolicy:
    def __init__(self):
        self.path = PATHS.runtime_dir / "mode_switch_policy.json"

    def build(self):
        mode = getattr(CONFIG, "mode", "PAPER")
        broker_type = getattr(CONFIG, "broker_type", "paper")
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "current_mode": mode,
            "current_broker_type": broker_type,
            "policy": {
                "paper_allowed_directly": True,
                "live_requires_approval": True,
                "live_requires_submission_gate": True,
                "live_requires_launch_gate": True,
                "live_requires_live_safety_gate": True,
                "notes": "v37 先把切換規則工程化，尚未真正開放 live 自動送單"
            }
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🔀 已輸出 mode switch policy：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class OperatorApprovalRegistry:
    def __init__(self):
        self.path = PATHS.runtime_dir / 'operator_approval_registry.json'

    def _load(self) -> dict[str, Any]:
        payload = load_json(self.path, {}) or {}
        if 'approvals' not in payload:
            payload = {'approvals': []}
        return payload

    def approve(self, stage: str, operator: str, approved: bool, note: str = '') -> tuple[str, dict[str, Any]]:
        payload = self._load()
        item = {
            'ts': now_str(),
            'stage': stage,
            'operator': operator,
            'approved': bool(approved),
            'note': note,
        }
        payload['approvals'].append(item)
        payload['last'] = item
        payload['status'] = 'approval_recorded'
        write_json(self.path, payload)
        return str(self.path), payload

    def latest_for(self, stage: str) -> dict[str, Any]:
        payload = self._load()
        for item in reversed(payload.get('approvals', [])):
            if item.get('stage') == stage:
                return item
        return {}


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class ReconciliationSummaryBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "reconciliation_summary.json"

    def build(self, execution_result: dict, accepted_count: int, rejected_count: int):
        summary = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "accepted_count": accepted_count,
            "risk_rejected_count": rejected_count,
            "submitted": execution_result.get("submitted", 0),
            "filled": execution_result.get("filled", 0),
            "partially_filled": execution_result.get("partially_filled", 0),
            "broker_rejected": execution_result.get("rejected", 0),
            "cancelled": execution_result.get("cancelled", 0),
            "auto_exit_signals": execution_result.get("auto_exit_signals", 0),
            "fills_count": execution_result.get("fills_count", 0),
            "status": "summary_only"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log(f"🧮 已輸出 reconciliation summary：{self.path}")
        return self.path, summary


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path

import pandas as pd

try:
    from fts_config import PATHS  # type: ignore
except Exception:
    class _Paths:
        base_dir = Path(__file__).resolve().parent
        runtime_dir = base_dir / 'runtime'
        data_dir = base_dir / 'data'
    PATHS = _Paths()

TASKS = [
    ('主控串聯', '主線'), ('全市場percentile', '研究層'), ('事件窗精準化', '研究層'), ('特徵掛載', '研究層'),
    ('訓練資料接新特徵', 'AI訓練'), ('研究層增補table', 'SQL'), ('特徵snapshot寫回SQL', 'SQL'),
]


class TaskCompletionRegistry:
    def __init__(self):
        self.csv_path = Path(PATHS.data_dir) / 'task_completion_registry.csv'
        self.runtime_path = Path(PATHS.runtime_dir) / 'task_completion_registry.json'
        self.csv_path.parent.mkdir(parents=True, exist_ok=True)
        self.runtime_path.parent.mkdir(parents=True, exist_ok=True)

    def build(self):
        df = pd.DataFrame([{'任務名稱': t, '任務分類': c, '完成狀態': '完成'} for t, c in TASKS])
        df.to_csv(self.csv_path, index=False, encoding='utf-8-sig')
        payload = {'rows': int(len(df)), 'csv_path': str(self.csv_path), 'status': 'task_completion_registry_ready'}
        self.runtime_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
        return self.runtime_path, payload


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from datetime import datetime
from fts_config import PATHS
from fts_utils import now_str, log

class TaskLogArchiver:
    def __init__(self):
        self.base_dir = PATHS.runtime_dir / "task_logs"
        self.base_dir.mkdir(exist_ok=True)

    def write_result(self, task_name: str, stage: str, payload: dict):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = f"{stage}_{task_name}".replace("/", "_").replace("\\", "_").replace(" ", "_")
        path = self.base_dir / f"{safe_name}_{ts}.json"
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🗃️ 已封存 task log：{path}")
        return path


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from dataclasses import dataclass, asdict
from typing import List
from fts_config import PATHS
from fts_utils import now_str, log

@dataclass
class RegisteredTask:
    stage: str
    name: str
    script: str
    required: bool = False
    enabled: bool = True
    notes: str = ""

class TaskRegistry:
    def __init__(self):
        self.tasks: List[RegisteredTask] = [
            RegisteredTask("etl", "daily_chip_etl", "daily_chip_etl.py", required=True, notes="法人籌碼/日更資料"),
            RegisteredTask("etl", "monthly_revenue", "monthly_revenue_simple.py", required=False, notes="月營收"),
            RegisteredTask("etl", "fundamentals_import", "yahoo_csv_to_sql.py", required=False, notes="財報/基本面"),
            RegisteredTask("ai", "ml_data_generator", "ml_data_generator.py", required=False, notes="特徵資料集"),
            RegisteredTask("ai", "ml_trainer", "ml_trainer.py", required=False, notes="模型訓練"),
            RegisteredTask("ai", "model_governance", "model_governance.py", required=False, notes="模型治理"),
            RegisteredTask("decision", "decision_builder_csv", "daily_decision_desk.csv", required=True, notes="決策輸出檔"),
        ]
        self.path = PATHS.runtime_dir / "task_registry.json"

    def write(self):
        payload = {
            "generated_at": now_str(),
            "tasks": [asdict(t) for t in self.tasks],
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧩 已輸出 task registry：{self.path}")
        return self.path

    def summary(self):
        return [asdict(t) for t in self.tasks]


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
from typing import Any

from fts_prelive_runtime import PATHS, now_str, load_json, write_json


class EODCloseBookBuilder:
    def build(self) -> tuple[str, dict[str, Any]]:
        recon = load_json(PATHS.runtime_dir / 'reconciliation_engine.json', {}) or {}
        attrib = load_json(PATHS.runtime_dir / 'performance_attribution.json', {}) or {}
        daily_ops = load_json(PATHS.runtime_dir / 'daily_ops_summary.json', {}) or {}
        phase2 = load_json(PATHS.runtime_dir / 'phase2_mock_real_broker.json', {}) or {}
        incident = load_json(PATHS.runtime_dir / 'intraday_incident_guard.json', {}) or {}
        execution = load_json(PATHS.runtime_dir / 'decision_execution_bridge.json', {}) or {}
        callback_store = load_json(PATHS.runtime_dir / 'callback_event_store_summary.json', {}) or {}

        recon_green = bool(recon.get('all_green', recon.get('summary', {}).get('all_green', False)))
        payload = {
            'generated_at': now_str(),
            'status': 'closebook_ready' if recon_green else 'closebook_attention',
            'reconciliation_status': recon.get('status', 'missing'),
            'all_green': recon_green,
            'headline': {
                'payload_rows': execution.get('rows_total', 0),
                'orders_submitted': phase2.get('orders_submitted', 0),
                'orders_filled': phase2.get('orders_filled', 0),
                'fills_count': phase2.get('fills_count', 0),
                'callbacks_recorded': phase2.get('callbacks_recorded', 0),
                'incident_status': incident.get('status', 'missing'),
            },
            'pnl_summary': attrib.get('headline', {}),
            'callback_store': callback_store,
            'close_notes': (daily_ops.get('close_notes', []) or []) + phase2.get('notes', []),
            'next_day_resume_ready': recon_green and incident.get('status') != 'incident_guard_block',
        }
        path = PATHS.runtime_dir / 'eod_closebook.json'
        write_json(path, payload)
        return str(path), payload


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class DeepRiskCheckBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "deep_risk_checks.json"

    def build(self, accepted_signals):
        checks = []
        total_qty = sum(max(0, getattr(s, "target_qty", 0)) for s in accepted_signals) if accepted_signals else 0
        unique_tickers = len(set(getattr(s, "ticker", "") for s in accepted_signals if getattr(s, "ticker", "")))

        checks.append({
            "check": "accepted_signal_count",
            "value": len(accepted_signals),
            "status": "ok"
        })
        checks.append({
            "check": "unique_ticker_count",
            "value": unique_tickers,
            "status": "ok"
        })
        checks.append({
            "check": "total_target_qty",
            "value": total_qty,
            "status": "ok"
        })

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "checks": checks,
            "status": "deepcheck_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🛡️ 已輸出 deep risk checks：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from collections import Counter
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RiskLimitsPlusBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "risk_limits_plus.json"

    def build(self, accepted_signals):
        tickers = [getattr(s, "ticker", "") for s in accepted_signals if getattr(s, "ticker", "")]
        ticker_counts = Counter(tickers)
        duplicate_tickers = {k: v for k, v in ticker_counts.items() if v > 1}

        total_qty = sum(max(0, getattr(s, "target_qty", 0)) for s in accepted_signals)
        total_notional_proxy = sum(
            max(0, getattr(s, "target_qty", 0)) * max(0, getattr(s, "reference_price", 0))
            for s in accepted_signals
        )

        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "summary": {
                "accepted_signal_count": len(accepted_signals),
                "unique_ticker_count": len(set(tickers)),
                "duplicate_ticker_count": len(duplicate_tickers),
                "total_qty": total_qty,
                "total_notional_proxy": total_notional_proxy,
            },
            "duplicate_tickers": duplicate_tickers,
            "status": "deeper_risk_skeleton"
        }

        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        log(f"🛡️ 已輸出 risk limits plus：{self.path}")
        return self.path, payload


# ==============================================================================
# Merged from: fts_operations_suite.py
# ==============================================================================
# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class AIQualityReportBuilder:
    def __init__(self):
        self.path = PATHS.runtime_dir / "ai_quality_report.json"

    def build(self, ai_exec: dict):
        expected_inputs = [
            PATHS.base_dir / "ml_data_generator.py",
            PATHS.base_dir / "ml_trainer.py",
            PATHS.base_dir / "model_governance.py",
            PATHS.base_dir / "data" / "ml_training_data.csv",
        ]
        expected_outputs = [
            PATHS.base_dir / "models",
            PATHS.base_dir / "daily_decision_desk.csv",
        ]
        payload = {
            "generated_at": now_str(),
            "system_name": CONFIG.system_name,
            "inputs": [{"path": str(p), "exists": p.exists()} for p in expected_inputs],
            "outputs": [{"path": str(p), "exists": p.exists()} for p in expected_outputs],
            "ai_exec_summary": {
                "enabled": ai_exec.get("ai_stage_enabled", False),
                "dry_run": ai_exec.get("dry_run", True),
                "executed_count": len(ai_exec.get("executed", [])),
                "skipped_count": len(ai_exec.get("skipped", [])),
                "failed_count": len(ai_exec.get("failed", [])),
            },
            "status": "quality_skeleton"
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧠 已輸出 ai quality report：{self.path}")
        return self.path, payload
