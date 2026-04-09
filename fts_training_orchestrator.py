# -*- coding: utf-8 -*-
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class TrainingOrchestrator:
    MODULE_VERSION = "v83"

    def __init__(self):
        self.report_path = PATHS.runtime_dir / "training_orchestrator.json"
        self.dataset_path = PATHS.data_dir / "ml_training_data.csv"
        self.models_dir = PATHS.model_dir
        self.registry_path = PATHS.runtime_dir / "training_feature_registry.csv"
        self.template_path = PATHS.data_dir / "ml_training_data_template.csv"
        self.bootstrap_plan_path = PATHS.runtime_dir / "training_bootstrap_plan.csv"
        self.required_model_files = [
            self.models_dir / "selected_features.pkl",
            self.models_dir / "model_趨勢多頭.pkl",
            self.models_dir / "model_區間盤整.pkl",
            self.models_dir / "model_趨勢空頭.pkl",
        ]

    def _find_decision_source(self) -> Path | None:
        for p in [PATHS.base_dir / 'daily_decision_desk.csv', PATHS.data_dir / 'normalized_decision_output.csv']:
            if p.exists():
                return p
        return None

    def _write_bootstrap_assets(self) -> Dict[str, Any]:
        src = self._find_decision_source()
        if src is None:
            return {"bootstrap_source": None, "registry_written": False, "template_written": False, 'bootstrap_plan_written': False}
        try:
            df = pd.read_csv(src, encoding='utf-8-sig')
        except Exception:
            df = pd.read_csv(src)
        rows = []
        for col in df.columns:
            col_lower = str(col).lower()
            role = 'meta'
            if any(k in col_lower for k in ['score', 'proba', 'ev', 'kelly', 'risk']):
                role = 'candidate_feature'
            elif col in {'Regime', 'Structure'}:
                role = 'context_feature'
            elif col in {'Ticker', 'Direction', 'Action'}:
                role = 'meta'
            rows.append({'column': col, 'role': role, 'dtype_guess': str(df[col].dtype)})
        pd.DataFrame(rows).to_csv(self.registry_path, index=False, encoding='utf-8-sig')

        template_cols = ['Ticker', 'Date', 'Regime', 'Label_Y', 'Target_Return'] + [r['column'] for r in rows if r['role'] in {'candidate_feature', 'context_feature'}]
        template_cols = list(dict.fromkeys(template_cols))
        pd.DataFrame(columns=template_cols).to_csv(self.template_path, index=False, encoding='utf-8-sig')

        plan_cols = [c for c in ['Ticker', 'Regime', 'Structure', 'Direction', 'AI_Proba', 'Heuristic_EV', 'Realized_EV', 'Kelly_Pos', 'Score', '風險金額'] if c in df.columns]
        bootstrap_plan = df[plan_cols].copy() if plan_cols else pd.DataFrame()
        if not bootstrap_plan.empty:
            bootstrap_plan.insert(1, 'Bootstrap_Status', 'needs_kline_and_labels')
            if 'Ticker' in bootstrap_plan.columns:
                bootstrap_plan = bootstrap_plan.drop_duplicates(['Ticker'], keep='first')
        bootstrap_plan.to_csv(self.bootstrap_plan_path, index=False, encoding='utf-8-sig')
        return {
            'bootstrap_source': str(src),
            'registry_written': True,
            'template_written': True,
            'bootstrap_plan_written': True,
            'candidate_feature_count': sum(1 for r in rows if r['role'] in {'candidate_feature', 'context_feature'}),
            'bootstrap_plan_rows': int(len(bootstrap_plan)),
        }

    def _dataset_summary(self) -> Dict[str, Any]:
        if not self.dataset_path.exists():
            return {
                "exists": False,
                "rows": 0,
                "columns": [],
                "feature_count": 0,
                "label_balance": {},
                "regime_counts": {},
                "status": "missing",
                "readiness_score": 15,
            }

        try:
            df = pd.read_csv(self.dataset_path, encoding="utf-8-sig")
        except Exception as e:
            return {
                "exists": True,
                "rows": 0,
                "columns": [],
                "feature_count": 0,
                "label_balance": {},
                "regime_counts": {},
                "status": f"read_failed: {e}",
                "readiness_score": 10,
            }

        base_drop = {"Ticker", "Date", "Setup", "Regime", "Label_Y", "Target_Return", "Stop_Hit", "Hold_Days"}
        feature_cols = [c for c in df.columns if c not in base_drop]
        label_balance = {}
        regime_counts = {}
        if "Label_Y" in df.columns:
            vc = df["Label_Y"].value_counts(dropna=False).to_dict()
            label_balance = {str(k): int(v) for k, v in vc.items()}
        if "Regime" in df.columns:
            vc = df["Regime"].value_counts(dropna=False).to_dict()
            regime_counts = {str(k): int(v) for k, v in vc.items()}

        status = "ready_for_training"
        score = 35
        if len(df) < 150:
            status = "too_few_rows"
            score = 45 if len(df) >= 30 else 25
        elif "Label_Y" not in df.columns or df["Label_Y"].nunique(dropna=False) < 2:
            status = "label_not_diverse_enough"
            score = 40
        else:
            score = 75

        return {
            "exists": True,
            "rows": int(len(df)),
            "columns": df.columns.tolist(),
            "feature_count": int(len(feature_cols)),
            "label_balance": label_balance,
            "regime_counts": regime_counts,
            "status": status,
            "readiness_score": score,
        }

    def _model_summary(self) -> Dict[str, Any]:
        files = [{"path": str(p), "exists": p.exists()} for p in self.required_model_files]
        existing = sum(1 for x in files if x["exists"])
        score = 5 + existing * 15
        return {
            "models_dir_exists": self.models_dir.exists(),
            "required_files": files,
            "existing_required_count": existing,
            "all_required_present": existing == len(files),
            "readiness_score": min(score, 65),
        }

    def _run_script(self, script_name: str) -> Dict[str, Any]:
        target = PATHS.base_dir / script_name
        if not target.exists():
            return {"script": script_name, "ok": False, "reason": "missing_script"}
        try:
            proc = subprocess.run(
                ["python", str(target)],
                cwd=str(PATHS.base_dir),
                capture_output=True,
                text=True,
                timeout=int(getattr(CONFIG, "upstream_timeout_seconds", 3600)),
            )
            return {
                "script": script_name,
                "ok": proc.returncode == 0,
                "returncode": int(proc.returncode),
                "stdout_tail": (proc.stdout or "")[-3000:],
                "stderr_tail": (proc.stderr or "")[-3000:],
            }
        except Exception as e:
            return {"script": script_name, "ok": False, "reason": str(e)}

    def maybe_execute(self) -> Dict[str, Any]:
        actions: List[Dict[str, Any]] = []
        bootstrap = self._write_bootstrap_assets()
        dataset = self._dataset_summary()
        models = self._model_summary()
        enabled = bool(getattr(CONFIG, "run_ai_stage", False))
        dry_run = bool(getattr(CONFIG, "dry_run_upstream_execution", True))

        if not enabled:
            actions.append({"stage": "ai", "action": "skip", "reason": "run_ai_stage=False"})
        elif dry_run:
            actions.append({"stage": "ai", "action": "skip", "reason": "dry_run_upstream_execution=True"})
        else:
            if not dataset["exists"]:
                log("🧠 TrainingOrchestrator：缺訓練資料，先嘗試生成 ml_training_data.csv")
                actions.append(self._run_script("ml_data_generator.py"))
                dataset = self._dataset_summary()
            if dataset["status"] == "ready_for_training":
                log("🧠 TrainingOrchestrator：訓練資料達標，開始訓練")
                actions.append(self._run_script("ml_trainer.py"))
                models = self._model_summary()
            else:
                actions.append({"stage": "training", "action": "skip", "reason": dataset["status"]})

        bootstrap_score = 0
        if bootstrap.get('registry_written'):
            bootstrap_score += 8
        if bootstrap.get('template_written'):
            bootstrap_score += 8
        if bootstrap.get('bootstrap_plan_written'):
            bootstrap_score += 9
        training_readiness_pct = min(int(round(dataset.get('readiness_score', 0) * 0.45 + models.get('readiness_score', 0) * 0.30 + bootstrap_score)), 100)
        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "ai_stage_enabled": enabled,
            "dry_run": dry_run,
            "bootstrap": bootstrap,
            "bootstrap_score": bootstrap_score,
            "dataset": dataset,
            "models": models,
            "actions": actions,
            "training_readiness_pct": training_readiness_pct,
            "go_for_training": dataset.get("status") == "ready_for_training",
            "go_for_inference": models.get("existing_required_count", 0) >= 2,
            "backend_entrypoint": "fts_trainer_backend.train_models",
            "governance_entrypoint": "model_governance.ModelGovernanceManager",
            "status": "training_governed_v83",
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"🧠 已輸出 training orchestrator：{self.report_path}")
        return payload
