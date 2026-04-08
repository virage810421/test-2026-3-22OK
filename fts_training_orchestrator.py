# -*- coding: utf-8 -*-
import json
import subprocess
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd

from fts_config import PATHS, CONFIG
from fts_utils import now_str, log


class TrainingOrchestrator:
    MODULE_VERSION = "v69"

    def __init__(self):
        self.report_path = PATHS.runtime_dir / "training_orchestrator.json"
        self.recipe_path = PATHS.runtime_dir / "training_bootstrap_recipe.json"
        self.input_manifest_path = PATHS.runtime_dir / "training_input_manifest.json"
        self.universe_csv_path = PATHS.data_dir / "training_bootstrap_universe.csv"
        self.dataset_path = PATHS.data_dir / "ml_training_data.csv"
        self.kline_cache_dir = PATHS.data_dir / "kline_cache"
        self.local_history_report_path = PATHS.runtime_dir / "local_history_bootstrap.json"
        self.request_csv_path = PATHS.data_dir / "kline_cache_request_list.csv"
        self.required_models = [
            PATHS.model_dir / "selected_features.pkl",
            PATHS.model_dir / "model_趨勢多頭.pkl",
            PATHS.model_dir / "model_區間盤整.pkl",
            PATHS.model_dir / "model_趨勢空頭.pkl",
        ]

    def _dataset_summary(self) -> Dict[str, Any]:
        if not self.dataset_path.exists():
            return {"exists": False, "rows": 0, "columns": [], "status": "missing_dataset"}
        try:
            df = pd.read_csv(self.dataset_path, encoding="utf-8-sig")
            columns = list(df.columns)
            feature_cols = [c for c in columns if c not in ("Ticker", "Date", "Regime", "Setup", "Label_Y", "Target_Return", "Stop_Hit", "Hold_Days")]
            label_ratio = {}
            if "Label_Y" in df.columns and len(df) > 0:
                vc = df["Label_Y"].value_counts(dropna=False).to_dict()
                label_ratio = {str(k): int(v) for k, v in vc.items()}
            regime_dist = {}
            if "Regime" in df.columns and len(df) > 0:
                vc = df["Regime"].value_counts(dropna=False).to_dict()
                regime_dist = {str(k): int(v) for k, v in vc.items()}
            return {
                "exists": True,
                "rows": int(len(df)),
                "column_count": int(len(columns)),
                "feature_count": int(len(feature_cols)),
                "columns": columns[:80],
                "label_distribution": label_ratio,
                "regime_distribution": regime_dist,
                "status": "ready_for_training" if len(df) >= 200 else "dataset_too_small",
            }
        except Exception as e:
            return {"exists": True, "rows": 0, "columns": [], "status": f"dataset_read_error: {e}"}

    def _model_summary(self) -> Dict[str, Any]:
        existing = [p.name for p in self.required_models if p.exists()]
        missing = [p.name for p in self.required_models if not p.exists()]
        return {
            "required": [p.name for p in self.required_models],
            "existing": existing,
            "missing": missing,
            "existing_required_count": int(len(existing)),
            "status": "ready_for_inference" if not missing else "models_incomplete",
        }

    def _cache_summary(self) -> Dict[str, Any]:
        csvs = sorted(self.kline_cache_dir.glob('*.csv')) if self.kline_cache_dir.exists() else []
        tickers = sorted({p.stem.split('_')[0] for p in csvs})
        local_history = {}
        if self.local_history_report_path.exists():
            try:
                local_history = json.loads(self.local_history_report_path.read_text(encoding='utf-8'))
            except Exception:
                local_history = {}
        request_rows = 0
        if self.request_csv_path.exists():
            try:
                request_rows = int(len(pd.read_csv(self.request_csv_path, encoding='utf-8-sig')))
            except Exception:
                request_rows = 0
        return {
            "exists": self.kline_cache_dir.exists(),
            "csv_count": int(len(csvs)),
            "ticker_count": int(len(tickers)),
            "tickers_preview": tickers[:20],
            "can_bootstrap_from_cache": bool(len(csvs) >= 5),
            "local_history_status": local_history.get('status', ''),
            "missing_cache_ticker_count": int(local_history.get('missing_cache_ticker_count', 0) or 0),
            "request_list_rows": request_rows,
        }

    def _build_local_universe(self) -> Dict[str, Any]:
        revenue_path = PATHS.base_dir / "monthly_revenue_simple.csv"
        chip_path = PATHS.base_dir / "daily_chip_data_backup.csv"
        funda_path = PATHS.base_dir / "market_financials_backup_fullspeed.csv"

        revenue = pd.read_csv(revenue_path, encoding="utf-8-sig") if revenue_path.exists() else pd.DataFrame()
        chip = pd.read_csv(chip_path, encoding="utf-8-sig") if chip_path.exists() else pd.DataFrame()
        funda = pd.read_csv(funda_path, encoding="utf-8-sig") if funda_path.exists() else pd.DataFrame()

        ticker_sets = []
        if not revenue.empty and "Ticker SYMBOL" in revenue.columns:
            ticker_sets.append(set(revenue["Ticker SYMBOL"].astype(str).str.strip()))
        if not chip.empty and "Ticker SYMBOL" in chip.columns:
            ticker_sets.append(set(chip["Ticker SYMBOL"].astype(str).str.strip()))
        if not funda.empty and "Ticker SYMBOL" in funda.columns:
            ticker_sets.append(set(funda["Ticker SYMBOL"].astype(str).str.strip()))

        common = set.intersection(*ticker_sets) if len(ticker_sets) >= 2 else (ticker_sets[0] if ticker_sets else set())
        universe = pd.DataFrame({"Ticker": sorted(x for x in common if x and x != 'nan')})
        if not universe.empty:
            universe["HasRevenue"] = universe["Ticker"].isin(set(revenue.get("Ticker SYMBOL", pd.Series(dtype=str)).astype(str).str.strip()))
            universe["HasChip"] = universe["Ticker"].isin(set(chip.get("Ticker SYMBOL", pd.Series(dtype=str)).astype(str).str.strip()))
            universe["HasFundamentals"] = universe["Ticker"].isin(set(funda.get("Ticker SYMBOL", pd.Series(dtype=str)).astype(str).str.strip()))
            universe.to_csv(self.universe_csv_path, index=False, encoding="utf-8-sig")
        elif self.universe_csv_path.exists():
            self.universe_csv_path.unlink()

        manifest = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "sources": {
                "monthly_revenue_simple.csv": revenue_path.exists(),
                "daily_chip_data_backup.csv": chip_path.exists(),
                "market_financials_backup_fullspeed.csv": funda_path.exists(),
            },
            "rows": {
                "revenue": int(len(revenue)),
                "chip": int(len(chip)),
                "fundamentals": int(len(funda)),
                "common_universe": int(len(universe)),
            },
            "universe_csv_path": str(self.universe_csv_path),
            "tickers_preview": universe["Ticker"].head(20).tolist() if not universe.empty else [],
            "bootstrap_possible_from_local_tables": bool(len(universe) >= 20),
            "blocking_reason": "missing_kline_cache" if len(universe) >= 1 and not self.kline_cache_dir.exists() else "",
        }
        self.input_manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding='utf-8')
        return manifest

    def _build_bootstrap_recipe(self, dataset: Dict[str, Any], models: Dict[str, Any], cache: Dict[str, Any], manifest: Dict[str, Any]) -> Dict[str, Any]:
        recipe = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "dataset_path": str(self.dataset_path),
            "model_dir": str(PATHS.model_dir),
            "steps": [
                {"step": 1, "title": "檢查本地訓練宇宙", "ready": manifest.get("bootstrap_possible_from_local_tables", False), "detail": "月營收/籌碼/基本面至少交集 20 檔，才值得開始本地 bootstrap"},
                {"step": 2, "title": "準備歷史 K 線快取", "ready": cache.get("can_bootstrap_from_cache", False), "detail": "data/kline_cache 至少 5 檔 CSV，才適合做本地 bootstrap"},
                {"step": 3, "title": "生成 ml_training_data.csv", "ready": dataset.get("exists", False), "detail": "執行 ml_data_generator.py，確認 Label_Y / Regime / Target_Return 完整"},
                {"step": 4, "title": "執行 ml_trainer.py", "ready": dataset.get("status") == "ready_for_training", "detail": "資料列數至少 200，才比較像正式訓練而不是暖機"},
                {"step": 5, "title": "檢查模型產物", "ready": models.get("existing_required_count", 0) >= 4, "detail": "selected_features.pkl + 三個 regime 模型都要落地"},
            ],
            "status": "bootstrap_ready" if cache.get("can_bootstrap_from_cache", False) and manifest.get("bootstrap_possible_from_local_tables", False) else "waiting_for_local_history_cache",
            "request_list_path": str(self.request_csv_path),
        }
        self.recipe_path.write_text(json.dumps(recipe, ensure_ascii=False, indent=2), encoding='utf-8')
        return recipe

    def _run_script(self, script_name: str) -> Dict[str, Any]:
        script_path = PATHS.base_dir / script_name
        if not script_path.exists():
            return {"script": script_name, "ok": False, "reason": "missing_script"}
        try:
            proc = subprocess.run(
                ["python", str(script_path)],
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
        dataset = self._dataset_summary()
        models = self._model_summary()
        cache = self._cache_summary()
        manifest = self._build_local_universe()
        recipe = self._build_bootstrap_recipe(dataset, models, cache, manifest)
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

        payload = {
            "generated_at": now_str(),
            "module_version": self.MODULE_VERSION,
            "ai_stage_enabled": enabled,
            "dry_run": dry_run,
            "dataset": dataset,
            "models": models,
            "cache": cache,
            "input_manifest_path": str(self.input_manifest_path),
            "local_manifest": manifest,
            "bootstrap_recipe_path": str(self.recipe_path),
            "actions": actions,
            "go_for_training": dataset.get("status") == "ready_for_training",
            "go_for_inference": models.get("existing_required_count", 0) >= 2,
            "status": "training_governed",
        }
        self.report_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return payload
