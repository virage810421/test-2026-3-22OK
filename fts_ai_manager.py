# -*- coding: utf-8 -*-
import json
import subprocess
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class AITrainingManager:
    def __init__(self):
        self.path = PATHS.runtime_dir / "ai_training_run.json"

    def inspect(self):
        info = {
            "generated_at": now_str(),
            "feature_data_script": str(PATHS.base_dir / "ml_data_generator.py"),
            "trainer_script": str(PATHS.base_dir / "ml_trainer.py"),
            "governance_script": str(PATHS.base_dir / "model_governance.py"),
            "feature_data_script_exists": (PATHS.base_dir / "ml_data_generator.py").exists(),
            "trainer_script_exists": (PATHS.base_dir / "ml_trainer.py").exists(),
            "governance_script_exists": (PATHS.base_dir / "model_governance.py").exists(),
            "training_data_exists": (PATHS.base_dir / "data" / "ml_training_data.csv").exists(),
            "models_dir_exists": (PATHS.base_dir / "models").exists(),
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        log(f"🧠 已輸出 AI manager inspect：{self.path}")
        return info

    def maybe_run_training_stage(self):
        """
        v30 先做安全納管：
        - 預設不自動訓練
        - 但主控已經知道要怎麼檢查、怎麼跑、怎麼記錄
        """
        enabled = getattr(CONFIG, "run_ai_stage", False)
        dry_run = getattr(CONFIG, "dry_run_upstream_execution", True)
        result = {
            "generated_at": now_str(),
            "ai_stage_enabled": enabled,
            "dry_run": dry_run,
            "executed": [],
            "skipped": [],
            "failed": [],
        }

        tasks = [
            ("ml_data_generator", "ml_data_generator.py"),
            ("ml_trainer", "ml_trainer.py"),
            ("model_governance", "model_governance.py"),
        ]

        if not enabled:
            result["skipped"].append({"reason": "run_ai_stage=False"})
            return result

        for name, script in tasks:
            target = PATHS.base_dir / script
            if not target.exists():
                result["failed"].append({"task": name, "script": script, "reason": "script missing"})
                continue

            if dry_run:
                result["skipped"].append({"task": name, "script": script, "reason": "dry_run_upstream_execution=True"})
                continue

            try:
                log(f"🧠 執行 AI 任務：{name} / {script}")
                proc = subprocess.run(
                    ["python", str(target)],
                    cwd=str(PATHS.base_dir),
                    capture_output=True,
                    text=True,
                    timeout=getattr(CONFIG, "upstream_timeout_seconds", 3600),
                )
                row = {
                    "task": name,
                    "script": script,
                    "returncode": proc.returncode,
                    "stdout_tail": proc.stdout[-3000:] if proc.stdout else "",
                    "stderr_tail": proc.stderr[-3000:] if proc.stderr else "",
                }
                if proc.returncode == 0:
                    result["executed"].append(row)
                else:
                    row["reason"] = "nonzero returncode"
                    result["failed"].append(row)
            except Exception as e:
                result["failed"].append({"task": name, "script": script, "reason": str(e)})

        out = PATHS.runtime_dir / "ai_training_exec_result.json"
        with open(out, "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False, indent=2)
        log(f"🧠 已輸出 AI training exec result：{out}")
        return result
