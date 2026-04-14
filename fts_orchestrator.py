# -*- coding: utf-8 -*-
import subprocess
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import log
from fts_operations_suite import TaskLogArchiver

class UpstreamOrchestrator:
    def __init__(self):
        self.log_archiver = TaskLogArchiver()

    def check_tasks(self, task_registry_summary):
        result = {"ready": [], "missing": []}
        for task in task_registry_summary:
            target = PATHS.base_dir / task["script"]
            exists = target.exists()
            row = {
                "stage": task["stage"],
                "name": task["name"],
                "script": task["script"],
                "exists": exists,
                "required": task["required"],
                "enabled": task["enabled"],
            }
            if exists:
                result["ready"].append(row)
            else:
                result["missing"].append(row)
        log(f"🛰️ 上游任務檢查 | ready={len(result['ready'])} | missing={len(result['missing'])}")
        for row in result["missing"][:20]:
            tag = "必要" if row["required"] else "可選"
            log(f"   - 缺少 [{tag}] {row['stage']} / {row['name']} / {row['script']}")
        return result

    def _stage_enabled(self, stage: str) -> bool:
        mapping = {
            "etl": getattr(CONFIG, "run_etl_stage", False),
            "ai": getattr(CONFIG, "run_ai_stage", False),
            "decision": getattr(CONFIG, "run_decision_stage", False),
        }
        return mapping.get(stage, False)

    def _archive(self, task, result_payload):
        try:
            self.log_archiver.write_result(task_name=task.get("name", "unknown"), stage=task.get("stage", "unknown"), payload=result_payload)
        except Exception as e:
            log(f"⚠️ task log archive 失敗：{e}")

    def _execute_one(self, task):
        stage = task["stage"]
        script = task["script"]
        target = PATHS.base_dir / script

        if not target.exists():
            result = {"status": "failed", "reason": "script missing", **task}
            self._archive(task, result)
            return result

        if target.suffix.lower() == ".csv":
            if stage == "decision" and getattr(CONFIG, "require_decision_file_after_decision_stage", True):
                exists = target.exists()
                result = {"status": "checked" if exists else "failed", "reason": "decision csv existence check", **task}
            else:
                result = {"status": "checked", "reason": "csv task exists", **task}
            self._archive(task, result)
            return result

        if getattr(CONFIG, "dry_run_upstream_execution", True):
            result = {"status": "dry_run", "reason": "dry run enabled", **task}
            self._archive(task, result)
            return result

        log(f"🚀 執行上游任務 | stage={stage} | task={task['name']} | script={script}")
        try:
            proc = subprocess.run(
                ["python", str(target)],
                cwd=str(PATHS.base_dir),
                capture_output=True,
                text=True,
                timeout=getattr(CONFIG, "upstream_timeout_seconds", 3600),
            )
            result = {
                "status": "ok" if proc.returncode == 0 else "failed",
                "returncode": proc.returncode,
                "stdout": proc.stdout[-5000:] if proc.stdout else "",
                "stderr": proc.stderr[-5000:] if proc.stderr else "",
                **task,
            }
            if proc.stdout:
                log(proc.stdout.strip()[:1500])
            if proc.stderr:
                log(proc.stderr.strip()[:1500])

            self._archive(task, result)
            return result
        except Exception as e:
            result = {"status": "failed", "reason": str(e), **task}
            self._archive(task, result)
            return result

    def execute_tasks(self, task_registry_summary):
        results = []
        if not getattr(CONFIG, "enable_upstream_execution", False):
            return {"executed": [], "skipped": [{"reason": "enable_upstream_execution=False"}], "failed": []}

        for task in task_registry_summary:
            stage = task["stage"]
            enabled = task["enabled"]

            if not enabled:
                results.append({"status": "skipped", "reason": "task disabled", **task})
                continue

            stage_enabled = self._stage_enabled(stage)
            if getattr(CONFIG, "require_manual_stage_enable", True) and not stage_enabled:
                results.append({"status": "skipped", "reason": f"{stage} stage manual enable required", **task})
                continue

            results.append(self._execute_one(task))

        return self._finalize(results)

    def execute_retry_items(self, retry_items):
        results = []
        for item in retry_items:
            stage = item["stage"]
            if getattr(CONFIG, "retry_only_same_stage_enabled", True) and not self._stage_enabled(stage):
                results.append({"status": "skipped", "reason": "retry stage not enabled", **item})
                continue
            if item.get("required") and not getattr(CONFIG, "auto_retry_required_tasks", False):
                results.append({"status": "skipped", "reason": "required auto retry disabled", **item})
                continue
            if (not item.get("required")) and not getattr(CONFIG, "auto_retry_failed_optional_tasks", False):
                results.append({"status": "skipped", "reason": "optional auto retry disabled", **item})
                continue

            task = {
                "stage": item["stage"],
                "name": item["name"],
                "script": item["script"],
                "required": item.get("required", False),
                "enabled": True,
            }
            results.append(self._execute_one(task))
        return self._finalize(results)

    def _finalize(self, results):
        executed = [r for r in results if r["status"] in ("ok", "checked")]
        failed = [r for r in results if r["status"] == "failed"]
        skipped = [r for r in results if r["status"] in ("skipped", "dry_run")]
        log(f"🛠️ 上游調度結果 | executed={len(executed)} | failed={len(failed)} | skipped={len(skipped)}")
        for row in failed[:20]:
            tag = "必要" if row.get("required") else "可選"
            log(f"   - 失敗 [{tag}] {row.get('stage')} / {row.get('name')} / {row.get('script')}")
        return {"executed": executed, "failed": failed, "skipped": skipped}
