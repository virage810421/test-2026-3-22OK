# -*- coding: utf-8 -*-
"""
cleanup_second_merge_retired_py_files.py

第二輪合併汰除檔安全清理器。

這版不是單純照清單刪檔，而是先做 dependency-aware 檢查：
  1. 核心交易檔永遠保護。
  2. 刪檔前掃描 import / 動態字串引用。
  3. 若檔案仍被 healthcheck/runtime/其他模組期待，會進入 blocked，不會刪。
  4. run_*.py 舊入口只有在 admin CLI 替代入口存在、且沒有其他引用時，才允許清理。

使用：
  預覽：
    python cleanup_second_merge_retired_py_files.py

  真正刪除 ready 檔：
    python cleanup_second_merge_retired_py_files.py --apply

  指定專案根目錄：
    python cleanup_second_merge_retired_py_files.py --project-root C:\test\test-2026-3-22OK --apply
"""

from __future__ import annotations

import argparse
import ast
import json
from datetime import datetime
from pathlib import Path
from typing import Any


RETIRED_PY_FILES = [
    "_clean_old_doors_manifest.py",
    "_patch_manifest_¼[║cª¼º└.py",
    "_patch_manifest_│]¡pñú¿}¡╫º∩5.py",
    "alert_manager.py",
    "alpha_miner.py",
    "apply_clean_old_doors.py",
    "feature_selector.py",
    "fts_ab_wave_upgrade.py",
    "fts_broker_approval.py",
    "fts_broker_shadow_mutator.py",
    "fts_daily_ops.py",
    "fts_execution.py",
    "fts_live_watchlist_loader.py",
    "fts_live_watchlist_promoter.py",
    "fts_live_watchlist_registry.py",
    "fts_merge_manifest.py",
    "fts_phase1_upgrade.py",
    "fts_phase2_mock_broker_stage.py",
    "fts_phase3_real_cutover_stage.py",
    "fts_portfolio_gate.py",
    "fts_progress.py",
    "fts_reconciliation_engine.py",
    "fts_recovery_engine.py",
    "fts_research_gate.py",
    "fts_retry_queue.py",
    "fts_runtime_cleanup.py",
    "fts_upgrade_plan.py",
    "run_backfill_resilience_audit.py",
    "run_full_market_percentile_snapshot.py",
    "run_precise_event_calendar_build.py",
    "run_project_completion_audit.py",
    "run_project_healthcheck.py",
    "run_sync_feature_snapshots_to_sql.py",
    "run_training_stress_audit.py",
]


# 防呆：這些核心檔永遠不允許被本工具刪除。
PROTECTED_FILES = {
    "formal_trading_system_v83_official_main.py",
    "fts_control_tower.py",
    "fts_feature_service.py",
    "fts_screening_engine.py",
    "fts_model_layer.py",
    "fts_strategy_policy_layer.py",
    "fts_training_data_builder.py",
    "fts_trainer_backend.py",
    "fts_training_orchestrator.py",
    "fts_training_governance_mainline.py",
    "fts_market_data_service.py",
    "fts_fundamentals_etl_mainline.py",
    "execution_engine.py",
    "paper_broker.py",
    "live_paper_trading.py",
    "db_logger.py",
    "db_setup.py",
    "fts_admin_cli.py",
    "fts_execution_runtime.py",
    "fts_upgrade_suite.py",
    # 第二輪安全補強：這些雖曾被列 retired，但仍屬 pre-live / recovery 核心能力。
    "fts_reconciliation_engine.py",
    "fts_recovery_engine.py",
    "fts_live_watchlist_loader.py",
    "fts_live_watchlist_promoter.py",
    "fts_live_watchlist_registry.py",
}

# 允許被 admin CLI 接管的舊入口；必須通過替代入口驗證與依賴掃描才會列為 ready。
ADMIN_CLI_REPLACEMENTS = {
    "run_project_healthcheck.py": "healthcheck",
    "run_project_completion_audit.py": "completion-audit",
    "run_training_stress_audit.py": "training-stress-audit",
    "run_backfill_resilience_audit.py": "backfill-resilience-audit",
    "run_full_market_percentile_snapshot.py": "full-market-percentile",
    "run_precise_event_calendar_build.py": "event-calendar-build",
    "run_sync_feature_snapshots_to_sql.py": "sync-feature-snapshots",
}

EXCLUDE_DIRS = {"__pycache__", ".git", ".venv", "venv", "env", "node_modules", "archive"}
THIS_TOOL = "cleanup_second_merge_retired_py_files.py"


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _iter_py_files(project_root: Path) -> list[Path]:
    out: list[Path] = []
    for p in project_root.rglob("*.py"):
        rel_parts = set(p.relative_to(project_root).parts)
        if rel_parts & EXCLUDE_DIRS:
            continue
        out.append(p)
    return sorted(out)


def _safe_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def _admin_cli_has_command(project_root: Path, command: str) -> bool:
    text = _safe_text(project_root / "fts_admin_cli.py")
    return bool(command and (f"'{command}'" in text or f'"{command}"' in text))


def _scan_references(project_root: Path, target_file: str) -> list[dict[str, Any]]:
    """Find imports or dynamic string references to a retiring module.

    Text references are intentionally conservative because runtime loaders often use
    service maps instead of static imports.
    """
    target_stem = Path(target_file).stem
    refs: list[dict[str, Any]] = []
    for p in _iter_py_files(project_root):
        rel = str(p.relative_to(project_root))
        if rel in {target_file, THIS_TOOL}:
            continue
        text = _safe_text(p)
        if not text:
            continue

        # Static import references.
        try:
            tree = ast.parse(text)
            for node in ast.walk(tree):
                imported: list[str] = []
                if isinstance(node, ast.Import):
                    imported = [alias.name.split(".")[0] for alias in node.names]
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported = [node.module.split(".")[0]]
                if target_stem in imported:
                    refs.append({"file": rel, "line": int(getattr(node, "lineno", 0) or 0), "type": "import"})
        except SyntaxError:
            # Syntax errors are handled by healthcheck; for cleanup safety, keep going with text scan.
            pass
        except Exception as exc:
            refs.append({"file": rel, "line": 0, "type": "scan_error", "error": repr(exc)})

        # Dynamic references, service maps, documentation-like hard references.
        if target_stem in text:
            refs.append({"file": rel, "line": 0, "type": "text_reference"})

    # Deduplicate.
    seen = set()
    deduped: list[dict[str, Any]] = []
    for r in refs:
        key = (r.get("file"), r.get("line"), r.get("type"), r.get("error"))
        if key not in seen:
            seen.add(key)
            deduped.append(r)
    return deduped


def _classify_candidate(project_root: Path, rel: str) -> dict[str, Any]:
    rel_path = Path(rel)
    target = (project_root / rel_path).resolve()
    item: dict[str, Any] = {
        "file": rel,
        "exists": target.exists(),
        "ready_to_delete": False,
        "action": "missing" if not target.exists() else "blocked",
        "reasons": [],
        "references": [],
        "replacement": None,
    }

    try:
        target.relative_to(project_root)
    except ValueError:
        item["reasons"].append("path_outside_project_root")
        return item

    if not target.exists():
        item["ready_to_delete"] = False
        item["action"] = "missing"
        item["reasons"].append("file_not_found")
        return item

    if not target.is_file():
        item["reasons"].append("not_a_file")
        return item

    if rel_path.name in PROTECTED_FILES:
        item["reasons"].append("protected_core_or_prelive_file")
        return item

    refs = _scan_references(project_root, rel)
    # Ignore references that only come from generated reports/manifests if any slipped through.
    refs = [r for r in refs if not str(r.get("file", "")).startswith("runtime/")]

    replacement_command = ADMIN_CLI_REPLACEMENTS.get(rel_path.name)
    if replacement_command:
        # fts_project_healthcheck.py may document replaced legacy runners. That is not a runtime dependency.
        refs = [r for r in refs if r.get("file") != "fts_project_healthcheck.py"]

    item["references"] = refs
    if refs:
        item["reasons"].append("still_referenced_by_project")
        return item

    if replacement_command:
        item["replacement"] = {"type": "fts_admin_cli", "command": replacement_command}
        if not _admin_cli_has_command(project_root, replacement_command):
            item["reasons"].append(f"missing_admin_cli_replacement:{replacement_command}")
            return item
        item["ready_to_delete"] = True
        item["action"] = "ready"
        item["reasons"].append("admin_cli_replacement_verified")
        return item

    # Non-wrapper retired files can be removed only when unreferenced and not protected.
    item["ready_to_delete"] = True
    item["action"] = "ready"
    item["reasons"].append("unreferenced_retired_file")
    return item


def cleanup(project_root: Path, apply: bool = False) -> dict[str, Any]:
    project_root = project_root.resolve()
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    ready: list[dict[str, Any]] = []
    removed: list[dict[str, Any]] = []
    missing: list[dict[str, Any]] = []
    blocked: list[dict[str, Any]] = []

    for rel in RETIRED_PY_FILES:
        item = _classify_candidate(project_root, rel)
        if item["action"] == "missing":
            missing.append(item)
            continue
        if not item.get("ready_to_delete"):
            blocked.append(item)
            continue
        if apply:
            try:
                (project_root / rel).unlink()
                item["deleted"] = True
                removed.append(item)
            except Exception as exc:
                item["deleted"] = False
                item["action"] = "blocked"
                item["reasons"].append(f"delete_failed:{exc}")
                blocked.append(item)
        else:
            ready.append(item)

    report = {
        "tool": "cleanup_second_merge_retired_py_files",
        "version": "dependency_aware_cleanup_v2",
        "generated_at": _now_text(),
        "project_root": str(project_root),
        "apply": apply,
        "policy": {
            "core_files_are_never_deleted": True,
            "delete_requires_no_import_or_text_reference": True,
            "run_wrappers_require_admin_cli_replacement": True,
            "reconciliation_recovery_watchlist_are_protected": True,
        },
        "counts": {
            "retired_list_total": len(RETIRED_PY_FILES),
            "ready": len(ready),
            "removed": len(removed),
            "missing": len(missing),
            "blocked": len(blocked),
        },
        "ready": ready,
        "removed": removed,
        "missing": missing,
        "blocked": blocked,
    }

    out = runtime_dir / "second_merge_retired_py_cleanup_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("🧹 第二輪合併汰除 .py 安全清理報告：", out)
    print(
        f"   apply={apply} | ready={len(ready)} | removed={len(removed)} | "
        f"missing={len(missing)} | blocked={len(blocked)}"
    )
    if not apply and ready:
        print("   目前只是預覽；只有 ready 檔案會在 --apply 時刪除。")
    if blocked:
        print("   🛡️ 有檔案被保護/阻擋，請查看 JSON 的 blocked reasons。")

    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="第二輪合併汰除 .py 安全清理器")
    parser.add_argument("--apply", action="store_true", help="真正刪除 ready 檔；未加時只預覽")
    parser.add_argument("--project-root", default=".", help="專案根目錄，預設為目前目錄")
    args = parser.parse_args(argv)
    cleanup(Path(args.project_root), apply=bool(args.apply))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
