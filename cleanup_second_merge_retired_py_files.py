# -*- coding: utf-8 -*-
"""
cleanup_second_merge_retired_py_files.py

用途：
  清除第二輪合併後已被汰除/收編的舊 .py 檔。
  這批是從 179 檔版本合併到 148 檔版本時被移除的舊檔名。

注意：
  148 檔版是「清掉舊門牌 + 保留功能本體」版本。
  本工具只清理已被合併或移除的舊門牌/殘留檔，不會刪核心主線檔。

使用：
  預覽：
    python cleanup_second_merge_retired_py_files.py

  真正刪除：
    python cleanup_second_merge_retired_py_files.py --apply

  指定專案根目錄：
    python cleanup_second_merge_retired_py_files.py --project-root C:\\test\\test-2026-3-22OK --apply
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path


# 第二輪合併：179 -> 148。
# 注意：淨少 31 支 .py，但實際被移除舊檔為 34 支，因為同時新增了 3 支新合併主檔：
#   fts_admin_cli.py
#   fts_execution_runtime.py
#   fts_upgrade_suite.py
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
    "run_training_stress_audit.py"
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
}


def _now_text() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def cleanup(project_root: Path, apply: bool = False) -> dict:
    project_root = project_root.resolve()
    runtime_dir = project_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)

    removed = []
    planned = []
    missing = []
    blocked = []

    for rel in RETIRED_PY_FILES:
        rel_path = Path(rel)
        target = (project_root / rel_path).resolve()

        # 防止路徑跳出專案根目錄
        try:
            target.relative_to(project_root)
        except ValueError:
            blocked.append({"file": rel, "reason": "path_outside_project_root"})
            continue

        if rel_path.name in PROTECTED_FILES:
            blocked.append({"file": rel, "reason": "protected_core_file"})
            continue

        if not target.exists():
            missing.append(rel)
            continue

        if not target.is_file():
            blocked.append({"file": rel, "reason": "not_a_file"})
            continue

        if apply:
            try:
                target.unlink()
                removed.append(rel)
            except Exception as exc:
                blocked.append({"file": rel, "reason": f"delete_failed: {exc}"})
        else:
            planned.append(rel)

    report = {
        "tool": "cleanup_second_merge_retired_py_files",
        "version": "148_merge_cleanup",
        "generated_at": _now_text(),
        "project_root": str(project_root),
        "apply": apply,
        "note": "179 檔版合併到 148 檔版：淨少 31 支 .py；實際舊檔移除清單 34 支，因新增 3 支合併主檔。",
        "counts": {
            "retired_list_total": len(RETIRED_PY_FILES),
            "removed": len(removed),
            "planned": len(planned),
            "missing": len(missing),
            "blocked": len(blocked),
        },
        "removed": removed,
        "planned": planned,
        "missing": missing,
        "blocked": blocked,
    }

    out = runtime_dir / "second_merge_retired_py_cleanup_report.json"
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    print("🧹 第二輪合併汰除 .py 清理報告：", out)
    print(
        f"   apply={apply} | removed={len(removed)} | planned={len(planned)} | "
        f"missing={len(missing)} | blocked={len(blocked)}"
    )
    if not apply and planned:
        print("   目前只是預覽，確認後請執行：python cleanup_second_merge_retired_py_files.py --apply")
    if blocked:
        print("   ⚠️ 有檔案被阻擋，請查看 JSON 報告。")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="清除第二輪合併後已汰除的 .py 檔")
    parser.add_argument("--apply", action="store_true", help="真正刪除；未加時只預覽")
    parser.add_argument(
        "--project-root",
        default=".",
        help="專案根目錄，預設為目前目錄",
    )
    args = parser.parse_args()

    cleanup(Path(args.project_root), apply=args.apply)


if __name__ == "__main__":
    main()
