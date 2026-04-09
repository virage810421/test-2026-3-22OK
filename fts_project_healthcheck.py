
from __future__ import annotations

import ast
import json
import os
import py_compile
import subprocess
import sys
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Set, Tuple

EXCLUDE_DIRS = {
    "__pycache__", ".git", ".venv", "venv", "env", "node_modules",
    "archive", ".mypy_cache", ".pytest_cache"
}

WRAPPER_MAP = {
    "advanced_chart.py": "fts_chart_service.py",
    "daily_chip_etl.py": "fts_etl_daily_chip_service.py",
    "monthly_revenue_simple.py": "fts_etl_monthly_revenue_service.py",
    "ml_data_generator.py": "fts_training_data_builder.py",
    "ml_trainer.py": "fts_trainer_backend.py",
    "screening.py": "fts_screening_engine.py",
}

CORE_MODULES = [
    "formal_trading_system_v83_official_main",
    "fts_fundamentals_etl_mainline",
    "fts_training_governance_mainline",
    "fts_feature_service",
    "fts_screening_engine",
    "fts_cross_sectional_percentile_service",
    "fts_event_calendar_service",
    "fts_decision_execution_bridge",
    "fts_phase1_upgrade",
    "fts_phase2_mock_broker_stage",
    "fts_phase3_real_cutover_stage",
    "model_governance",
    "db_setup",
]

SINGLE_ENTRY_EXPECTED_FILES = [
    "formal_trading_system_v83_official_main.py",
    "db_setup_research_plus.py",
    "run_full_market_percentile_snapshot.py",
    "run_precise_event_calendar_build.py",
    "run_sync_feature_snapshots_to_sql.py",
    "ml_data_generator.py",
    "ml_trainer.py",
]

REQUIRED_DIRS = ["data", "runtime", "models", "state", "logs"]
REQUIRED_RUNTIME_JSON = [
    "formal_trading_system_v83_official_main.json",
    "training_orchestrator.json",
    "training_governance_mainline.json",
]
CORE_TABLE_NAMES = [
    "fundamentals_clean",
    "monthly_revenue_simple",
    "daily_chip_data",
    "execution_orders",
    "execution_fills",
    "execution_account_snapshot",
    "execution_positions_snapshot",
    "feature_cross_section_snapshot",
    "feature_event_calendar",
    "live_feature_mount",
]

@dataclass
class CompileResult:
    file: str
    ok: bool
    error: str = ""

@dataclass
class ImportResult:
    module: str
    ok: bool
    error: str = ""
    seconds: float = 0.0

class ProjectHealthcheck:
    def __init__(self, project_root: str | Path | None = None):
        if project_root is None:
            project_root = Path(__file__).resolve().parent
        self.project_root = Path(project_root).resolve()
        self.runtime_dir = self.project_root / "runtime"

    def _iter_py_files(self) -> List[Path]:
        files: List[Path] = []
        for p in self.project_root.rglob("*.py"):
            if any(part in EXCLUDE_DIRS for part in p.parts):
                continue
            files.append(p)
        return sorted(files)

    def _compile_all(self) -> List[CompileResult]:
        out: List[CompileResult] = []
        for p in self._iter_py_files():
            try:
                py_compile.compile(str(p), doraise=True)
                out.append(CompileResult(file=str(p.relative_to(self.project_root)), ok=True))
            except Exception as e:
                out.append(CompileResult(file=str(p.relative_to(self.project_root)), ok=False, error=str(e)))
        return out

    def _ast_import_graph(self) -> Tuple[Dict[str, List[str]], Dict[str, List[str]]]:
        """Returns (local_imports_by_file, missing_local_imports_by_file)"""
        py_files = self._iter_py_files()
        known_modules: Set[str] = {p.stem for p in py_files if p.parent == self.project_root}
        local_imports: Dict[str, List[str]] = {}
        missing_imports: Dict[str, List[str]] = {}
        for p in py_files:
            rel = str(p.relative_to(self.project_root))
            try:
                tree = ast.parse(p.read_text(encoding="utf-8"))
            except Exception:
                continue
            imports: Set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        top = alias.name.split(".")[0]
                        if top in known_modules:
                            imports.add(top)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        top = node.module.split(".")[0]
                        if top in known_modules:
                            imports.add(top)
            local_imports[rel] = sorted(imports)
            missing = [m for m in imports if not (self.project_root / f"{m}.py").exists()]
            if missing:
                missing_imports[rel] = sorted(missing)
        return local_imports, missing_imports

    def _subprocess_import(self, module: str, timeout: int = 20) -> ImportResult:
        start = time.time()
        cmd = [sys.executable, "-c", f"import importlib; importlib.import_module('{module}')"]
        try:
            proc = subprocess.run(
                cmd,
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, "PYTHONPATH": str(self.project_root)},
            )
            ok = proc.returncode == 0
            err = (proc.stderr or proc.stdout).strip()
            return ImportResult(module=module, ok=ok, error=err[:4000], seconds=round(time.time() - start, 3))
        except Exception as e:
            return ImportResult(module=module, ok=False, error=str(e), seconds=round(time.time() - start, 3))

    def _core_import_smoke(self) -> List[ImportResult]:
        results: List[ImportResult] = []
        for mod in CORE_MODULES:
            if (self.project_root / f"{mod}.py").exists():
                results.append(self._subprocess_import(mod))
        return results

    def _wrapper_linkage(self) -> Dict[str, Dict[str, object]]:
        payload: Dict[str, Dict[str, object]] = {}
        for wrapper, service in WRAPPER_MAP.items():
            wp = self.project_root / wrapper
            sp = self.project_root / service
            payload[wrapper] = {
                "wrapper_exists": wp.exists(),
                "service_exists": sp.exists(),
                "linked_ok": wp.exists() and sp.exists(),
            }
        return payload

    def _single_entry_readiness(self) -> Dict[str, object]:
        return {
            "expected_files": {
                f: (self.project_root / f).exists()
                for f in SINGLE_ENTRY_EXPECTED_FILES
            },
            "required_dirs": {
                d: (self.project_root / d).exists()
                for d in REQUIRED_DIRS
            }
        }

    def _db_setup_audit(self) -> Dict[str, object]:
        targets = ["db_setup.py", "db_setup_research_plus.py"]
        content = ""
        present_files = []
        for name in targets:
            p = self.project_root / name
            if p.exists():
                present_files.append(name)
                try:
                    content += "\n" + p.read_text(encoding="utf-8", errors="ignore")
                except Exception:
                    pass
        table_hits = {t: (t in content) for t in CORE_TABLE_NAMES}
        return {
            "present_files": present_files,
            "core_table_names": table_hits,
            "all_core_tables_declared": all(table_hits.values()) if present_files else False,
        }

    def _runtime_presence(self) -> Dict[str, object]:
        payload = {}
        for name in REQUIRED_RUNTIME_JSON:
            payload[name] = (self.runtime_dir / name).exists()
        return {
            "runtime_dir_exists": self.runtime_dir.exists(),
            "required_runtime_json": payload,
        }

    def _deep_subprocess_run(self, args: List[str], timeout: int = 120) -> Dict[str, object]:
        start = time.time()
        try:
            proc = subprocess.run(
                [sys.executable, *args],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                timeout=timeout,
                env={**os.environ, "PYTHONPATH": str(self.project_root), "FTS_HEALTHCHECK": "1"},
            )
            return {
                "args": args,
                "ok": proc.returncode == 0,
                "seconds": round(time.time() - start, 3),
                "stdout_tail": (proc.stdout or "")[-2000:],
                "stderr_tail": (proc.stderr or "")[-2000:],
                "returncode": proc.returncode,
            }
        except Exception as e:
            return {
                "args": args,
                "ok": False,
                "seconds": round(time.time() - start, 3),
                "error": str(e),
            }

    def build_report(self, deep: bool = False) -> tuple[str, dict]:
        compile_results = self._compile_all()
        local_imports, missing_local_imports = self._ast_import_graph()
        import_results = self._core_import_smoke()
        wrapper_linkage = self._wrapper_linkage()
        single_entry = self._single_entry_readiness()
        db_setup = self._db_setup_audit()
        runtime_presence = self._runtime_presence()

        report = {
            "project_root": str(self.project_root),
            "summary": {
                "python_files_total": len(compile_results),
                "compile_failures": sum(0 if r.ok else 1 for r in compile_results),
                "core_import_failures": sum(0 if r.ok else 1 for r in import_results),
                "missing_local_import_edges": sum(len(v) for v in missing_local_imports.values()),
                "wrapper_linkage_failures": sum(
                    0 if v["linked_ok"] else 1 for v in wrapper_linkage.values()
                ),
            },
            "compile_results": [asdict(r) for r in compile_results],
            "core_import_smoke": [asdict(r) for r in import_results],
            "local_imports_by_file": local_imports,
            "missing_local_imports_by_file": missing_local_imports,
            "wrapper_linkage": wrapper_linkage,
            "single_entry_readiness": single_entry,
            "db_setup_audit": db_setup,
            "runtime_presence": runtime_presence,
        }

        if deep:
            deep_runs = []
            main_file = self.project_root / "formal_trading_system_v83_official_main.py"
            if main_file.exists():
                deep_runs.append(self._deep_subprocess_run(["formal_trading_system_v83_official_main.py"], timeout=180))
                deep_runs.append(self._deep_subprocess_run(["formal_trading_system_v83_official_main.py", "--train"], timeout=180))
            report["deep_runs"] = deep_runs

        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        out = self.runtime_dir / "project_healthcheck.json"
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        return str(out), report

def main(argv: List[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description="Run full project healthcheck for v83 stack.")
    parser.add_argument("--deep", action="store_true", help="Also subprocess-run main daily/train smoke tests.")
    parser.add_argument("--project-root", default=".", help="Project root path.")
    args = parser.parse_args(argv)

    path, report = ProjectHealthcheck(args.project_root).build_report(deep=args.deep)
    summary = report["summary"]
    print("=" * 72)
    print("🩺 v83 專案健康檢查完成")
    print(f"📄 報告：{path}")
    print(f"📦 Python 檔總數：{summary['python_files_total']}")
    print(f"🧪 可編譯失敗：{summary['compile_failures']}")
    print(f"🔌 核心 import 失敗：{summary['core_import_failures']}")
    print(f"🧷 本地缺失 import 邊：{summary['missing_local_import_edges']}")
    print(f"🧩 wrapper link 失敗：{summary['wrapper_linkage_failures']}")
    print("=" * 72)
    return 0 if (summary["compile_failures"] == 0 and summary["core_import_failures"] == 0 and summary["missing_local_import_edges"] == 0) else 1

if __name__ == "__main__":
    raise SystemExit(main())
