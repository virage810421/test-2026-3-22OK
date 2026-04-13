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
    "master_pipeline.py": "fts_pipeline.py",
    "yahoo_csv_to_sql.py": "fts_fundamentals_etl_mainline.py",
    "formal_trading_system_v83_official_main.py": "fts_control_tower.py",
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

TRI_LANE_MODULES = [
    "fts_tri_lane_orchestrator",
    "fts_live_watchlist_promoter",
    "fts_live_watchlist_loader",
    "fts_live_watchlist_registry",
    "fts_execution_layer",
    "fts_execution_ledger",
    "fts_execution_state_machine",
    "fts_callback_event_store",
    "fts_reconciliation_engine",
    "fts_repair_workflow_engine",
    "fts_model_layer",
    "live_paper_trading",
]

SINGLE_ENTRY_EXPECTED_FILES = [
    "formal_trading_system_v83_official_main.py",
    "fts_control_tower.py",
    "launcher.py",
    "master_pipeline.py",
    "db_setup_research_plus.py",
    "run_full_market_percentile_snapshot.py",
    "run_precise_event_calendar_build.py",
    "run_sync_feature_snapshots_to_sql.py",
    "ml_data_generator.py",
    "ml_trainer.py",
    "run_project_healthcheck.py",
]

REQUIRED_DIRS = ["data", "runtime", "models", "state", "logs"]
REQUIRED_RUNTIME_JSON = [
    "formal_trading_system_v83_official_main.json",
    "training_orchestrator.json",
    "training_governance_mainline.json",
]
TRI_LANE_RUNTIME_JSON = [
    "tri_lane_orchestrator.json",
    "live_watchlist_promoter.json",
    "live_watchlist_loader.json",
    "callback_event_store_summary.json",
    "execution_ledger_summary.json",
    "reconciliation_engine.json",
    "repair_workflow_execution.json",
]
TRI_LANE_STATE_JSON = [
    "directional_execution_state_machine.json",
    "broker_side_ledger_shadow.json",
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
    "stock_master",
    "company_quality_snapshot",
    "revenue_momentum_snapshot",
    "price_liquidity_daily",
    "chip_factors_daily",
    "training_universe_daily",
]
TRI_LANE_CRITICAL_FILES = [
    "fts_control_tower.py",
    "fts_tri_lane_orchestrator.py",
    "fts_execution_ledger.py",
    "fts_execution_state_machine.py",
    "fts_repair_workflow_engine.py",
    "fts_callback_event_store.py",
    "fts_reconciliation_engine.py",
    "fts_live_watchlist_loader.py",
    "fts_live_watchlist_promoter.py",
    "fts_live_watchlist_registry.py",
    "live_paper_trading.py",
]

SOFT_IMPORT_ERRORS = (
    "No module named 'pyodbc'",
    'No module named "pyodbc"',
)

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
    severity: str = "hard"

class ProjectHealthcheck:
    def __init__(self, project_root: str | Path | None = None):
        if project_root is None:
            project_root = Path(__file__).resolve().parent
        self.project_root = Path(project_root).resolve()
        self.runtime_dir = self.project_root / "runtime"
        self.state_dir = self.project_root / "state"

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
        py_files = self._iter_py_files()
        known_modules: Set[str] = {p.stem for p in py_files if p.parent == self.project_root}
        local_imports: Dict[str, List[str]] = {}
        missing_imports: Dict[str, List[str]] = {}
        for p in py_files:
            rel = str(p.relative_to(self.project_root))
            try:
                tree = ast.parse(p.read_text(encoding="utf-8", errors='ignore'))
            except Exception:
                continue
            imports: Set[str] = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        top = alias.name.split(".")[0]
                        if top in known_modules:
                            imports.add(top)
                elif isinstance(node, ast.ImportFrom) and node.module:
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
                cmd, cwd=str(self.project_root), capture_output=True, text=True, timeout=timeout,
                env={**os.environ, "PYTHONPATH": str(self.project_root), "FTS_HEALTHCHECK": "1"},
            )
            ok = proc.returncode == 0
            err = (proc.stderr or proc.stdout).strip()
            severity = "hard"
            if not ok and any(token in err for token in SOFT_IMPORT_ERRORS):
                ok = True
                severity = "soft_optional_dependency"
            return ImportResult(module=module, ok=ok, error=err[:4000], seconds=round(time.time() - start, 3), severity=severity)
        except Exception as e:
            return ImportResult(module=module, ok=False, error=str(e), seconds=round(time.time() - start, 3), severity="hard")

    def _core_import_smoke(self, modules: List[str]) -> List[ImportResult]:
        results: List[ImportResult] = []
        for mod in modules:
            if (self.project_root / f"{mod}.py").exists():
                results.append(self._subprocess_import(mod))
        return results

    def _bridge_interface_audit(self) -> Dict[str, object]:
        findings: Dict[str, object] = {}
        try:
            wrapper_file = self.project_root / "screening.py"
            engine_file = self.project_root / "fts_screening_engine.py"
            wrapper_params: List[str] = []
            engine_params: List[str] = []
            if wrapper_file.exists():
                tree = ast.parse(wrapper_file.read_text(encoding="utf-8", errors="ignore"))
                for node in ast.walk(tree):
                    if isinstance(node, ast.FunctionDef) and node.name == 'inspect_stock':
                        wrapper_params = [a.arg for a in node.args.args]
                        break
            if engine_file.exists():
                tree = ast.parse(engine_file.read_text(encoding="utf-8", errors="ignore"))
                for node in ast.walk(tree):
                    if isinstance(node, ast.ClassDef) and node.name == 'ScreeningEngine':
                        for body in node.body:
                            if isinstance(body, ast.FunctionDef) and body.name == 'inspect_stock':
                                engine_params = [a.arg for a in body.args.args]
                                break
            required_by_wrapper = [p for p in wrapper_params if p not in ('ticker', 'self') and p not in engine_params]
            findings['screening.inspect_stock'] = {
                'wrapper_params': wrapper_params,
                'engine_params': engine_params,
                'compatible': len(required_by_wrapper) == 0,
                'missing_in_engine': required_by_wrapper,
                'audit_mode': 'ast_static',
            }
        except Exception as e:
            findings['screening.inspect_stock'] = {'compatible': False, 'error': str(e), 'audit_mode': 'ast_static'}
        return findings

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
            "expected_files": {f: (self.project_root / f).exists() for f in SINGLE_ENTRY_EXPECTED_FILES},
            "required_dirs": {d: (self.project_root / d).exists() for d in REQUIRED_DIRS},
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
        payload = {name: (self.runtime_dir / name).exists() for name in REQUIRED_RUNTIME_JSON}
        tri_runtime = {name: (self.runtime_dir / name).exists() for name in TRI_LANE_RUNTIME_JSON}
        tri_state = {name: (self.state_dir / name).exists() for name in TRI_LANE_STATE_JSON}
        return {
            "runtime_dir_exists": self.runtime_dir.exists(),
            "state_dir_exists": self.state_dir.exists(),
            "required_runtime_json": payload,
            "tri_lane_runtime_json": tri_runtime,
            "tri_lane_state_json": tri_state,
        }

    def _subprocess_code(self, code: str, timeout: int = 30) -> Dict[str, object]:
        start = time.time()
        try:
            proc = subprocess.run([sys.executable, "-c", code], cwd=str(self.project_root), capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=timeout, env={**os.environ, "PYTHONPATH": str(self.project_root), "FTS_HEALTHCHECK": "1", "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"})
            return {"ok": proc.returncode == 0, "seconds": round(time.time() - start, 3), "stdout_tail": (proc.stdout or "")[-1000:], "stderr_tail": (proc.stderr or "")[-1000:], "returncode": proc.returncode}
        except Exception as e:
            return {"ok": False, "seconds": round(time.time() - start, 3), "error": str(e)}

    def _tri_lane_smoke(self) -> Dict[str, object]:
        checks = {}
        smoke_map = {
            'tri_lane_orchestrator': "from fts_tri_lane_orchestrator import TriLaneOrchestrator; print(TriLaneOrchestrator().build())",
            'watchlist_promoter': "from fts_live_watchlist_promoter import LiveWatchlistPromoter; print(LiveWatchlistPromoter().build())",
            'watchlist_loader': "from fts_live_watchlist_loader import LiveWatchlistLoader; print(LiveWatchlistLoader().resolve_live_watchlist())",
            'callback_event_store': "from fts_callback_event_store import CallbackEventStore; print(CallbackEventStore().record({'broker_order_id':'HC1','client_order_id':'HC1','event_type':'fill','status':'FILLED','symbol':'2330.TW','timestamp':'2026-04-12 09:01:00','direction_bucket':'LONG','strategy_bucket':'LONG','approved_pool_type':'LONG','model_scope':'LONG'}))",
            'reconciliation_engine': "from fts_reconciliation_engine import ReconciliationEngine; print(ReconciliationEngine().reconcile([], [], [], [], [], [], 0.0, 0.0))",
        }
        for name, code in smoke_map.items():
            checks[name] = self._subprocess_code(code, timeout=45)
        return checks

    def _json_persistence_audit(self) -> Dict[str, object]:
        direct_writes = []
        helper_backed = []
        for rel in TRI_LANE_CRITICAL_FILES:
            p = self.project_root / rel
            if not p.exists():
                continue
            text = p.read_text(encoding='utf-8', errors='ignore')
            if 'write_text(json.dumps' in text:
                direct_writes.append(rel)
            if 'write_json' in text or 'append_jsonl' in text:
                helper_backed.append(rel)
        runtime_helper = self.project_root / 'fts_prelive_runtime.py'
        helper_text = runtime_helper.read_text(encoding='utf-8', errors='ignore') if runtime_helper.exists() else ''
        atomic_enabled = 'os.replace' in helper_text and 'O_EXCL' in helper_text
        return {
            'atomic_write_helper_enabled': atomic_enabled,
            'tri_lane_helper_backed_files': sorted(helper_backed),
            'tri_lane_direct_json_write_files': sorted(set(direct_writes)),
            'risk_ok': atomic_enabled and len(direct_writes) == 0,
        }

    def _tri_lane_completion_audit(self) -> Dict[str, object]:
        payload = {'control_tower': {}, 'repair_mutator': {}}
        ct_text = (self.project_root / 'fts_control_tower.py').read_text(encoding='utf-8', errors='ignore') if (self.project_root / 'fts_control_tower.py').exists() else ''
        rw_text = (self.project_root / 'fts_repair_workflow_engine.py').read_text(encoding='utf-8', errors='ignore') if (self.project_root / 'fts_repair_workflow_engine.py').exists() else ''
        ledger_text = (self.project_root / 'fts_execution_ledger.py').read_text(encoding='utf-8', errors='ignore') if (self.project_root / 'fts_execution_ledger.py').exists() else ''
        sm_text = (self.project_root / 'fts_execution_state_machine.py').read_text(encoding='utf-8', errors='ignore') if (self.project_root / 'fts_execution_state_machine.py').exists() else ''
        payload['control_tower'] = {
            'tri_lane_orchestrator_hooked': ('TriLaneOrchestrator' in ct_text) or ('fts_tri_lane_orchestrator' in ct_text),
            'tri_lane_stage_status_present': any(token in ct_text for token in ['tri_lane_stage_status', 'tri_lane_execution_status', 'tri_lane_stage_runs']),
            'deep_full_split_complete': all(token in ct_text for token in ['TriLaneOrchestrator', 'tri_lane_stage_status', 'tri_lane_execution_status']),
        }
        payload['repair_mutator'] = {
            'repair_workflow_engine_present': bool(rw_text),
            'ledger_mutation_present': 'mutate_from_repair' in ledger_text,
            'state_machine_repair_present': 'force_repair' in sm_text,
            'fully_automated_mutator_complete': all(token in rw_text for token in ['execute', 'executed_actions']) and 'mutate_from_repair' in ledger_text and 'force_repair' in sm_text and 'broker_side_ledger_shadow' in ledger_text,
        }
        return payload

    def _deep_subprocess_run(self, args: List[str], timeout: int = 120) -> Dict[str, object]:
        start = time.time()
        try:
            proc = subprocess.run([sys.executable, *args], cwd=str(self.project_root), capture_output=True, text=True, encoding='utf-8', errors='replace', timeout=timeout, env={**os.environ, 'PYTHONPATH': str(self.project_root), 'FTS_HEALTHCHECK': '1', 'PYTHONIOENCODING': 'utf-8', 'PYTHONUTF8': '1'})
            return {'args': args, 'ok': proc.returncode == 0, 'seconds': round(time.time() - start, 3), 'stdout_tail': (proc.stdout or '')[-2000:], 'stderr_tail': (proc.stderr or '')[-2000:], 'returncode': proc.returncode}
        except Exception as e:
            return {'args': args, 'ok': False, 'seconds': round(time.time() - start, 3), 'error': str(e)}


    def _training_dataset_audit(self) -> Dict[str, object]:
        dataset_path = self.project_root / 'data' / 'ml_training_data.csv'
        if not dataset_path.exists():
            return {'exists': False, 'status': 'missing'}
        try:
            size = dataset_path.stat().st_size
        except Exception:
            size = -1
        if size == 0:
            return {'exists': True, 'status': 'zero_bytes', 'size': size}
        try:
            import pandas as pd
            df = pd.read_csv(dataset_path)
            return {
                'exists': True,
                'status': 'ok' if (not df.empty and len(df.columns) > 0) else 'empty_dataframe',
                'size': size,
                'rows': int(len(df)),
                'columns': int(len(df.columns)),
            }
        except Exception as e:
            return {'exists': True, 'status': 'unreadable', 'size': size, 'error': str(e)}

    def build_report(self, deep: bool = False) -> tuple[str, dict]:
        compile_results = self._compile_all()
        local_imports, missing_local_imports = self._ast_import_graph()
        import_results = self._core_import_smoke(CORE_MODULES)
        tri_lane_import_results = self._core_import_smoke(TRI_LANE_MODULES)
        wrapper_linkage = self._wrapper_linkage()
        single_entry = self._single_entry_readiness()
        db_setup = self._db_setup_audit()
        runtime_presence = self._runtime_presence()
        bridge_interface_audit = self._bridge_interface_audit()
        tri_lane_smoke = self._tri_lane_smoke()
        json_persistence_audit = self._json_persistence_audit()
        tri_lane_completion = self._tri_lane_completion_audit()
        training_dataset_audit = self._training_dataset_audit()

        report = {
            'project_root': str(self.project_root),
            'summary': {
                'python_files_total': len(compile_results),
                'compile_failures': sum(0 if r.ok else 1 for r in compile_results),
                'core_import_failures': sum(0 if r.ok else 1 for r in import_results),
                'tri_lane_import_failures': sum(0 if r.ok else 1 for r in tri_lane_import_results),
                'tri_lane_import_failed_modules': [r.module for r in tri_lane_import_results if not r.ok],
                'missing_local_import_edges': sum(len(v) for v in missing_local_imports.values()),
                'wrapper_linkage_failures': sum(0 if v['linked_ok'] else 1 for v in wrapper_linkage.values()),
                'bridge_interface_failures': sum(0 if v.get('compatible', False) else 1 for v in bridge_interface_audit.values() if isinstance(v, dict)),
                'tri_lane_smoke_failures': sum(0 if x.get('ok') else 1 for x in tri_lane_smoke.values()),
                'json_persistence_risk_failures': 0 if json_persistence_audit.get('risk_ok') else 1,
            },
            'compile_results': [asdict(r) for r in compile_results],
            'core_import_smoke': [asdict(r) for r in import_results],
            'tri_lane_import_smoke': [asdict(r) for r in tri_lane_import_results],
            'local_imports_by_file': local_imports,
            'missing_local_imports_by_file': missing_local_imports,
            'wrapper_linkage': wrapper_linkage,
            'single_entry_readiness': single_entry,
            'db_setup_audit': db_setup,
            'runtime_presence': runtime_presence,
            'bridge_interface_audit': bridge_interface_audit,
            'tri_lane_smoke': tri_lane_smoke,
            'json_persistence_audit': json_persistence_audit,
            'tri_lane_completion_audit': tri_lane_completion,
            'training_dataset_audit': training_dataset_audit,
        }
        if deep:
            deep_runs = []
            if (self.project_root / 'formal_trading_system_v83_official_main.py').exists():
                deep_runs.append(self._deep_subprocess_run(['formal_trading_system_v83_official_main.py'], timeout=180))
                deep_runs.append(self._deep_subprocess_run(['formal_trading_system_v83_official_main.py', '--train'], timeout=180))
            report['deep_runs'] = deep_runs
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        out = self.runtime_dir / 'project_healthcheck.json'
        out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        return str(out), report

def main(argv: List[str] | None = None) -> int:
    import argparse
    parser = argparse.ArgumentParser(description='Run full project healthcheck for v83 stack.')
    parser.add_argument('--deep', action='store_true', help='Also subprocess-run main daily/train smoke tests.')
    parser.add_argument('--project-root', default='.', help='Project root path.')
    args = parser.parse_args(argv)
    path, report = ProjectHealthcheck(args.project_root).build_report(deep=args.deep)
    summary = report['summary']
    print('=' * 72)
    print('🩺 v83 專案健康檢查完成')
    print(f'📄 報告：{path}')
    print(f'📦 Python 檔總數：{summary["python_files_total"]}')
    print(f'🧪 可編譯失敗：{summary["compile_failures"]}')
    print(f'🔌 核心 import 失敗：{summary["core_import_failures"]}')
    print(f'🧭 tri-lane import 失敗：{summary["tri_lane_import_failures"]}')
    if summary.get('tri_lane_import_failed_modules'):
        print('   ↳ 失敗模組：' + ', '.join(summary['tri_lane_import_failed_modules']))
    td = report.get('training_dataset_audit', {})
    if td:
        print(f"🧫 訓練資料集狀態：{td.get('status')}")
    print(f'🧷 本地缺失 import 邊：{summary["missing_local_import_edges"]}')
    print(f'🧩 wrapper link 失敗：{summary["wrapper_linkage_failures"]}')
    print(f'🪛 bridge 介面失敗：{summary.get("bridge_interface_failures", 0)}')
    print(f'🚦 tri-lane smoke 失敗：{summary.get("tri_lane_smoke_failures", 0)}')
    print(f'🧱 JSON 併發風險失敗：{summary.get("json_persistence_risk_failures", 0)}')
    print('=' * 72)
    ok = (summary['compile_failures'] == 0 and summary['core_import_failures'] == 0 and summary['tri_lane_import_failures'] == 0 and summary['missing_local_import_edges'] == 0 and summary.get('bridge_interface_failures', 0) == 0 and summary.get('tri_lane_smoke_failures', 0) == 0 and summary.get('json_persistence_risk_failures', 0) == 0)
    return 0 if ok else 1

if __name__ == '__main__':
    raise SystemExit(main())
