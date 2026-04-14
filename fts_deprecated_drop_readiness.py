# -*- coding: utf-8 -*-
from __future__ import annotations

"""
Deprecated scan + drop readiness report.

用途：
  python fts_deprecated_drop_readiness.py
  python fts_deprecated_drop_readiness.py --project-root C:\test\test-2026-3-22OK
  python fts_admin_cli.py drop-readiness

設計原則：
  1. 這支工具只掃描與產報告，不會刪檔、不會 DROP 欄位。
  2. execution / broker / risk / model / governance 屬核心交易路徑，偏 fail-closed。
  3. ETL / research / legacy wrapper 允許 fail-open，但必須留下 diagnostics。
  4. Ticker SYMBOL 不全域硬刪；只判斷是否可以從 execution 舊欄位或舊 wrapper 退役。
"""

import argparse
import ast
import json
import re
from collections import Counter, defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

try:
    from fts_config import PATHS  # type: ignore
except Exception:  # pragma: no cover
    class _Paths:
        runtime_dir = Path('runtime')
    PATHS = _Paths()


CORE_RUNTIME_FILES = {
    'formal_trading_system_v83_official_main.py',
    'fts_control_tower.py',
    'live_paper_trading.py',
    'execution_engine.py',
    'fts_execution_layer.py',
    'fts_execution_runtime.py',
    'fts_execution_ledger.py',
    'fts_execution_state_machine.py',
    'fts_broker_api_adapter.py',
    'fts_broker_real_stub.py',
    'fts_broker_core.py',
    'fts_broker_interface.py',
    'fts_kill_switch.py',
    'fts_live_readiness_gate.py',
    'fts_live_safety.py',
    'fts_model_layer.py',
    'fts_training_governance_mainline.py',
    'db_logger.py',
    'fts_db_migrations.py',
    'fts_db_schema.py',
    'fts_exception_policy.py',
    'fts_symbol_contract.py',
    'fts_system_guard_service.py',
    'system_guard.py',
}

FAIL_OPEN_PREFIXES = (
    'fts_etl_', 'daily_chip_', 'monthly_', 'yahoo_', 'insert_', 'advanced_',
    'screening', 'strategy', 'strategies', 'research', 'event_backtester',
    'fts_chip_', 'fts_company_', 'fts_fundamentals_', 'fts_market_data_',
    'fts_cross_sectional_', 'fts_feature_', 'fts_chart_', 'fts_legacy_',
)

LEGACY_SYMBOL = 'Ticker SYMBOL'
CANONICAL_SYMBOL = 'ticker_symbol'

EXECUTION_KEYWORDS = (
    'execution_orders', 'execution_fills', 'execution_positions_snapshot',
    'execution_position_lots', 'execution_broker_callbacks',
    'execution_reconciliation_report', 'order_id', 'fill_id', 'broker_order_id',
    'callback_id', 'position_lots', 'broker_callbacks',
)


@dataclass
class Finding:
    severity: str
    category: str
    file: str
    line: int
    message: str
    recommendation: str


@dataclass
class DropCandidate:
    target: str
    current_status: str
    drop_readiness: str
    evidence: list[str]
    blockers: list[str]
    required_steps_before_drop: list[str]


def _now() -> str:
    return datetime.now().isoformat(timespec='seconds')


def _read(path: Path) -> str:
    try:
        return path.read_text(encoding='utf-8', errors='replace')
    except Exception:
        return ''


def _iter_py(project_root: Path) -> list[Path]:
    skip_dirs = {'__pycache__', '.git', '.venv', 'venv', 'env', 'runtime', 'data', 'models'}
    out: list[Path] = []
    for p in project_root.rglob('*.py'):
        if any(part in skip_dirs for part in p.relative_to(project_root).parts[:-1]):
            continue
        out.append(p)
    return sorted(out)


def _classify_file(rel: str) -> str:
    name = Path(rel).name
    if name in CORE_RUNTIME_FILES:
        return 'core_runtime'
    if name.startswith(FAIL_OPEN_PREFIXES):
        return 'etl_research_or_legacy'
    if 'legacy' in name.lower() or 'old' in name.lower() or 'cleanup' in name.lower():
        return 'etl_research_or_legacy'
    return 'general'


def _line_no(text: str, pos: int) -> int:
    return text.count('\n', 0, pos) + 1


def _count_function_defs(text: str) -> Counter:
    """Count duplicate module-level functions only.

    Class methods such as as_dict()/build()/load() may repeat by design across
    dataclasses/builders and should not be treated as overwritten definitions.
    """
    c = Counter()
    try:
        tree = ast.parse(text)
    except SyntaxError:
        return c
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            c[node.name] += 1
    return c


def _detect_system_guard_wrapper(text: str) -> tuple[bool, list[str]]:
    evidence = []
    delegates = 'fts_system_guard_service' in text or 'SystemGuardService' in text
    evidence.append('delegates_to_fts_system_guard_service=' + str(bool(delegates)))
    risky_patterns = [
        'pyodbc.connect', 'CREATE TABLE', 'INSERT INTO', 'LINE', 'line_notify',
        'requests.post', 'system_guard_report', 'SERVER=', 'DATABASE=',
    ]
    risky_found = [p for p in risky_patterns if p in text]
    evidence.append('risky_legacy_patterns=' + (','.join(risky_found) if risky_found else 'none'))
    wrapper_only = delegates and not risky_found
    return wrapper_only, evidence


def _scan_import_references(files: list[Path], project_root: Path, module_name: str) -> list[str]:
    refs = []
    rx1 = re.compile(r'^\s*import\s+' + re.escape(module_name) + r'(\s|$|,)', re.M)
    rx2 = re.compile(r'^\s*from\s+' + re.escape(module_name) + r'\s+import\s+', re.M)
    for p in files:
        rel = str(p.relative_to(project_root))
        if rel == module_name + '.py':
            continue
        text = _read(p)
        if rx1.search(text) or rx2.search(text):
            refs.append(rel)
    return refs


def _scan_static(project_root: Path) -> dict[str, Any]:
    files = _iter_py(project_root)
    findings: list[Finding] = []
    counters: Counter[str] = Counter()
    by_file: dict[str, dict[str, Any]] = {}
    ticker_refs_by_file: dict[str, int] = {}
    execution_legacy_symbol_refs: list[dict[str, Any]] = []
    duplicate_defs: dict[str, dict[str, int]] = {}

    for p in files:
        rel = str(p.relative_to(project_root))
        name = p.name
        cls = _classify_file(rel)
        text = _read(p)
        counters['py_files'] += 1
        counters[f'file_class.{cls}'] += 1

        token_counts = {
            'except_exception': len(re.findall(r'except\s+Exception\b', text)),
            'bare_except': len(re.findall(r'except\s*:', text)),
            'pass': len(re.findall(r'\bpass\b', text)),
            'fallback': len(re.findall(r'fallback', text, flags=re.I)),
            'legacy_Ticker_SYMBOL': text.count(LEGACY_SYMBOL),
            'canonical_ticker_symbol': text.count(CANONICAL_SYMBOL),
            'todo': len(re.findall(r'TODO|FIXME|stub|placeholder', text, flags=re.I)),
        }
        for k, v in token_counts.items():
            counters[k] += v
        by_file[rel] = {'class': cls, **token_counts}

        if token_counts['legacy_Ticker_SYMBOL']:
            ticker_refs_by_file[rel] = token_counts['legacy_Ticker_SYMBOL']
            if cls == 'core_runtime' or any(k in text for k in EXECUTION_KEYWORDS):
                for m in re.finditer(re.escape(LEGACY_SYMBOL), text):
                    line = _line_no(text, m.start())
                    snippet = text[max(0, m.start()-140):m.start()+180]
                    file_level_compat_marker = any(marker in text for marker in [
                        'LEGACY_SCHEMA_COMPAT_MARKER',
                        'LEGACY_SQL_SYMBOL_COMPAT_MARKER',
                        'LEGACY_SYMBOL_MIGRATION_COMPAT_MARKER',
                        'legacy_symbol_contract',
                    ])
                    allowed_alias_context = file_level_compat_marker or any(s in snippet for s in [
                        'legacy', 'alias', 'backfill', 'COALESCE', '_pick', 'canonical',
                        'normalize', 'accepted', 'compat', '保留', '相容', '回填',
                    ])
                    execution_legacy_symbol_refs.append({
                        'file': rel,
                        'line': line,
                        'allowed_alias_context': bool(allowed_alias_context),
                    })
                    if not allowed_alias_context:
                        findings.append(Finding(
                            severity='medium' if cls == 'core_runtime' else 'low',
                            category='legacy_symbol_in_execution_context',
                            file=rel,
                            line=line,
                            message='core/execution 相關檔案仍直接出現 Ticker SYMBOL，需確認是否只是 alias 相容。',
                            recommendation='正式 execution contract 應輸出/寫入 ticker_symbol；Ticker SYMBOL 只允許在 normalize/backfill/legacy alias 區塊出現。',
                        ))

        # exception/fallback policy hints
        if cls == 'core_runtime':
            rec_except = '核心交易路徑的 except Exception 應搭配 diagnostics 並 fail-closed。'
            rec_pass = '核心交易路徑不應用 pass 靜默跳過，需寫 runtime diagnostic。'
            rec_fallback = '核心 fallback 必須標示 source/status，不能讓系統誤認正式生效。'
            try:
                tree_for_policy = ast.parse(text)
            except SyntaxError:
                tree_for_policy = None
            if tree_for_policy is not None:
                for node in ast.walk(tree_for_policy):
                    if isinstance(node, ast.ExceptHandler):
                        typ = node.type
                        is_broad = typ is None or (isinstance(typ, ast.Name) and typ.id == 'Exception')
                        if not is_broad:
                            continue
                        segment = ast.get_source_segment(text, node) or ''
                        has_diag = any(tok in segment for tok in ['diagnostic', 'diagnostics', 'record_issue', 'record_diagnostic', 'fail_closed', 'fail-closed', 'fail_mode', 'hard_block', 'record_exception', 'write_runtime', 'runtime', 'raise'])
                        if not has_diag:
                            findings.append(Finding('high', 'core_except_exception', rel, getattr(node, 'lineno', 0), f'{rel} 核心路徑出現 broad except 且 handler 未見 diagnostics/fail-closed。', rec_except))
                    elif isinstance(node, ast.Pass):
                        parent_segment = ''
                        # A literal pass is acceptable only when nearby comments mark diagnostics/no-op intent.
                        line = getattr(node, 'lineno', 0)
                        lines = text.splitlines()
                        window = '\n'.join(lines[max(0, line-4):min(len(lines), line+4)])
                        has_diag = any(tok in window for tok in ['diagnostic', 'diagnostics', 'record_issue', 'record_diagnostic', 'intentional', 'no-op', 'noop', 'runtime', 'fail_closed', 'fail-open'])
                        if not has_diag:
                            findings.append(Finding('high', 'core_pass', rel, line, f'{rel} 核心路徑出現 pass 且附近未見 diagnostics/intentional no-op。', rec_pass))
            for m in re.finditer(r'fallback', text, flags=re.I):
                line = _line_no(text, m.start())
                window = text[max(0, m.start()-220):m.start()+260]
                explicit_disabled = any(tok in window for tok in ['fallback_disabled_by_default', 'fallback_to_hazard: False', 'EXIT_MODEL_FALLBACK_TO_HAZARD=False', 'exit_ai_model_unavailable'])
                explicit_disabled = explicit_disabled or ('EXIT_FALLBACK_TO_HAZARD = bool(getattr(CONFIG' in text and 'EXIT_MODEL_FALLBACK_TO_HAZARD' in text and 'False' in text)
                if 'hazard' in window.lower() and not explicit_disabled:
                    findings.append(Finding('medium', 'core_fallback', rel, line, f'{rel} 出現 fallback 字樣，需確認不是 exit AI 靜默降級。', rec_fallback))

        fn_counts = _count_function_defs(text)
        dups = {k: v for k, v in fn_counts.items() if v > 1}
        if dups:
            duplicate_defs[rel] = dups
            sev = 'high' if rel == 'fts_model_layer.py' else 'medium'
            findings.append(Finding(
                severity=sev,
                category='duplicate_function_definitions',
                file=rel,
                line=0,
                message='同一檔案存在重複函式定義：' + ', '.join(f'{k}x{v}' for k, v in dups.items()),
                recommendation='保留最後正式版，刪除前段覆寫版本，避免修到無效函式。',
            ))

    return {
        'files': [str(p.relative_to(project_root)) for p in files],
        'counters': dict(counters),
        'by_file': by_file,
        'ticker_refs_by_file': ticker_refs_by_file,
        'execution_legacy_symbol_refs': execution_legacy_symbol_refs,
        'duplicate_defs': duplicate_defs,
        'findings': [asdict(f) for f in findings],
        'system_guard_import_refs': _scan_import_references(files, project_root, 'system_guard'),
    }


def _optional_db_probe() -> dict[str, Any]:
    """Best-effort DB inspection. Never raises; report stays useful offline."""
    result: dict[str, Any] = {
        'available': False,
        'status': 'not_checked',
        'tables': {},
        'error': None,
    }
    try:
        from fts_db_engine import DBConfig, DatabaseSession  # type: ignore
    except Exception as exc:
        result.update({'status': 'db_module_unavailable', 'error': f'{type(exc).__name__}: {exc}'})
        return result

    tables = [
        'execution_orders', 'execution_fills', 'execution_positions_snapshot',
        'execution_position_lots', 'execution_broker_callbacks',
        'active_positions', 'trade_history',
    ]
    try:
        with DatabaseSession(DBConfig()) as db:
            result['available'] = True
            result['status'] = 'checked'
            for table in tables:
                exists = bool(db.table_exists(table))
                info = {'exists': exists, 'has_Ticker_SYMBOL': False, 'has_ticker_symbol': False, 'row_count': None, 'null_ticker_symbol_count': None}
                if exists:
                    info['has_Ticker_SYMBOL'] = bool(db.column_exists(table, 'Ticker SYMBOL'))
                    info['has_ticker_symbol'] = bool(db.column_exists(table, 'ticker_symbol'))
                    try:
                        rows = db.fetchall(f'SELECT COUNT(*) AS cnt FROM dbo.{table}')
                        if rows:
                            info['row_count'] = int(rows[0][0])
                    except Exception as exc:
                        info['row_count_error'] = f'{type(exc).__name__}: {exc}'
                    if info['has_ticker_symbol']:
                        try:
                            rows = db.fetchall(f"SELECT COUNT(*) AS cnt FROM dbo.{table} WHERE ticker_symbol IS NULL OR ticker_symbol = ''")
                            if rows:
                                info['null_ticker_symbol_count'] = int(rows[0][0])
                        except Exception as exc:
                            info['null_ticker_symbol_error'] = f'{type(exc).__name__}: {exc}'
                result['tables'][table] = info
    except Exception as exc:
        result.update({'status': 'db_check_failed', 'error': f'{type(exc).__name__}: {exc}'})
    return result


def _candidate_report(project_root: Path, static: dict[str, Any], db: dict[str, Any]) -> list[DropCandidate]:
    candidates: list[DropCandidate] = []
    by_file = static['by_file']

    # system_guard.py wrapper retirement
    sg_path = project_root / 'system_guard.py'
    if sg_path.exists():
        wrapper_only, evidence = _detect_system_guard_wrapper(_read(sg_path))
        refs = static.get('system_guard_import_refs') or []
        evidence += [f'import_refs={len(refs)}'] + [f'import_ref:{r}' for r in refs[:20]]
        blockers = []
        if not wrapper_only:
            blockers.append('system_guard.py 仍含舊 SQL/LINE/建表/報告等實作，不是純 wrapper。')
        if refs:
            blockers.append('仍有其他檔案 import system_guard；刪檔前需改 import 到 fts_system_guard_service。')
        candidates.append(DropCandidate(
            target='file:system_guard.py',
            current_status='wrapper_only' if wrapper_only else 'legacy_logic_present',
            drop_readiness='READY_TO_RETIRE' if not blockers else 'NOT_READY',
            evidence=evidence,
            blockers=blockers,
            required_steps_before_drop=[] if not blockers else ['改掉 import refs', '確認 fts_system_guard_service.py 已是唯一主線', '跑 healthcheck/bootstrap/daily'],
        ))

    # fts_model_layer duplicate defs
    dups = static.get('duplicate_defs', {}).get('fts_model_layer.py', {})
    candidates.append(DropCandidate(
        target='duplicate_defs:fts_model_layer.py',
        current_status='duplicate_defs_found' if dups else 'clean',
        drop_readiness='READY_CLEAN' if not dups else 'NOT_READY',
        evidence=[f'duplicate_defs={dups or "none"}'],
        blockers=[] if not dups else ['fts_model_layer.py 仍有重複函式定義'],
        required_steps_before_drop=[] if not dups else ['刪除被後段覆寫的舊函式', '重新跑 py_compile', '確認 exit runtime 欄位存在'],
    ))

    # exit fallback hard block
    cfg_text = _read(project_root / 'config.py') + '\n' + _read(project_root / 'fts_config.py')
    ml_text = _read(project_root / 'fts_model_layer.py')
    fallback_enabled = bool(re.search(r'EXIT_MODEL_FALLBACK_TO_HAZARD\s*=\s*True|exit_model_fallback_to_hazard\s*=\s*True', cfg_text))
    hard_block_seen = bool(re.search(r'EXIT_MODEL_HARD_BLOCK_WHEN_UNAVAILABLE\s*=\s*True|exit_model_hard_block_when_unavailable\s*=\s*True|hard_block', cfg_text + ml_text, re.I))
    blockers = []
    if fallback_enabled:
        blockers.append('config 仍允許 EXIT_MODEL_FALLBACK_TO_HAZARD=True。')
    if not hard_block_seen:
        blockers.append('未看到 exit model unavailable 時 hard block 的明確設定/邏輯。')
    candidates.append(DropCandidate(
        target='config:exit_model_hazard_fallback',
        current_status='fallback_enabled' if fallback_enabled else 'fallback_disabled',
        drop_readiness='READY_CLEAN' if not blockers else 'NOT_READY',
        evidence=[f'fallback_enabled={fallback_enabled}', f'hard_block_seen={hard_block_seen}'],
        blockers=blockers,
        required_steps_before_drop=[] if not blockers else ['關閉 fallback', '確認 runtime 輸出 exit_model_source', '模型缺失時禁止 silent fallback'],
    ))

    # execution old Ticker SYMBOL in code
    exec_refs = static.get('execution_legacy_symbol_refs', [])
    unapproved = [r for r in exec_refs if not r.get('allowed_alias_context')]
    candidates.append(DropCandidate(
        target='code:execution_Ticker_SYMBOL_references',
        current_status=f'{len(exec_refs)} refs, {len(unapproved)} unapproved',
        drop_readiness='CONDITIONAL_READY' if not unapproved else 'NOT_READY',
        evidence=[f'total_execution_context_refs={len(exec_refs)}', f'unapproved_refs={len(unapproved)}'] + [f"{r['file']}:{r['line']}" for r in unapproved[:30]],
        blockers=[] if not unapproved else ['核心/execution 檔案仍有未標示 alias/backfill/compat 的 Ticker SYMBOL 使用。'],
        required_steps_before_drop=['跑 fts_db_migrations.py upgrade 回填 DB', '確認所有 execution runtime/output 都有 ticker_symbol', '觀察 3~5 輪 daily/paper 無舊欄位讀取告警'],
    ))

    # DB old columns readiness
    tables = db.get('tables') or {}
    for table in ['execution_orders', 'execution_fills', 'execution_positions_snapshot', 'execution_position_lots', 'execution_broker_callbacks']:
        info = tables.get(table)
        if not info:
            candidates.append(DropCandidate(
                target=f'db:{table}.[Ticker SYMBOL]',
                current_status='db_not_checked',
                drop_readiness='UNKNOWN_DB_NOT_CHECKED',
                evidence=['DB 無法檢查或未連線；此報告只完成靜態掃描。'],
                blockers=['需要在本機 SQL Server 跑本工具才知道舊欄位是否存在。'],
                required_steps_before_drop=['python fts_db_migrations.py upgrade', 'python fts_admin_cli.py drop-readiness'],
            ))
        else:
            has_old = bool(info.get('has_Ticker_SYMBOL'))
            has_new = bool(info.get('has_ticker_symbol'))
            null_new = info.get('null_ticker_symbol_count')
            blockers = []
            if has_old and not has_new:
                blockers.append('DB 表仍只有舊欄位，需先 migration rename/backfill。')
            if has_new and null_new not in (None, 0):
                blockers.append(f'ticker_symbol 仍有 NULL/空值：{null_new}')
            readiness = 'READY_TO_DROP' if has_old and has_new and not blockers else ('NO_OLD_COLUMN' if not has_old else 'NOT_READY')
            candidates.append(DropCandidate(
                target=f'db:{table}.[Ticker SYMBOL]',
                current_status=f"exists={info.get('exists')} old={has_old} new={has_new} null_new={null_new}",
                drop_readiness=readiness,
                evidence=[json.dumps(info, ensure_ascii=False)],
                blockers=blockers,
                required_steps_before_drop=[] if readiness == 'NO_OLD_COLUMN' else ['備份資料庫', '確認 no unapproved code refs', '手動審核後才允許 drop 舊欄位'],
            ))

    for table in ['active_positions', 'trade_history']:
        info = tables.get(table)
        if not info:
            readiness = 'KEEP_COMPAT_DB_NOT_CHECKED'
            evidence = ['DB 無法檢查；舊表建議保留相容欄位。']
        else:
            readiness = 'KEEP_COMPAT_NOT_DROP'
            evidence = [json.dumps(info, ensure_ascii=False)]
        candidates.append(DropCandidate(
            target=f'db:{table}.[Ticker SYMBOL]',
            current_status='legacy_table_compat_layer',
            drop_readiness=readiness,
            evidence=evidence,
            blockers=['active_positions/trade_history 是舊相容層；目前不建議破壞式 drop。'],
            required_steps_before_drop=['若未來要 drop：先改所有報表/SQL/CSV/training 讀 ticker_symbol，再連跑多輪確認。'],
        ))

    # exception policy global cleanup readiness
    c = static['counters']
    high_findings = [f for f in static['findings'] if f['severity'] == 'high']
    candidates.append(DropCandidate(
        target='policy:global_exception_fail_closed',
        current_status=f"except_exception={c.get('except_exception', 0)}, pass={c.get('pass', 0)}, fallback={c.get('fallback', 0)}, high_findings={len(high_findings)}",
        drop_readiness='NOT_READY_FOR_GLOBAL_FAIL_CLOSED' if high_findings else 'CORE_READY_KEEP_ETL_FAIL_OPEN',
        evidence=[f"py_files={c.get('py_files', 0)}", f"core_runtime_files={c.get('file_class.core_runtime', 0)}"],
        blockers=['ETL/research/legacy wrapper 不應全改 fail-closed；只需 diagnostics。'] + ([f"core_high_findings={len(high_findings)}"] if high_findings else []),
        required_steps_before_drop=['核心交易路徑 fail-closed', 'ETL/research fail-open + diagnostics', '不要全域硬刪 except Exception'],
    ))

    return candidates


def build_report(project_root: Path, check_db: bool = False) -> dict[str, Any]:
    project_root = project_root.resolve()
    static = _scan_static(project_root)
    db = _optional_db_probe() if check_db else {
        'available': False,
        'status': 'not_checked_static_only',
        'tables': {},
        'error': 'Use --check-db on the target machine to inspect SQL Server columns.',
    }
    candidates = _candidate_report(project_root, static, db)
    severity_counts = Counter(f['severity'] for f in static['findings'])
    readiness_counts = Counter(c.drop_readiness for c in candidates)
    status = 'ready_with_manual_db_review'
    if severity_counts.get('high', 0) > 0:
        status = 'not_ready_core_findings'
    elif any(c.drop_readiness.startswith('NOT_READY') for c in candidates):
        status = 'partial_ready_some_blockers'

    report = {
        'generated_at': _now(),
        'tool': 'fts_deprecated_drop_readiness.py',
        'tool_version': 'v20260414_deprecated_drop_readiness_audit',
        'project_root': str(project_root),
        'status': status,
        'summary': {
            'py_files': static['counters'].get('py_files', 0),
            'findings_total': len(static['findings']),
            'severity_counts': dict(severity_counts),
            'drop_readiness_counts': dict(readiness_counts),
            'legacy_Ticker_SYMBOL_refs': static['counters'].get('legacy_Ticker_SYMBOL', 0),
            'canonical_ticker_symbol_refs': static['counters'].get('canonical_ticker_symbol', 0),
            'except_exception_count': static['counters'].get('except_exception', 0),
            'pass_count': static['counters'].get('pass', 0),
            'fallback_count': static['counters'].get('fallback', 0),
            'db_probe_status': db.get('status'),
        },
        'drop_candidates': [asdict(c) for c in candidates],
        'top_findings': static['findings'][:100],
        'static_counters': static['counters'],
        'ticker_refs_by_file_top': dict(sorted(static['ticker_refs_by_file'].items(), key=lambda kv: kv[1], reverse=True)[:50]),
        'duplicate_defs': static['duplicate_defs'],
        'db_probe': db,
        'policy': {
            'execution_contract': 'ticker_symbol',
            'legacy_symbol_contract': 'Ticker SYMBOL allowed only in ETL/research/legacy alias/backfill',
            'core_runtime_policy': 'fail-closed + runtime diagnostics',
            'etl_research_policy': 'fail-open + runtime diagnostics',
            'destructive_actions': 'report_only; this tool never deletes files or drops columns',
        },
    }
    return report


def write_markdown(report: dict[str, Any], out_path: Path) -> None:
    lines = []
    s = report['summary']
    lines.append('# Deprecated Scan + Drop Readiness Report')
    lines.append('')
    lines.append(f"生成時間：{report['generated_at']}")
    lines.append(f"狀態：`{report['status']}`")
    lines.append('')
    lines.append('## 摘要')
    lines.append('')
    lines.append(f"- Python 檔案數：{s['py_files']}")
    lines.append(f"- Findings：{s['findings_total']}，severity={s['severity_counts']}")
    lines.append(f"- Drop readiness：{s['drop_readiness_counts']}")
    lines.append(f"- Ticker SYMBOL refs：{s['legacy_Ticker_SYMBOL_refs']}")
    lines.append(f"- ticker_symbol refs：{s['canonical_ticker_symbol_refs']}")
    lines.append(f"- except Exception：{s['except_exception_count']}")
    lines.append(f"- pass：{s['pass_count']}")
    lines.append(f"- fallback：{s['fallback_count']}")
    lines.append(f"- DB 檢查：{s['db_probe_status']}")
    lines.append('')
    lines.append('## Drop / Retire Candidates')
    lines.append('')
    for c in report['drop_candidates']:
        lines.append(f"### {c['target']}")
        lines.append(f"- current_status：`{c['current_status']}`")
        lines.append(f"- drop_readiness：`{c['drop_readiness']}`")
        if c['blockers']:
            lines.append('- blockers：')
            for b in c['blockers']:
                lines.append(f"  - {b}")
        if c['required_steps_before_drop']:
            lines.append('- required_steps_before_drop：')
            for step in c['required_steps_before_drop']:
                lines.append(f"  - {step}")
        lines.append('')
    lines.append('## Top Findings')
    lines.append('')
    for f in report['top_findings'][:40]:
        lines.append(f"- `{f['severity']}` `{f['category']}` {f['file']}:{f['line']} - {f['message']}")
    lines.append('')
    out_path.write_text('\n'.join(lines), encoding='utf-8')


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description='FTS deprecated scan + drop readiness report')
    p.add_argument('--project-root', default='.', help='Project root. Default: current directory.')
    p.add_argument('--json-out', default=None, help='Output JSON path. Default: runtime/deprecated_drop_readiness_report.json')
    p.add_argument('--md-out', default=None, help='Output markdown path. Default: runtime/deprecated_drop_readiness_report.md')
    p.add_argument('--fail-on-high', action='store_true', help='Return non-zero when high severity findings exist.')
    p.add_argument('--check-db', action='store_true', help='Also inspect SQL Server table/column status. Default is static-only to avoid blocking bootstrap.')
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = Path(args.project_root).resolve()
    runtime_dir = Path(getattr(PATHS, 'runtime_dir', project_root / 'runtime'))
    if not runtime_dir.is_absolute():
        runtime_dir = project_root / runtime_dir
    json_out = Path(args.json_out) if args.json_out else runtime_dir / 'deprecated_drop_readiness_report.json'
    md_out = Path(args.md_out) if args.md_out else runtime_dir / 'deprecated_drop_readiness_report.md'
    json_out.parent.mkdir(parents=True, exist_ok=True)
    md_out.parent.mkdir(parents=True, exist_ok=True)

    report = build_report(project_root, check_db=bool(args.check_db))
    json_out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    write_markdown(report, md_out)

    print(json.dumps({
        'status': report['status'],
        'json': str(json_out),
        'markdown': str(md_out),
        'summary': report['summary'],
    }, ensure_ascii=False, indent=2))

    if args.fail_on_high and report['summary']['severity_counts'].get('high', 0) > 0:
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
