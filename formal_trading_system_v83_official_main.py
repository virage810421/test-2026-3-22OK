# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import contextlib
import csv
import importlib
import io
import json
import os
import subprocess
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

BASE_DIR = Path(__file__).resolve().parent
RUNTIME_DIR = BASE_DIR / 'runtime'
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)


def now_str() -> str:
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def _safe_console_write(line: str) -> None:
    try:
        print(line)
    except UnicodeEncodeError:
        encoding = getattr(sys.stdout, 'encoding', None) or 'utf-8'
        fallback = str(line).encode(encoding, errors='replace').decode(encoding, errors='replace')
        print(fallback)


def log(msg: str) -> None:
    _safe_console_write(f'[{now_str()[11:19]}] {msg}')


def _safe_import(module_name: str):
    try:
        return importlib.import_module(module_name)
    except Exception:
        return None


def _capture_callable(fn, *args, **kwargs):
    """Run callables quietly so only the main controller prints one summary line."""
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
        result = fn(*args, **kwargs)
    return result, buf_out.getvalue(), buf_err.getvalue()


def _call_script(script_name: str, args: Optional[List[str]] = None, allow_missing: bool = True) -> Dict[str, Any]:
    args = args or []
    script_path = BASE_DIR / script_name
    if not script_path.exists():
        if allow_missing:
            return {'status': 'missing', 'script': script_name, 'returncode': None, 'args': args}
        raise FileNotFoundError(f'找不到腳本：{script_name}')
    cmd = [sys.executable, str(script_path), *args]
    env = os.environ.copy()
    env['PYTHONUTF8'] = '1'
    env['PYTHONIOENCODING'] = 'utf-8'
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding='utf-8',
        errors='replace',
        cwd=str(BASE_DIR),
        env=env,
    )
    stdout_tail = '\n'.join(proc.stdout.strip().splitlines()[-20:]) if proc.stdout else ''
    stderr_tail = '\n'.join(proc.stderr.strip().splitlines()[-20:]) if proc.stderr else ''
    return {
        'status': 'ok' if proc.returncode == 0 else 'error',
        'script': script_name,
        'args': args,
        'returncode': proc.returncode,
        'stdout_tail': stdout_tail,
        'stderr_tail': stderr_tail,
    }

def _log_script_result(label: str, result: Dict[str, Any]) -> None:
    status = result.get('status')
    script = result.get('script', '')
    if status == 'missing':
        log(f'⚪ {label}：略過（找不到 {script}）')
        return
    if status == 'ok':
        log(f'✅ {label}：成功（{script}）')
        if result.get('stdout_tail'):
            for line in result['stdout_tail'].splitlines():
                log(f'   {line}')
        return
    log(f'❌ {label}：失敗（{script}）| returncode={result.get("returncode")})')
    if result.get('stdout_tail'):
        log('   stdout_tail:')
        for line in result['stdout_tail'].splitlines():
            log(f'   {line}')
    if result.get('stderr_tail'):
        log('   stderr_tail:')
        for line in result['stderr_tail'].splitlines():
            log(f'   {line}')



def _get_data_dir() -> Path:
    try:
        from fts_config import PATHS  # type: ignore
        data_dir = Path(PATHS.data_dir)
    except Exception:
        data_dir = BASE_DIR / 'data'
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir


def _bootstrap_candidate_tickers() -> List[str]:
    candidates: List[str] = []
    try:
        from fts_watchlist_service import WatchlistService  # type: ignore
        for ticker in WatchlistService().build_final_watchlist(limit=10):
            if ticker and ticker not in candidates:
                candidates.append(str(ticker))
    except Exception:
        pass
    try:
        import config  # type: ignore
        for name in ('WATCH_LIST', 'TRAINING_POOL', 'BREAK_TEST_POOL'):
            for ticker in list(getattr(config, name, []) or []):
                if ticker and ticker not in candidates:
                    candidates.append(str(ticker))
    except Exception:
        pass
    if '2330.TW' not in candidates:
        candidates.insert(0, '2330.TW')
    return candidates[:10]


def _write_live_mount_csv_rows(ticker: str, mounted: Dict[str, Any], note: str) -> Path:
    csv_path = _get_data_dir() / 'selected_live_feature_mounts.csv'
    rows: List[Dict[str, Any]] = []
    for feature_name, feature_value in mounted.items():
        try:
            numeric_value = float(feature_value)
        except Exception:
            continue
        rows.append({
            'ticker': str(ticker),
            'feature_name': str(feature_name),
            'feature_value': numeric_value,
        })
    with csv_path.open('w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['ticker', 'feature_name', 'feature_value'])
        writer.writeheader()
        writer.writerows(rows)
    runtime_payload = {
        'generated_at': now_str(),
        'module_version': 'v83_bootstrap_selected_live_feature_mount_hotfix',
        'ticker': str(ticker),
        'row_count': len(rows),
        'csv_path': str(csv_path),
        'note': note,
        'status': 'selected_live_feature_mount_ready',
    }
    runtime_path = RUNTIME_DIR / 'bootstrap_selected_live_feature_mount.json'
    runtime_path.write_text(json.dumps(runtime_payload, ensure_ascii=False, indent=2), encoding='utf-8')
    return csv_path


def _ensure_selected_live_feature_mount_csv() -> Dict[str, Any]:
    csv_path = _get_data_dir() / 'selected_live_feature_mounts.csv'
    errors: List[str] = []
    try:
        from fts_screening_engine import ScreeningEngine  # type: ignore
        engine = ScreeningEngine()
        for ticker in _bootstrap_candidate_tickers():
            try:
                hist = engine.market.smart_download(ticker, period='1y')
                if hist is None or getattr(hist, 'empty', True) or 'Close' not in hist.columns:
                    errors.append(f'{ticker}: 無有效歷史價格')
                    continue
                prepared = engine._prepare(hist)
                if prepared is None or getattr(prepared, 'empty', True):
                    errors.append(f'{ticker}: 特徵前處理失敗')
                    continue
                latest = prepared.iloc[-1].to_dict()
                latest = engine.chips.enrich_row(ticker, latest)
                _, mounted = engine.features.mount_live_features(ticker, latest, history_df=prepared)
                _write_live_mount_csv_rows(ticker, mounted, note='bootstrap_auto_generated')
                return {
                    'status': 'ok',
                    'ticker': ticker,
                    'rows': int(len(mounted)),
                    'csv_path': str(csv_path),
                    'errors': errors,
                }
            except Exception as exc:
                errors.append(f'{ticker}: {type(exc).__name__}: {exc}')
    except Exception as exc:
        errors.append(f'ScreeningEngine 啟動失敗: {type(exc).__name__}: {exc}')

    _write_live_mount_csv_rows('2330.TW', {}, note='bootstrap_header_only_fallback')
    return {
        'status': 'fallback',
        'ticker': '2330.TW',
        'rows': 0,
        'csv_path': str(csv_path),
        'errors': errors,
    }

def _build_once(module_name: str, class_name: str, method_name: str, label: str) -> Tuple[str, Dict[str, Any]]:
    mod = _safe_import(module_name)
    if mod and hasattr(mod, class_name):
        runner = getattr(mod, class_name)()
        (path, payload), captured_out, captured_err = _capture_callable(getattr(runner, method_name))
        log(f'{label}：{path}')
        return str(path), payload
    return '', {'status': 'missing', 'module': module_name}


def _build_fundamentals_local_sync() -> Tuple[str, Dict[str, Any]]:
    return _build_once('fts_fundamentals_etl_mainline', 'FundamentalsETLMainline', 'build_summary', '📚 fundamentals ETL 主線完成')


def _build_training_governance() -> Tuple[str, Dict[str, Any]]:
    mod = _safe_import('fts_training_governance_mainline')
    if mod and hasattr(mod, 'TrainingGovernanceMainline'):
        runner = getattr(mod, 'TrainingGovernanceMainline')()
        (path, payload), captured_out, captured_err = _capture_callable(runner.build_summary, execute_backend=False)
        log(f'🧠 training governance 主線盤點完成：{path}')
        return str(path), payload
    return '', {'status': 'missing', 'module': 'fts_training_governance_mainline'}


def _run_upgrade_stages() -> Dict[str, Any]:
    results: Dict[str, Any] = {}
    stage_specs = [
        ('fts_phase1_upgrade', 'Phase1Upgrade', 'run', '🥇 Phase1 完成'),
        ('fts_phase2_mock_broker_stage', 'Phase2MockBrokerStage', 'run', '🥈 Phase2 完成'),
        ('fts_phase3_real_cutover_stage', 'Phase3RealCutoverStage', 'run', '🥉 Phase3 完成'),
    ]
    for module_name, class_name, method_name, label in stage_specs:
        mod = _safe_import(module_name)
        if mod and hasattr(mod, class_name):
            runner = getattr(mod, class_name)()
            (path, payload), captured_out, captured_err = _capture_callable(getattr(runner, method_name))
            results[module_name] = {'path': str(path), 'payload': payload}
            log(f'{label}：{path}')
        else:
            results[module_name] = {'status': 'missing'}
    return results


def _run_optional_build(module_name: str, class_name: str, method_name: str, label: str, kwargs: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    kwargs = kwargs or {}
    mod = _safe_import(module_name)
    if mod and hasattr(mod, class_name):
        builder = getattr(mod, class_name)()
        (path, payload), captured_out, captured_err = _capture_callable(getattr(builder, method_name), **kwargs)
        log(f'{label}：{path}')
        return {'path': str(path), 'payload': payload}
    return {'status': 'missing', 'module': module_name}


def _run_mode_daily() -> Dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：DAILY')
    log('=' * 72)

    outputs: Dict[str, Any] = {}
    f_path, f_payload = _build_fundamentals_local_sync()
    outputs['fundamentals_etl_mainline'] = {'path': f_path, 'payload': f_payload}

    tg_path, tg_payload = _build_training_governance()
    outputs['training_governance_mainline'] = {'path': tg_path, 'payload': tg_payload}

    outputs['training_stress_audit'] = _run_optional_build('fts_training_stress_audit', 'TrainingStressAudit', 'build', '🧪 training stress audit')
    outputs['backfill_resilience_audit'] = _run_optional_build('fts_backfill_resilience_audit', 'BackfillResilienceAudit', 'build', '🧪 backfill resilience audit')
    outputs['feature_stack_audit'] = _run_optional_build('fts_feature_stack_audit', 'FeatureStackAudit', 'build', '🧪 feature stack audit')
    outputs['cross_sectional_percentile'] = _run_optional_build('fts_cross_sectional_percentile_service', 'CrossSectionalPercentileService', 'build_summary', '📊 全市場 percentile 狀態')
    outputs['event_calendar'] = _run_optional_build('fts_event_calendar_service', 'EventCalendarService', 'build_summary', '🗓️ event calendar ready')
    outputs['mainline_linkage'] = _run_optional_build('fts_mainline_linkage', 'MainlineLinkage', 'build', '🔗 主線串聯檢查')
    outputs['project_completion_audit'] = _run_optional_build('fts_project_completion_audit', 'ProjectCompletionAudit', 'build', '📋 專案完成度稽核')
    outputs['file_classification'] = _run_optional_build('fts_file_classification', 'FileClassificationBuilder', 'build', '🗂️ file classification updated')
    outputs['upgrade_stages'] = _run_upgrade_stages()

    payload = {
        'generated_at': now_str(),
        'mode': 'daily',
        'module_version': 'v83_bootstrap_auto_selected_live_feature_mount_hotfix',
        'outputs': outputs,
        'status': 'daily_ready',
    }
    out = RUNTIME_DIR / 'formal_trading_system_v83_official_main.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'✅ v83 正式主控版完成：{out}')
    return payload


def _run_mode_bootstrap() -> Dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：BOOTSTRAP')
    log('=' * 72)
    script_results: List[Dict[str, Any]] = []

    bootstrap_steps = [
        ('核心資料表升級', 'db_setup.py', ['--mode', 'upgrade'], False),
        ('研究層資料表建立', 'db_setup_research_plus.py', [], True),
        ('全市場 percentile 建置', 'run_full_market_percentile_snapshot.py', [], True),
        ('事件窗日曆建置', 'run_precise_event_calendar_build.py', [], True),
    ]

    for label, script, args, allow_missing in bootstrap_steps:
        result = _call_script(script, args=args, allow_missing=allow_missing)
        _log_script_result(label, result)
        script_results.append(result)

    mount_result = _ensure_selected_live_feature_mount_csv()
    if mount_result.get('status') == 'ok':
        log(f"✅ selected_live_feature_mounts 建置：成功（{mount_result.get('ticker')} | rows={mount_result.get('rows', 0)}）")
    else:
        log(f"⚠️ selected_live_feature_mounts 建置：fallback（僅建立檔案骨架）")
    if mount_result.get('csv_path'):
        log(f"   {mount_result['csv_path']}")
    for err in mount_result.get('errors', [])[-5:]:
        log(f"   {err}")

    sync_result = _call_script('run_sync_feature_snapshots_to_sql.py', [], True)
    _log_script_result('特徵快照同步 SQL', sync_result)
    script_results.append({
        'status': mount_result.get('status'),
        'script': 'internal_selected_live_feature_mount_builder',
        'args': [],
        'returncode': 0 if mount_result.get('status') in ('ok', 'fallback') else 1,
        'stdout_tail': json.dumps(mount_result, ensure_ascii=False),
        'stderr_tail': '',
    })
    script_results.append(sync_result)

    daily_payload = _run_mode_daily()
    completion = _call_script('run_project_completion_audit.py', allow_missing=True)
    _log_script_result('專案完成度稽核腳本', completion)

    payload = {
        'generated_at': now_str(),
        'mode': 'bootstrap',
        'module_version': 'v83_bootstrap_auto_selected_live_feature_mount_hotfix',
        'bootstrap_scripts': script_results,
        'completion_audit_script': completion,
        'daily_status': daily_payload.get('status'),
        'status': 'bootstrap_ready',
    }
    out = RUNTIME_DIR / 'formal_trading_system_v83_bootstrap.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'✅ bootstrap 完成：{out}')
    return payload


def _run_mode_train() -> Dict[str, Any]:
    log('=' * 72)
    log('🚀 啟動 正式交易主控版_v83_official_main')
    log('🧭 模式：TRAIN')
    log('=' * 72)
    steps = [
        _call_script('ml_data_generator.py', allow_missing=True),
        _call_script('ml_trainer.py', allow_missing=True),
        _call_script('run_project_completion_audit.py', allow_missing=True),
    ]
    governance_path, governance_payload = _build_training_governance()
    outputs = {
        'train_steps': steps,
        'training_governance_mainline': {'path': governance_path, 'payload': governance_payload},
        'training_stress_audit': _run_optional_build('fts_training_stress_audit', 'TrainingStressAudit', 'build', '🧪 training stress audit'),
    }
    payload = {
        'generated_at': now_str(),
        'mode': 'train',
        'module_version': 'v83_bootstrap_auto_selected_live_feature_mount_hotfix',
        'outputs': outputs,
        'status': 'train_ready',
    }
    out = RUNTIME_DIR / 'formal_trading_system_v83_train.json'
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')
    log(f'✅ train 完成：{out}')
    return payload


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='正式交易主控版_v83 單一入口三模式（bootstrap 內建建表版，精簡輸出版）')
    group = parser.add_mutually_exclusive_group()
    group.add_argument('--bootstrap', action='store_true', help='第一次建置/新電腦初始化')
    group.add_argument('--train', action='store_true', help='重建訓練資料並訓練模型')
    group.add_argument('--daily', action='store_true', help='日常執行（預設）')
    return parser.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    try:
        args = parse_args(argv)
        if args.bootstrap:
            _run_mode_bootstrap()
        elif args.train:
            _run_mode_train()
        else:
            _run_mode_daily()
        return 0
    except Exception as exc:
        err = {
            'generated_at': now_str(),
            'module_version': 'v83_bootstrap_auto_selected_live_feature_mount_hotfix',
            'error_type': type(exc).__name__,
            'error': str(exc),
            'traceback': traceback.format_exc(),
        }
        out = RUNTIME_DIR / 'formal_trading_system_v83_official_main_error.json'
        out.write_text(json.dumps(err, ensure_ascii=False, indent=2), encoding='utf-8')
        log(f'❌ 主控執行失敗：{exc}')
        log(f'📄 錯誤報告：{out}')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
