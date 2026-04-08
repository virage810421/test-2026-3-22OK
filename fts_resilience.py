# -*- coding: utf-8 -*-
import json
import time
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from fts_config import PATHS, CONFIG
from fts_utils import log


class CheckpointStore:
    def __init__(self, path: Optional[Path] = None):
        self.path = path or (PATHS.state_dir / f'stage_checkpoints_{CONFIG.package_version}.json')
        self.path.parent.mkdir(exist_ok=True)
        self.data = self._load()

    def _load(self) -> Dict[str, Any]:
        if self.path.exists():
            try:
                return json.loads(self.path.read_text(encoding='utf-8'))
            except Exception:
                return {}
        return {}

    def get_stage(self, stage_key: str) -> Dict[str, Any]:
        raw = self.data.get(stage_key)
        return raw if isinstance(raw, dict) else {}

    def save_stage(self, stage_key: str, payload: Dict[str, Any]) -> None:
        self.data[stage_key] = payload
        self.path.write_text(json.dumps(self.data, ensure_ascii=False, indent=2), encoding='utf-8')


class StageGuard:
    def __init__(self, checkpoint_store: Optional[CheckpointStore] = None):
        self.checkpoints = checkpoint_store or CheckpointStore()
        self.report_path = PATHS.runtime_dir / 'stage_guard_report.json'
        self.report = {
            'package_version': CONFIG.package_version,
            'safe_upgrade_mode': CONFIG.safe_upgrade_mode,
            'continue_on_stage_failure': CONFIG.continue_on_stage_failure,
            'resume_completed_stages': CONFIG.resume_completed_stages,
            'soft_timeout_seconds': CONFIG.stage_soft_timeout_seconds,
            'max_stage_retries': CONFIG.max_stage_retries,
            'stages': []
        }

    def run(
        self,
        stage_key: str,
        stage_label: str,
        fn: Callable[[], Any],
        fallback_fn: Optional[Callable[[], Any]] = None,
    ) -> Any:
        checkpoint = self.checkpoints.get_stage(stage_key)
        if CONFIG.resume_completed_stages and checkpoint.get('status') == 'ok':
            log(f'♻️ StageGuard 快速略過：{stage_label}（沿用既有 checkpoint）')
            record = {
                'stage_key': stage_key,
                'stage_label': stage_label,
                'status': 'skipped_from_checkpoint',
                'elapsed_seconds': 0,
            }
            self.report['stages'].append(record)
            self._flush()
            return None

        attempts = max(int(CONFIG.max_stage_retries), 0) + 1
        last_error: Optional[Exception] = None
        last_traceback = ''

        for attempt in range(1, attempts + 1):
            started = time.time()
            log(f'🛡️ StageGuard 進入：{stage_label}（attempt {attempt}/{attempts}）')
            try:
                result = fn()
                elapsed = round(time.time() - started, 3)
                timeout_exceeded = elapsed > CONFIG.stage_soft_timeout_seconds
                if timeout_exceeded:
                    log(f'⏱️ StageGuard 軟超時告警：{stage_label} | {elapsed}s > {CONFIG.stage_soft_timeout_seconds}s')
                record = {
                    'stage_key': stage_key,
                    'stage_label': stage_label,
                    'status': 'ok',
                    'attempt': attempt,
                    'elapsed_seconds': elapsed,
                    'soft_timeout_exceeded': timeout_exceeded,
                }
                self.report['stages'].append(record)
                self.checkpoints.save_stage(stage_key, {
                    'status': 'ok',
                    'stage_label': stage_label,
                    'completed_at_epoch': time.time(),
                    'elapsed_seconds': elapsed,
                    'attempt': attempt,
                    'soft_timeout_exceeded': timeout_exceeded,
                })
                self._flush()
                return result
            except Exception as e:
                elapsed = round(time.time() - started, 3)
                last_error = e
                last_traceback = traceback.format_exc(limit=12)
                record = {
                    'stage_key': stage_key,
                    'stage_label': stage_label,
                    'status': 'error',
                    'attempt': attempt,
                    'elapsed_seconds': elapsed,
                    'error_type': type(e).__name__,
                    'error_message': str(e),
                    'traceback_tail': last_traceback,
                }
                self.report['stages'].append(record)
                self._flush()
                log(f'❌ StageGuard 捕捉異常：{stage_label} | attempt {attempt}/{attempts} | {type(e).__name__}: {e}')
                if attempt < attempts:
                    log(f'🔁 準備重試：{stage_label}')
                    continue

        if fallback_fn is not None:
            log(f'🪂 啟用 fallback：{stage_label}')
            fallback_started = time.time()
            try:
                fallback_result = fallback_fn()
                elapsed = round(time.time() - fallback_started, 3)
                record = {
                    'stage_key': stage_key,
                    'stage_label': stage_label,
                    'status': 'fallback_ok',
                    'elapsed_seconds': elapsed,
                }
                self.report['stages'].append(record)
                self.checkpoints.save_stage(stage_key, {
                    'status': 'fallback_ok',
                    'stage_label': stage_label,
                    'completed_at_epoch': time.time(),
                    'elapsed_seconds': elapsed,
                    'fallback_used': True,
                })
                self._flush()
                return fallback_result
            except Exception as fallback_error:
                self.report['stages'].append({
                    'stage_key': stage_key,
                    'stage_label': stage_label,
                    'status': 'fallback_error',
                    'elapsed_seconds': round(time.time() - fallback_started, 3),
                    'error_type': type(fallback_error).__name__,
                    'error_message': str(fallback_error),
                })
                self._flush()
                log(f'❌ fallback 也失敗：{stage_label} | {type(fallback_error).__name__}: {fallback_error}')

        self.checkpoints.save_stage(stage_key, {
            'status': 'error',
            'stage_label': stage_label,
            'completed_at_epoch': time.time(),
            'error_type': type(last_error).__name__ if last_error else 'UnknownError',
            'error_message': str(last_error) if last_error else 'Unknown error',
        })
        if CONFIG.continue_on_stage_failure:
            return None
        if last_error is not None:
            raise last_error
        raise RuntimeError(f'Stage failed: {stage_label}\n{last_traceback}')

    def _flush(self) -> None:
        self.report_path.write_text(json.dumps(self.report, ensure_ascii=False, indent=2), encoding='utf-8')
