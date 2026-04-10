# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

LOG_DIR = Path('runtime_logs')
LOG_DIR.mkdir(exist_ok=True)
PIPELINE_LOG = LOG_DIR / 'launcher_runtime.log'
STATUS_JSON = LOG_DIR / 'launcher_status.json'


def log(msg: str):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    PIPELINE_LOG.write_text((PIPELINE_LOG.read_text(encoding='utf-8') if PIPELINE_LOG.exists() else '') + line + '\n', encoding='utf-8')


def save_status(stage: str, status: str, detail: str = ''):
    payload = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'stage': stage,
        'status': status,
        'detail': detail,
    }
    STATUS_JSON.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding='utf-8')


def main() -> int:
    start = time.time()
    cmd = [sys.executable, 'formal_trading_system_v83_official_main.py', '--daily']
    log('🚀 啟動 Level-3 全主控入口 formal_trading_system_v83_official_main.py --daily')
    save_status('launcher', 'RUNNING')
    proc = subprocess.run(cmd, capture_output=True, text=True, encoding='utf-8', errors='replace')
    if proc.stdout:
        for line in proc.stdout.splitlines()[-80:]:
            log(f'   {line}')
    if proc.stderr:
        for line in proc.stderr.splitlines()[-40:]:
            log(f'   {line}')
    ok = proc.returncode == 0
    save_status('launcher', 'SUCCESS' if ok else 'FAILED', f'returncode={proc.returncode}')
    summary = {
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'duration_sec': round(time.time() - start, 2),
        'returncode': proc.returncode,
        'status': 'SUCCESS' if ok else 'FAILED',
    }
    (LOG_DIR / 'daily_launcher_summary.json').write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding='utf-8')
    return proc.returncode


if __name__ == '__main__':
    raise SystemExit(main())
