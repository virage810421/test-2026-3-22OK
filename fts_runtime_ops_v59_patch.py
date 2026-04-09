# -*- coding: utf-8 -*-
"""
v59 runtime lock patch
目的：
1. 修復 stale lock 導致 formal_trading_system_v58.py / v59.py 無法啟動
2. 保留防雙開保護
3. 若 lock 太舊或內容損壞，允許自動清理
"""

import json
import os
import time
from pathlib import Path
from typing import Optional


class RuntimeLock:
    def __init__(self, lock_path: Optional[Path] = None, stale_seconds: int = 6 * 3600):
        if lock_path is None:
            base_dir = Path(__file__).resolve().parent
            runtime_dir = base_dir / "runtime"
            runtime_dir.mkdir(parents=True, exist_ok=True)
            lock_path = runtime_dir / "engine.lock"
        self.lock_path = Path(lock_path)
        self.stale_seconds = int(stale_seconds)

    def _now(self) -> float:
        return time.time()

    def _read_lock(self) -> dict:
        try:
            if not self.lock_path.exists():
                return {}
            raw = self.lock_path.read_text(encoding="utf-8").strip()
            if not raw:
                return {}
            return json.loads(raw)
        except Exception:
            return {}

    def _is_process_alive(self, pid: Optional[int]) -> bool:
        if not pid:
            return False
        try:
            if os.name == "nt":
                import ctypes
                PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
                handle = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, int(pid))
                if handle == 0:
                    return False
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            else:
                os.kill(int(pid), 0)
                return True
        except Exception:
            return False

    def _is_stale(self, payload: dict) -> bool:
        if not self.lock_path.exists():
            return False
        mtime_age = self._now() - self.lock_path.stat().st_mtime
        pid = payload.get("pid")
        started_at = payload.get("started_at_ts")

        # 1) pid 不存在 / 活著檢查失敗，而且檔案已經有點舊
        if not self._is_process_alive(pid) and mtime_age > 30:
            return True

        # 2) payload 有 started_at，而且超過 stale_seconds
        if isinstance(started_at, (int, float)) and (self._now() - float(started_at) > self.stale_seconds):
            return True

        # 3) 檔案非常舊
        if mtime_age > self.stale_seconds:
            return True

        return False

    def acquire(self):
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)

        if self.lock_path.exists():
            payload = self._read_lock()

            # lock 壞掉或過舊：自動清理
            if self._is_stale(payload) or payload == {}:
                try:
                    self.lock_path.unlink(missing_ok=True)
                except Exception:
                    pass

        if self.lock_path.exists():
            payload = self._read_lock()
            raise RuntimeError(
                f"偵測到執行鎖存在：{self.lock_path}，疑似已有另一個實例在跑。"
                f" 若確定沒有在跑，請刪除 lock 或升級到 v59 patch。lock_payload={payload}"
            )

        payload = {
            "pid": os.getpid(),
            "started_at_ts": self._now(),
            "started_at_local": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        self.lock_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def release(self):
        try:
            self.lock_path.unlink(missing_ok=True)
        except Exception:
            pass
