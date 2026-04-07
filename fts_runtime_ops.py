# -*- coding: utf-8 -*-
import json
import os
import shutil
from dataclasses import asdict, is_dataclass
from pathlib import Path
from datetime import datetime
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RuntimeLock:
    def __init__(self):
        self.lock_path = PATHS.runtime_dir / "engine.lock"
        self.owner = {
            "system_name": CONFIG.system_name,
            "created_at": now_str(),
            "pid": os.getpid(),
        }

    def acquire(self):
        if self.lock_path.exists():
            raise RuntimeError(f"偵測到執行鎖存在：{self.lock_path}，疑似已有另一個實例在跑")
        with open(self.lock_path, "w", encoding="utf-8") as f:
            json.dump(self.owner, f, ensure_ascii=False, indent=2)
        log(f"🔒 已建立 runtime lock：{self.lock_path}")

    def release(self):
        try:
            if self.lock_path.exists():
                self.lock_path.unlink()
                log("🔓 已釋放 runtime lock")
        except Exception as e:
            log(f"⚠️ 釋放 runtime lock 失敗：{e}")

class HeartbeatWriter:
    def __init__(self):
        self.path = PATHS.runtime_dir / "heartbeat.json"

    def write(self, stage: str, extra: dict | None = None):
        payload = {
            "system_name": CONFIG.system_name,
            "time": now_str(),
            "stage": stage,
            "pid": os.getpid(),
            "extra": extra or {},
        }
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"💓 heartbeat 更新：{stage}")

class DecisionArchiver:
    def archive(self, decision_path: Path):
        if not decision_path.exists():
            return None
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        out = PATHS.runtime_dir / f"decision_input_{ts}{decision_path.suffix}"
        shutil.copy2(decision_path, out)
        log(f"🗂️ 已封存 decision input：{out}")
        return out

class AuditTrail:
    def __init__(self):
        self.path = PATHS.runtime_dir / "audit_events.jsonl"

    def append(self, event_type: str, payload: dict):
        row = {
            "time": now_str(),
            "event_type": event_type,
            "payload": payload,
        }
        with open(self.path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

class ConfigSnapshotWriter:
    def write(self):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        path = PATHS.runtime_dir / f"config_snapshot_{ts}.json"
        payload = {}
        for k, v in CONFIG.__dict__.items():
            payload[k] = v
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        log(f"🧾 已輸出 config snapshot：{path}")
        return path
