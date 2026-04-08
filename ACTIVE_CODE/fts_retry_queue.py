# -*- coding: utf-8 -*-
import json
from pathlib import Path
from fts_config import PATHS, CONFIG
from fts_utils import now_str, log

class RetryQueueManager:
    def __init__(self):
        self.path = PATHS.runtime_dir / "retry_queue.json"

    def _load(self):
        if not self.path.exists():
            return {"generated_at": now_str(), "items": []}
        with open(self.path, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save(self, payload):
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

    def add_failed_tasks(self, failed_tasks):
        if not getattr(CONFIG, "enable_retry_queue", False):
            return []
        payload = self._load()
        items = payload.get("items", [])
        for task in failed_tasks:
            key = f"{task.get('stage')}::{task.get('name')}::{task.get('script')}"
            existing = next((x for x in items if x.get("key") == key), None)
            if existing:
                existing["last_failed_at"] = now_str()
                existing["attempts"] = int(existing.get("attempts", 0)) + 1
                existing["status"] = "pending_retry"
            else:
                row = {
                    "key": key,
                    "stage": task.get("stage"),
                    "name": task.get("name"),
                    "script": task.get("script"),
                    "required": task.get("required", False),
                    "attempts": 1,
                    "first_failed_at": now_str(),
                    "last_failed_at": now_str(),
                    "status": "pending_retry",
                }
                items.append(row)
        payload["generated_at"] = now_str()
        payload["items"] = items
        self._save(payload)
        log(f"🧯 已更新 retry queue：{self.path} | total={len(items)}")
        return items

    def summarize(self):
        payload = self._load()
        items = payload.get("items", [])
        required_items = [x for x in items if x.get("required")]
        optional_items = [x for x in items if not x.get("required")]
        return {
            "total": len(items),
            "required": len(required_items),
            "optional": len(optional_items),
            "items": items,
        }

    def list_retryable_items(self):
        summary = self.summarize()
        max_attempts = getattr(CONFIG, "max_retry_attempts", 3)
        retryable = [x for x in summary["items"] if int(x.get("attempts", 0)) < max_attempts and x.get("status") == "pending_retry"]
        return retryable

    def mark_success(self, key: str):
        payload = self._load()
        changed = False
        for item in payload.get("items", []):
            if item.get("key") == key:
                item["status"] = "resolved"
                item["resolved_at"] = now_str()
                changed = True
        if changed:
            payload["generated_at"] = now_str()
            self._save(payload)

    def validate_required_queue(self):
        summary = self.summarize()
        pending_required = [x for x in summary["items"] if x.get("required") and x.get("status") == "pending_retry"]
        if getattr(CONFIG, "fail_on_retry_queue_required_items", True) and pending_required:
            raise RuntimeError(f"retry queue 內仍有必要任務待補跑: {len(pending_required)} 筆")
        return summary
