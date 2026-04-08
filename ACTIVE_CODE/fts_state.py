# -*- coding: utf-8 -*-
import json
from dataclasses import asdict
from fts_config import PATHS
from fts_models import Position
from fts_utils import now_str, log

class StateStore:
    def __init__(self):
        self.state_file = PATHS.state_dir / "engine_state.json"

    def save(self, cash, positions, last_prices, meta=None):
        payload = {"saved_at": now_str(), "cash": cash, "positions": {k: asdict(v) for k, v in positions.items()}, "last_prices": last_prices, "meta": meta or {}}
        with open(self.state_file, "w", encoding="utf-8") as f: json.dump(payload, f, ensure_ascii=False, indent=2)
        return self.state_file

    def load(self):
        if not self.state_file.exists(): return None
        with open(self.state_file, "r", encoding="utf-8") as f: data = json.load(f)
        positions = {k: Position(**v) for k, v in data.get("positions", {}).items()}
        return {"saved_at": data.get("saved_at",""), "cash": float(data.get("cash",0.0)), "positions": positions, "last_prices": data.get("last_prices",{}), "meta": data.get("meta",{})}

class RecoveryManager:
    def __init__(self, broker, state_store):
        self.broker = broker; self.state_store = state_store

    def recover_if_possible(self):
        state = self.state_store.load()
        if not state: return {"recovered": False, "reason": "no_state_file"}
        self.broker.restore_state(state["cash"], state["positions"], state.get("last_prices", {}))
        log(f"♻️ 已從 state 檔恢復狀態，saved_at={state['saved_at']} positions={len(state['positions'])}")
        return {"recovered": True, "saved_at": state["saved_at"], "positions_count": len(state["positions"])}
