# -*- coding: utf-8 -*-
import importlib
from fts_utils import log

class PackageConsistencyGuard:
    REQUIRED = {
        "fts_signal": ["SignalLoader", "ExecutionReadinessChecker"],
        "fts_compat": ["DecisionCompatibilityLayer"],
        "fts_state": ["StateStore", "RecoveryManager"],
    }

    def run(self):
        info = {"passed": True, "issues": [], "modules": {}}
        for module_name, attrs in self.REQUIRED.items():
            try:
                mod = importlib.import_module(module_name)
                info["modules"][module_name] = {"file": getattr(mod, "__file__", ""), "attrs": []}
            except Exception as e:
                info["passed"] = False
                info["issues"].append(f"import 失敗 {module_name}: {e}")
                continue

            for attr in attrs:
                ok = hasattr(mod, attr)
                info["modules"][module_name]["attrs"].append({"name": attr, "ok": ok})
                if not ok:
                    info["passed"] = False
                    info["issues"].append(f"{module_name} 缺少 {attr}")

        try:
            from fts_signal import SignalLoader
            loader = SignalLoader()
            if not hasattr(loader, "load_from_normalized_df"):
                info["passed"] = False
                info["issues"].append("SignalLoader 缺少 load_from_normalized_df")
        except Exception as e:
            info["passed"] = False
            info["issues"].append(f"SignalLoader 檢查失敗: {e}")

        if info["passed"]:
            log("🧷 package consistency check 通過")
        else:
            log(f"❌ package consistency check 失敗: {info['issues']}")
        return info
