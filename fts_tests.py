# -*- coding: utf-8 -*-
from fts_config import CONFIG, PATHS
from fts_utils import resolve_decision_csv

class PreflightTestSuite:
    def run(self):
        tests = {
            "config_check": self._config_check(),
            "paths_check": self._paths_check(),
            "decision_file_check": self._decision_file_check(),
        }
        tests["all_passed"] = all(x["passed"] for x in tests.values())
        return tests

    def _config_check(self):
        errors = []
        if CONFIG.starting_cash <= 0: errors.append("starting_cash 必須 > 0")
        if CONFIG.max_single_position_pct <= 0 or CONFIG.max_single_position_pct > 1: errors.append("max_single_position_pct 必須介於 0~1")
        return {"passed": len(errors) == 0, "errors": errors}

    def _paths_check(self):
        errors = []
        if not PATHS.base_dir.exists(): errors.append("base_dir 不存在")
        if not PATHS.data_dir.exists(): errors.append("data_dir 不存在")
        if not PATHS.log_dir.exists(): errors.append("log_dir 不存在")
        if not PATHS.state_dir.exists(): errors.append("state_dir 不存在")
        return {"passed": len(errors) == 0, "errors": errors}

    def _decision_file_check(self):
        p = resolve_decision_csv(); exists = p.exists()
        return {"passed": exists, "path": str(p), "errors": [] if exists else ["找不到決策檔"]}
