# -*- coding: utf-8 -*-
class ExternalScriptRunner:
    def run_script(self, script_name: str, timeout: int = 1800) -> bool:
        return False

class StageManager:
    def __init__(self, runner: ExternalScriptRunner): self.runner = runner
    def run(self): return {"etl": [], "ai_training": [], "decision_build": []}
