# -*- coding: utf-8 -*-
class ProgressTracker:
    def __init__(self):
        self.modules = {
            "ETL資料層": 85,
            "AI訓練層": 82,
            "研究/選股層": 80,
            "決策相容層": 90,
            "訊號診斷/自修復": 88,
            "版本一致性檢查": 88,
            "模擬執行可視行為": 90,
            "持倉生命週期管理": 88,
            "穩定運行營運層": 86,
            "主架構掛回/對齊": 88,
            "上游任務註冊/調度骨架": 85,
            "風控層": 85,
            "主控整合層": 93,
            "恢復機制骨架": 76,
            "測試/驗證框架": 75,
            "實盤工程化": 68,
        }

    def overall_percent(self) -> float:
        return round(sum(self.modules.values()) / len(self.modules), 1)

    def summary(self) -> dict:
        return {
            "module_progress": self.modules,
            "overall_progress_pct": self.overall_percent(),
            "interpretation": "v20 把你的上游 ETL / AI / decision builder 任務正式註冊到主控，開始從『有主架構』走向『主架構可被主控辨識與檢查』。",
        }

class VersionPolicy:
    def summary(self) -> dict:
        return {
            "recommended_keep": ["formal_trading_system_v19.py", "formal_trading_system_v20.py"],
            "current_recommended_entry": "formal_trading_system_v20.py",
        }
