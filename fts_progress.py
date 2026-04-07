# -*- coding: utf-8 -*-
class ProgressTracker:
    def __init__(self):
        self.modules = {
            "ETL資料層": 85,
            "AI訓練層": 89,
            "研究/選股層": 86,
            "決策輸出層": 87,
            "風控層": 86,
            "模擬執行層": 90,
            "主控整合層": 95,
            "真券商介面預留": 74,
            "委託狀態機/對帳骨架": 84,
            "恢復機制骨架": 79,
            "測試/驗證框架": 78,
            "實盤工程化": 81,
            "調度安全閘門": 90,
            "失敗重試/補跑策略": 90,
            "任務快照/日誌歸檔": 88,
            "健康儀表板/總覽": 90,
            "每日營運摘要/異常告警": 87,
            "發車前驗證閘門": 89,
            "AI訓練掛回/橋接": 91,
            "模型版本治理/選模閘門": 87,
            "實盤保護層 / paper-live 防呆": 89,
            "真券商前審批/雙重確認": 86,
            "研究/選股品質閘門": 88,
        }

    def overall_percent(self) -> float:
        return round(sum(self.modules.values()) / len(self.modules), 1)

    def legacy_mapping(self) -> dict:
        return {
            "整體": self.overall_percent(),
            "ETL資料層": self.modules["ETL資料層"],
            "AI訓練層": self.modules["AI訓練層"],
            "研究/選股層": self.modules["研究/選股層"],
            "決策輸出層": self.modules["決策輸出層"],
            "風控層": self.modules["風控層"],
            "模擬執行層": self.modules["模擬執行層"],
            "主控整合層": self.modules["主控整合層"],
            "真券商介面預留": self.modules["真券商介面預留"],
            "委託狀態機/對帳骨架": self.modules["委託狀態機/對帳骨架"],
            "恢復機制骨架": self.modules["恢復機制骨架"],
            "測試/驗證框架": self.modules["測試/驗證框架"],
            "實盤工程化": self.modules["實盤工程化"],
        }

    def summary(self) -> dict:
        return {
            "module_progress": self.modules,
            "overall_progress_pct": self.overall_percent(),
            "legacy_mapping": self.legacy_mapping(),
            "interpretation": "v34 把研究/選股層從『已掛回』再推到『有正式品質閘門』，開始檢查 research output 是否足以接到 decision/execution。",
        }

class VersionPolicy:
    def summary(self) -> dict:
        return {
            "recommended_keep": ["formal_trading_system_v33.py", "formal_trading_system_v34.py"],
            "current_recommended_entry": "formal_trading_system_v34.py",
        }
