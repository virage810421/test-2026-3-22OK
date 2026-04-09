# v83 Hardening / Cleanup Upgrade

這一包專門處理四件事：

1. 主入口收口：將 v79~v82 舊主控移到 `archive/versions/`，新增 `formal_trading_system.py` 指向 v83。
2. runtime 清理：把 `config_snapshot_*`、舊 `run_manifest_v*`、舊版報告與 `*error*.json` 分流到 `runtime/history/` 與 `runtime/errors/`。
3. data 清理：統一 `daily_decision_desk.csv`，保留根目錄相容副本，建立 `data/templates/` 與 `data/audit/`。
4. fundamentals 真資料補強：當 `data/market_financials_backup_fullspeed.csv` 太小或缺失時，從 seed data 補入較完整版本，並補 `latest_monthly_revenue_with_industry.csv`、`stock_revenue_industry_tw.csv`。

## 完整升級到位
- Step1 主入口收口：完整
- Step2 runtime 清理：完整
- Step3 data 清理：完整
- Step4 fundamentals 真資料補強：完整（seed/backfill 範圍）

## 仍非 100% live-ready 的部分
- 真券商憑證與 live smoke test 仍需你未來填入真 API / 帳號後完成。
