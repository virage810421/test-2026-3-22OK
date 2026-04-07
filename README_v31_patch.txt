v31 升級 patch
==============
這版重點：
把模型版本治理與選模閘門正式納進主控。

新增：
1. fts_model_gate.py
   - model_registry.json
   - model_selection_gate.json
2. formal_trading_system_v31.py
   - 啟動時輸出 model registry
   - decision / readiness 後跑 model gate
3. progress 更新
   - legacy mapping 直接對應你原本熟悉的那組進度

對你剛剛那個問題的直接回答：
- 可以繼續往 95%~100% 升
- 但不是每一層都必要
- 真正應該追高的是：
  主控整合 / 風控 / 恢復機制 / 測試驗證 / 實盤工程化 / 真券商介面預留
- 研究/選股層不一定需要為了數字硬追到 100%
