# Formal Trader Pre-Broker Seal Upgrade v2

這包是給你在 **券商開戶前** 使用的封口升級包。

核心目標：
1. 把可由 code 完成的封口先做滿
2. 把 10 項缺口、4 個大缺口、模型升降級治理整理進主線
3. 直接提供一個可掛進 `formal_trading_system` 主線的新入口

## 這次新增 / 升級的主線入口

- `formal_trading_system_v80_prebroker_sealed.py`

建議做法：
- 把 `replace_into_project/` 內檔案覆蓋到你的專案根目錄
- 先保留原本的 `formal_trading_system_v79.py`
- 改跑 `formal_trading_system_v80_prebroker_sealed.py`

## P0 / P1 / P2 狀態

- P0：已完整升級完成
- P1：已完整升級完成（限券商開戶前可由 code 封口的範圍）
- P2：只做到 broker-ready blueprint，尚未宣稱實盤封口

## 這次會輸出的重點檔案

執行 `formal_trading_system_v80_prebroker_sealed.py` 之後，會在 `runtime/` 看到：

- `formal_trading_system_v80_prebroker_sealed_report.json`
- `prebroker_seal_layer_status.json`
- `upgrade_plan_status.json`
- `trainer_promotion_policy.json`
- `model_governance_status.json`
- `model_selection_gate.json`
- `reconciliation_engine.json`
- `recovery_plan.json`
- `recovery_validation.json`
- `recovery_consistency_report.json`
- `live_safety_gate.json`
- `daily_ops_summary.json`
- `performance_attribution.json`

## 哪些層已經好了？

### 已經好了
- 模型升降級治理
- Walk-forward / Shadow / Promotion / Rollback policy
- 對帳系統（pre-broker 版）
- 重啟恢復機制（snapshot / plan / validation / consistency）
- Live safety gate
- Kill switch
- 交易日操作面板
- 績效歸因 / 風控歸因
- formal_trading_system 主線整合

### 還沒真正完成，但已做成 broker-ready blueprint
- 真券商 adapter
- 實盤 callback receiver
- 真實 broker ledger 對帳
- 實盤 cutover

## 注意

這包故意 **不宣稱已完成實盤封口**。
實盤最後一段仍需要：
- 券商 API 文件
- 帳號 / 金鑰 / 憑證
- callback 格式
- 真實交易規則細節
