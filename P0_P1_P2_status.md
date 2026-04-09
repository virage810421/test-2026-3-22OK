# P0 / P1 / P2 狀態總表

## P0：完整升級完成

已完成：
- 模型升降級治理
- walk-forward 正式化
- shadow trading 流程
- promotion / rollback policy
- 對帳引擎
- recovery snapshot / recovery plan / validation / consistency
- live safety gate
- kill switch

## P1：完整升級完成（券商開戶前 code 可封口範圍）

已完成：
- 交易日操作面板
- 今日候選 / 禁買清單 / 風險占用 / 訂單看板 / 收盤檢討
- 績效歸因 / 風控歸因
- formal_trading_system 主線整合

## P2：尚未真正完成，只做到 broker-ready blueprint

已完成：
- real broker adapter blueprint
- callback / ledger / reconciliation 所需欄位骨架
- 可銜接實盤前的接口定義

仍待券商資訊：
- API 文件
- 帳號與憑證
- callback 規格
- 下單 / 改單 / 回報 / ledger 真實欄位

## 10 項缺口狀態

- 真券商 adapter：P2 blueprint only
- 實盤回報接收器：P2 blueprint only
- 對帳系統：done pre-broker
- 重啟恢復機制：done pre-broker
- Kill switch：done pre-broker
- Walk-forward 正式化：done
- Shadow trading：done pre-broker
- Promotion / rollback policy：done
- 交易日操作面板：done
- 績效歸因 / 風控歸因：done

## 4 大缺口狀態

- 真執行：P2 blueprint only
- 對帳恢復：done pre-broker
- 模型治理：done
- 實盤安全機制：done pre-broker
