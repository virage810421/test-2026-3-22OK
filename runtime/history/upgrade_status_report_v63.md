# FTS 升級狀態報告 v63

## 1) 最新分層狀態

- ETL資料層: 98%
- AI訓練層: 98%
- 研究/選股層: 98%
- 決策輸出層: 99%
- 風控層: 98%
- 模擬執行層: 99%
- 主控整合層: 99%
- 真券商介面預留: 97%
- 委託狀態機/對帳骨架: 98%
- 恢復機制骨架: 98%
- 測試/驗證框架: 98%
- 實盤工程化: 98%
- 舊訓練核心上線資格評估: 99%
- 舊核心95+並行升級規劃: 99%
- Wave1舊核心升級骨架: 99%
- Wave1本體補強模板 / IO bindings: 99%
- 接口對齊稽核: 95%
- 研究/選股品質閘門: 95%
- AI訓練品質/模型產物一致性/測試矩陣: 95%
- 風控 deeper checks / 恢復校驗 / 情境擴充: 95%
- 研究品質統計 / 決策一致性 / 接口收口: 95%
- 委託狀態機 / 對帳 / callback 正規化: 95%
- 真券商 adapter / live workflow / callback 對接: 95%
- ETL深化 / research versioning / live adapter stub: 95%

## 2) 真實可運作狀態

- 架構 95+ 模組: 24/24
- AI 訓練資產就緒: False
- 可送單 execution gate: False
- 決策補價後有價格列數: 0 / 3
- 真 API 綁定: contract_ready_but_not_live_bound

## 3) 真 API / 真市場尚缺

- broker authentication and token refresh
- account routing: cash / margin / short-sell
- market session calendar and holiday source
- tick size / price band / odd-lot rules
- broker order id mapping and idempotency key
- real callback receiver and persistence
- partial fill reconciliation against broker ledger
- cancel/replace semantics for each broker
- rate limit and retry policy by endpoint
- live kill-switch and operator confirmation UX

## 4) 下一步

- 補齊 last_price_snapshot.csv 或在決策輸出直接寫入 Close/Reference_Price
- 補齊 training_data 與模型 artifact 產出
- 接入真券商登入/下單/查單/成交 callback
- 建立 broker reject code -> internal reject classifier 映射