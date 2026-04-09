# Gemini 問題複核與修復包

## 我確認成立、而且已修的問題
1. AlertManager 同步 requests.post 會阻塞主線 → 已改非同步背景執行緒
2. Execution loop 沒有連續拒單 / 連續例外熔斷 → 已補 circuit breaker
3. Execution 接受 SHORT/COVER，但 broker 邊界不完整 → 已補 paper_broker 支援 SHORT/COVER
4. evaluate_live_health 缺最小樣本數 → 已補 min_trades_required
5. ATR_Pctl_252 使用 rolling.apply(rank) 效能差 → 已改成 sorted-window percentile
6. db_setup 對動態識別字缺保護 → 已補 identifier 驗證
7. yahoo_csv_to_sql 的 verify=False 會默默啟用 → 改成必須顯式設定 ALLOW_INSECURE_SSL_FALLBACK=1

## 我沒有硬說已修到 100% 的
- 全專案所有 silent failure：這包先處理高優先級主線，未逐檔掃完
- fts_etl_daily_chip_service 的 SSL fallback：目前可驗證來源裡沒看到同樣問題，未硬改
- 完整保證金制度：paper broker 先補到邏輯正確與不再亂算，仍是簡化版
