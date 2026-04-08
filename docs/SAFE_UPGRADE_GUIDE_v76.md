# SAFE UPGRADE GUIDE v76

這一輪的重點不是再塞更多模組，而是讓系統在大幅升級時更不容易整包崩掉。

## 這版新增了什麼
- StageGuard：每個階段獨立保護
- continue_on_stage_failure：某一段失敗時，主控可繼續跑後面段落
- stage checkpoints：每一段完成或失敗都會落到 `state/stage_checkpoints_v76.json`
- stage guard report：詳細異常紀錄會寫到 `runtime/stage_guard_report.json`
- run manifest：本輪主控摘要在 `runtime/run_manifest_v76.json`

## 為什麼這樣適合大升級
因為大型升級最怕的是「一段出錯，整包中止」。
這版改成分段隔離後，你可以更放心地：
- 擴增模組
- 插入更多檢查
- 保留報告輸出
- 快速定位是哪一段出錯

## 建議做法
1. 先用 v76 跑一次，確認 `stage_guard_report.json` 為 ok
2. 再往 training / execution 端補新功能
3. 不要一次改所有上游資料源，採波段式切入
