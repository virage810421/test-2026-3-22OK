# SAFE UPGRADE GUIDE v77

## 這輪新增
- StageGuard retry：單段失敗可自動重試 1 次
- fallback runtime json：某段失敗後仍能落地最小可用報告
- resume_completed_stages：已成功階段可直接略過，避免重跑
- soft timeout warning：單段超過門檻會告警，但不會硬殺

## 目前限制
- soft timeout 是告警，不是硬中止
- 若 stage 內部本身卡在外部 I/O，仍需從該 stage 內部再補更細的 timeout

## 你怎麼用
1. 先跑 `formal_trading_system_v77.py`
2. 若你只是重跑同一輪，已完成 stage 會直接略過
3. 看 `runtime/stage_guard_report.json`
4. 看 `runtime/run_manifest_v77.json`
5. 看 `state/stage_checkpoints_v77.json`

## 環境控制
- `FTS_BASE_DIR`
- `FTS_SOURCE_MOUNT_DIRS`
- `FTS_HISTORY_SCAN_DIRS`
- `FTS_PRICE_SCAN_DIRS`
