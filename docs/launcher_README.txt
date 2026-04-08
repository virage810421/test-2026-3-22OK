# launcher 使用說明

## 檔案
- `launcher.py`：每日總啟動器
- `run_launcher.bat`：Windows 手動 / 排程啟動用

## 建議每日流程
1. `master_pipeline.py`
2. `monitor_center.py`
3. `system_guard.py`
4. 週末可額外執行 `event_backtester.py`

## Windows 工作排程器建議
- 程式或指令碼：`run_launcher.bat`
- 起始於：你的專案資料夾
- 建議時間：
  - 平日：18:50 之後
  - 週末：上午或下午皆可

## 輸出
- `runtime_logs/launcher_runtime.log`
- `runtime_logs/launcher_status.json`
- `runtime_logs/daily_launcher_summary.json`

## 注意
- `db_setup.py` 若仍是互動式版本，不建議放入每日自動排程
- 若要正式告警推播，記得在 `config.py` 補上：
  - `ALERT_LINE_BOT_TOKEN`
  - `ALERT_LINE_USER_ID`
  - `ALERT_TEST_MODE = False`
