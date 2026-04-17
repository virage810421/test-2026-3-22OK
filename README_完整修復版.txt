完整修復版更新檔（非單點補丁）

本包完整修復目前 bootstrap 日誌中的三個實際問題：
1. structural bias 缺欄位時對 float 呼叫 fillna() 導致 full-market-percentile crash
2. fts_control_tower.py bootstrap 呼叫了 CLI 不支援的 maturity-upgrade / patch-retirement-report
3. bootstrap 狀態永遠寫 ready，未區分 stage error 與 partial

修復內容：
- fts_feature_service.py：重寫 _compute_structural_bias_frame，對 Close / MA20 / MA60 / MA120 / ADX / BB_Width / Range_Width_Pct 缺欄位都安全 fallback 成 Series。
- fts_admin_cli.py：正式加入 maturity-upgrade 與 patch-retirement-report 指令。
- fts_control_tower.py：新增 _admin_cli_supports / _call_admin_command，bootstrap 改為安全檢查 CLI 指令是否支援，並把最終狀態改成 bootstrap_ready / bootstrap_partial。

說明：
- 這是完整修復目前觀察到的 bootstrap crash / command mismatch 問題。
- prebroker_95_audit 若仍顯示 no_trades，屬於交易樣本尚未建立，不是本次 crash 類問題。
