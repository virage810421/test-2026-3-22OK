db_setup_auto 使用方式

1. 安全補欄模式（預設）
python db_setup.py
或
python db_setup.py --mode upgrade

2. 全重建模式
python db_setup.py --mode reset --yes

3. 環境變數方式
set DB_SETUP_MODE=upgrade
python db_setup.py

set DB_SETUP_MODE=reset
set DB_SETUP_CONFIRM_RESET=true
python db_setup.py

說明
- 預設是 upgrade，這是最安全的模式
- reset 一定要明確確認，否則會直接拒絕執行
- 很適合給 launcher.py 或 Windows 工作排程器呼叫
