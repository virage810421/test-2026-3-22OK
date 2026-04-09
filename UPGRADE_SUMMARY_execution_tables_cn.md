# execution_* 表中文欄名升級摘要

## 本次更新目標
將下列四張表在 **建立表時改成全中文欄名**，並同步把 **實際寫入 SQL 的 Python 程式** 改成使用中文欄名：

- `execution_orders`
- `execution_fills`
- `execution_account_snapshot`
- `execution_positions_snapshot`

## 已更新檔案
- `db_setup.py`
- `fts_logger.py`
- `db_logger.py`

## 完整升級內容
### 1. `db_setup.py`
- 新增四張 execution 系列表的中文 schema
- `reset` 模式會一併刪除並重建這四張表
- `upgrade` 模式會自動補齊缺少的中文欄位
- 若舊表原本是英文欄位，會嘗試把英文欄位內容回填到新中文欄位

### 2. `fts_logger.py`
- `ensure_tables()` 改成建立中文欄名
- `insert_order()` 改寫為插入中文欄位
- `update_order_status()` 改寫為更新中文欄位
- `insert_fill()` 改寫為插入中文欄位
- 新增 `upsert_account_snapshot()`
- 新增 `replace_positions_snapshot()`

### 3. `db_logger.py`
- `insert_order()` 改成寫中文欄位
- `insert_fill()` 改成寫中文欄位
- 新增 `upsert_account_snapshot()`
- 新增 `replace_positions_snapshot()`
- 支援中文 key，並兼容舊英文 key 做過渡

## 中文欄名對照
### execution_orders
- 委託單號
- 股票代號
- 買賣方向
- 委託股數
- 已成交股數
- 剩餘股數
- 平均成交價
- 參考價
- 委託價格
- 委託類型
- 委託狀態
- 建立時間
- 更新時間
- 拒單原因
- 策略名稱
- 訊號編號
- 客戶委託編號
- 產業名稱
- 訊號分數
- AI信心分數
- 備註

### execution_fills
- 成交編號
- 委託單號
- 股票代號
- 買賣方向
- 成交股數
- 成交價格
- 成交時間
- 手續費
- 交易稅
- 滑價
- 策略名稱
- 訊號編號
- 備註

### execution_account_snapshot
- 快照時間
- 帳戶名稱
- 可用現金
- 總市值
- 總權益
- 買進力
- 未實現損益
- 已實現損益
- 當日損益
- 曝險比率
- 幣別
- 備註

### execution_positions_snapshot
- 快照時間
- 股票代號
- 持倉方向
- 持股數量
- 可用股數
- 庫存均價
- 現價
- 市值
- 未實現損益
- 已實現損益
- 策略名稱
- 產業名稱
- 備註

## 使用建議
1. 先備份原資料庫
2. 先跑：`python db_setup.py --mode upgrade`
3. 確認 execution 四表已出現中文欄位
4. 若是全新重建，再跑：`python db_setup.py --mode reset --yes`

