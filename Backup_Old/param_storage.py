# param_storage.py
import json
import os

STORAGE_FILE = "automated_sector_params.json"

def save_sector_params(sector_name, best_params):
    """將優化後的最佳參數存入 JSON"""
    data = {}
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            data = json.load(f)
    
    data[sector_name] = best_params
    with open(STORAGE_FILE, "w") as f:
        json.dump(data, f, indent=4)
    print(f"💾 【{sector_name}】最新最佳參數已自動存檔。")

def load_all_params():
    """供實戰機台讀取所有產業參數"""
    if os.path.exists(STORAGE_FILE):
        with open(STORAGE_FILE, "r") as f:
            return json.load(f)
    return {}