import pandas as pd
import joblib
import os # 記得在最上方補上 import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import RFECV

# 🌟 關鍵：請檢查這行名字是否一字不差
def auto_select_best_features(csv_file="data/ml_training_data.csv"):
    print("🎯 [精選引擎] 開始從指標池中挑選黃金組合...")
    
    try:
        df = pd.read_csv(csv_file)
    except FileNotFoundError:
        print("❌ 找不到資料，請先執行 ml_data_generator.py")
        return None

    # 1. 定義原始指標池 (A~H)
    raw_feature_pool = [
        'RSI', 'MACD_Hist', 'BB_Width', 'Volume_Ratio', 
        'ADX', 'Foreign_Net', 'Trust_Net'
    ]
    
    df.fillna(0, inplace=True)
    X = df[raw_feature_pool]
    y = df['Label_Y']

    if len(X) < 100:
        print("⚠️ 樣本數太少，無法進行精選，請增加股票數量。")
        return raw_feature_pool

    # 2. 啟動遞迴消除法 (RFECV)
    estimator = RandomForestClassifier(n_estimators=50, random_state=42, n_jobs=-1)
    selector = RFECV(estimator, step=1, cv=5, scoring='accuracy')
    selector = selector.fit(X, y)

    # 3. 取得入選名單
    selected_features = [f for f, s in zip(raw_feature_pool, selector.support_) if s]
    
    # 確保至少選出一個特徵，防呆機制
    if not selected_features:
        selected_features = raw_feature_pool
        
    print(f"\n💡 [篩選結果] 原始指標：{', '.join(raw_feature_pool)}")
    print(f"✅ [自動精選]：{', '.join(selected_features)}")
    
    dropped_features = set(raw_feature_pool) - set(selected_features)
    if dropped_features:
        print(f"📉 [已剔除雜訊]：{', '.join(dropped_features)}")
    else:
        print("📉 [已剔除雜訊]：無 (所有指標皆有效)")

    # 儲存精選名單，供大腦訓練與未來實戰機台讀取
    os.makedirs("models", exist_ok=True)
    joblib.dump(selected_features, "models/selected_features.pkl")
    return selected_features

if __name__ == "__main__":
    auto_select_best_features()