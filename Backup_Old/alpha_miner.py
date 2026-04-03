import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import mutual_info_classif
import warnings
warnings.filterwarnings('ignore')

def run_alpha_forge():
    print("🏭 [研發中心] 啟動全自動武器鍛造爐 (Auto-Feature Forge)...")
    
    # 1. 讀取兵工廠已經印好的最新歷史數據
    try:
        df = pd.read_csv("data/ml_training_data.csv")
    except FileNotFoundError:
        print("❌ 找不到訓練教材！請先執行 ml_data_generator.py。")
        return
    
    # 區分特徵與答案
    drop_cols = ['Ticker', 'Date', 'Setup', 'Regime', 'Label_Y']
    base_features = [c for c in df.columns if c not in drop_cols]
    X = df[base_features].fillna(0)
    y = df['Label_Y']
    
    print(f"🔧 已載入 {len(base_features)} 把基礎武器，準備啟動高壓碰撞與合金鍛造...")
    
    new_features = pd.DataFrame()
    
    # ==========================================
    # 2. 自動產生組合 (Generative Design)
    # ==========================================
    # 將武器分類：數值型 (如 RSI) 與 開關型 (如 爆量=1或0)
    numeric_cols = [c for c in base_features if df[c].nunique() > 2]
    bool_cols = [c for c in base_features if df[c].nunique() <= 2]
    
    # 🧪 合金 A：數值武器交叉相乘 (捕捉非線性動能)
    for i in range(len(numeric_cols)):
        for j in range(i+1, len(numeric_cols)):
            col1, col2 = numeric_cols[i], numeric_cols[j]
            new_features[f"Alpha_{col1}_x_{col2}"] = X[col1] * X[col2]
            
    # 🧪 合金 B：開關武器邏輯串聯 (捕捉極端共振)
    for i in range(len(bool_cols)):
        for j in range(i+1, len(bool_cols)):
            col1, col2 = bool_cols[i], bool_cols[j]
            new_features[f"Alpha_{col1}_AND_{col2}"] = X[col1] & X[col2]
            
    print(f"🔥 鍛造爐高溫運轉中... 成功組合出 {new_features.shape[1]} 把全新未知的試驗武器！")
    print("🎯 開始進入實彈測試與勝率關聯性分析 (Mutual Information)...")
    
    # ==========================================
    # 3. 嚴格篩選 (品質檢驗與 Alpha 發現)
    # ==========================================
    X_new = new_features.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    # 使用 Mutual Information 評估每個新武器對「最終勝率 (Label_Y)」的預測貢獻度
    mi_scores = mutual_info_classif(X_new, y, random_state=42)
    
    # 將分數排序
    mi_series = pd.Series(mi_scores, index=X_new.columns).sort_values(ascending=False)
    
    print("\n" + "="*60)
    print("🏆 [研發中心] 測試完畢！以下是發掘出的【Top 5 終極隱藏武器】")
    print("="*60)
    for idx, (weapon, score) in enumerate(mi_series.head(5).items(), 1):
        print(f" 第 {idx} 名: {weapon}")
        print(f"    ► Alpha 預測貢獻度: {score:.4f} (分數越高，對勝率影響越強)")
        
    print("-" * 60)
    print("💡 總司令，研發中心已給出設計圖！")
    print("如果您看中了某把武器，只需打開 screening.py，將它的公式加入 extract_ai_features 中，即可正式投入全軍使用！")

if __name__ == "__main__":
    run_alpha_forge()