import pandas as pd
import os
import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit  # 🔥 新增：時間序列滾動切割工具

# ==========================================
# 🧩 新增：Walk-Forward 壓力測試模組
# ==========================================
def walk_forward_analysis(X, y):
    """將資料依時間切成 5 局，模擬 AI 邊走邊學的實戰狀況"""
    tscv = TimeSeriesSplit(n_splits=5)
    results = []
    
    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        
        # 防呆：如果某個時間段的考卷答案只有一種(全贏或全輸)，跳過這局
        if len(y_train.unique()) < 2 or len(y_test.unique()) < 2:
            continue
            
        # 使用輕量化模型快速進行壓力測試
        model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, class_weight='balanced')
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        
        # 計算勝率 (Hit Rate)
        hit_rate = (pred == y_test).mean()
        
        # 模擬期望報酬 (將標籤 0/1 轉為 -1/1，方向對=賺，方向錯=賠)
        pred_sign = np.where(pred > 0, 1, -1)
        actual_sign = np.where(y_test > 0, 1, -1)
        strategy_return = (pred_sign * actual_sign).mean()
        
        results.append({"hit_rate": hit_rate, "return": strategy_return})
        
    return results

# ==========================================
# 🧩 新增：Alpha 穩定度評估模組
# ==========================================
def evaluate_stability(results):
    """計算這 5 局測驗下來的平均表現與一致性"""
    if not results: 
        return {"ret_mean": 0, "consistency": 0}
    
    returns = [r["return"] for r in results]
    stability = {
        "ret_mean": np.mean(returns),
        "consistency": np.mean([r > 0 for r in returns]) # 計算有幾局是正報酬
    }
    return stability


# ==========================================
# 🧠 主幹：AI 大腦鍛造程序
# ==========================================
def train_models():
    print("🧠 [精神時光屋] 啟動 AI 三核心大腦鍛造程序...")

    dataset_path = "data/ml_training_data.csv"
    if not os.path.exists(dataset_path):
        print(f"❌ 找不到訓練教材 ({dataset_path})！請先執行兵工廠。")
        return

    df = pd.read_csv(dataset_path)
    os.makedirs("models", exist_ok=True)
    
    # 🌟 為了確保 Walk-Forward 照時間推進，強制將資料依日期排序
    if 'Date' in df.columns:
        df = df.sort_values('Date').reset_index(drop=True)

    drop_cols = ['Ticker', 'Date', 'Setup', 'Regime', 'Label_Y']
    feature_cols = [c for c in df.columns if c not in drop_cols]

    joblib.dump(feature_cols, "models/selected_features.pkl")
    print(f"📦 已鎖定 {len(feature_cols)} 項戰術特徵，雙向武器庫已全數上線！")

    regimes = ['趨勢多頭', '區間盤整', '趨勢空頭']

    for regime in regimes:
        print(f"\n" + "="*50)
        print(f"⏳ 正在萃取並訓練【{regime}】專屬大腦...")
        print("="*50)
        
        regime_df = df[df['Regime'] == regime]

        if len(regime_df) < 50: # 稍微提高樣本數要求，因為要切 5 份
            print(f"⚠️ {regime} 的戰鬥數據過少 ({len(regime_df)}筆)，不夠進行壓力測試，跳過訓練。")
            continue

        X = regime_df[feature_cols].copy()
        y = regime_df['Label_Y']

        # 🌟 清洗 NaN 與 無限大 (Infinity)
        X = X.replace([np.inf, -np.inf], np.nan)
        X = X.fillna(0)

        if len(y.unique()) < 2:
            print(f"⚠️ 考卷結果過於單一，AI 無法學習差異，跳過訓練！")
            continue

        # ==========================================
        # 🔥 品管閘門：執行 Walk-Forward 前向推進測試
        # ==========================================
        print(f"   ► 進入 Walk-Forward 滾動壓力測試 (5 局模擬)...")
        wf_results = walk_forward_analysis(X, y)
        stability = evaluate_stability(wf_results)
        
        print(f"   ► [品管報告] 平均期望報酬: {stability['ret_mean']:.4f} | 獲利一致性: {stability['consistency']:.1%}")
        
        # 🚨 拔除不良品：期望值<=0，或獲利局數不到 60% 者，直接銷毀！
        if stability['ret_mean'] > 0 and stability['consistency'] >= 0.60:
            print("   ✅ 檢驗合格！具備實戰穩定性，獲准進行最終鍛造...")
        else:
            print("   🛑 檢驗失敗！此模型極度不穩定或會虧損，銷毀不予採用！")
            # 刪除舊的廢物模型 (如果有的話)，避免實戰機台用到過期品
            if os.path.exists(f"models/model_{regime}.pkl"):
                os.remove(f"models/model_{regime}.pkl")
                print(f"   🗑️ 已同步移除失效的舊版 {regime} 大腦。")
            continue # 直接跳到下一個市場環境，這顆大腦不存檔！

        # ==========================================
        # 🤖 最終鍛造：通過測試後，用「全部歷史資料」做最後強化並存檔
        # ==========================================
        model = RandomForestClassifier(
            n_estimators=200, 
            max_depth=7, 
            random_state=42, 
            class_weight='balanced'
        )
        model.fit(X, y)

        model_path = f"models/model_{regime}.pkl"
        joblib.dump(model, model_path)
        
        score = model.score(X, y)
        print(f"   🚀 最終大腦鍛造完成並成功掛載！(全域記憶準確率: {score:.2%})")

    print("\n🎉 精神時光屋結訓！所有合格的 AI 大腦均已就位！")

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    train_models()