import pandas as pd
import os
import joblib
import numpy as np
import itertools  # 🌟 新增這行：負責將武器兩兩配對的兵工廠工具
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit
# ==========================================
# 🧩 模組 1：Alpha 武器海關審查 (單一特徵檢驗)
# ==========================================
def evaluate_alpha_full(signal, future_return):
    """檢驗單一把武器的期望值與穩定性"""
    df = pd.DataFrame({
        "signal": signal,
        "ret": future_return
    }).dropna()

    if len(df) < 30: # 確保有足夠的觸發次數來計算
        return None

    # 把訊號跟真實報酬轉成方向 (-1 或 1)
    direction = np.sign(df["signal"])
    actual = np.sign(df["ret"])
    
    # 避免方向為 0 導致計算錯誤
    direction = np.where(direction == 0, 1, direction)

    hit_rate = (direction == actual).mean()
    returns = direction * df["ret"]
    avg_return = returns.mean()

    wins = returns[returns > 0]
    losses = returns[returns <= 0]

    if len(wins) == 0 or len(losses) == 0:
        expectancy = 0
    else:
        expectancy = wins.mean() * len(wins)/len(df) + losses.mean() * len(losses)/len(df)

    consistency = (returns > 0).mean()

    score = (expectancy * 0.5) + (avg_return * 0.3) + (hit_rate * 0.1) + (consistency * 0.1)

    return {
        "expectancy": expectancy,
        "avg_return": avg_return,
        "hit_rate": hit_rate,
        "consistency": consistency,
        "score": score
    }

# ==========================================
# 🧩 模組 2：Walk-Forward 壓力測試模組 (大腦檢驗)
# ==========================================
def walk_forward_analysis(X, y):
    tscv = TimeSeriesSplit(n_splits=5)
    results = []
    
    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        
        if len(y_train.unique()) < 2 or len(y_test.unique()) < 2:
            continue
            
        model = RandomForestClassifier(n_estimators=100, max_depth=5, random_state=42, class_weight='balanced')
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        
        hit_rate = (pred == y_test).mean()
        pred_sign = np.where(pred > 0, 1, -1)
        actual_sign = np.where(y_test > 0, 1, -1)
        strategy_return = (pred_sign * actual_sign).mean()
        
        results.append({"hit_rate": hit_rate, "return": strategy_return})
        
    return results

def evaluate_stability(results):
    if not results: 
        return {"ret_mean": 0, "consistency": 0}
    returns = [r["return"] for r in results]
    return {
        "ret_mean": np.mean(returns),
        "consistency": np.mean([r > 0 for r in returns])
    }

# ==========================================
# 🧠 主幹：AI 大腦鍛造程序
# ==========================================
def train_models():
    print("🧠 [精神時光屋] 啟動 AI 兵工廠 (搭載 Alpha 特徵海關)...")

    dataset_path = "data/ml_training_data.csv"
    if not os.path.exists(dataset_path):
        print(f"❌ 找不到訓練教材 ({dataset_path})！")
        return

    df = pd.read_csv(dataset_path)
    os.makedirs("models", exist_ok=True)

    # ==========================================
    # 🧠 🌟 終極升級：Regime 穩定性檢測 (Drift Check)
    # ==========================================
    print("\n🔍 [系統安檢] 啟動 Regime 漂移檢測 (Distribution Drift Check)...")
    try:
        # 將資料切分成「前半段(舊歷史)」與「後半段(近期)」
        half_idx = len(df) // 2
        past_dist = df['Regime'].iloc[:half_idx].value_counts(normalize=True)
        recent_dist = df['Regime'].iloc[half_idx:].value_counts(normalize=True)
        
        # 對齊 index，避免某些 Regime 在近期沒出現導致計算錯誤
        all_regimes = list(set(past_dist.index.tolist() + recent_dist.index.tolist()))
        past_dist = past_dist.reindex(all_regimes).fillna(0)
        recent_dist = recent_dist.reindex(all_regimes).fillna(0)

        # 計算絕對值差異總和 (Drift Score)
        drift_score = (past_dist - recent_dist).abs().sum()
        
        print(f"   ► 歷史生態分布: 多頭({past_dist.get('趨勢多頭', 0):.1%}) | 空頭({past_dist.get('趨勢空頭', 0):.1%}) | 盤整({past_dist.get('區間盤整', 0):.1%})")
        print(f"   ► 近期生態分布: 多頭({recent_dist.get('趨勢多頭', 0):.1%}) | 空頭({recent_dist.get('趨勢空頭', 0):.1%}) | 盤整({recent_dist.get('區間盤整', 0):.1%})")
        print(f"   ► Regime 分布漂移指數 (Drift Score): {drift_score:.3f}")

        # 判斷標準
        if drift_score < 0.2:
            print("   ✅ 判定：市場結構極度穩定，完美適合 AI 深度學習！")
        elif drift_score < 0.4:
            print("   ⚠️ 判定：市場結構輕微偏移，尚在容許範圍內。")
        else:
            print("   🛑 警告：偵測到嚴重 Regime 漂移 (Drift > 0.4)！近期市場生態已發生劇變！")
            print("   💡 系統建議：請密切關注本次訓練之大腦『獲利一致性』，若過低系統將自動啟動銷毀機制。")
            
    except Exception as e:
        print(f"   ⚠️ 漂移檢測模組異常，跳過檢驗: {e}")
    # ==========================================

    if 'Date' in df.columns:
        df = df.sort_values('Date').reset_index(drop=True)

    # 1. 定義不要當作武器的系統欄位
    drop_cols = ['Ticker', 'Date', 'Setup', 'Regime', 'Label_Y', 'Target_Return']
    all_features = [c for c in df.columns if c not in drop_cols]


    # ==========================================
    # 🧠 🌟 終極升級：Alpha 記憶特徵池 (永久記憶與動態重鑄)
    # ==========================================
    
    old_features = []
    if os.path.exists("models/selected_features.pkl"):
        try:
            old_features = joblib.load("models/selected_features.pkl")
            print(f"📦 [記憶讀取] 成功尋獲前人遺留的戰術背包，發現 {len(old_features)} 把歷史精銳武器。")
        except Exception as e:
            print(f"⚠️ [記憶讀取] 無法解析舊有背包，將以全新狀態啟動: {e}")

    # 1. 將今天從課本抓到的新武器，與歷史武器庫合併，並用 set() 消除重複
    all_features = list(set(all_features + old_features))

    # 2. 🛠️ 自動重鑄歷史連擊武器 (全面支援雙重與三重以上組合技)
    for feature in all_features:
        if "_X_" in feature and feature not in df.columns:
            parts = feature.split("_X_")
            
            # 檢查組成這把連擊武器的所有零件，今天是不是都在課本裡？
            if all(p in df.columns for p in parts):
                # 動態把所有零件乘起來，完美還原這把武器
                temp_signal = df[parts[0]]
                for p in parts[1:]:
                    temp_signal = temp_signal * df[p]
                df[feature] = temp_signal
                    
    # 3. 最終防線：再次過濾，確保 all_features 裡面的武器真的都在今天的課本裡
    all_features = [f for f in all_features if f in df.columns]
    print(f"⚔️ [軍火庫整編] 新舊武器庫合併與重鑄完畢，今日送審武器總數：{len(all_features)} 把。")
    # ==========================================

    # 🌟 取得真實報酬率供海關審查使用 (防呆機制：若無此欄位則用 Label_Y 替代模擬)
    if 'Target_Return' in df.columns:
        future_return = df['Target_Return']
    else:
        future_return = np.where(df['Label_Y'] == 1, 0.05, -0.05) 

    # ==========================================
    # 🔥 階段一：啟動 Alpha 特徵海關 (汰除爛武器)
    # ==========================================
    print(f"\n🕵️‍♂️ [海關審查] 正在單獨檢驗 {len(all_features)} 把候選武器的期望值...")
    qualified_features = []
    
    for col in all_features:
        # 因為有些武器可能是類別型或布林值，先轉成數值
        signal = pd.to_numeric(df[col], errors='coerce').fillna(0)
        
        # 如果這個特徵全部都是 0 (從未觸發過)，直接跳過
        if signal.nunique() <= 1:
            continue
            
        result = evaluate_alpha_full(signal, future_return)
        
        if result is None: continue
            
        # 🎯 武器及格標準：期望值 > 0 且 穩定性 >= 50%
        if result['expectancy'] > 0 and result['consistency'] >= 0.50:
            qualified_features.append(col)
            print(f"   ✅ [保留] {col}: EV={result['expectancy']:.2%} | 穩定度={result['consistency']:.0%}")
        else:
            print(f"   🗑️ [銷毀] {col}: EV為負或不穩定 (EV={result['expectancy']:.2%})")

    # 防呆：如果審查太嚴格導致武器全軍覆沒，降級使用所有武器
    if not qualified_features:
        print("⚠️ 警告：沒有任何武器通過嚴格審查！系統強制保留所有武器以維持運作。")
        qualified_features = all_features
    # ==========================================
    # 🔥🔥🔥 請從這裡開始貼上「連擊研發中心」代碼 🔥🔥🔥
    # ==========================================
    print(f"\n⚔️ 啟動連擊武器研發：正在測試 {len(qualified_features)} 把及格武器的交叉組合...")
    combo_features = []
    
    # 測試 2 把與 3 把武器的組合
    for r in [2, 3]: 
        for combo_tuple in itertools.combinations(qualified_features, r):
            combo_name = "_X_".join(combo_tuple)
            
            # 把所有武器相乘 (如果是 3 把就是 A * B * C)
            combo_signal = df[list(combo_tuple)].prod(axis=1)
        
        # 如果這個連擊招式在歷史上出現太少次 (例如不到 20 次)，不具統計意義，跳過
        if combo_signal.sum() < 20: 
            continue
            
        result = evaluate_alpha_full(combo_signal, future_return)
        if result is None: continue
            
        # 🎯 連擊武器標準：期望值 > 0 且 穩定性 >= 60%
        if result['expectancy'] > 0 and result['consistency'] >= 0.60:
            df[combo_name] = combo_signal # 把這把連擊武器寫入真實 DataFrame 讓大腦學習
            combo_features.append(combo_name)
            print(f"   🔥 [最強連擊] {combo_name}: EV={result['expectancy']:.2%} | 穩定={result['consistency']:.0%}")

    # 把新研發出來的連擊武器，加進精銳部隊清單中
    qualified_features.extend(combo_features)
    print(f"📦 連擊研發完畢！額外新增了 {len(combo_features)} 把組合技武器。")

    joblib.dump(qualified_features, "models/selected_features.pkl")
    print(f"\n📦 特徵審查完畢！已將 {len(qualified_features)} 把「正期望值精銳武器」裝載至戰術背包！")

    # ==========================================
    # 🔥 階段二：訓練 AI 大腦 (大腦檢驗)
    # ==========================================
    regimes = ['趨勢多頭', '區間盤整', '趨勢空頭']

    for regime in regimes:
        print(f"\n" + "="*50)
        print(f"⏳ 正在使用精銳武器，訓練【{regime}】專屬大腦...")
        
        regime_df = df[df['Regime'] == regime]
        if len(regime_df) < 50: 
            print(f"⚠️ 數據過少，跳過。")
            continue

        # 🌟 這裡最重要：AI 現在只拿「及格的武器 (qualified_features)」去學習！
        X = regime_df[qualified_features].copy()
        y = regime_df['Label_Y']

        X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
        if len(y.unique()) < 2: continue

        # 執行 Walk-Forward 大腦壓力測試
        wf_results = walk_forward_analysis(X, y)
        stability = evaluate_stability(wf_results)
        print(f"   ► [大腦品管] 平均期望報酬: {stability['ret_mean']:.4f} | 獲利一致性: {stability['consistency']:.1%}")
        
        if stability['ret_mean'] > 0 and stability['consistency'] >= 0.60:
            print("   ✅ 檢驗合格！進行最終鍛造...")
            model = RandomForestClassifier(n_estimators=200, max_depth=7, random_state=42, class_weight='balanced')
            model.fit(X, y)
            joblib.dump(model, f"models/model_{regime}.pkl")
        else:
            print("   🛑 檢驗失敗！大腦不穩定，銷毀！")
            if os.path.exists(f"models/model_{regime}.pkl"): os.remove(f"models/model_{regime}.pkl")

    print("\n🎉 精神時光屋結訓！精英武器與合格大腦均已就位！")

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore')
    train_models()