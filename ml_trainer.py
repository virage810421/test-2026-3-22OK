import pandas as pd
import os
import joblib
from sklearn.ensemble import RandomForestClassifier

def train_models():
    print("🧠 [精神時光屋] 啟動 AI 三核心大腦鍛造程序...")

    # 1. 讀取兵工廠剛剛印好的課本
    dataset_path = "data/ml_training_data.csv"
    if not os.path.exists(dataset_path):
        print(f"❌ 找不到訓練教材 ({dataset_path})！請先執行兵工廠。")
        return

    df = pd.read_csv(dataset_path)
    os.makedirs("models", exist_ok=True)

    # 2. 定義什麼是「考題(X)」，什麼是「答案(Y)」與「非作答區(Meta)」
    # 把不能讓 AI 偷看的答案與文字標籤排除掉
    drop_cols = ['Ticker', 'Date', 'Setup', 'Regime', 'Label_Y']
    feature_cols = [c for c in df.columns if c not in drop_cols]

    # 🌟 把您那 16 把終極武器的「清單」存起來，讓前線實戰機台對齊！
    joblib.dump(feature_cols, "models/selected_features.pkl")
    print(f"📦 已鎖定 {len(feature_cols)} 項戰術特徵，雙向武器庫已全數上線！")

    # 3. 🌟 核心升級：因應不同的市場環境，鍛造三顆獨立大腦！
    regimes = ['趨勢多頭', '區間盤整', '趨勢空頭']

    for regime in regimes:
        print(f"\n⏳ 正在萃取並訓練【{regime}】專屬大腦...")
        
        # 只挑出該市場環境的考卷給對應的大腦寫
        regime_df = df[df['Regime'] == regime]

        if len(regime_df) < 20: # 樣本數防呆
            print(f"⚠️ {regime} 的戰鬥數據過少 ({len(regime_df)}筆)，暫無法訓練此大腦。")
            continue

        # 插入在 ml_trainer.py 大約第 37 行 (X 和 y 定義之後)
        X = regime_df[feature_cols].copy()
        y = regime_df['Label_Y']

        # 🌟 終極縫合：清洗 NaN 與 無限大 (Infinity)，防止 AI 訓練崩潰！
        import numpy as np # 確保檔案最上方有 import numpy as np
        X = X.replace([np.inf, -np.inf], np.nan) # 把無限大轉成空值
        X = X.fillna(0) # 把所有空值補 0 (中性數值)

        # 🌟 防呆鎖：確保該環境的考卷有勝有負，否則 AI 會無法分類
        if len(y.unique()) < 2:
            print(f"⚠️ {regime} 考卷結果過於單一 (全勝或全敗)，AI 無法學習差異，跳過訓練！")
            continue

        # 🤖 演算法：隨機森林特種部隊 
        # class_weight='balanced' 可以防止 AI 因為常常輸而產生偏見
        model = RandomForestClassifier(
            n_estimators=200, 
            max_depth=7, 
            random_state=42, 
            class_weight='balanced'
        )
        
        # 進行高壓訓練
        model.fit(X, y)

        # 將訓練好的大腦晶片存入軍火庫
        model_path = f"models/model_{regime}.pkl"
        joblib.dump(model, model_path)
        
        # 內部準確率測試 (看 AI 吸收了多少)
        score = model.score(X, y)
        print(f"✅ {regime} 大腦鍛造完成！(內部記憶準確率: {score:.2%})")

    print("\n🎉 精神時光屋結訓！所有 AI 大腦均已就位！")

if __name__ == "__main__":
    import warnings
    warnings.filterwarnings('ignore') # 關閉煩人的警告
    train_models()