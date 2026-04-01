import pandas as pd
import os
import joblib
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report # 🌟 高級裝備 1：詳細戰報
from feature_selector import auto_select_best_features # 🌟 高級裝備 2：精選引擎

def train_regime_models():
    print("🧠 [訓練中心] 準備啟動 AI 模型訓練...")
    
    # 🌟 1. 啟動精選引擎，獲取黃金指標組合 (正式呼叫！)
    best_features = auto_select_best_features("data/ml_training_data.csv")
    if not best_features:
        print("❌ 無法獲取精選特徵，訓練中止。")
        return
    
    try:
        df = pd.read_csv("data/ml_training_data.csv")
    except FileNotFoundError:
        print("❌ 找不到訓練資料 ml_training_data.csv，請先執行兵工廠。")
        return

    # 填補缺失值
    df.fillna(0, inplace=True)
    
    # 定義要分開訓練的市場環境
    regimes = ['趨勢多頭', '區間盤整', '趨勢空頭']

    for regime in regimes:
        print(f"\n" + "="*50)
        print(f"⚔️ 開始訓練【{regime}】專用 AI 模型...")
        
        # 篩選出該環境的專屬戰鬥紀錄
        regime_df = df[df['Regime'] == regime]
        
        if len(regime_df) < 50:
            print(f"⚠️ {regime} 樣本數過少 ({len(regime_df)}筆)，跳過訓練。請收集更多數據。")
            continue

        # 🌟 2. AI 訓練時，只看精選過後的黃金指標 (X)
        X = regime_df[best_features]
        y = regime_df['Label_Y']

        # 切分 80% 作為訓練教材，20% 作為期末考盲測
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        # 建立隨機森林指揮官 (設定 500 棵決策樹，深度限制 5 層防止死背答案)
        clf = RandomForestClassifier(n_estimators=500, max_depth=5, random_state=42, n_jobs=-1)
        clf.fit(X_train, y_train)

        # 🌟 3. 考試與產生詳細戰報
        accuracy = clf.score(X_test, y_test)
        print(f"🎯 {regime} 模型基礎預測準確度: {accuracy*100:.1f}%\n")
        
        print("📊 [深度戰報] 模型對『贏(1)』與『輸(0)』的判斷力分析：")
        y_pred = clf.predict(X_test)
        # 這裡會印出 Precision (看對的機率) 與 Recall (抓出飆股的機率)
        print(classification_report(y_test, y_pred, zero_division=0)) 
        
        # 4. 儲存模型 (打包成武器檔)
        os.makedirs("models", exist_ok=True)
        model_filename = f"models/model_{regime}.pkl" 
        joblib.dump(clf, model_filename)
        print(f"💾 模型已保存為: {model_filename}")

    print("\n✅ [訓練中心] 報告長官！所有環境模型皆已訓練並打包完畢！")

if __name__ == "__main__":
    train_regime_models()