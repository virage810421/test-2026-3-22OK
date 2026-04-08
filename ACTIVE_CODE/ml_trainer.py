import itertools
import json
import os
from datetime import datetime

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import TimeSeriesSplit

from config import PARAMS
from model_governance import (
    create_version_tag,
    get_best_version_entry,
    promote_best_version,
    restore_version,
    snapshot_current_models,
)


def evaluate_alpha_full(signal, future_return):
    df = pd.DataFrame({
        "signal": pd.to_numeric(signal, errors="coerce"),
        "ret": pd.to_numeric(future_return, errors="coerce"),
    }).dropna()

    if len(df) < 30:
        return None

    direction = np.sign(df["signal"])
    actual = np.sign(df["ret"])
    direction = np.where(direction == 0, 1, direction)

    hit_rate = (direction == actual).mean()
    returns = direction * df["ret"]
    avg_return = returns.mean()

    wins = returns[returns > 0]
    losses = returns[returns <= 0]

    if len(wins) == 0 or len(losses) == 0:
        expectancy = 0.0
    else:
        expectancy = wins.mean() * len(wins) / len(df) + losses.mean() * len(losses) / len(df)

    consistency = (returns > 0).mean()
    score = (expectancy * 0.5) + (avg_return * 0.3) + (hit_rate * 0.1) + (consistency * 0.1)

    return {
        "expectancy": float(expectancy),
        "avg_return": float(avg_return),
        "hit_rate": float(hit_rate),
        "consistency": float(consistency),
        "score": float(score),
    }


def walk_forward_analysis(X, y, target_return, p=PARAMS):
    splits = int(p.get("WF_SPLITS", 5))
    tscv = TimeSeriesSplit(n_splits=splits)
    results = []

    for train_idx, test_idx in tscv.split(X):
        X_train, X_test = X.iloc[train_idx], X.iloc[test_idx]
        y_train, y_test = y.iloc[train_idx], y.iloc[test_idx]
        ret_test = target_return.iloc[test_idx]

        if len(pd.Series(y_train).unique()) < 2 or len(pd.Series(y_test).unique()) < 2:
            continue

        model = RandomForestClassifier(
            n_estimators=int(p.get("MODEL_N_ESTIMATORS", 100)),
            max_depth=int(p.get("MODEL_MAX_DEPTH", 5)),
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X_train, y_train)
        pred = model.predict(X_test)
        pred_proba = model.predict_proba(X_test)[:, 1]

        strategy_returns = np.where(pred == 1, ret_test.values, 0.0)

        hit_rate = float(np.mean(strategy_returns > 0))
        avg_return = float(np.mean(strategy_returns))
        pred_accuracy = float(np.mean(pred == y_test))

        gross_profit = strategy_returns[strategy_returns > 0].sum() if np.any(strategy_returns > 0) else 0.0
        gross_loss = abs(strategy_returns[strategy_returns < 0].sum()) if np.any(strategy_returns < 0) else 0.0
        profit_factor = float(gross_profit / gross_loss) if gross_loss > 0 else 99.9
        sharpe_like = float(np.mean(strategy_returns) / np.std(strategy_returns)) if np.std(strategy_returns) > 0 else 0.0
        coverage = float(np.mean(pred == 1))

        results.append({
            "hit_rate": hit_rate,
            "return": avg_return,
            "profit_factor": profit_factor,
            "pred_accuracy": pred_accuracy,
            "sharpe_like": sharpe_like,
            "coverage": coverage,
            "mean_proba": float(np.mean(pred_proba)),
        })

    return results


def evaluate_stability(results):
    if not results:
        return {
            "ret_mean": 0.0,
            "consistency": 0.0,
            "pf_mean": 0.0,
            "sharpe_mean": 0.0,
            "coverage_mean": 0.0,
        }

    returns = [r["return"] for r in results]
    pfs = [r["profit_factor"] for r in results]
    sharpes = [r["sharpe_like"] for r in results]
    coverages = [r["coverage"] for r in results]

    return {
        "ret_mean": float(np.mean(returns)),
        "consistency": float(np.mean([r > 0 for r in returns])),
        "pf_mean": float(np.mean(pfs)),
        "sharpe_mean": float(np.mean(sharpes)),
        "coverage_mean": float(np.mean(coverages)),
    }


def train_models():
    print("🧠 [精神時光屋] 啟動 AI 兵工廠（版本治理版）...")

    dataset_path = "data/ml_training_data.csv"
    if not os.path.exists(dataset_path):
        print(f"❌ 找不到訓練教材 ({dataset_path})！")
        return

    # 先備份現役模型
    pretrain_version = create_version_tag("pretrain")
    snapshot_current_models(pretrain_version, note="重訓前自動備份")
    print(f"📦 已自動備份重訓前現役模型：{pretrain_version}")

    df = pd.read_csv(dataset_path)
    os.makedirs("models", exist_ok=True)

    if "Date" in df.columns:
        df = df.sort_values("Date").reset_index(drop=True)

    if "Target_Return" in df.columns:
        df["Target_Return"] = pd.to_numeric(df["Target_Return"], errors="coerce").fillna(0.0)
    else:
        df["Target_Return"] = np.where(df["Label_Y"] == 1, 0.05, -0.05)

    drop_cols = ["Ticker", "Date", "Setup", "Regime", "Label_Y", "Target_Return", "Stop_Hit", "Hold_Days"]
    all_features = [c for c in df.columns if c not in drop_cols]

    old_features = []
    if os.path.exists("models/selected_features.pkl"):
        try:
            old_features = joblib.load("models/selected_features.pkl")
        except Exception:
            old_features = []

    all_features = list(dict.fromkeys(all_features + old_features))

    for feature in all_features:
        if "_X_" in feature and feature not in df.columns:
            parts = feature.split("_X_")
            if all(p in df.columns for p in parts):
                temp_signal = pd.to_numeric(df[parts[0]], errors="coerce").fillna(0)
                for part in parts[1:]:
                    temp_signal = temp_signal * pd.to_numeric(df[part], errors="coerce").fillna(0)
                df[feature] = temp_signal

    all_features = [f for f in all_features if f in df.columns]

    future_return = pd.to_numeric(df["Target_Return"], errors="coerce").fillna(0)
    qualified_features = []

    for col in all_features:
        signal = pd.to_numeric(df[col], errors="coerce").fillna(0)

        if signal.nunique() <= 1:
            continue

        result = evaluate_alpha_full(signal, future_return)
        if result is None:
            continue

        if result["expectancy"] > 0 and result["consistency"] >= 0.50:
            qualified_features.append(col)

    if not qualified_features:
        qualified_features = all_features.copy()

    combo_features = []
    seed_limit = int(PARAMS.get("MODEL_SEED_FEATURE_LIMIT", 12))
    seed_features = qualified_features[:seed_limit]

    for r in [2, 3]:
        for combo_tuple in itertools.combinations(seed_features, r):
            combo_name = "_X_".join(combo_tuple)

            if combo_name in df.columns:
                combo_signal = pd.to_numeric(df[combo_name], errors="coerce").fillna(0)
            else:
                combo_signal = df[list(combo_tuple)].apply(pd.to_numeric, errors="coerce").fillna(0).prod(axis=1)

            active_count = int((combo_signal != 0).sum())
            if active_count < 20:
                continue

            result = evaluate_alpha_full(combo_signal, future_return)
            if result is None:
                continue

            if result["expectancy"] > 0 and result["consistency"] >= 0.60:
                df[combo_name] = combo_signal
                combo_features.append(combo_name)

    qualified_features = list(dict.fromkeys(qualified_features + combo_features))
    joblib.dump(qualified_features, "models/selected_features.pkl")

    regimes = ["趨勢多頭", "區間盤整", "趨勢空頭"]
    metrics_by_regime = {}

    for regime in regimes:
        regime_df = df[df["Regime"] == regime].copy()
        if len(regime_df) < int(PARAMS.get("MODEL_MIN_REGIME_SAMPLES", 50)):
            metrics_by_regime[regime] = {"status": "SKIP", "reason": "樣本不足"}
            continue

        safe_features = [f for f in qualified_features if f in regime_df.columns]
        if not safe_features:
            metrics_by_regime[regime] = {"status": "SKIP", "reason": "無可用特徵"}
            continue

        X = regime_df[safe_features].copy()
        y = pd.to_numeric(regime_df["Label_Y"], errors="coerce").fillna(0).astype(int)
        target_return = pd.to_numeric(regime_df["Target_Return"], errors="coerce").fillna(0.0)

        X = X.replace([np.inf, -np.inf], np.nan).fillna(0)
        if len(pd.Series(y).unique()) < 2:
            metrics_by_regime[regime] = {"status": "SKIP", "reason": "標籤單一"}
            continue

        wf_results = walk_forward_analysis(X, y, target_return)
        stability = evaluate_stability(wf_results)
        metrics_by_regime[regime] = {"status": "EVAL", **stability}

        model_path = f"models/model_{regime}.pkl"
        if (
            stability["ret_mean"] > 0
            and stability["consistency"] >= 0.60
            and stability["pf_mean"] >= 1.05
        ):
            model = RandomForestClassifier(
                n_estimators=int(PARAMS.get("MODEL_N_ESTIMATORS", 200)),
                max_depth=int(PARAMS.get("MODEL_MAX_DEPTH", 7)),
                random_state=42,
                class_weight="balanced",
            )
            model.fit(X, y)
            joblib.dump(model, model_path)
            metrics_by_regime[regime]["status"] = "SAVE"
        else:
            if os.path.exists(model_path):
                os.remove(model_path)
            metrics_by_regime[regime]["status"] = "REJECT"

    # 計算本次總體分數
    save_metrics = [v for v in metrics_by_regime.values() if v.get("status") == "SAVE"]
    overall_score = 0.0
    if save_metrics:
        overall_score = float(np.mean([
            v.get("ret_mean", 0.0) * 100 + v.get("pf_mean", 0.0) + v.get("consistency", 0.0) * 10
            for v in save_metrics
        ]))

    version_tag = create_version_tag("trained")
    snapshot_entry = snapshot_current_models(
        version_tag,
        metrics={
            "trained_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "overall_score": round(overall_score, 4),
            "regimes": metrics_by_regime,
            "feature_count": len(qualified_features),
        },
        note="本次重訓完成快照"
    )

    print(f"📦 已建立新模型版本：{version_tag}")
    print(json.dumps(snapshot_entry["metrics"], ensure_ascii=False, indent=2))

    # 與 best version 比較，若更好則升任；否則保留 current 但不升 best
    best_entry = get_best_version_entry()
    best_score = -1e18
    if best_entry and isinstance(best_entry.get("metrics"), dict):
        best_score = float(best_entry["metrics"].get("overall_score", -1e18))

    if overall_score > best_score and len(save_metrics) > 0:
        promote_best_version(version_tag)
        print(f"🏆 新版本 {version_tag} 已升任 BEST MODEL")
    elif len(save_metrics) == 0:
        # 本次幾乎失敗，直接回退到 pretrain 備份
        restore_version(pretrain_version)
        print(f"🛑 本次無任何合格模型，已自動回退到 {pretrain_version}")

    print("🎉 版本治理版訓練完成")


if __name__ == "__main__":
    import warnings
    warnings.filterwarnings("ignore")
    train_models()
