import webbrowser
import os

mermaid_code = """
graph TD
    %% 定義樣式
    classDef data fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef process fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef ai fill:#e8f5e9,stroke:#2e7d32,stroke-width:2px,stroke-dasharray: 5 5;
    classDef db fill:#ede7f6,stroke:#5e35b1,stroke-width:2px,rx:10,ry:10;
    classDef exe fill:#ffebee,stroke:#c62828,stroke-width:2px,rx:5,ry:5;

    subgraph Data_Inputs [外部數據源]
        YF[yfinance API<br/>價格/成交量]:::data
        FM[FinMind API<br/>營收/財報]:::data
    end

    BC[config.py<br/>核心參數/DB連線]:::db

    subgraph Stage_R&D [階段一：離線研發與參數最佳化]
        AO[advanced_optimizer.py<br/>貝氏最佳化機台]:::process
        OPT[optimizer.py<br/>基礎最佳化]:::process
        SCR_BT[screening.py<br/>回測模式]:::process
        
        AO -->|呼叫| SCR_BT
        OPT -->|呼叫| SCR_BT
        YF --> SCR_BT
        SCR_BT -->|回報績效| AO
        BC -.->|讀取| AO
        AO -->|產出黃金參數| BC
    end

    subgraph Stage_ML [階段二：AI 兵工廠]
        MDG[ml_data_generator.py<br/>資料萃取兵]:::process
        MLT[ml_trainer.py<br/>AI 訓練師]:::process
        SCR_DG[screening.py<br/>資料萃取模式]:::process
        
        BC -.->|啟動旁路| MDG
        YF --> SCR_DG
        MDG -->|呼叫| SCR_DG
        
        TCSV(ml_training_data.csv<br/>歷史戰鬥特徵):::data
        SCR_DG -->|產出| TCSV
        TCSV -->|輸入| MLT
        
        FS[feature_selector.py<br/>特徵瘦身]:::ai
        MLT <--> FS
        
        PKL(AI 大腦模型庫 .pkl):::ai
        MLT -->|鍛造產出| PKL
    end

    subgraph Stage_Live [階段三：線上實戰執行]
        LPT[live_paper_trading.py<br/>實戰機台 CEO]:::exe
        
        subgraph Risk_Control [內部風控]
            MDD_M[MDD 分級熔斷]:::exe
        end
        
        SCR_LIVE[screening.py<br/>實戰掃描模式]:::process
        STR[strategies.py<br/>AI 武器庫]:::ai
        
        LPT --> Risk_Control
        BC -.-> LPT
        LPT -->|呼叫| SCR_LIVE
        
        YF --> SCR_LIVE
        FM --> SCR_LIVE
        
        SIGNAL(今日系統燈號<br/>例: SNIPER 買訊):::data
        SCR_LIVE --> SIGNAL
        SIGNAL --> LPT
        
        LPT -->|訊號審核| STR
        PKL -.->|載入 AI| STR
        STR -->|回傳 勝率/資金倍數| LPT
    end

    subgraph Storage [數據持久層 - SQL Server]
        SQL_CHIP[(daily_chip_data)]:::db
        SQL_BT[(backtest_history)]:::db
        SQL_PORT[(paper_portfolio)]:::db
    end

    LPT -->|寫入明細| SQL_PORT
    AO -->|寫入回測| SQL_BT
"""

# 💡 使用傳統且相容性最高的 CDN 載入方式
html_content = f"""
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>HFA 系統架構圖</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>mermaid.initialize({{startOnLoad:true}});</script>
    <style>
        body {{ font-family: sans-serif; background-color: #2b2b2b; color: #fff; padding: 40px; display: flex; flex-direction: column; align-items: center; }}
        h1 {{ color: #4CAF50; }}
        .card {{ background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.3); width: 95%; max-width: 1400px; overflow-x: auto; }}
    </style>
</head>
<body>
    <h1>🚀 混合量化交易系統架構 (HFA)</h1>
    <div class="card">
        <div class="mermaid">
        {mermaid_code}
        </div>
    </div>
</body>
</html>
"""

file_path = "system_architecture.html"
with open(file_path, "w", encoding="utf-8") as f:
    f.write(html_content)

print(f"✅ 報告長官！架構圖已生成完畢，正在呼叫瀏覽器顯示...")
webbrowser.open("file://" + os.path.realpath(file_path))