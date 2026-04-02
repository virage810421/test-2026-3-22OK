import webbrowser
import os

# ==========================================
# 📊 圖表 1：兵工廠的萃取流程 (ml_data_generator.py)
# ==========================================
mermaid_ml_data = """
graph TD
    classDef start fill:#dcedc8,stroke:#689f38,stroke-width:2px;
    classDef process fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef io fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef decision fill:#ffe0b2,stroke:#f57c00,stroke-width:2px;

    Start([1. 啟動兵工廠]):::start --> Setup[2. 參數初始化<br/>IS_OPTIMIZING = True<br/>旁路開啟避免死機]:::process
    Setup --> TickerLoop{3. 遍歷觀察清單}:::decision
    TickerLoop --> Download[4. 下載 3 年歷史資料<br/>yfinance]:::io
    Download --> Chip[5. 掛載法人籌碼<br/>SQL Server]:::io
    Chip --> Inspect[6. 呼叫雷達計算指標<br/>screening.inspect_stock]:::process
    Inspect --> RowLoop{7. 逐日掃描歷史}:::decision
    
    RowLoop --> IsLong{8. 是否為做多訊號?<br/>陣型標籤含 LONG<br/>或買分 >= 2}:::decision
    
    IsLong -- 是 --> ExtractX[9. 萃取特徵 Features X<br/>RSI, MACD, BB_Width,<br/>Volume_Ratio, ADX,<br/>法人買賣超]:::process
    IsLong -- 否 --> Skip[跳過該日]:::process
    
    ExtractX --> PeekFuture[10. 偷看未來 5 天<br/>計算未來最高價]:::process
    PeekFuture --> DefineY{11. 是否獲利 > 停損點?}:::decision
    DefineY -- 是 --> Label1[標記 Label_Y = 1<br/>好球/成功樣本]:::process
    DefineY -- 否 --> Label0[標記 Label_Y = 0<br/>騙線/失敗樣本]:::process
    
    Label1 --> Collect[加入樣本池]:::process
    Label0 --> Collect
    
    Collect --> RowLoop
    Skip --> RowLoop
    
    RowLoop -- 結束 --> TickerLoop
    TickerLoop -- 結束 --> Save[12. 儲存 CSV<br/>ml_training_data.csv]:::io
"""

# ==========================================
# 📊 圖表 2：雷達兵的加工流程 (screening.py)
# ==========================================
mermaid_screening = """
graph TD
    classDef start fill:#dcedc8,stroke:#689f38,stroke-width:2px;
    classDef process fill:#fff9c4,stroke:#fbc02d,stroke-width:2px;
    classDef io fill:#e1f5fe,stroke:#01579b,stroke-width:2px;
    classDef highlight fill:#f8bbd0,stroke:#c2185b,stroke-width:2px;

    Input([原始數據輸入]):::start --> Clean[1. 疫苗除蟲<br/>剔除無效收盤價與空值]:::process
    Clean --> Indicators[2. 基礎指標計算<br/>RSI, MACD, BBands,<br/>BBI, DMI, ATR]:::process
    Indicators --> Div[3. 背離偵測<br/>價格 vs 指標/籌碼<br/>滾動視窗 20 天]:::process
    
    Div --> Regime[4. 市場環境分類<br/>趨勢多/趨勢空/盤整]:::process
    Regime --> Scores[5. 指標計分引擎<br/>計算買/賣 0-10 分]:::process
    
    Scores --> Setup[6. 陣型辨識<br/>突破/均值/籌碼/趨勢]:::highlight
    Setup --> Backtest[7. 內建回測引擎<br/>模擬 2 年實戰紀錄]:::process
    
    subgraph Backtest_Engine [回測內部細節]
        BT_Loop[逐日執行] --> Risk[風控計算: 停損/停利/風報比]
        Risk --> Position[倉位控管: 股數計算]
        Position --> Exit[移動停利與追蹤止損]
        Exit --> SQL[寫入 SQL 回測歷史紀錄]
    end
    
    Backtest --> Backtest_Engine
    Backtest_Engine --> Stats[8. 性能指標萃取<br/>勝率, 期望值 EV,<br/>累計報酬率, 交易次數]:::highlight
    Stats --> Tagging[9. 陣型標籤化<br/>SNIPER 狙擊 / 一般陣型]:::process
    Tagging --> Return[10. 輸出診斷字典<br/>供實戰機台與畫圖使用]:::io
"""

# ==========================================
# 🌐 建立 HTML 模板 (包含兩個圖表區塊)
# ==========================================
html_template = f"""
<!DOCTYPE html>
<html lang="zh-TW">
<head>
    <meta charset="UTF-8">
    <title>模組數據萃取流程圖</title>
    <script src="https://cdn.jsdelivr.net/npm/mermaid/dist/mermaid.min.js"></script>
    <script>
        mermaid.initialize({{ 
            startOnLoad: true, 
            theme: 'default',
            securityLevel: 'loose',
            flowchart: {{ useMaxWidth: false, htmlLabels: true }}
        }});
    </script>
    <style>
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; 
            background-color: #f0f2f5; 
            margin: 0; padding: 40px; 
            display: flex; flex-direction: column; align-items: center; 
        }}
        h1 {{ color: #1a237e; margin-bottom: 10px; }}
        h2 {{ color: #004d40; margin-top: 40px; border-bottom: 3px solid #004d40; padding-bottom: 10px; width: 100%; max-width: 1200px; }}
        .card {{ 
            background: white; 
            padding: 30px; 
            border-radius: 12px; 
            box-shadow: 0 8px 20px rgba(0,0,0,0.1); 
            margin-bottom: 40px;
            width: 100%; max-width: 1200px;
            overflow-x: auto;
        }}
        .mermaid {{ display: flex; justify-content: center; }}
    </style>
</head>
<body>
    <h1>⚙️ 核心模組數據萃取流程解析</h1>

    <h2>🏭 1. 歷史戰鬥特徵萃取 (ml_data_generator.py)</h2>
    <div class="card">
        <div class="mermaid">
        {mermaid_ml_data}
        </div>
    </div>

    <h2>📡 2. 多層級指標與陣型加工 (screening.py)</h2>
    <div class="card">
        <div class="mermaid">
        {mermaid_screening}
        </div>
    </div>
</body>
</html>
"""

# ==========================================
# 💾 儲存並透過瀏覽器開啟
# ==========================================
file_name = "extraction_flow_web.html"
with open(file_name, "w", encoding="utf-8") as f:
    f.write(html_template)

print(f"✅ 報告長官！【模組數據萃取流程圖】網頁已生成完畢！")
webbrowser.open("file://" + os.path.realpath(file_name))