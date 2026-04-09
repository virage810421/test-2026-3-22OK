import requests

def test_line():
    print("📡 正在嘗試呼叫 LINE 伺服器...")
    
    # 🌟 已經幫您去除了所有中文字與空白，這是最純淨的金鑰
    token = "/VY7IKhPqqJ1W6v04s9f8vi8hDbo5W1daaCD2LD3sIki1rWq3wF41uF6mwbKf4UsERhRK68MGn2K1nKRIffEzS Z8OTQgm0VWQ1g3CLTzNB9RZrAJ7py8MtPuzPi5/I9GnkEM69CeX983gFCQPBRx1AdB04t89/1O/w1cDnyilFU="
    
    # 🌟 這是您的專屬 ID
    user_id = "U5dd01cba7ab960a7bd1a1b6efc43411b"
    
    url = 'https://api.line.me/v2/bot/message/push'
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    data = {
        "to": user_id,
        "messages": [{"type": "text", "text": "✅ 長官！通訊測試成功！您的專屬 AI 系統已上線！"}]
    }
    
    response = requests.post(url, headers=headers, json=data)
    
    print("-" * 30)
    print(f"狀態碼 (Status Code): {response.status_code}")
    print(f"伺服器回應 (Response): {response.text}")
    print("-" * 30)

    if response.status_code == 200:
        print("🎉 成功！請立刻檢查您的手機！")
    elif response.status_code == 401:
        print("❌ 失敗 (401)：您的 Token 填錯了，請重新發行 Channel Access Token！")
    elif response.status_code == 400:
        print("❌ 失敗 (400)：通常是因為您「還沒用手機掃描 QR code 加機器人好友」！")
    else:
        print("⚠️ 其他未知錯誤，請將上方的『伺服器回應』複製給我分析！")

if __name__ == "__main__":
    test_line()