import requests
from datetime import datetime


class AlertManager:
    def __init__(self, line_bot_token="", line_user_id="", is_test_mode=True):
        self.line_bot_token = line_bot_token
        self.line_user_id = line_user_id
        self.is_test_mode = is_test_mode

    def send_line_message(self, message: str):
        if self.is_test_mode:
            print("\n🔇 [測試模式] 攔截告警推播")
            print("-" * 40)
            print(message)
            print("-" * 40)
            return True

        if not self.line_bot_token or not self.line_user_id:
            print("⚠️ LINE token / user id 未設定，無法推播")
            return False

        url = "https://api.line.me/v2/bot/message/push"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.line_bot_token}",
        }
        payload = {
            "to": self.line_user_id,
            "messages": [{"type": "text", "text": message}],
        }

        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=20)
            if resp.status_code == 200:
                print("📲 LINE 告警已送出")
                return True
            print(f"⚠️ LINE 推播失敗: {resp.status_code} | {resp.text}")
            return False
        except Exception as e:
            print(f"⚠️ LINE 推播例外: {e}")
            return False

    def format_guard_alert(self, payload: dict) -> str:
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        overall = payload.get("overall", "UNKNOWN")
        block_new_positions = payload.get("block_new_positions", False)
        alerts = payload.get("alerts", [])
        desk = payload.get("decision_desk", {})
        trades = payload.get("recent_trades", {})
        models = payload.get("model_status", {})

        lines = [
            "🚨【交易系統守門員告警】",
            f"時間：{now_str}",
            f"總體狀態：{overall}",
            f"阻止建倉：{'是' if block_new_positions else '否'}",
            "-" * 20,
            f"模型狀態：{models.get('health', 'UNKNOWN')} ({models.get('ok_count', 0)}/4)",
            f"決策桌：{desk.get('health', 'UNKNOWN')} | rows={desk.get('rows', 0)} | avgEV={desk.get('avg_realized_ev', 0):.3f}",
            f"近期戰績：{trades.get('health', 'UNKNOWN')} | win={trades.get('win_rate', 0):.2%} | PF={trades.get('profit_factor', 0):.2f}",
            "-" * 20,
        ]

        if alerts:
            lines.extend([f"• {msg}" for msg in alerts[:8]])
        else:
            lines.append("• 無異常")

        return "\n".join(lines)

    def maybe_send_guard_alert(self, payload: dict):
        overall = payload.get("overall", "OK")
        if overall not in ("WARN", "BLOCK"):
            print("✅ 守門員狀態正常，略過告警推播")
            return False

        message = self.format_guard_alert(payload)
        return self.send_line_message(message)
