import atexit
import queue
import threading
from datetime import datetime

import requests


class AlertManager:
    def __init__(self, line_bot_token="", line_user_id="", is_test_mode=True, async_mode=True, request_timeout=6):
        self.line_bot_token = line_bot_token
        self.line_user_id = line_user_id
        self.is_test_mode = is_test_mode
        self.async_mode = async_mode
        self.request_timeout = int(request_timeout)
        self._queue: queue.Queue = queue.Queue(maxsize=500)
        self._stop = threading.Event()
        self._worker = None
        if self.async_mode and not self.is_test_mode:
            self._worker = threading.Thread(target=self._run_worker, name="line-alert-worker", daemon=True)
            self._worker.start()
            atexit.register(self.close)

    def _post_line_message(self, message: str) -> bool:
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
        headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.line_bot_token}"}
        payload = {"to": self.line_user_id, "messages": [{"type": "text", "text": message}]}
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=self.request_timeout)
            if resp.status_code == 200:
                print("📲 LINE 告警已送出")
                return True
            print(f"⚠️ LINE 推播失敗: {resp.status_code} | {resp.text}")
            return False
        except Exception as e:
            print(f"⚠️ LINE 推播例外: {e}")
            return False

    def _run_worker(self):
        while not self._stop.is_set() or not self._queue.empty():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                self._queue.task_done()
                break
            try:
                self._post_line_message(item)
            finally:
                self._queue.task_done()

    def send_line_message(self, message: str):
        if self.async_mode and not self.is_test_mode:
            try:
                self._queue.put_nowait(message)
                return True
            except queue.Full:
                print("⚠️ LINE 告警佇列已滿，退回同步送出")
                return self._post_line_message(message)
        return self._post_line_message(message)

    def flush(self):
        if self.async_mode and not self.is_test_mode:
            try:
                self._queue.join()
            except Exception:
                pass

    def close(self):
        if self.async_mode and not self.is_test_mode:
            self._stop.set()
            try:
                self._queue.put_nowait(None)
            except Exception:
                pass
            if self._worker and self._worker.is_alive():
                self._worker.join(timeout=2)

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
        return self.send_line_message(self.format_guard_alert(payload))
