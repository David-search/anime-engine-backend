"""Ship backend logs to a Telegram channel (mirrors goongle's telegram_logger).

A logging.Handler that queues records and POSTs them to the Telegram Bot API on a
daemon worker thread — never blocks the request path, never crashes the app on a
Telegram failure. Configured from env (Settings): TELEGRAM_BOT_TOKEN,
TELEGRAM_CHAT_ID, optional TELEGRAM_TOPIC_ID (forum topic thread), SERVER_ID.
No-op when unset. Uses stdlib urllib so it adds no dependency.
"""
import json
import logging
import queue
import threading
import urllib.request
from typing import Optional

from .config import settings

_EMOJI = {"DEBUG": "🔍", "INFO": "ℹ️", "WARNING": "⚠️", "ERROR": "❌", "CRITICAL": "🚨"}


def _esc(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


class TelegramHandler(logging.Handler):
    def __init__(self, bot_token: str, chat_id: str, topic_id: Optional[str] = None,
                 level: int = logging.INFO):
        super().__init__(level)
        self.chat_id = chat_id
        self.topic_id = topic_id
        self.api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        self._queue: queue.Queue = queue.Queue()
        self._worker = threading.Thread(target=self._process_queue, daemon=True)
        self._worker.start()

    def _process_queue(self):
        while True:
            try:
                payload = self._queue.get()
                if payload is None:
                    break
                req = urllib.request.Request(
                    self.api_url, data=json.dumps(payload).encode(),
                    headers={"Content-Type": "application/json"},
                )
                urllib.request.urlopen(req, timeout=5)
            except Exception:  # noqa: BLE001 — telegram failures must never matter
                pass

    def emit(self, record: logging.LogRecord):
        try:
            entry = _esc(self.format(record))[:3800]
            emoji = _EMOJI.get(record.levelname, "📝")
            payload = {
                "chat_id": self.chat_id,
                "text": f"{emoji} <b>{record.levelname}</b>\n\n<code>{entry}</code>",
                "parse_mode": "HTML",
            }
            if self.topic_id:
                payload["message_thread_id"] = int(self.topic_id)
            self._queue.put_nowait(payload)
        except Exception:  # noqa: BLE001 — never let logging crash the app
            pass


def setup_telegram_logging(level: int = logging.INFO) -> Optional[TelegramHandler]:
    token, chat_id = settings.TELEGRAM_BOT_TOKEN, settings.TELEGRAM_CHAT_ID
    if not token or not chat_id:
        print("[telegram] logging not configured (no bot token / chat id)")
        return None
    handler = TelegramHandler(token, chat_id, settings.TELEGRAM_TOPIC_ID or None, level)
    handler.setFormatter(logging.Formatter(
        f"[{settings.SERVER_ID}] %(asctime)s · %(name)s · %(levelname)s · %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    ))
    return handler
