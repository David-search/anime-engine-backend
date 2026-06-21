"""Persistent browser session for Cloudflare-walled providers (allanime).

Cloudflare binds cf_clearance to the solving browser's TLS fingerprint, so
reusing the cookie from a plain HTTP client (even curl_cffi) is unreliable. The
robust approach: keep ONE real (headful) Chromium alive and make the JSON API
calls *through it* via page navigation. It solves the Turnstile once (you click
"I am human"), then every subsequent call reuses that cleared session — fast and
fingerprint-consistent — until Cloudflare re-challenges.

A single Chromium window stays open while the backend runs. That's the price of
Cloudflare; for a local pet project it's fine.
"""
from __future__ import annotations

import asyncio
import json
from pathlib import Path

from .providers.base import ProviderBlocked

_BACKEND_DIR = Path(__file__).resolve().parent.parent
PROFILE_DIR = _BACKEND_DIR / ".cf_profile"


class BrowserManager:
    def __init__(self) -> None:
        self._pw = None
        self._ctx = None
        self._page = None
        self._lock = asyncio.Lock()

    async def _ensure(self) -> None:
        if self._page is not None:
            return
        from playwright.async_api import async_playwright

        PROFILE_DIR.mkdir(exist_ok=True)
        self._pw = await async_playwright().start()
        self._ctx = await self._pw.chromium.launch_persistent_context(
            str(PROFILE_DIR),
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            locale="en-US",
            viewport={"width": 1200, "height": 800},
        )
        await self._ctx.add_init_script(
            "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
        )
        self._page = self._ctx.pages[0] if self._ctx.pages else await self._ctx.new_page()

    async def fetch_json(self, url: str, wait_seconds: int = 75) -> dict:
        """Navigate to `url` and return the JSON body. If Cloudflare challenges,
        the open window lets you click 'I am human'; we poll until JSON appears."""
        async with self._lock:
            await self._ensure()
            page = self._page
            await page.goto(url, wait_until="domcontentloaded", timeout=90000)
            for _ in range(wait_seconds):
                txt = (await page.evaluate("() => document.body ? document.body.innerText : ''")).strip()
                if txt.startswith("{") or txt.startswith("["):
                    try:
                        return json.loads(txt)
                    except ValueError:
                        pass
                # Don't reload — it re-triggers the Turnstile loop. Just wait it out.
                await page.wait_for_timeout(1000)
            raise ProviderBlocked(
                "Cloudflare challenge not cleared — click 'I am human' in the open browser window, "
                "or run: ./.venv/bin/python -m app.cf_refresh"
            )

    async def close(self) -> None:
        try:
            if self._ctx:
                await self._ctx.close()
            if self._pw:
                await self._pw.stop()
        except Exception:
            pass
        self._pw = self._ctx = self._page = None


manager = BrowserManager()
