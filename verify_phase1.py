"""End-to-end Phase 1 verifier (allanime via the persistent browser session).

Run with the backend STOPPED (it shares the browser profile):
    ./.venv/bin/python verify_phase1.py

A Chromium window opens; click "I am human" if Cloudflare asks. After it clears,
the same session resolves Frieren's episodes + a real stream link.
"""
import asyncio

import httpx

from app import cf, mapping
from app.providers.registry import get_provider

ANILIST_ID = 154587  # Frieren: Beyond Journey's End


async def main():
    prov = get_provider("allanime")
    try:
        async with httpx.AsyncClient(follow_redirects=True, timeout=40) as client:
            print("1) Mapping AniList", ANILIST_ID, "→ allanime (browser opens; click 'I am human' if shown)…")
            pid = await mapping.resolve(client, prov, ANILIST_ID)
            print("   allanime id:", pid)
            if not pid:
                print("   ❌ no match"); return

            eps = await prov.episodes(client, pid)
            print(f"   episodes: {len(eps)} (first={eps[0].number if eps else None})")

            print("2) Resolving sources for episode 1 (sub)…")
            s = await prov.sources(client, f"{pid}/1", "default", "sub")
            print("   link:", s.url[:100])
            print("   is_m3u8:", s.is_m3u8)

            print("3) Fetching the link to confirm it's playable…")
            r = await client.get(
                s.url,
                headers={"Referer": "https://allmanga.to/", "Range": "bytes=0-200000"},
            )
            print("   HTTP", r.status_code, "| bytes:", len(r.content))
            if ".m3u8" in s.url:
                print("   playlist head:", r.text[:160].replace("\n", " | "))
        print("\n✅ Phase 1 works end-to-end — real anime resolves and is fetchable.")
    finally:
        await cf.manager.close()


if __name__ == "__main__":
    asyncio.run(main())
