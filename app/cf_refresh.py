"""Pre-warm the allanime browser session (solve Cloudflare ahead of time).

Run with the backend STOPPED:  ./.venv/bin/python -m app.cf_refresh
A browser opens; click "I am human" if asked. Once it clears, the persistent
profile remembers it, so the backend usually won't re-challenge for a while.
"""
import asyncio
import json
from urllib.parse import quote

from . import cf
from .providers.allanime import API, SEARCH_Q


def _probe_url() -> str:
    variables = {
        "search": {"allowAdult": False, "allowUnknown": False, "query": "frieren"},
        "limit": 1,
        "page": 1,
        "translationType": "sub",
        "countryOrigin": "ALL",
    }
    return f"{API}?variables={quote(json.dumps(variables))}&query={quote(SEARCH_Q)}"


async def _run() -> None:
    print("Opening browser to clear Cloudflare… (click 'I am human' if shown)")
    try:
        await cf.manager.fetch_json(_probe_url())
        print("✅ Cleared — session is warm. Start the backend now.")
    except Exception as e:  # noqa: BLE001
        print("❌", e)
    finally:
        await cf.manager.close()


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()
