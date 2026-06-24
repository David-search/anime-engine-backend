"""MongoDB (motor) accessor. No-op if MONGO_URI is unset (local dev)."""
from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

from .config import settings

_client: AsyncIOMotorClient | None = None


def get_db() -> AsyncIOMotorDatabase | None:
    global _client
    if not settings.MONGO_URI:
        return None
    if _client is None:
        _client = AsyncIOMotorClient(settings.MONGO_URI, serverSelectionTimeoutMS=5000)
    return _client[settings.MONGO_DB]


async def ensure_indexes() -> None:
    db = get_db()
    if db is None:
        return
    await db.users.create_index("email", unique=True)
    await db.anime.create_index("idMal")
    await db.anime.create_index("genres")
    await db.anime.create_index("startDate.year")
    await db.anime.create_index([("popularity", -1)])
    await db.comments.create_index([("anime_id", 1), ("created", -1)])
    await db.likes.create_index([("anime_id", 1), ("user_id", 1)], unique=True)
    await db.history.create_index([("user_id", 1), ("anime_id", 1)], unique=True)
    # watchlist ("My List") — one entry per (user, anime)
    await db.watchlist.create_index([("user_id", 1), ("anime_id", 1)], unique=True)
    await db.watchlist.create_index([("user_id", 1), ("added", -1)])
    # user-built lists: public "tops" (ranked, rateable) + private "collections"
    await db.lists.create_index([("user_id", 1), ("updated", -1)])
    await db.lists.create_index([("kind", 1), ("public", 1), ("ratingAvg", -1)])
    await db.list_ratings.create_index([("list_id", 1), ("user_id", 1)], unique=True)


async def close() -> None:
    global _client
    if _client is not None:
        _client.close()
        _client = None
