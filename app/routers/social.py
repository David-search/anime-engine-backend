"""Comments, likes, watch-history -> MongoDB (authed where it mutates)."""
import datetime

from fastapi import APIRouter, Body, Depends, HTTPException

from ..auth import current_user, optional_user
from ..db import get_db

router = APIRouter()


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


@router.get("/comments")
async def list_comments(anime_id: str, limit: int = 50):
    db = get_db()
    if db is None:
        return {"results": []}
    cur = db.comments.find({"anime_id": anime_id}).sort("created", -1).limit(min(limit, 100))
    return {"results": [
        {"id": str(c["_id"]), "user": c.get("username"), "text": c["text"],
         "likes": c.get("likes", 0), "created": c["created"]}
        async for c in cur
    ]}


@router.post("/comments")
async def add_comment(anime_id: str = Body(...), text: str = Body(...), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    text = (text or "").strip()[:2000]
    if not text:
        raise HTTPException(400, "empty comment")
    doc = {"anime_id": anime_id, "user_id": user["_id"], "username": user.get("username"),
           "text": text, "likes": 0, "created": _now()}
    res = await db.comments.insert_one(doc)
    return {"id": str(res.inserted_id), "user": doc["username"], "text": text, "likes": 0, "created": doc["created"]}


@router.get("/likes")
async def get_likes(anime_id: str, user=Depends(optional_user)):
    db = get_db()
    if db is None:
        return {"count": 0, "liked": False}
    count = await db.likes.count_documents({"anime_id": anime_id})
    liked = bool(user) and (await db.likes.find_one({"anime_id": anime_id, "user_id": user["_id"]})) is not None
    return {"count": count, "liked": liked}


@router.post("/likes")
async def toggle_like(anime_id: str = Body(..., embed=True), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    existing = await db.likes.find_one({"anime_id": anime_id, "user_id": user["_id"]})
    if existing:
        await db.likes.delete_one({"_id": existing["_id"]})
        liked = False
    else:
        await db.likes.insert_one({"anime_id": anime_id, "user_id": user["_id"]})
        liked = True
    count = await db.likes.count_documents({"anime_id": anime_id})
    return {"count": count, "liked": liked}


@router.post("/history")
async def save_history(anime_id: str = Body(...), ep: int = Body(1), position: float = Body(0.0),
                       user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    await db.history.update_one(
        {"user_id": user["_id"], "anime_id": anime_id},
        {"$set": {"ep": ep, "position": position, "updated": _now()}},
        upsert=True,
    )
    return {"ok": True}


@router.get("/history")
async def get_history(user=Depends(current_user)):
    db = get_db()
    cur = db.history.find({"user_id": user["_id"]}).sort("updated", -1).limit(50)
    return {"results": [
        {"anime_id": h["anime_id"], "ep": h.get("ep"), "position": h.get("position"), "updated": h.get("updated")}
        async for h in cur
    ]}
