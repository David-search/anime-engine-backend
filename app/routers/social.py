"""Comments, likes, watch-history, watchlist ("My List"), and user-built lists
(public rateable "tops" + private "collections") -> MongoDB (authed where it
mutates)."""
import datetime

from bson import ObjectId
from fastapi import APIRouter, Body, Depends, HTTPException, Query

from ..auth import current_user, optional_user
from ..db import get_db

router = APIRouter()

MAX_LISTS = 60          # per user
MAX_ITEMS = 200         # per list


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


def _oid(s: str):
    try:
        return ObjectId(s)
    except Exception:  # noqa: BLE001
        return None


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


# ─────────────────────────── Watchlist ("My List") ───────────────────────────
# A single flat bookmark list per user. Denormalised title/poster so the My List
# page renders without N catalog round-trips.

@router.get("/watchlist")
async def get_watchlist(user=Depends(current_user)):
    db = get_db()
    if db is None:
        return {"results": []}
    cur = db.watchlist.find({"user_id": user["_id"]}).sort("added", -1).limit(500)
    return {"results": [
        {"anime_id": w["anime_id"], "title": w.get("title"), "poster": w.get("poster"), "added": w.get("added")}
        async for w in cur
    ]}


@router.get("/watchlist/contains")
async def watchlist_contains(anime_id: str, user=Depends(optional_user)):
    db = get_db()
    if db is None or not user:
        return {"in_list": False}
    hit = await db.watchlist.find_one({"user_id": user["_id"], "anime_id": str(anime_id)})
    return {"in_list": hit is not None}


@router.post("/watchlist")
async def add_to_watchlist(anime_id: str = Body(...), title: str = Body(""), poster: str = Body(""),
                           user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    await db.watchlist.update_one(
        {"user_id": user["_id"], "anime_id": str(anime_id)},
        {"$set": {"title": title, "poster": poster}, "$setOnInsert": {"added": _now()}},
        upsert=True,
    )
    return {"ok": True, "in_list": True}


@router.delete("/watchlist")
async def remove_from_watchlist(anime_id: str = Query(...), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    res = await db.watchlist.delete_one({"user_id": user["_id"], "anime_id": str(anime_id)})
    return {"ok": True, "in_list": False, "deleted": res.deleted_count}


# ──────────────── User lists: public "tops" + private "collections" ───────────
# One model, two kinds. `kind="top"` defaults public + ordered + rateable;
# `kind="collection"` defaults private. Items are denormalised {anime_id,title,poster}.

def _list_out(doc: dict, user: dict | None, my_rating: int | None = None) -> dict:
    owner = bool(user) and doc.get("user_id") == user["_id"]
    return {
        "id": str(doc["_id"]),
        "title": doc.get("title"),
        "kind": doc.get("kind", "collection"),
        "public": bool(doc.get("public")),
        "username": doc.get("username"),
        "owner": owner,
        "items": doc.get("items", []),
        "count": len(doc.get("items", [])),
        "ratingAvg": round(doc.get("ratingAvg", 0.0), 2),
        "ratingCount": doc.get("ratingCount", 0),
        "myRating": my_rating,
        "created": doc.get("created"),
        "updated": doc.get("updated"),
    }


async def _own_list(db, list_id: str, user: dict):
    oid = _oid(list_id)
    if oid is None:
        raise HTTPException(404, "not found")
    doc = await db.lists.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "not found")
    if doc.get("user_id") != user["_id"]:
        raise HTTPException(403, "not your list")
    return doc


@router.get("/lists")
async def my_lists(kind: str | None = None, user=Depends(current_user)):
    db = get_db()
    if db is None:
        return {"results": []}
    q: dict = {"user_id": user["_id"]}
    if kind in ("top", "collection"):
        q["kind"] = kind
    cur = db.lists.find(q).sort("updated", -1).limit(MAX_LISTS)
    return {"results": [_list_out(d, user) async for d in cur]}


@router.get("/lists/public")
async def public_lists(kind: str = "top", limit: int = 40, user=Depends(optional_user)):
    """Discover public tops (and public collections), ranked by rating then recency."""
    db = get_db()
    if db is None:
        return {"results": []}
    q: dict = {"public": True}
    if kind in ("top", "collection"):
        q["kind"] = kind
    cur = db.lists.find(q).sort([("ratingAvg", -1), ("ratingCount", -1), ("updated", -1)]).limit(min(limit, 100))
    return {"results": [_list_out(d, user) async for d in cur]}


@router.get("/lists/{list_id}")
async def get_list(list_id: str, user=Depends(optional_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    oid = _oid(list_id)
    if oid is None:
        raise HTTPException(404, "not found")
    doc = await db.lists.find_one({"_id": oid})
    if not doc:
        raise HTTPException(404, "not found")
    owner = bool(user) and doc.get("user_id") == user["_id"]
    if not doc.get("public") and not owner:
        raise HTTPException(404, "not found")  # private lists are invisible to others
    my_rating = None
    if user:
        r = await db.list_ratings.find_one({"list_id": oid, "user_id": user["_id"]})
        my_rating = r.get("value") if r else None
    return _list_out(doc, user, my_rating)


@router.post("/lists")
async def create_list(title: str = Body(...), kind: str = Body("collection"),
                      public: bool | None = Body(None), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    title = (title or "").strip()[:120]
    if not title:
        raise HTTPException(400, "title required")
    kind = kind if kind in ("top", "collection") else "collection"
    if await db.lists.count_documents({"user_id": user["_id"]}) >= MAX_LISTS:
        raise HTTPException(409, f"list limit reached ({MAX_LISTS})")
    is_public = (kind == "top") if public is None else bool(public)
    doc = {
        "user_id": user["_id"], "username": user.get("username"),
        "title": title, "kind": kind, "public": is_public, "items": [],
        "ratingAvg": 0.0, "ratingCount": 0, "created": _now(), "updated": _now(),
    }
    res = await db.lists.insert_one(doc)
    doc["_id"] = res.inserted_id
    return _list_out(doc, user)


@router.patch("/lists/{list_id}")
async def update_list(list_id: str, title: str | None = Body(None),
                      public: bool | None = Body(None), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    await _own_list(db, list_id, user)
    fields: dict = {"updated": _now()}
    if title is not None and title.strip():
        fields["title"] = title.strip()[:120]
    if public is not None:
        fields["public"] = bool(public)
    await db.lists.update_one({"_id": _oid(list_id)}, {"$set": fields})
    doc = await db.lists.find_one({"_id": _oid(list_id)})
    return _list_out(doc, user)


@router.delete("/lists/{list_id}")
async def delete_list(list_id: str, user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    await _own_list(db, list_id, user)
    await db.lists.delete_one({"_id": _oid(list_id)})
    await db.list_ratings.delete_many({"list_id": _oid(list_id)})
    return {"ok": True}


@router.post("/lists/{list_id}/items")
async def add_item(list_id: str, anime_id: str = Body(...), title: str = Body(""),
                   poster: str = Body(""), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    doc = await _own_list(db, list_id, user)
    items = doc.get("items", [])
    if any(str(it.get("anime_id")) == str(anime_id) for it in items):
        return _list_out(doc, user)  # already present — no dup
    if len(items) >= MAX_ITEMS:
        raise HTTPException(409, f"item limit reached ({MAX_ITEMS})")
    item = {"anime_id": str(anime_id), "title": title, "poster": poster}
    await db.lists.update_one({"_id": doc["_id"]},
                              {"$push": {"items": item}, "$set": {"updated": _now()}})
    doc = await db.lists.find_one({"_id": doc["_id"]})
    return _list_out(doc, user)


@router.delete("/lists/{list_id}/items/{anime_id}")
async def remove_item(list_id: str, anime_id: str, user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    doc = await _own_list(db, list_id, user)
    await db.lists.update_one({"_id": doc["_id"]},
                              {"$pull": {"items": {"anime_id": str(anime_id)}}, "$set": {"updated": _now()}})
    doc = await db.lists.find_one({"_id": doc["_id"]})
    return _list_out(doc, user)


@router.post("/lists/{list_id}/reorder")
async def reorder_items(list_id: str, anime_ids: list[str] = Body(..., embed=True),
                        user=Depends(current_user)):
    """Re-sort items to match the given anime_id order (for ranking a 'top')."""
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    doc = await _own_list(db, list_id, user)
    by_id = {str(it.get("anime_id")): it for it in doc.get("items", [])}
    ordered = [by_id[i] for i in (str(x) for x in anime_ids) if i in by_id]
    # keep any items the client didn't mention (defensive) appended in old order
    seen = {str(it.get("anime_id")) for it in ordered}
    ordered += [it for it in doc.get("items", []) if str(it.get("anime_id")) not in seen]
    await db.lists.update_one({"_id": doc["_id"]}, {"$set": {"items": ordered, "updated": _now()}})
    doc = await db.lists.find_one({"_id": doc["_id"]})
    return _list_out(doc, user)


@router.post("/lists/{list_id}/rate")
async def rate_list(list_id: str, value: int = Body(..., embed=True), user=Depends(current_user)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "not configured")
    oid = _oid(list_id)
    if oid is None:
        raise HTTPException(404, "not found")
    doc = await db.lists.find_one({"_id": oid})
    if not doc or not doc.get("public"):
        raise HTTPException(404, "not found")  # can only rate public lists
    if doc.get("user_id") == user["_id"]:
        raise HTTPException(400, "can't rate your own list")
    value = max(1, min(5, int(value)))
    await db.list_ratings.update_one(
        {"list_id": oid, "user_id": user["_id"]},
        {"$set": {"value": value, "updated": _now()}},
        upsert=True,
    )
    # recompute aggregate cheaply and cache on the list doc for sorting
    agg = db.list_ratings.aggregate([
        {"$match": {"list_id": oid}},
        {"$group": {"_id": "$list_id", "avg": {"$avg": "$value"}, "count": {"$sum": 1}}},
    ])
    stats = await agg.to_list(1)
    avg = stats[0]["avg"] if stats else 0.0
    count = stats[0]["count"] if stats else 0
    await db.lists.update_one({"_id": oid}, {"$set": {"ratingAvg": avg, "ratingCount": count}})
    return {"ratingAvg": round(avg, 2), "ratingCount": count, "myRating": value}
