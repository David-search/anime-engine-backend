"""Auth endpoints: email/password (+ Google id-token verify). Users in Mongo."""
import datetime

import httpx
from fastapi import APIRouter, Body, Depends, HTTPException

from ..auth import current_user, hash_pw, make_token, public_user, verify_pw
from ..config import settings
from ..db import get_db

router = APIRouter(prefix="/auth")


def _now() -> str:
    return datetime.datetime.utcnow().isoformat()


@router.post("/register")
async def register(email: str = Body(...), password: str = Body(...), username: str = Body("")):
    db = get_db()
    if db is None:
        raise HTTPException(503, "auth not configured")
    email = email.strip().lower()
    if not email or len(password) < 6:
        raise HTTPException(400, "email required, password >= 6 chars")
    if await db.users.find_one({"email": email}):
        raise HTTPException(409, "email already registered")
    doc = {"email": email, "username": (username or email.split("@")[0]),
           "password_hash": hash_pw(password), "provider": "local", "created": _now()}
    res = await db.users.insert_one(doc)
    doc["_id"] = res.inserted_id
    return {"token": make_token(res.inserted_id, email), "user": public_user(doc)}


@router.post("/login")
async def login(email: str = Body(...), password: str = Body(...)):
    db = get_db()
    if db is None:
        raise HTTPException(503, "auth not configured")
    email = email.strip().lower()
    user = await db.users.find_one({"email": email})
    if not user or not verify_pw(password, user.get("password_hash", "")):
        raise HTTPException(401, "invalid credentials")
    return {"token": make_token(user["_id"], email), "user": public_user(user)}


@router.post("/google")
async def google(credential: str = Body(..., embed=True)):
    if not settings.GOOGLE_CLIENT_ID:
        raise HTTPException(501, "Google sign-in not configured (set GOOGLE_CLIENT_ID)")
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.get("https://oauth2.googleapis.com/tokeninfo", params={"id_token": credential})
    if r.status_code != 200:
        raise HTTPException(401, "invalid google token")
    info = r.json()
    if info.get("aud") != settings.GOOGLE_CLIENT_ID:
        raise HTTPException(401, "wrong audience")
    db = get_db()
    email = (info.get("email") or "").lower()
    user = await db.users.find_one({"email": email})
    if not user:
        doc = {"email": email, "username": info.get("name") or email.split("@")[0],
               "provider": "google", "created": _now()}
        res = await db.users.insert_one(doc)
        doc["_id"] = res.inserted_id
        user = doc
    return {"token": make_token(user["_id"], email), "user": public_user(user)}


@router.get("/me")
async def me(user=Depends(current_user)):
    return public_user(user)
