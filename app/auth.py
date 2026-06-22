"""Auth helpers: bcrypt passwords + JWT. Users live in Mongo `users`."""
from __future__ import annotations

import datetime

import bcrypt
import jwt
from bson import ObjectId
from fastapi import Header, HTTPException

from .config import settings
from .db import get_db


def hash_pw(pw: str) -> str:
    return bcrypt.hashpw(pw.encode()[:72], bcrypt.gensalt()).decode()


def verify_pw(pw: str, h: str) -> bool:
    try:
        return bcrypt.checkpw(pw.encode()[:72], h.encode())
    except Exception:
        return False


def make_token(user_id, email: str) -> str:
    payload = {
        "sub": str(user_id),
        "email": email,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(days=settings.JWT_TTL_DAYS),
    }
    return jwt.encode(payload, settings.JWT_SECRET, algorithm="HS256")


def decode_token(token: str) -> dict | None:
    try:
        return jwt.decode(token, settings.JWT_SECRET, algorithms=["HS256"])
    except Exception:
        return None


def public_user(user: dict) -> dict:
    return {"id": str(user["_id"]), "email": user.get("email"), "username": user.get("username")}


async def current_user(authorization: str = Header(default="")) -> dict:
    db = get_db()
    if db is None:
        raise HTTPException(503, "auth not configured")
    if not authorization.startswith("Bearer "):
        raise HTTPException(401, "missing token")
    data = decode_token(authorization[7:])
    if not data:
        raise HTTPException(401, "invalid token")
    try:
        user = await db.users.find_one({"_id": ObjectId(data["sub"])})
    except Exception:
        user = None
    if not user:
        raise HTTPException(401, "user not found")
    return user


async def optional_user(authorization: str = Header(default="")) -> dict | None:
    try:
        return await current_user(authorization)
    except Exception:
        return None
