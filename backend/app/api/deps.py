from fastapi import Depends, Header, HTTPException
from app.core.database import get_pool
from app.services.security import decode_token


async def current_user(authorization: str | None = Header(None), pool=Depends(get_pool)):
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(401, "Authentication required")
    payload = decode_token(authorization.split(" ", 1)[1], expected_type="access")
    if not payload:
        raise HTTPException(401, "Invalid or expired session")
    async with pool.acquire() as conn:
        user = await conn.fetchrow("select id, email, created_at, last_login_at from users where id = $1", int(payload["sub"]))
    if not user:
        raise HTTPException(401, "User not found")
    return dict(user)
