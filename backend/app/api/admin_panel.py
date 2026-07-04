"""
Admin Panel API — user approval workflow, user management, dashboard stats.

Fully separate from:
  - app.api.admin        (engine/data admin, protected by X-Admin-Secret header)
  - app.api.auth          (normal user registration/login)

Admin accounts live in their own `admins` table and are never mixed with `users`.
All routes below (except /login) require a valid admin bearer token.

POST /api/admin/panel/login                          — admin login
GET  /api/admin/panel/dashboard                       — summary cards
GET  /api/admin/panel/pending-users                   — list pending registrations
POST /api/admin/panel/pending-users/{id}/approve      — approve → moves into `users`
POST /api/admin/panel/pending-users/{id}/reject       — reject
DELETE /api/admin/panel/pending-users/{id}            — delete a pending registration
GET  /api/admin/panel/users                           — list approved users
POST /api/admin/panel/users/{id}/enable               — enable an account
POST /api/admin/panel/users/{id}/disable              — disable an account
DELETE /api/admin/panel/users/{id}                    — delete a user
POST /api/admin/panel/users/{id}/reset-password       — admin sets a temporary password
"""
import logging
import secrets
import uuid

from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import current_admin
from app.core.database import get_pool
from app.models.auth import AdminLoginRequest
from app.services.security import create_token, hash_password, verify_password

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/login", summary="Admin login")
async def admin_login(payload: AdminLoginRequest, pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        admin = await conn.fetchrow(
            "select id, username, password_hash from admins where username = $1", payload.username,
        )
    if not admin or not verify_password(payload.password, admin["password_hash"]):
        raise HTTPException(401, "Invalid admin credentials")
    token = create_token(str(admin["id"]), token_type="admin", expires_minutes=60 * 12)
    return {"access_token": token, "token_type": "bearer", "admin": {"id": admin["id"], "username": admin["username"]}}


@router.get("/dashboard", summary="Dashboard summary cards", dependencies=[Depends(current_admin)])
async def dashboard(pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        stats = await conn.fetchrow(
            """
            select
                (select count(*) from users)                                  as total_users,
                (select count(*) from pending_users where status = 'pending')  as pending_users,
                (select count(*) from users where active = true)               as active_users,
                (select count(*) from users where active = false)              as disabled_users
            """
        )
    return dict(stats)


# ── Pending users ──────────────────────────────────────────────────────
@router.get("/pending-users", summary="List pending registrations", dependencies=[Depends(current_admin)])
async def list_pending_users(pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "select id, full_name, email, phone, status, created_at "
            "from pending_users order by created_at desc"
        )
    return {"count": len(rows), "data": [dict(r) for r in rows]}


@router.post("/pending-users/{pending_id}/approve", summary="Approve a pending registration")
async def approve_pending_user(pending_id: int, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        async with conn.transaction():
            pending = await conn.fetchrow(
                "select id, full_name, email, phone, password_hash from pending_users "
                "where id = $1 for update",
                pending_id,
            )
            if not pending:
                raise HTTPException(404, "Pending registration not found")

            dup = await conn.fetchval(
                "select id from users where lower(email) = lower($1) or phone = $2",
                pending["email"], pending["phone"],
            )
            if dup:
                raise HTTPException(409, "A user with this email or phone already exists")

            user = await conn.fetchrow(
                """
                insert into users (full_name, email, phone, password_hash, active, approved_at, created_at)
                values ($1, $2, $3, $4, true, now(), now())
                returning id, full_name, email, phone, active, created_at, approved_at
                """,
                pending["full_name"], pending["email"], pending["phone"], pending["password_hash"],
            )
            await conn.execute("delete from pending_users where id = $1", pending_id)
    logger.info("Admin %s approved user %s", admin["username"], user["email"])
    return {"message": "User approved", "user": dict(user)}


@router.post("/pending-users/{pending_id}/reject", summary="Reject a pending registration")
async def reject_pending_user(pending_id: int, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        result = await conn.execute(
            "update pending_users set status = 'rejected' where id = $1", pending_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(404, "Pending registration not found")
    return {"message": "Registration rejected"}


@router.delete("/pending-users/{pending_id}", summary="Delete a pending registration")
async def delete_pending_user(pending_id: int, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        result = await conn.execute("delete from pending_users where id = $1", pending_id)
    if result == "DELETE 0":
        raise HTTPException(404, "Pending registration not found")
    return {"message": "Pending registration deleted"}


# ── Existing users ──────────────────────────────────────────────────────
@router.get("/users", summary="List approved users", dependencies=[Depends(current_admin)])
async def list_users(pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "select id, full_name, email, phone, created_at, last_login_at, active "
            "from users order by created_at desc"
        )
    return {"count": len(rows), "data": [dict(r) for r in rows]}


@router.post("/users/{user_id}/enable", summary="Enable a user account")
async def enable_user(user_id: uuid.UUID, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        result = await conn.execute("update users set active = true where id = $1", user_id)
    if result == "UPDATE 0":
        raise HTTPException(404, "User not found")
    return {"message": "User enabled"}


@router.post("/users/{user_id}/disable", summary="Disable a user account")
async def disable_user(user_id: uuid.UUID, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        result = await conn.execute("update users set active = false where id = $1", user_id)
    if result == "UPDATE 0":
        raise HTTPException(404, "User not found")
    return {"message": "User disabled"}


@router.delete("/users/{user_id}", summary="Delete a user account")
async def delete_user(user_id: uuid.UUID, admin=Depends(current_admin), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        result = await conn.execute("delete from users where id = $1", user_id)
    if result == "DELETE 0":
        raise HTTPException(404, "User not found")
    return {"message": "User deleted"}


@router.post("/users/{user_id}/reset-password", summary="Admin resets a user's password to a temporary one")
async def reset_password(user_id: uuid.UUID, admin=Depends(current_admin), pool=Depends(get_pool)):
    temp_password = secrets.token_urlsafe(9)
    async with pool.acquire() as conn:
        result = await conn.execute(
            "update users set password_hash = $1 where id = $2", hash_password(temp_password), user_id,
        )
    if result == "UPDATE 0":
        raise HTTPException(404, "User not found")
    return {
        "message": "Password reset. Share this temporary password with the user securely; "
                    "they should change it after logging in.",
        "temporary_password": temp_password,
    }
