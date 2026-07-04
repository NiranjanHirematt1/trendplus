"""
User Authentication API — manual Admin Approval workflow.

POST /api/auth/register         — submit a registration request (goes to pending_users)
POST /api/auth/login             — log in with email + password (approved users only)
POST /api/auth/change-password   — authenticated users change their own password
GET  /api/auth/me                — current logged-in user's profile

No email verification, OTP, or magic links. New accounts require admin approval
(see app.api.admin_panel) before they can log in.
"""
import logging

from fastapi import APIRouter, Depends, HTTPException

from app.api.deps import current_user
from app.core.database import get_pool
from app.models.auth import ChangePasswordRequest, LoginRequest, RegisterRequest
from app.services.security import create_token, hash_password, verify_password

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/register", summary="Submit a registration request for admin approval")
async def register(payload: RegisterRequest, pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing_user = await conn.fetchval(
                "select id from users where lower(email) = lower($1) or phone = $2",
                payload.email, payload.phone,
            )
            if existing_user:
                raise HTTPException(409, "An account with this email or phone already exists")

            existing_pending = await conn.fetchval(
                "select id from pending_users where lower(email) = lower($1) or phone = $2",
                payload.email, payload.phone,
            )
            if existing_pending:
                raise HTTPException(409, "A registration with this email or phone is already awaiting approval")

            await conn.execute(
                """
                insert into pending_users (full_name, email, phone, password_hash, status)
                values ($1, $2, $3, $4, 'pending')
                """,
                payload.full_name, payload.email, payload.phone, hash_password(payload.password),
            )
    logger.info("New registration pending approval: %s", payload.email)
    return {"message": "Registration submitted successfully. Your account is awaiting admin approval."}


@router.post("/login", summary="Log in with email and password")
async def login(payload: LoginRequest, pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "select id, full_name, email, phone, password_hash, active, created_at, approved_at, last_login_at "
            "from users where lower(email) = lower($1)",
            payload.email,
        )
        if not user:
            pending = await conn.fetchval(
                "select status from pending_users where lower(email) = lower($1)",
                payload.email,
            )
            if pending == "pending":
                raise HTTPException(403, "Your account is still awaiting admin approval")
            if pending == "rejected":
                raise HTTPException(403, "Your registration was rejected. Contact an administrator.")
            raise HTTPException(401, "Invalid email or password")

        if not verify_password(payload.password, user["password_hash"]):
            raise HTTPException(401, "Invalid email or password")

        if not user["active"]:
            raise HTTPException(403, "Your account has been disabled. Contact an administrator.")

        updated = await conn.fetchrow(
            "update users set last_login_at = now() where id = $1 "
            "returning id, full_name, email, phone, active, created_at, approved_at, last_login_at",
            user["id"],
        )
    token = create_token(str(updated["id"]), token_type="access")
    return {"access_token": token, "token_type": "bearer", "user": dict(updated)}


@router.get("/me", summary="Current user's profile")
async def me(user=Depends(current_user)):
    return {"user": user}


@router.post("/change-password", summary="Change your own password")
async def change_password(payload: ChangePasswordRequest, user=Depends(current_user), pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("select password_hash from users where id = $1", user["id"])
        if not row or not verify_password(payload.current_password, row["password_hash"]):
            raise HTTPException(401, "Current password is incorrect")
        await conn.execute(
            "update users set password_hash = $1 where id = $2",
            hash_password(payload.new_password), user["id"],
        )
    return {"message": "Password updated successfully"}
