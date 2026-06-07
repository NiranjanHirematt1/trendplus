import logging
import secrets
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException

from app.core.database import get_pool
from app.models.auth import LoginRequest, OTPRequest, OTPVerify, RegisterRequest
from app.services.security import create_token, decode_token, hash_password, verify_password

logger = logging.getLogger(__name__)
router = APIRouter()

OTP_TTL_MINUTES = 10


@router.post("/request-otp", summary="Request email OTP for registration")
async def request_otp(payload: OTPRequest, pool=Depends(get_pool)):
    otp = f"{secrets.randbelow(1_000_000):06d}"
    expires_at = datetime.now(timezone.utc) + timedelta(minutes=OTP_TTL_MINUTES)
    async with pool.acquire() as conn:
        existing = await conn.fetchval("select id from users where email = $1", payload.email)
        if existing:
            raise HTTPException(409, "An account with this email already exists")
        await conn.execute(
            """
            insert into user_email_otps (email, otp_code, expires_at)
            values ($1, $2, $3)
            """,
            payload.email, otp, expires_at,
        )
    logger.info("Generated registration OTP for %s", payload.email)
    return {
        "message": "OTP generated. Email delivery can be connected through the notification provider.",
        "expires_in_minutes": OTP_TTL_MINUTES,
        "dev_otp": otp,
    }


@router.post("/verify-otp", summary="Verify registration OTP")
async def verify_otp(payload: OTPVerify, pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            select id, otp_code, expires_at, consumed_at
            from user_email_otps
            where email = $1
            order by created_at desc
            limit 1
            """,
            payload.email,
        )
        now = datetime.now(timezone.utc)
        if not row or row["consumed_at"] or row["otp_code"] != payload.otp or row["expires_at"] < now:
            raise HTTPException(400, "Invalid or expired OTP")
        await conn.execute("update user_email_otps set consumed_at = now() where id = $1", row["id"])
    token = create_token(payload.email, token_type="email_verification", expires_minutes=30)
    return {"verification_token": token, "message": "Email verified. Create a password to complete registration."}


@router.post("/register", summary="Create a user after email verification")
async def register(payload: RegisterRequest, pool=Depends(get_pool)):
    token = decode_token(payload.verification_token, expected_type="email_verification")
    if not token or token.get("sub") != payload.email:
        raise HTTPException(400, "Invalid verification token")
    async with pool.acquire() as conn:
        async with conn.transaction():
            existing = await conn.fetchval("select id from users where email = $1", payload.email)
            if existing:
                raise HTTPException(409, "An account with this email already exists")
            user = await conn.fetchrow(
                """
                insert into users (email, password_hash)
                values ($1, $2)
                returning id, email, created_at, last_login_at
                """,
                payload.email, hash_password(payload.password),
            )
            await conn.execute(
                "insert into portfolios (user_id, portfolio_name) values ($1, $2)",
                user["id"], "My Portfolio",
            )
    access_token = create_token(str(user["id"]))
    return {"access_token": access_token, "token_type": "bearer", "user": dict(user)}


@router.post("/login", summary="Login with email and password")
async def login(payload: LoginRequest, pool=Depends(get_pool)):
    async with pool.acquire() as conn:
        user = await conn.fetchrow(
            "select id, email, password_hash, created_at, last_login_at from users where email = $1",
            payload.email,
        )
        if not user or not verify_password(payload.password, user["password_hash"]):
            raise HTTPException(401, "Invalid email or password")
        updated = await conn.fetchrow(
            "update users set last_login_at = now() where id = $1 returning id, email, created_at, last_login_at",
            user["id"],
        )
    return {"access_token": create_token(str(user["id"])), "token_type": "bearer", "user": dict(updated)}
