"""
One-off CLI to create (or reset) an admin account in the `admins` table.

Usage:
    python scripts/create_admin.py <username> <password>

Requires DATABASE_URL to be set in the environment (same Postgres/Supabase
instance the backend uses). Run this once after applying
sql/migration_v5_admin_approval.sql to bootstrap your first admin login.
"""
import asyncio
import os
import sys

import asyncpg
import bcrypt


async def main(username: str, password: str) -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        print("DATABASE_URL is not set", file=sys.stderr)
        sys.exit(1)

    password_hash = bcrypt.hashpw(password.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute(
            """
            insert into admins (username, password_hash)
            values ($1, $2)
            on conflict (username) do update set password_hash = excluded.password_hash
            """,
            username, password_hash,
        )
        print(f"Admin '{username}' created/updated successfully.")
    finally:
        await conn.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python scripts/create_admin.py <username> <password>", file=sys.stderr)
        sys.exit(1)
    asyncio.run(main(sys.argv[1], sys.argv[2]))
