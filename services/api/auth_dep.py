"""FastAPI wiring for identity, roles and the user store."""

from __future__ import annotations

import json
import os
from typing import Any

import asyncpg
import structlog
from fastapi import Depends, HTTPException, Request

from guardian_platform.authz import (
    ANONYMOUS, AuthSettings, Principal, Role, hash_password,
    principal_from_local_token, principal_from_oidc_token, verify_password,
)

log = structlog.get_logger(__name__)

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
    subject       TEXT PRIMARY KEY,
    username      TEXT UNIQUE NOT NULL,
    display_name  TEXT NOT NULL,
    email         TEXT,
    password_hash TEXT NOT NULL,
    roles         JSONB NOT NULL DEFAULT '["viewer"]'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_login    TIMESTAMPTZ
);
"""


class UserStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self, retries: int = 30) -> None:
        import asyncio

        for attempt in range(retries):
            try:
                self._pool = await asyncpg.create_pool(self._dsn, min_size=1, max_size=3)
                break
            except Exception:  # noqa: BLE001
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(2)
        assert self._pool is not None
        async with self._pool.acquire() as conn:
            await conn.execute(SCHEMA)
        await self._seed()

    async def _seed(self) -> None:
        """Create the bootstrap admin when the table is empty.

        The password comes from the environment. There is no hardcoded
        default: a shipped default admin password is a shipped vulnerability,
        and every deployment that forgets to change it is compromised by
        anyone who has read the repository.
        """
        assert self._pool is not None
        count = await self._pool.fetchval("SELECT COUNT(*) FROM users")
        if count:
            return
        password = os.getenv("BOOTSTRAP_ADMIN_PASSWORD", "").strip()
        if not password:
            log.warning(
                "no_bootstrap_admin",
                note="AUTH_MODE requires a user; set BOOTSTRAP_ADMIN_PASSWORD "
                     "to create the initial admin account.")
            return
        await self.create(
            username=os.getenv("BOOTSTRAP_ADMIN_USER", "admin"),
            password=password,
            display_name="Bootstrap administrator",
            roles=[Role.ADMIN],
            email=os.getenv("BOOTSTRAP_ADMIN_EMAIL"),
        )
        log.info("bootstrap_admin_created",
                 username=os.getenv("BOOTSTRAP_ADMIN_USER", "admin"))

    async def create(
        self, username: str, password: str, display_name: str,
        roles: list[Role], email: str | None = None,
    ) -> str:
        assert self._pool is not None
        subject = f"local:{username}"
        await self._pool.execute(
            """
            INSERT INTO users (subject, username, display_name, email,
                               password_hash, roles)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb)
            ON CONFLICT (username) DO NOTHING
            """,
            subject, username, display_name, email, hash_password(password),
            json.dumps([r.value for r in roles]),
        )
        return subject

    async def authenticate(self, username: str, password: str) -> Principal | None:
        if not self._pool:
            return None
        row = await self._pool.fetchrow(
            "SELECT subject, username, display_name, email, password_hash, roles "
            "FROM users WHERE username = $1", username
        )
        if row is None:
            # Hash anyway so a missing user and a wrong password take the same
            # time; otherwise the endpoint enumerates valid usernames.
            verify_password(password, "$2b$12$" + "x" * 53)
            return None
        if not verify_password(password, row["password_hash"]):
            return None
        await self._pool.execute(
            "UPDATE users SET last_login = now() WHERE subject = $1", row["subject"]
        )
        raw = row["roles"]
        roles = json.loads(raw) if isinstance(raw, str) else raw
        return Principal(
            subject=row["subject"], display_name=row["display_name"],
            email=row["email"],
            roles=tuple(Role(r) for r in roles) or (Role.VIEWER,),
            issuer="local",
        )

    async def list_users(self) -> list[dict[str, Any]]:
        if not self._pool:
            return []
        rows = await self._pool.fetch(
            "SELECT subject, username, display_name, email, roles, last_login "
            "FROM users ORDER BY username"
        )
        return [
            {"subject": r["subject"], "username": r["username"],
             "display_name": r["display_name"], "email": r["email"],
             "roles": (json.loads(r["roles"]) if isinstance(r["roles"], str)
                       else r["roles"]),
             "last_login": r["last_login"].isoformat() if r["last_login"] else None}
            for r in rows
        ]

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()


_settings = AuthSettings()


def settings() -> AuthSettings:
    return _settings


def _bearer(request: Request) -> str | None:
    header = request.headers.get("authorization", "")
    if header.lower().startswith("bearer "):
        return header[7:].strip()
    return None


async def current_principal(request: Request) -> Principal:
    """Resolve the caller. Never trusts anything in the request body."""
    s = _settings
    if not s.enabled:
        return ANONYMOUS

    token = _bearer(request)
    if not token:
        raise HTTPException(401, "Authentication required")
    try:
        if s.mode == "oidc":
            return principal_from_oidc_token(token, s)
        return principal_from_local_token(token, s.secret)
    except Exception as exc:  # noqa: BLE001 — any validation failure is a 401
        raise HTTPException(401, f"Invalid token: {type(exc).__name__}") from None


def require(capability: set[Role]):
    """Dependency factory for capability checks."""

    async def _check(p: Principal = Depends(current_principal)) -> Principal:
        if not _settings.enabled:
            return p
        if not p.has(capability):
            raise HTTPException(
                403,
                f"Requires one of: {', '.join(sorted(r.value for r in capability))}. "
                f"You have: {', '.join(r.value for r in p.roles)}.",
            )
        return p

    return _check
