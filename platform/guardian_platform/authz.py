"""Identity and authorisation.

Three modes, chosen by AUTH_MODE:

    disabled  no auth at all — the local demo default
    local     username/password against a users table, issues a signed JWT
    oidc      validates a JWT issued by an external IdP against its JWKS

The important property is not which mode is used but where identity comes
from once you have it: **the caller's token, never the request body**. The
previous approval endpoint accepted `approved_by` as a field, which meant the
audit record said whatever the client typed. An audit trail whose subject is
client-supplied is not an audit trail.

Authorisation is expressed as a blast-radius ceiling per role, so it lines up
with the same scale the remediation policy uses. A viewer reads; an operator
approves small reversible things; an approver signs off on region failovers.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any

import base64
import hashlib

import bcrypt
import jwt

ALGORITHM = "HS256"
TOKEN_TTL_HOURS = 12


class Role(str, Enum):
    VIEWER = "viewer"
    OPERATOR = "operator"
    APPROVER = "approver"
    ADMIN = "admin"


# The highest blast radius each role may approve. Mirrors the 0-5 scale in
# policy/guardian.rego, so "who may approve this" and "how far does this
# reach" are measured in the same units.
MAX_BLAST: dict[Role, int] = {
    Role.VIEWER: -1,      # may approve nothing
    Role.OPERATOR: 3,     # service-level disruption
    Role.APPROVER: 5,     # region failover
    Role.ADMIN: 5,
}

# Capabilities beyond approvals.
CAN_CONFIGURE = {Role.ADMIN}
CAN_INJECT_CHAOS = {Role.OPERATOR, Role.APPROVER, Role.ADMIN}
CAN_ROLLBACK = {Role.OPERATOR, Role.APPROVER, Role.ADMIN}


@dataclass(frozen=True)
class Principal:
    """Who is making a request."""

    subject: str          # stable id — the audit trail's actual subject
    display_name: str
    email: str | None
    roles: tuple[Role, ...]
    issuer: str = "local"

    @property
    def max_blast(self) -> int:
        return max((MAX_BLAST[r] for r in self.roles), default=-1)

    def may_approve(self, blast_radius: int) -> bool:
        return blast_radius <= self.max_blast

    def has(self, capability: set[Role]) -> bool:
        return any(r in capability for r in self.roles)

    def to_json(self) -> dict[str, Any]:
        return {
            "subject": self.subject,
            "display_name": self.display_name,
            "email": self.email,
            "roles": [r.value for r in self.roles],
            "max_blast": self.max_blast,
            "issuer": self.issuer,
            "can_configure": self.has(CAN_CONFIGURE),
            "can_inject_chaos": self.has(CAN_INJECT_CHAOS),
            "can_rollback": self.has(CAN_ROLLBACK),
        }


# The principal used when AUTH_MODE=disabled. Named so it is obvious in an
# audit record that no real identity was established.
ANONYMOUS = Principal(
    subject="anonymous",
    display_name="Unauthenticated (auth disabled)",
    email=None,
    roles=(Role.ADMIN,),
    issuer="none",
)


class AuthSettings:
    def __init__(self) -> None:
        self.mode = os.getenv("AUTH_MODE", "disabled").lower()
        self.secret = os.getenv("AUTH_JWT_SECRET", "")
        self.oidc_issuer = os.getenv("OIDC_ISSUER", "")
        self.oidc_audience = os.getenv("OIDC_AUDIENCE", "")
        self.oidc_jwks_url = os.getenv("OIDC_JWKS_URL", "")
        self.roles_claim = os.getenv("OIDC_ROLES_CLAIM", "roles")

    @property
    def enabled(self) -> bool:
        return self.mode in ("local", "oidc")

    def validate(self) -> list[str]:
        """Configuration problems worth refusing to start over."""
        problems: list[str] = []
        if self.mode == "local" and len(self.secret) < 32:
            problems.append(
                "AUTH_MODE=local requires AUTH_JWT_SECRET of at least 32 characters"
            )
        if self.mode == "oidc" and not (self.oidc_issuer and self.oidc_jwks_url):
            problems.append(
                "AUTH_MODE=oidc requires OIDC_ISSUER and OIDC_JWKS_URL"
            )
        return problems


def _prehash(password: str) -> bytes:
    """SHA-256 then base64, so bcrypt never sees more than 72 bytes.

    bcrypt silently truncates at 72 bytes, which turns two different long
    passwords into the same hash. Pre-hashing removes the limit entirely and
    is the standard fix; base64 keeps the digest free of NUL bytes, which
    bcrypt also truncates on.
    """
    return base64.b64encode(hashlib.sha256(password.encode()).digest())


def hash_password(password: str) -> str:
    return bcrypt.hashpw(_prehash(password), bcrypt.gensalt()).decode()


def verify_password(password: str, hashed: str) -> bool:
    try:
        return bcrypt.checkpw(_prehash(password), hashed.encode())
    except (ValueError, TypeError):  # a malformed hash is a failed login
        return False


def issue_token(principal: Principal, secret: str) -> tuple[str, datetime]:
    expires = datetime.now(timezone.utc) + timedelta(hours=TOKEN_TTL_HOURS)
    payload = {
        "sub": principal.subject,
        "name": principal.display_name,
        "email": principal.email,
        "roles": [r.value for r in principal.roles],
        "iss": "kafka-guardian",
        "exp": expires,
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, secret, algorithm=ALGORITHM), expires


def _roles_from(values: Any) -> tuple[Role, ...]:
    if isinstance(values, str):
        values = [values]
    roles: list[Role] = []
    for v in values or []:
        try:
            roles.append(Role(str(v).lower()))
        except ValueError:
            continue
    # An authenticated user with no recognised role can still read.
    return tuple(roles) or (Role.VIEWER,)


def principal_from_local_token(token: str, secret: str) -> Principal:
    claims = jwt.decode(token, secret, algorithms=[ALGORITHM],
                        issuer="kafka-guardian")
    return Principal(
        subject=claims["sub"],
        display_name=claims.get("name") or claims["sub"],
        email=claims.get("email"),
        roles=_roles_from(claims.get("roles")),
        issuer="local",
    )


def principal_from_oidc_token(token: str, settings: AuthSettings) -> Principal:
    """Validate against the IdP's published keys.

    PyJWKClient caches the key set, so this is not a network call per request.
    """
    client = jwt.PyJWKClient(settings.oidc_jwks_url, cache_keys=True)
    signing_key = client.get_signing_key_from_jwt(token)
    claims = jwt.decode(
        token,
        signing_key.key,
        algorithms=["RS256", "ES256"],
        audience=settings.oidc_audience or None,
        issuer=settings.oidc_issuer,
    )
    return Principal(
        subject=claims["sub"],
        display_name=claims.get("name") or claims.get("preferred_username")
                     or claims["sub"],
        email=claims.get("email"),
        roles=_roles_from(claims.get(settings.roles_claim)),
        issuer=settings.oidc_issuer,
    )
