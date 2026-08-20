"""Reading secrets from files rather than environment variables.

Compose passes secrets as environment variables, which means every one of
them is readable by anyone who can run `docker inspect` — the whole value,
in plaintext, from outside the container. That is acceptable on a laptop and
not acceptable anywhere else.

The convention here is the widely used `*_FILE` suffix: if
`AUTH_JWT_SECRET_FILE` points at a readable file, its contents are used.
Docker secrets, Kubernetes secret volumes, and Vault Agent all deliver
secrets as files, so this is the seam that lets the same image run in all
three without the value ever appearing in `docker inspect` or a process
listing.

**A readable `*_FILE` always wins over the plain variable.** The file is the
more secure source, so when a deployment supplies both — usually mid-migration
from compose env vars to real secrets — the safer one should be the one that
takes effect. Both resolution paths in this module apply that same rule; an
earlier version had `read()` prefer the file while `promote()` preferred the
variable, so the same secret resolved differently depending on which was
called.
"""

from __future__ import annotations

import os
from pathlib import Path

import structlog

log = structlog.get_logger(__name__)


def read(name: str, default: str = "") -> str:
    """Resolve a secret from `NAME` or, preferably, from `NAME_FILE`."""
    file_var = f"{name}_FILE"
    path = os.getenv(file_var, "").strip()
    if path:
        try:
            value = Path(path).read_text(encoding="utf-8").strip()
            if value:
                return value
            log.warning("secret_file_empty", var=file_var, path=path)
        except OSError as exc:
            log.error("secret_file_unreadable", var=file_var, path=path,
                      error=str(exc))
    return os.getenv(name, default)


def promote(*names: str) -> None:
    """Copy any `NAME_FILE` values into `NAME` before settings are built.

    Called once at start-up so pydantic-settings and anything else reading
    os.environ sees the resolved value without needing to know the convention.
    Follows the same file-wins precedence as `read()`.
    """
    for name in names:
        if not os.getenv(f"{name}_FILE", "").strip():
            continue
        value = read(name)
        if value and value != os.getenv(name):
            os.environ[name] = value
            log.info("secret_loaded_from_file", var=f"{name}_FILE",
                     overrode_env=bool(os.getenv(name)))
