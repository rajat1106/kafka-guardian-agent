"""Secret storage.

Plugin credentials are envelope-encrypted before they touch the database, so
a database dump is not a credential dump. The data key is derived from
GUARDIAN_SECRET_KEY.

This is a real improvement over plaintext and an honest half-measure: if the
key sits in the same .env as the database password, an attacker with the host
has both. The provider indirection below is the seam for a KMS — AWS KMS,
Vault transit, or GCP KMS — where the key never exists in the application at
all. `KmsProvider` documents that path.
"""

from __future__ import annotations

import base64
import hashlib
import os
from typing import Protocol

import structlog
from cryptography.fernet import Fernet, InvalidToken

log = structlog.get_logger(__name__)

PREFIX = "enc:v1:"


class SecretProvider(Protocol):
    def encrypt(self, plaintext: str) -> str: ...
    def decrypt(self, ciphertext: str) -> str: ...
    @property
    def name(self) -> str: ...


class PlaintextProvider:
    """No encryption. Only for the local demo, and it says so loudly."""

    name = "plaintext"

    def encrypt(self, plaintext: str) -> str:
        return plaintext

    def decrypt(self, ciphertext: str) -> str:
        return ciphertext


class FernetProvider:
    """Symmetric encryption with a key derived from an environment secret."""

    name = "fernet"

    def __init__(self, key_material: str) -> None:
        # Fernet needs 32 url-safe base64 bytes; accept any passphrase and
        # derive deterministically so operators are not forced to generate a
        # key in a specific format.
        digest = hashlib.sha256(key_material.encode()).digest()
        self._f = Fernet(base64.urlsafe_b64encode(digest))

    def encrypt(self, plaintext: str) -> str:
        if not plaintext:
            return plaintext
        return PREFIX + self._f.encrypt(plaintext.encode()).decode()

    def decrypt(self, ciphertext: str) -> str:
        if not ciphertext or not ciphertext.startswith(PREFIX):
            # Written before encryption was enabled. Returned as-is so
            # enabling encryption does not break an existing install; values
            # are re-encrypted on next write.
            return ciphertext
        try:
            return self._f.decrypt(ciphertext[len(PREFIX):].encode()).decode()
        except InvalidToken:
            log.error("secret_decrypt_failed",
                      hint="GUARDIAN_SECRET_KEY changed; stored secrets are "
                           "unreadable and must be re-entered")
            return ""


class KmsProvider:
    """Placeholder for a real KMS-backed provider.

    Deliberately not implemented rather than faked. The production shape is:
    generate a data key per secret via the KMS, encrypt locally with it, store
    the wrapped key alongside the ciphertext, and never hold the master key in
    the process. Wiring that requires a cloud account, so it is documented
    rather than stubbed into something that looks functional and is not.
    """

    name = "kms"

    def __init__(self, key_id: str) -> None:
        raise NotImplementedError(
            "KMS-backed secrets are not implemented. Set GUARDIAN_SECRET_KEY "
            "to use envelope encryption with a local key, or implement this "
            "provider against your cloud KMS."
        )


def build_provider() -> SecretProvider:
    backend = os.getenv("SECRETS_BACKEND", "auto").lower()
    key = os.getenv("GUARDIAN_SECRET_KEY", "").strip()

    if backend == "kms":
        return KmsProvider(os.getenv("GUARDIAN_KMS_KEY_ID", ""))
    if backend == "plaintext":
        log.warning("secrets_plaintext",
                    note="SECRETS_BACKEND=plaintext — credentials are stored "
                         "unencrypted. Do not use outside a local demo.")
        return PlaintextProvider()
    if key:
        return FernetProvider(key)

    log.warning("secrets_plaintext",
                note="GUARDIAN_SECRET_KEY is not set — plugin credentials are "
                     "stored unencrypted. Set it to enable encryption at rest.")
    return PlaintextProvider()
