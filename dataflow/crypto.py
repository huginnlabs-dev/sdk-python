"""AES-256-GCM payload protection, key-derived from a user secret via
PBKDF2-SHA256 — the exact scheme used by the Go SDK (encoder package)."""

from __future__ import annotations

import hashlib
import os
from typing import Optional, Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

KEY_LEN = 32
IV_LEN = 12
ITERATIONS = 10000


def new_salt() -> bytes:
    return os.urandom(16)


def salt_from_hex(raw: Optional[str]) -> bytes:
    if not raw:
        return new_salt()
    return bytes.fromhex(raw)


def derive_key(secret: str, salt: bytes) -> bytes:
    if not secret:
        raise ValueError("empty encryption secret")
    return hashlib.pbkdf2_hmac("sha256", secret.encode("utf-8"), salt, ITERATIONS, dklen=KEY_LEN)


def encrypt(key: bytes, plaintext: bytes) -> Tuple[bytes, bytes]:
    """Seal plaintext under key; returns (ciphertext, iv)."""
    if len(key) != KEY_LEN:
        raise ValueError("key must be 32 bytes")
    iv = os.urandom(IV_LEN)
    return AESGCM(key).encrypt(iv, plaintext, None), iv


def decrypt(key: bytes, ciphertext: bytes, iv: bytes) -> bytes:
    if len(iv) != IV_LEN:
        raise ValueError("invalid iv length")
    return AESGCM(key).decrypt(iv, ciphertext, None)
