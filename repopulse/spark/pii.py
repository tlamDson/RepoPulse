"""Salted SHA-256 for PII before silver. The salt only comes from the environment (no default)."""

from __future__ import annotations

import hashlib
import os
from collections.abc import Mapping

PII_SALT_ENV = "PII_HASH_SALT"


class MissingSaltError(ValueError):
    """The PII salt environment variable is unset or blank."""


def salt_from_env(environ: Mapping[str, str] | None = None) -> str:
    env = os.environ if environ is None else environ
    salt = env.get(PII_SALT_ENV)
    if salt is None or not salt.strip():
        raise MissingSaltError(f"{PII_SALT_ENV} is not set (or blank); PII cannot be hashed")
    return salt


def hash_pii(value: str, salt: str) -> str:
    """Hex SHA-256 of UTF-8 ``salt + value.strip().lower()``."""
    return hashlib.sha256((salt + value.strip().lower()).encode("utf-8")).hexdigest()
