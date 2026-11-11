"""PII hashing for silver (pure Python, no Spark): salted SHA-256, salt only from the env."""

from __future__ import annotations

import hashlib
import re

import pytest

from repopulse.spark import pii

SALT = "test-salt-not-a-secret"
EMAIL = "Alice.Nguyen@Example.com"


def test_salt_env_name():
    assert pii.PII_SALT_ENV == "PII_HASH_SALT"


def test_hash_is_salted_sha256_of_normalised_value():
    expected = hashlib.sha256((SALT + "alice.nguyen@example.com").encode("utf-8")).hexdigest()
    assert pii.hash_pii(EMAIL, SALT) == expected


def test_hash_is_64_lowercase_hex_chars():
    assert re.fullmatch(r"[0-9a-f]{64}", pii.hash_pii(EMAIL, SALT))


def test_same_input_same_hash():
    assert pii.hash_pii(EMAIL, SALT) == pii.hash_pii(EMAIL, SALT)


def test_different_salt_different_hash():
    assert pii.hash_pii(EMAIL, SALT) != pii.hash_pii(EMAIL, SALT + "x")


def test_different_value_different_hash():
    assert pii.hash_pii("bob@example.com", SALT) != pii.hash_pii("carol@example.com", SALT)


def test_hash_normalises_case_and_surrounding_whitespace():
    assert pii.hash_pii("  ALICE.NGUYEN@example.COM\t", SALT) == pii.hash_pii(EMAIL, SALT)


def test_hash_does_not_contain_raw_value():
    digest = pii.hash_pii(EMAIL, SALT)
    assert EMAIL.lower() not in digest
    assert "@" not in digest
    assert "alice" not in digest


def test_hash_handles_unicode_as_utf8():
    value = "Đặng Thị Hoa"
    expected = hashlib.sha256((SALT + value.strip().lower()).encode("utf-8")).hexdigest()
    assert pii.hash_pii(value, SALT) == expected


def test_salt_from_explicit_environ():
    assert pii.salt_from_env({"PII_HASH_SALT": "abc123"}) == "abc123"


def test_salt_from_process_environment(monkeypatch):
    monkeypatch.setenv("PII_HASH_SALT", "from-process-env")
    assert pii.salt_from_env() == "from-process-env"


@pytest.mark.parametrize("environ", [{}, {"PII_HASH_SALT": ""}, {"PII_HASH_SALT": "  \t "}])
def test_missing_or_blank_salt_raises(environ):
    with pytest.raises(pii.MissingSaltError, match="PII_HASH_SALT"):
        pii.salt_from_env(environ)


def test_missing_salt_from_process_environment_raises(monkeypatch):
    # No default salt: an unset variable must never fall back to a hardcoded value.
    monkeypatch.delenv("PII_HASH_SALT", raising=False)
    with pytest.raises(pii.MissingSaltError):
        pii.salt_from_env()


def test_missing_salt_error_is_a_value_error():
    assert issubclass(pii.MissingSaltError, ValueError)
