"""JWT secret loading (ADR-020)."""

from pathlib import Path

import pytest

from gateway.auth.secret import JwtSecretError, load_jwt_secret


def test_load_jwt_secret_reads_hex(tmp_path: Path) -> None:
    path = tmp_path / "jwt_secret"
    path.write_text("ab" * 32 + "\n", encoding="utf-8")
    assert load_jwt_secret(path) == bytes.fromhex("ab" * 32)


def test_load_jwt_secret_rejects_missing(tmp_path: Path) -> None:
    with pytest.raises(JwtSecretError, match="not found"):
        load_jwt_secret(tmp_path / "missing")


def test_load_jwt_secret_rejects_short(tmp_path: Path) -> None:
    path = tmp_path / "jwt_secret"
    path.write_text("abcd", encoding="utf-8")
    with pytest.raises(JwtSecretError, match="32 bytes"):
        load_jwt_secret(path)


def test_load_jwt_secret_uses_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    path = tmp_path / "from-env"
    path.write_text("cd" * 32, encoding="utf-8")
    monkeypatch.setenv("JWT_SECRET_FILE", str(path))
    assert load_jwt_secret() == bytes.fromhex("cd" * 32)
