"""transform/profiles.yml has a Snowflake output fed only by env vars; pyproject has its extra."""

import re
import tomllib
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
PROFILES = ROOT / "transform" / "profiles.yml"
PYPROJECT = ROOT / "pyproject.toml"
ENV_FIELDS = {
    "account": "SNOWFLAKE_ACCOUNT",
    "user": "SNOWFLAKE_USER",
    "private_key": "SNOWFLAKE_PRIVATE_KEY",
    "role": "SNOWFLAKE_ROLE",
    "warehouse": "SNOWFLAKE_WAREHOUSE",
    "database": "SNOWFLAKE_DATABASE",
}


def profile() -> dict:
    return yaml.safe_load(PROFILES.read_text(encoding="utf-8"))["repopulse"]


def snowflake() -> dict:
    outputs = profile()["outputs"]
    assert "snowflake" in outputs, f"no `snowflake` output in {PROFILES}"
    return outputs["snowflake"]


def extras() -> dict[str, list[str]]:
    with PYPROJECT.open("rb") as fh:
        return tomllib.load(fh)["project"]["optional-dependencies"]


def test_default_target_is_still_ci():
    assert profile()["target"] == "ci"


def test_snowflake_output_uses_the_snowflake_adapter():
    out = snowflake()
    assert out["type"] == "snowflake"
    assert isinstance(out.get("threads"), int) and 1 <= out["threads"] <= 4
    assert isinstance(out.get("schema"), str) and out["schema"]
    assert "{{" not in out["schema"], "schema is fixed, not taken from the environment"


@pytest.mark.parametrize("field", sorted(ENV_FIELDS))
def test_connection_field_comes_from_its_env_var(field):
    value = snowflake().get(field)
    assert isinstance(value, str), field
    pattern = rf"^\{{\{{\s*env_var\(\s*'{ENV_FIELDS[field]}'\s*,\s*''\s*\)\s*\}}\}}$"
    assert re.fullmatch(pattern, value.strip()), (field, value)


def test_no_password_or_inline_key_material():
    out = snowflake()
    assert "password" not in out
    assert "private_key_passphrase" not in out
    text = PROFILES.read_text(encoding="utf-8")
    assert "BEGIN" not in text and "PRIVATE KEY-----" not in text
    for key, value in out.items():
        if key in ENV_FIELDS or not isinstance(value, str):
            continue
        assert "SNOWFLAKE_" not in value or "env_var(" in value, (key, value)


def test_pyproject_has_a_separate_snowflake_extra():
    deps = extras()
    assert "snowflake" in deps
    assert any(re.match(r"dbt-snowflake\s*>=\s*1\.8", d) for d in deps["snowflake"]), deps
    assert not any(d.startswith("dbt-snowflake") for d in deps["dbt"]), deps["dbt"]
