from pathlib import Path

import yaml

_ROOT = Path(__file__).resolve().parents[3]


def _recipe(name: str, text: str) -> str:
    body: list[str] = []
    capture = False
    for line in text.splitlines():
        if not capture:
            if line.startswith(f"{name}:"):
                capture = True
            continue
        if line and not line.startswith((" ", "\t")):
            break
        body.append(line)
    return "\n".join(body)


def test_make_up_does_not_start_the_stand() -> None:
    text = (_ROOT / "Makefile").read_text(encoding="utf-8")
    assert "baseline" not in _recipe("up", text)
    baseline = _recipe("baseline-up", text)
    assert "--profile baseline" in baseline
    assert "baseline-writer" in baseline
    assert "baseline-api" in baseline


def test_baseline_services_are_profile_only_and_postgres_is_sized() -> None:
    compose = yaml.safe_load((_ROOT / "infra" / "docker-compose.yml").read_text(encoding="utf-8"))
    services = compose["services"]
    for name in ("baseline-writer", "baseline-api"):
        assert services[name]["profiles"] == ["baseline"]
        assert str(services[name]["environment"]["METRICS_PORT"]) == "9000"
    assert "ports" not in services["baseline-writer"]
    assert "8003:8003" in services["baseline-api"]["ports"]
    postgres = services["postgres"]
    assert postgres["deploy"]["resources"]["limits"]["memory"] == "8g"
    assert postgres["shm_size"] == "1gb"
    assert "shared_buffers=1GB" in postgres["command"]
    assert "effective_cache_size=6GB" in postgres["command"]
    assert "SVC_BASELINE_PASSWORD" in postgres["environment"]


def test_role_is_not_a_member_of_the_product_tiers() -> None:
    roles = (_ROOT / "infra" / "postgres" / "00-roles.sh").read_text(encoding="utf-8")
    example = (_ROOT / "infra" / ".env.example").read_text(encoding="utf-8")
    schema = (_ROOT / "infra" / "postgres" / "30-baseline.sql").read_text(encoding="utf-8")
    assert "CREATE ROLE svc_baseline" in roles
    assert "SVC_BASELINE_PASSWORD" in roles
    assert "SVC_BASELINE_PASSWORD=svc_baseline_dev" in example
    assert "CREATE SCHEMA" in schema
    assert "baseline.transactions" in schema
    for line in roles.splitlines():
        if "GRANT fs_write" in line or "GRANT fs_read" in line:
            assert "svc_baseline" not in line
