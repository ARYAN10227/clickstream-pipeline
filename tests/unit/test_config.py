from collections.abc import Iterator
from pathlib import Path

import pytest

from clickpipe.config import Settings, get_settings

ENV_EXAMPLE = Path(__file__).resolve().parents[2] / ".env.example"


def _env_example() -> dict[str, str]:
    pairs = (
        line.split("=", 1)
        for line in ENV_EXAMPLE.read_text().splitlines()
        if line.strip() and not line.startswith("#")
    )
    return {key.strip(): value.strip() for key, value in pairs}


@pytest.fixture(autouse=True)
def isolated_env(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Iterator[None]:
    """Run each test with no pipeline env vars set and no .env file in the cwd."""
    for key in _env_example():
        monkeypatch.delenv(key, raising=False)
    monkeypatch.chdir(tmp_path)
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def test_defaults_match_env_example() -> None:
    settings = Settings()
    for key, value in _env_example().items():
        actual = getattr(settings, key.lower())
        if value == "":
            assert actual is None, key
        else:
            assert str(actual) == value, key
    assert isinstance(settings.clickhouse_port, int)
    assert isinstance(settings.batch_max_size, int)


def test_environment_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP", "redpanda:9092")
    monkeypatch.setenv("BATCH_MAX_SIZE", "123")
    monkeypatch.setenv("GEOIP_DB_PATH", "/data/GeoLite2-Country.mmdb")
    get_settings.cache_clear()

    settings = get_settings()

    assert settings.kafka_bootstrap == "redpanda:9092"
    assert settings.batch_max_size == 123
    assert settings.geoip_db_path == "/data/GeoLite2-Country.mmdb"
    assert settings.consumer_config()["bootstrap.servers"] == "redpanda:9092"


def test_consumer_disables_auto_commit() -> None:
    assert Settings().consumer_config()["enable.auto.commit"] is False


def test_producer_is_idempotent() -> None:
    assert Settings().producer_config()["enable.idempotence"] is True
