from config_facts import base_yaml, database_name, default_environment_name
from pydantic import ValidationError

from mercury.core.config import Settings, load_config, redact_database_url


def test_config_loads(settings):
    assert isinstance(settings, Settings)
    assert settings.base.project.name == "Mercury Trader"
    assert settings.base.deployment.mode in ("live", "paper", "read_only")
    assert len(settings.strategies.strategies) >= 1


def test_config_matches_base_yaml(settings):
    """Loader output mirrors config/base.yaml (mode + active environment)."""
    raw = base_yaml()
    assert settings.base.deployment.mode == raw["deployment"]["mode"]
    assert settings.base.environment == raw["environment"]


def test_risk_defaults(settings):
    risk = settings.risk
    assert 0 < risk.risk_per_trade_percent <= risk.max_risk_per_trade_percent
    assert risk.guards.max_open_positions >= 1


def test_strategy_config(settings):
    strategy = settings.strategies.strategies[0]
    assert strategy.symbol == "GOLD"
    assert strategy.timeframe == "M15"
    assert strategy.order.magic == 77001


def test_invalid_deployment_mode_rejected():
    from mercury.core.config import DeploymentConfig

    try:
        DeploymentConfig(mode="bad")
        raise AssertionError("should have raised")
    except ValidationError:
        pass


def test_database_url_default():
    s = load_config()
    assert s.database_url.startswith("postgresql")


def test_blank_database_url_falls_back_to_environment_default(monkeypatch):
    # A blank DATABASE_URL must fall through to the active environment
    # profile's database (see environments.yaml), not a hardcoded name.
    monkeypatch.delenv("MERCURY_ENV", raising=False)
    monkeypatch.setenv("DATABASE_URL", "")
    s = load_config()
    expected = (
        "postgresql+psycopg://mercury:mercury@localhost:5432/"
        f"{database_name(default_environment_name())}"
    )
    assert s.database_url == expected


def test_postgres_engine_has_connect_timeout():
    from mercury.core.db import Database

    db = Database("postgresql+psycopg://x:y@localhost:5432/z")
    try:
        import inspect

        params = inspect.getclosurevars(db.engine.pool._creator).nonlocals.get("cparams", {})
        assert params.get("connect_timeout") == 10
    finally:
        db.dispose()


def test_redact_database_url_hides_password():
    redacted = redact_database_url("postgresql+psycopg://user:secret@host:5432/db")
    assert "secret" not in redacted
    assert "user:***@host:5432/db" in redacted


def test_redact_database_url_preserves_url_without_credentials():
    redacted = redact_database_url("postgresql+psycopg://host:5432/db")
    assert redacted == "postgresql+psycopg://host:5432/db"


def test_redact_database_url_falls_back_on_garbage():
    assert redact_database_url("not a url") == "not a url"
