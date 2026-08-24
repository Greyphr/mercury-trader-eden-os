"""Tests for the symbol mapping layer and environment profile resolution."""

from __future__ import annotations

import logging

import pytest
from config_facts import database_name, default_environment_name, environment_profile

from mercury.core.config import EnvironmentConfig, EnvironmentsConfig, load_config
from mercury.core.symbols import SymbolMappingError, get_symbol_mapper


# ── Symbol mapper ─────────────────────────────────────────────
def test_mapper_resolves_canonical_to_broker(settings):
    mapper = get_symbol_mapper(settings)
    assert mapper.broker_symbol("GOLD") == "XAUUSD"
    assert mapper.canonical("XAUUSD") == "GOLD"
    assert mapper.contract_size("GOLD") == 100.0


def test_mapper_spec_metadata(settings):
    spec = get_symbol_mapper(settings).spec("GOLD")
    assert spec.preferred is True
    assert spec.point == 0.01
    assert spec.digits == 2
    assert spec.min_lot == 0.01
    assert spec.lot_step == 0.01


def test_mapper_unmapped_raises(settings):
    mapper = get_symbol_mapper(settings)
    with pytest.raises(SymbolMappingError):
        mapper.broker_symbol("SILVER")
    with pytest.raises(SymbolMappingError):
        mapper.canonical("EURUSD")
    with pytest.raises(SymbolMappingError):
        mapper.contract_size("SILVER")


def test_mapper_pass_through_canonical_as_broker(settings):
    mapper = get_symbol_mapper(settings)
    assert mapper.canonical("GOLD") == "GOLD"


def test_mapper_verify_available(settings, caplog):
    mapper = get_symbol_mapper(settings)
    with caplog.at_level(logging.WARNING, logger="mercury"):
        verified = mapper.verify_available(["XAUUSD", "XAUUSDm", "EURUSD"])
    assert verified == ["GOLD"]
    assert "ambiguous" in caplog.text


def test_mapper_verify_available_missing_preferred(settings, caplog):
    mapper = get_symbol_mapper(settings)
    with caplog.at_level(logging.WARNING, logger="mercury"):
        verified = mapper.verify_available(["EURUSD"])
    assert verified == []
    assert "not found" in caplog.text


# ── Environment profiles ──────────────────────────────────────
def test_default_environment_matches_config(monkeypatch):
    """The resolved default profile mirrors config/base.yaml + environments.yaml."""
    monkeypatch.delenv("MERCURY_ENV", raising=False)
    settings = load_config()
    profile = environment_profile()
    assert settings.environment.name == default_environment_name()
    assert settings.environment.broker_backend == profile["broker_backend"]
    assert settings.environment.trading_enabled is profile["trading_enabled"]
    assert settings.database_url.endswith(database_name())
    assert settings.base.paths.log_dir == profile["log_dir"]


def test_environment_selection_via_arg(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)  # per-env database must apply
    s = load_config(environment="metaquotes_demo")
    assert s.environment.name == "metaquotes_demo"
    assert s.environment.broker_backend == "mt5"
    assert s.database_url.endswith(database_name("metaquotes_demo"))
    assert s.base.paths.log_dir == "logs/demo"


def test_live_environment_ships_armed_off(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    s = load_config(environment="exness_live")
    profile = environment_profile("exness_live")
    assert s.environment.name == "exness_live"
    assert s.environment.trading_enabled is profile["trading_enabled"] is False
    assert s.environment.broker_backend == profile["broker_backend"]
    assert s.database_url.endswith(database_name("exness_live"))


def test_environment_selection_via_env_var(monkeypatch):
    monkeypatch.setenv("MERCURY_ENV", "exness_live")
    s = load_config()
    assert s.environment.name == "exness_live"
    assert s.environment.trading_enabled is False


def test_cli_flag_wins_over_env_var(monkeypatch):
    monkeypatch.setenv("MERCURY_ENV", "exness_live")
    s = load_config(environment="development")
    assert s.environment.name == "development"


def test_unknown_environment_rejected(monkeypatch):
    monkeypatch.setenv("MERCURY_ENV", "nope")
    with pytest.raises(ValueError):
        load_config()


def test_per_env_mt5_credential_env_names(monkeypatch):
    monkeypatch.setenv("MT5_LOGIN_LIVE", "12345")
    s = load_config(environment="exness_live")
    creds = s.environment.mt5.credentials()
    assert creds["login"] == "12345"
    assert creds["server"] == "Exness-MT5"


def test_environments_config_resolve_default(monkeypatch):
    monkeypatch.delenv("MERCURY_ENV", raising=False)
    envs = EnvironmentsConfig(environments={"development": EnvironmentConfig()})
    resolved = envs.resolve(None)
    assert resolved.name == "development"
