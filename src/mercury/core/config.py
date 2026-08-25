"""Typed configuration system.

Loads YAML files from ``config/`` and overlays environment variables.
All trading criteria, risk, strategy, and provider settings are validated
against Pydantic models so misconfiguration fails fast at startup.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, cast

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, model_validator

# Load the project .env file (repo root) as process-env defaults. Real
# environment variables always win (override=False), and load_dotenv is
# idempotent so repeated imports/config loads are safe.
load_dotenv(Path(__file__).resolve().parents[3] / ".env", override=False)


# ──────────────────────────────────────────────────────────────
# Section models
# ──────────────────────────────────────────────────────────────
class ProjectConfig(BaseModel):
    name: str = "Mercury Trader"
    version: str = "0.1.0"


class DeploymentConfig(BaseModel):
    mode: Literal["live", "paper", "read_only"] = "paper"

    @property
    def is_live(self) -> bool:
        return self.mode == "live"

    @property
    def is_paper(self) -> bool:
        return self.mode == "paper"

    @property
    def can_trade(self) -> bool:
        return self.mode in ("live", "paper")


class TradingSession(BaseModel):
    name: str
    days: list[str] = Field(default_factory=lambda: ["mon", "tue", "wed", "thu", "fri"])
    start: str = "00:00"
    end: str = "23:59"
    pause: dict[str, str] | None = None


def session_allows(trading_sessions: list[TradingSession], dt: datetime) -> bool:
    """True when ``dt`` (UTC) falls inside a configured session and not in any
    session's pause window (e.g. the first 15 minutes after London open)."""
    weekday = dt.strftime("%a").lower()[:3]
    t = dt.strftime("%H:%M")
    for session in trading_sessions:
        if weekday not in session.days:
            continue
        if session.start <= t <= session.end:
            pause = session.pause
            if pause is not None:
                start = pause.get("start")
                end = pause.get("end")
                if start is not None and end is not None and start <= t <= end:
                    return False
            return True
    return False


class JobsConfig(BaseModel):
    market_data: int = 60
    price_monitor: int = 5
    news_collection: int = 300
    hermes_daily: int = 21600
    hermes_daily_hour: str = "00:30"
    reports_daily: str = "23:55"
    reports_weekly: str = "fri 23:55"
    reports_monthly: str = "last-day 23:55"
    health_check: int = 60
    backtest_queue: int = 900


class PathsConfig(BaseModel):
    data_dir: str = "data"
    log_dir: str = "logs"


class EventAuditConfig(BaseModel):
    """Topics recorded to the durable event audit log (``event_audit`` table)."""

    audit_topics: list[str] = Field(default_factory=lambda: [
        "trade.opened",
        "trade.closed",
        "trade.rejected",
        "signal.approved",
        "signal.rejected",
        "hermes.proposals",
        "system.critical",
    ])


class BaseConfig(BaseModel):
    project: ProjectConfig = ProjectConfig()
    deployment: DeploymentConfig = DeploymentConfig()
    environment: str = "development"
    timezone: str = "UTC"
    jobs: JobsConfig = JobsConfig()
    trading_sessions: list[TradingSession] = Field(default_factory=list)
    paths: PathsConfig = PathsConfig()
    events: EventAuditConfig = EventAuditConfig()


# ── trading_criteria.yaml ─────────────────────────────────────
class SuccessCriteria(BaseModel):
    win_on_tp: bool = True
    count_breakeven_as_win: bool = True
    exclude_unfilled: bool = True
    max_spread_points: int = 30
    max_slippage_points: int = 20


class PromotionGate(BaseModel):
    min_trades: int
    min_win_rate: float
    min_profit_factor: float
    max_drawdown_percent: float


class PromotionGates(BaseModel):
    paper: PromotionGate
    live: PromotionGate


class TradingCriteria(BaseModel):
    min_tp_pips: int = 10
    min_sl_pips: int = 10
    min_risk_reward_ratio: float = 1.5
    success: SuccessCriteria = SuccessCriteria()
    evaluation_metrics: list[str] = Field(default_factory=list)
    promotion_gates: PromotionGates | None = None


# ── risk.yaml ─────────────────────────────────────────────────
class AdaptiveSizing(BaseModel):
    enabled: bool = False
    confidence_multiplier: bool = False


class SizingConfig(BaseModel):
    mode: Literal["fixed_percent", "adaptive"] = "fixed_percent"
    fixed_percent: float = 0.5
    contract_size: float = 100.0  # units of the underlying per 1.0 lot (XAUUSD = 100 oz)
    max_volume: float = 10_000.0  # hard cap on lots per order; sizing rejects above this
    adaptive: AdaptiveSizing = AdaptiveSizing()


class RiskGuards(BaseModel):
    max_open_positions: int = 5
    max_daily_trades: int = 5
    max_daily_drawdown_percent: float = 3.0
    max_total_drawdown_percent: float = 8.0
    min_account_equity: float = 100.0
    max_spread_points: int = 30
    session_check: bool = True
    min_confidence: float = 0.55
    allow_rule_based_trading: bool = False
    news_blackout_minutes: int = 5
    max_slippage_points: int = 20


class ManagementConfig(BaseModel):
    move_to_breakeven: dict[str, Any] = Field(default_factory=dict)
    partial_take_profit: dict[str, Any] = Field(default_factory=dict)


class KillSwitchConfig(BaseModel):
    enabled: bool = True
    rearm_after_hours: int = 24


class RiskConfig(BaseModel):
    risk_per_trade_percent: float = 0.5
    min_risk_per_trade_percent: float = 0.1
    max_risk_per_trade_percent: float = 1.0
    sizing: SizingConfig = SizingConfig()
    guards: RiskGuards = RiskGuards()
    management: ManagementConfig = ManagementConfig()
    kill_switch: KillSwitchConfig = KillSwitchConfig()


# ── strategy_xauusd_m5.yaml ───────────────────────────────────
class StrategyEntryConfig(BaseModel):
    fast_ema_period: int = 9
    slow_ema_period: int = 21
    trend_ema_period: int = 50
    rsi_period: int = 14
    rsi_buy_max: float = 70
    rsi_sell_min: float = 30
    atr_period: int = 14
    max_atr_multiplier: float = 2.5
    candle_confirmation: bool = True


class StrategyOrderConfig(BaseModel):
    order_type: str = "market"
    direction: Literal["long", "short", "both"] = "both"
    sl_pips: float = 12
    tp_pips: float = 20
    sl_atr_multiplier: float = 1.5
    tp_atr_multiplier: float = 2.5
    use_atr_levels: bool = True
    pip_size: float = 0.1
    magic: int = 77001


class StrategyFilters(BaseModel):
    min_spread_points: int = 5
    max_spread_points: int = 30
    session: str = "full_xauusd"
    avoid_news_minutes: int = 5


# ── ICT / SMC strategy (Trading Strategy Specification V1) ─────
class ICTContextConfig(BaseModel):
    """Swing / liquidity structure parameters."""

    h1_bars: int = 1500
    h4_bars: int = 500
    swing_floor_atr: float = 0.25      # swing height must be >= floor x ATR(14) of its timeframe
    eq_tolerance_atr: float = 0.1      # equal-high/low pairing tolerance (x ATR)
    round_number_step: float = 10.0    # exclude levels near multiples of this price step
    round_number_tol: float = 0.05
    exclude_session_high_low: bool = True


class ICTDisplacementConfig(BaseModel):
    period: int = 20                   # average-body lookback for displacement
    body_mult: float = 1.5             # displacement body >= mult x avg body
    ob_lookback: int = 5               # last opposite-colored candle within N bars


class ICTTrendlineConfig(BaseModel):
    min_touches: int = 2
    prefer_touches: int = 3
    tolerance_atr: float = 0.15        # wick-touch tolerance (x ATR)
    max_swings: int = 30               # most recent swings considered for a line
    recent_bars: int = 48              # last touch must be within N H1 bars


class ICTSweepConfig(BaseModel):
    max_distance_points: float = 300.0  # H1 liquidity must be within this of price (0.01 units)
    fresh_bars: int = 12               # sweep must be within the last N M5 candles


class ICTConfirmationConfig(BaseModel):
    lookback_bars: int = 8             # confirmation close must occur within N M5 candles


class ICTManagementConfig(BaseModel):
    breakeven_at_r: float = 1.0        # move SL to breakeven at +1R
    early_exit_on_opposite_bos: bool = True


class ICTReentryConfig(BaseModel):
    max_attempts_per_level: int = 2    # 1 initial attempt + 1 re-entry


class ICTConfig(BaseModel):
    context: ICTContextConfig = ICTContextConfig()
    displacement: ICTDisplacementConfig = ICTDisplacementConfig()
    trendline: ICTTrendlineConfig = ICTTrendlineConfig()
    sweep: ICTSweepConfig = ICTSweepConfig()
    confirmation: ICTConfirmationConfig = ICTConfirmationConfig()
    sl_buffer_atr: float = 0.5         # SL = structural level +- 0.5 x M5 ATR(14)
    min_rr: float = 2.0                # skip the trade when next liquidity < 2R
    management: ICTManagementConfig = ICTManagementConfig()
    reentry: ICTReentryConfig = ICTReentryConfig()


# ── Merged EMA + trendline confluence strategy ────────────────
class TrendlineConfig(BaseModel):
    """Action Line / Safety Line confluence parameters.

    Signals require an M5 EMA cross AND an action-line trendline break on the
    same closed candle, both agreeing on direction. Entries have no fixed
    take-profit: the stop trails the opposing 'safety line' each management
    cycle and the trade exits when a candle closes beyond it.
    """

    timeframe: str = "H1"              # primary timeframe for the trendlines
    bars: int = 1500                   # candles requested from the provider
    swing_floor_atr: float = 0.25      # swing height must be >= floor x ATR(14) of its timeframe
    tolerance_atr: float = 0.15        # wick-touch / violation tolerance (x ATR)
    min_touches: int = 2               # segment must be touched at least this many times
    sl_buffer_atr: float = 0.5         # initial SL = safety line +- buffer x M5 ATR(14)
    bias_timeframes: list[str] = Field(default_factory=list)  # optional HTF bias gate


class StrategyConfig(BaseModel):
    id: str
    enabled: bool = True
    symbol: str
    timeframe: str
    description: str = ""
    entry: StrategyEntryConfig = StrategyEntryConfig()
    order: StrategyOrderConfig = StrategyOrderConfig()
    filters: StrategyFilters = StrategyFilters()
    success: dict[str, Any] | None = None
    ict: ICTConfig | None = None
    trendline: TrendlineConfig | None = None

    @model_validator(mode="after")
    def _check_management_exclusive(self) -> StrategyConfig:
        if self.ict is not None and self.trendline is not None:
            raise ValueError(
                f"strategy '{self.id}' declares both 'ict' and 'trendline'; "
                "only one management model is allowed"
            )
        return self


class StrategiesConfig(BaseModel):
    strategies: list[StrategyConfig] = Field(default_factory=list)


# ── environments.yaml ─────────────────────────────────────────
class InstrumentContract(BaseModel):
    """Broker-side contract metadata for one canonical instrument."""

    broker_symbol: str
    preferred: bool = False
    contract_size: float = 100.0    # units of the underlying per 1.0 lot
    point: float = 0.01
    digits: int = 2
    min_lot: float = 0.01
    lot_step: float = 0.01


class MT5EnvironmentConfig(BaseModel):
    """Per-environment MT5 connection: which env vars hold the secrets."""

    login_env: str = "MT5_LOGIN"
    password_env: str = "MT5_PASSWORD"
    server: str | None = None
    terminal_path_env: str = "MT5_TERMINAL_PATH"
    enable_launch_env: str = "MT5_ENABLE_TERMINAL_LAUNCH"

    def credentials(self) -> dict[str, Any]:
        """Resolve MT5 credentials for this environment from env vars."""
        return {
            "login": os.getenv(self.login_env, ""),
            "password": os.getenv(self.password_env, ""),
            "server": self.server or os.getenv("MT5_SERVER", "Exness-MT5"),
            "terminal_path": os.getenv(self.terminal_path_env, ""),
            "enable_launch": os.getenv(self.enable_launch_env, "true").lower() != "false",
        }


class EnvironmentConfig(BaseModel):
    """An environment profile: broker, symbol map, database, log paths.

    ``trading_enabled`` is the manual arm flag. Live profiles ship with it
    ``False``; it must be set to ``True`` (and the environment verified) before
    any real order can be placed.
    """

    name: str = ""
    description: str = ""
    broker_backend: Literal["mt5", "paper"] = "paper"
    trading_enabled: bool = False
    mt5: MT5EnvironmentConfig = MT5EnvironmentConfig()
    symbols: dict[str, InstrumentContract] = Field(default_factory=dict)
    database_url: str | None = None     # full override (takes precedence over database_name)
    database_name: str | None = None    # derives postgres URL when database_url absent
    log_dir: str | None = None
    data_dir: str | None = None


class EnvironmentsConfig(BaseModel):
    default: str = "development"
    environments: dict[str, EnvironmentConfig] = Field(default_factory=dict)

    def resolve(self, name: str | None = None) -> EnvironmentConfig:
        """Pick the active profile: explicit arg > MERCURY_ENV > ``default``."""
        key = name or os.getenv("MERCURY_ENV") or self.default
        env = self.environments.get(key)
        if env is None:
            raise ValueError(f"unknown environment '{key}' (known: {sorted(self.environments)})")
        return env.model_copy(update={"name": key})


# ── providers.yaml ────────────────────────────────────────────
class MT5ProviderConfig(BaseModel):
    symbol: str = "GOLD"
    timeframe: str = "M5"
    order_mode: str = "MARKET"
    magic: int = 77001
    slippage_points: int = 20


class BrokerConfig(BaseModel):
    backend: Literal["mt5", "paper"] = "paper"
    mt5: MT5ProviderConfig = MT5ProviderConfig()


class DataProviderConfig(BaseModel):
    backend: str = "mt5"
    mt5: dict[str, Any] = Field(default_factory=dict)


class EconomicCalendarConfig(BaseModel):
    source: str = "forex_factory"
    major_impact_only: bool = True
    blackout_minutes: int = 5


class RSSConfig(BaseModel):
    feeds: list[str] = Field(default_factory=list)


class SentimentConfig(BaseModel):
    source: str = "none"
    fear_greed_url: str = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"


class NewsProviderConfig(BaseModel):
    backends: list[str] = Field(default_factory=list)
    economic_calendar: EconomicCalendarConfig = EconomicCalendarConfig()
    rss: RSSConfig = RSSConfig()
    sentiment: SentimentConfig = SentimentConfig()


class ExternalLLMConfig(BaseModel):
    provider: Literal["openai", "anthropic", "lm_studio"] = "openai"
    openai: dict[str, Any] = Field(default_factory=dict)
    anthropic: dict[str, Any] = Field(default_factory=dict)
    lm_studio: dict[str, Any] = Field(default_factory=dict)


class LocalLLMConfig(BaseModel):
    provider: str = "ollama"
    timeout_seconds: int = 30


class StructuredLLMConfig(BaseModel):
    temperature: float = 0.2
    max_tokens: int = 1500


class ConfidenceConfig(BaseModel):
    min_gate: float = 0.55


class LLMProviderConfig(BaseModel):
    mode: Literal["hybrid", "local", "external", "none"] = "hybrid"
    external: ExternalLLMConfig = ExternalLLMConfig()
    local: LocalLLMConfig = LocalLLMConfig()
    structured: StructuredLLMConfig = StructuredLLMConfig()
    confidence: ConfidenceConfig = ConfidenceConfig()


class TelegramConfig(BaseModel):
    chat_id: str = ""
    disable_notification_for: list[str] = Field(default_factory=list)


class NotificationsConfig(BaseModel):
    backend: Literal["telegram"] = "telegram"
    telegram: TelegramConfig = TelegramConfig()


class WebhookConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 9100
    secret: str = ""


class SignalProviderConfig(BaseModel):
    providers: list[str] = Field(default_factory=lambda: ["internal", "tradingview"])
    webhook: WebhookConfig = WebhookConfig()


# ── Eden OS agent mesh (services/agent_mesh) ─────────────────
# Contract IDs mirror Eden action_layer/trading_mesh/trading_contracts.py.
# trading.kill_switch is a split pair there; trading.autonomous_live.* are
# Eden-local and never dispatched to agents, so they are not declared.
EDEN_TRADING_CAPABILITIES: list[str] = [
    "trading.list_proposals",
    "trading.approve",
    "trading.promote",
    "trading.promote_live",
    "trading.demote",
    "trading.kill_switch.enable",
    "trading.kill_switch.disable",
    "trading.stages",
    "trading.health",
    "trading.backtest",
]


class EdenAgentConfig(BaseModel):
    """Outbound-only Eden agent-mesh client (``AgentMeshService``).

    Environment-variable names deliberately mirror Eden's own
    ``agent_runtime/config.py`` so both repos read as one system (see
    ``.env.example``). The Ed25519 identity key is never configured via env
    or YAML — it is generated on first connect and persisted at
    ``{paths.data_dir}/eden/keys/{agent_id}_key``, and needs one-time owner
    approval on the Eden side before commands dispatch.
    """

    enabled: bool = False
    url: str = "ws://localhost:8765"
    agent_id: str = "mercury_trader"
    agent_name: str = "Mercury Trader"
    risk_tier: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"] = "HIGH"
    capabilities: list[str] = Field(default_factory=lambda: list(EDEN_TRADING_CAPABILITIES))
    heartbeat_interval: float = 30.0
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 60.0
    reconnect_backoff_factor: float = 2.0


class ProvidersConfig(BaseModel):
    broker: BrokerConfig = BrokerConfig()
    data: DataProviderConfig = DataProviderConfig()
    news: NewsProviderConfig = NewsProviderConfig()
    llm: LLMProviderConfig = LLMProviderConfig()
    notifications: NotificationsConfig = NotificationsConfig()
    signal: SignalProviderConfig = SignalProviderConfig()
    eden: EdenAgentConfig = EdenAgentConfig()


# ──────────────────────────────────────────────────────────────
# Aggregate settings + loader
# ──────────────────────────────────────────────────────────────
class Settings(BaseModel):
    base: BaseConfig = BaseConfig()
    trading_criteria: TradingCriteria = TradingCriteria()
    risk: RiskConfig = RiskConfig()
    strategies: StrategiesConfig = StrategiesConfig()
    providers: ProvidersConfig = ProvidersConfig()
    environment: EnvironmentConfig = EnvironmentConfig()

    # Env-backed values surfaced for convenience.
    database_url: str = "postgresql+psycopg://mercury:mercury@localhost:5432/mercury"
    deployment_mode_override: str | None = None

    @property
    def deployment_mode(self) -> str:
        return self.deployment_mode_override or self.base.deployment.mode


def redact_database_url(url: str) -> str:
    """Return a database URL with the password hidden, for logs/stdout.

    Uses SQLAlchemy's URL parser (``render_as_string(hide_password=True)``)
    so non-default URL shapes are handled correctly. Falls back to the raw
    string if the URL cannot be parsed.
    """
    from sqlalchemy.engine import make_url

    try:
        return make_url(url).render_as_string(hide_password=True)
    except Exception:  # noqa: BLE001
        return url


def _read_yaml(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"Config file must contain a mapping: {path}")
    return data


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def _load_strategy_entries(cfg_dir: Path) -> list[dict[str, Any]]:
    """Merge ``strategies:`` lists from every ``strategy_*.yaml`` file."""
    entries: list[dict[str, Any]] = []
    for path in sorted(cfg_dir.glob("strategy_*.yaml")):
        data = _read_yaml(path)
        entries.extend(data.get("strategies") or [])
    return entries


def load_config(config_dir: str | Path | None = None, *, environment: str | None = None) -> Settings:
    """Load and validate the full configuration.

    Looks for ``config/*.yaml`` relative to ``config_dir`` (default: repo root).
    Environment variables (see ``.env.example``) override selected fields.

    The active environment profile (see ``config/environments.yaml``) is picked
    from the ``--env`` argument, ``MERCURY_ENV``, ``base.yaml``'s
    ``environment:`` field, then ``environments.yaml``'s ``default:``. It
    selects broker backend/credentials, the symbol map, database, and log/data
    directories.
    """
    base_dir = Path(config_dir) if config_dir else Path(__file__).resolve().parents[3]
    cfg_dir = Path(base_dir) if (Path(base_dir) / "base.yaml").exists() else Path(base_dir) / "config"

    files = {
        "base": "base.yaml",
        "trading_criteria": "trading_criteria.yaml",
        "risk": "risk.yaml",
        "providers": "providers.yaml",
    }

    raw: dict[str, Any] = {}
    for key, fname in files.items():
        raw[key] = _read_yaml(cfg_dir / fname)

    # Strategies may be spread across multiple strategy_*.yaml files.
    raw["strategies"] = {"strategies": _load_strategy_entries(cfg_dir)}

    settings = Settings(
        base=BaseConfig.model_validate(raw["base"]),
        trading_criteria=TradingCriteria.model_validate(raw["trading_criteria"]),
        risk=RiskConfig.model_validate(raw["risk"]),
        strategies=StrategiesConfig.model_validate(raw["strategies"]),
        providers=ProvidersConfig.model_validate(raw["providers"]),
    )

    # ── Environment profile ──────────────────────────────────
    environments = EnvironmentsConfig.model_validate(_read_yaml(cfg_dir / "environments.yaml"))
    env_name = environment or os.getenv("MERCURY_ENV") or settings.base.environment
    env = environments.resolve(env_name)
    settings.environment = env
    settings.providers.broker.backend = env.broker_backend

    # Per-environment database (DATABASE_URL still wins when set).
    if env.database_url:
        db_url = env.database_url
    elif env.database_name:
        db_url = f"postgresql+psycopg://mercury:mercury@localhost:5432/{env.database_name}"
    else:
        db_url = f"postgresql+psycopg://mercury:mercury@localhost:5432/mercury_{env.name}"
    settings.database_url = os.getenv("DATABASE_URL") or db_url

    # Per-environment log/data directories (LOG_DIR/DATA_DIR still win).
    settings.base.paths.log_dir = env.log_dir or os.getenv("LOG_DIR") or f"logs/{env.name}"
    settings.base.paths.data_dir = env.data_dir or os.getenv("DATA_DIR") or f"data/{env.name}"

    # ── Environment overrides ────────────────────────────────
    mode = os.getenv("DEPLOYMENT_MODE")
    if mode:
        settings.deployment_mode_override = mode

    if os.getenv("TELEGRAM_CHAT_ID"):
        settings.providers.notifications.telegram.chat_id = os.getenv("TELEGRAM_CHAT_ID", "")

    port = _env_int("SIGNAL_WEBHOOK_PORT", settings.providers.signal.webhook.port)
    settings.providers.signal.webhook.port = port
    if os.getenv("SIGNAL_WEBHOOK_SECRET"):
        settings.providers.signal.webhook.secret = os.getenv("SIGNAL_WEBHOOK_SECRET", "")

    if os.getenv("HERMES_LLM_PROVIDER"):
        mode_ = os.getenv("HERMES_LLM_PROVIDER", "")
        if mode_ in {"hybrid", "local", "external", "none"}:
            settings.providers.llm.mode = cast(Literal["hybrid", "local", "external", "none"], mode_)

    if os.getenv("HERMES_EXTERNAL_PROVIDER"):
        ext_provider = os.getenv("HERMES_EXTERNAL_PROVIDER", "")
        if ext_provider in {"openai", "anthropic", "lm_studio"}:
            settings.providers.llm.external.provider = cast(
                Literal["openai", "anthropic", "lm_studio"], ext_provider
            )

    # ── Eden OS agent mesh ───────────────────────────────────
    # Same variable names as Eden's agent_runtime/config.py (from_env).
    # EDEN_AGENT_ENABLED is Mercury-side only: the integration stays off
    # until explicitly turned on.
    eden = settings.providers.eden
    if os.getenv("EDEN_AGENT_ENABLED") is not None:
        eden.enabled = os.getenv("EDEN_AGENT_ENABLED", "").strip().lower() in ("1", "true", "yes")
    if os.getenv("EDEN_URL"):
        eden.url = os.getenv("EDEN_URL", "").strip()
    if os.getenv("EDEN_AGENT_ID"):
        eden.agent_id = os.getenv("EDEN_AGENT_ID", "").strip()
    if os.getenv("EDEN_AGENT_NAME"):
        eden.agent_name = os.getenv("EDEN_AGENT_NAME", "")
    tier = (os.getenv("EDEN_AGENT_RISK_TIER") or "").strip().upper()
    if tier in ("LOW", "MEDIUM", "HIGH", "CRITICAL"):
        eden.risk_tier = cast(Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"], tier)
    if os.getenv("EDEN_AGENT_CAPABILITIES"):
        eden.capabilities = [c.strip() for c in os.getenv("EDEN_AGENT_CAPABILITIES", "").split(",") if c.strip()]
    for env_name, attr, _default in (
        ("EDEN_HEARTBEAT_INTERVAL", "heartbeat_interval", 30.0),
        ("EDEN_RECONNECT_BASE_DELAY", "reconnect_base_delay", 1.0),
        ("EDEN_RECONNECT_MAX_DELAY", "reconnect_max_delay", 60.0),
        ("EDEN_RECONNECT_BACKOFF_FACTOR", "reconnect_backoff_factor", 2.0),
    ):
        raw = os.getenv(env_name)
        if raw:
            setattr(eden, attr, float(raw))

    return settings
