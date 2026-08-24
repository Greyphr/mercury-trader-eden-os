# Mercury Trader

An adaptive, self-improving intelligent trading bot/agent built on a modular,
service-based architecture. It runs 24/7, collects market data and news,
reasons over everything through a dedicated reasoning engine (Hermes),
executes risk-managed trades on MetaTrader 5 (Exness, XAU/USD M5), and
continuously learns from its own performance.

> **Status: foundation scaffold.** The full architecture is in place and runs
> in *paper* mode out of the box. Live trading requires completing the
> configuration steps below and passing the validation pipeline.

---

## Architecture at a glance

```
                         ┌───────────────────────────────────────┐
                         │        ORCHESTRATOR (scheduler)       │
                         └──────┬────────────────────────────────┘
                                │  in-process EVENT BUS
   DataCollector  NewsService  StrategyEngine  SignalService  Hermes
   (MT5 candles)  (news/sent.) (rule strategies)(TV webhooks) (reasoning)
        │             │              │              │             │
   ┌────┴─────────────┴──────────────┴──────────────┴──────┬──────┘
   │                RiskManager (guards + sizing)           │
   │                ExecutionService (MT5/paper orders)     │
   │                LearningService (ledger + proposals)    │
   │                AnalyticsService (metrics snapshots)    │
   │                NotificationService (Telegram)          │
   └─────────────────────────────────────────────────────────┘
```

### Key principles
- **Hermes never modifies the live strategy.** It proposes improvements that
  flow through `backtest → human approval → paper → human approval → live`.
- **Clean architecture**: every external system (broker, LLM, news, notifier)
  sits behind an interface, so MT5/OpenAI/Telegram/etc. are swappable.
- **Everything is persisted**: signals, trades, reasoning outputs, proposals,
  news, and metric snapshots land in PostgreSQL for future analysis.

### Services
| Service | Responsibility |
|---|---|
| `data.collector` | Polls MT5/paper provider for quotes + candles |
| `news.collector` | Collects RSS, calendar, and sentiment events |
| `strategy.engine` | Runs configured rule strategies on closed candles |
| `signal` | Validates signals from internal strategies + TradingView webhooks |
| `hermes` | Pre-trade confidence, post-trade reviews, daily analysis, proposals |
| `risk` | Guard checks + fixed-% position sizing + kill switch |
| `execution` | Order routing (MT5 or paper) + position monitoring |
| `learning` | Trade ledger, win-vs-loss analysis, proposal backtests |
| `analytics` | Performance metrics snapshots |
| `notifications` | Telegram alerts + daily/weekly/monthly reports |

---

## Project layout

```
config/                 YAML trading criteria, risk, strategy, providers
src/mercury/
  core/                 config, logging, event bus, database, validation
  models/               Pydantic schemas + SQLAlchemy ORM
  services/             data, news, strategy, signal, hermes, risk,
                        execution, learning, analytics, backtest, notifications
  orchestrator/         wiring, scheduling, lifecycle, health
  main.py               CLI entrypoint
scripts/                VPS / MT5 / DB / service install scripts
tests/                  pytest suite
```

---

## Quick start (development)

```powershell
# 1. Create virtual environment and install dependencies
python -m venv .venv
.\.venv\Scripts\pip install -r requirements\core.txt -r requirements\dev.txt

# 2. Copy and edit environment config
Copy-Item .env.example .env   # set DATABASE_URL, Telegram, LLM keys

# 3. Create the PostgreSQL database
.\scripts\init_db.ps1          # (or create mercury db/role manually)

# 4. Verify configuration loads
.\.venv\Scripts\python -m mercury.main health

# 5. Run in paper mode (default) — safe, no real orders
.\.venv\Scripts\python -m mercury.main run
```

Without a configured PostgreSQL, the system still starts in paper mode but the
database steps will fail on startup (`create_tables`). A local PostgreSQL is
recommended; `DATABASE_URL` supports any SQLAlchemy URL.

---

## Going live (Exness + MT5)

> The `MetaTrader5` Python package (in `requirements/core.txt`, marked
> `sys_platform == "win32"`) is installed automatically on Windows; on other
> dev machines pip skips it and MT5 backends stay unavailable (paper broker
> still works).

1. **Windows VPS**: run `scripts/setup_vps.ps1` (Python, PostgreSQL, deps).
2. **MT5 terminal**: run `scripts/setup_mt5.ps1`, then in the terminal:
   - log in to your Exness **live** account,
   - add `XAUUSD` to Market Watch,
   - enable **Algo Trading** (Tools → Options → Expert Advisors → *Allow algorithmic trading*).
3. **Environment**: set in `.env`:
   - `MT5_LOGIN`, `MT5_PASSWORD`, `MT5_SERVER`, `MT5_TERMINAL_PATH`
   - `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`
   - LLM keys for Hermes (optional — rule-based fallback otherwise)
4. **Switch to live**: set `deployment.mode: live` in `config/base.yaml` and
   broker backend `mt5` in `config/providers.yaml`. **Start on demo first.**
5. **24/7**: run `scripts/install_service.ps1` (NSSM Windows service).

---

## Trading criteria & risk configuration

Everything is declarative in `config/` and Pydantic-validated at startup:

- `trading_criteria.yaml` — TP/SL minimums, success criteria, evaluation
  metrics, promotion gates (paper → live thresholds).
- `risk.yaml` — risk % per trade, sizing, max positions, daily/drawdown
  guards, spread filter, news blackout, kill switch.
- `strategy_xauusd_m5.yaml` — the M5 XAUUSD strategy parameters (EMA/RSI/ATR).
- `providers.yaml` — broker, data, news, LLM, notifications, webhook.

---

## Hermes improvement pipeline

1. Hermes runs daily analysis → emits structured `proposals`.
2. `learning` backtests each proposal on 10k historical candles.
3. Backtest results are published to Telegram; status → `awaiting_human`.
4. A human reviews and approves:
   ```powershell
   .\.venv\Scripts\python -m mercury.main proposals
   .\.venv\Scripts\python -m mercury.main approve 3          # → paper
   .\.venv\Scripts\python -m mercury.main approve 3 --live   # → live
   ```
5. Hermes never applies anything directly.

---

## CLI reference

```powershell
python -m mercury.main run                  # start the bot
python -m mercury.main health               # config summary
python -m mercury.main backtest             # backtest current strategy
python -m mercury.main proposals            # list proposals awaiting review
python -m mercury.main approve <id> [--live]
python -m mercury.main kill-switch on|off
```

---

## Roadmap (post-scaffold)

- [ ] Human-approval management UI / bot commands (approve via Telegram)
- [ ] Forward-testing / paper promotion automation
- [ ] TradingView webhook signal schema documentation
- [ ] Additional news adapters (Myfxbook, TradingEconomics)
- [ ] Redis-backed event bus for multi-process scale-out
- [ ] alembic migrations for schema evolution
