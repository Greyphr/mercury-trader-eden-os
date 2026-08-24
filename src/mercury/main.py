"""Mercury Trader CLI entrypoint.

Commands:
    run                       Start the 24/7 trading orchestrator
    health                    Print service config summary and exit
    backtest --strategy ID    Run a backtest of the configured strategy
    proposals                 List Hermes proposals awaiting review
    approve PROPOSAL_ID [--live]   Human approval gate for a proposal
    promote STRATEGY_ID --to STAGE   Advance a strategy one lifecycle stage
    demote STRATEGY_ID --to STAGE    Roll a strategy back a stage
    stages                    Show lifecycle stage of each strategy
    kill-switch on|off        Toggle the global kill switch
    enroll                    Register this machine's identity with Eden
                              (one-shot; prints pairing status and exits)

Options:
    --env NAME                Environment profile (config/environments.yaml);
                              overrides MERCURY_ENV and base.yaml `environment:`.
"""

from __future__ import annotations

import argparse
import json
import sys

from mercury.core.logging import get_logger

logger = get_logger("cli")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mercury", description="Mercury Trader bot/agent")
    parser.add_argument("--env", default=None, help="environment profile (e.g. development, metaquotes_demo, exness_live)")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="run the trading orchestrator 24/7")
    sub.add_parser("health", help="show configuration summary")

    bt = sub.add_parser("backtest", help="backtest a configured strategy")
    bt.add_argument("--strategy", default="xauusd_m5_trend")
    bt.add_argument("--bars", type=int, default=10000)

    proposals = sub.add_parser("proposals", help="list Hermes proposals")
    proposals.add_argument("--all", action="store_true", help="show proposals in all statuses")

    approve = sub.add_parser("approve", help="approve a proposal (human gate)")
    approve.add_argument("proposal_id", type=int)
    approve.add_argument("--live", action="store_true", help="promote to live stage instead of paper")

    ks = sub.add_parser("kill-switch", help="toggle the global kill switch")
    ks.add_argument("state", choices=["on", "off"])

    promote = sub.add_parser("promote", help="advance a strategy one lifecycle stage")
    promote.add_argument("strategy_id", help="strategy id (e.g. xauusd_m5_trend)")
    promote.add_argument("--to", dest="to", required=True, choices=["paper", "demo", "review", "approved", "live"])
    promote.add_argument("--actor", default="cli", help="who is promoting (human name for approved/live)")
    promote.add_argument("--reason", default="", help="reason for the promotion")
    promote.add_argument("--metrics", default=None, help='JSON metrics to check: {"trades":55,"win_rate":0.5,...}')
    promote.add_argument("--check-gates", action="store_true", help="reject if metrics fail configured promotion gates")

    demote = sub.add_parser("demote", help="roll a strategy back a lifecycle stage")
    demote.add_argument("strategy_id", help="strategy id (e.g. xauusd_m5_trend)")
    demote.add_argument("--to", dest="to", required=True, choices=["draft", "paper", "demo", "review", "approved"])
    demote.add_argument("--actor", default="cli", help="who is demoting")
    demote.add_argument("--reason", default="", help="reason for the demotion")

    stages = sub.add_parser("stages", help="show lifecycle stage of each strategy")
    stages.add_argument("--strategy", default=None, help="show only this strategy")
    stages.add_argument("--history", action="store_true", help="also print promotion audit history")

    sub.add_parser(
        "enroll",
        help="one-shot Eden agent registration: generate/persist the Ed25519 "
        "identity, connect out to Eden, print pairing status, exit",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = _build_parser().parse_args(argv)

    if args.command == "run":
        from mercury.orchestrator.orchestrator import main as run_bot

        run_bot(environment=args.env)
        return

    if args.command == "health":
        from mercury.core.config import load_config, redact_database_url

        settings = load_config(environment=args.env)
        env = settings.environment
        print(f"project: {settings.base.project.name} v{settings.base.project.version}")
        print(f"deployment mode: {settings.deployment_mode}")
        print(f"environment: {env.name} ({env.description})")
        print(f"trading enabled: {env.trading_enabled}")
        print(f"symbol map: {[f'{c} -> {s.broker_symbol}' for c, s in env.symbols.items()]}")
        print(f"database: {redact_database_url(settings.database_url)}")
        print(f"log dir: {settings.base.paths.log_dir}")
        print(f"broker backend: {settings.providers.broker.backend}")
        print(f"llm mode: {settings.providers.llm.mode}")
        print(f"notifications: {settings.providers.notifications.backend}")
        print(f"strategies: {[s.id for s in settings.strategies.strategies if s.enabled]}")
        return

    from mercury.core.config import load_config
    from mercury.core.db import Database

    settings = load_config(environment=args.env)
    db = Database.from_settings(settings)
    db.create_tables()

    if args.command == "backtest":
        _cli_backtest(settings, db, args)
        return

    if args.command == "proposals":
        _cli_proposals(db, args)
        return

    if args.command == "approve":
        _cli_approve(db, settings, args)
        return

    if args.command == "kill-switch":
        _cli_kill_switch(db, settings, args)
        return

    if args.command == "promote":
        _cli_promote(db, settings, args)
        return

    if args.command == "demote":
        _cli_demote(db, settings, args)
        return

    if args.command == "stages":
        _cli_stages(db, settings, args)
        return

    if args.command == "enroll":
        _cli_enroll(settings)
        return


def _cli_enroll(settings) -> None:
    """One-shot Eden registration (doc §5): ensure identity → connect out →
    register → print pairing status → exit. Never starts the background loop.
    """
    from mercury.services.agent_mesh.client import MercuryAgentClient
    from mercury.services.agent_mesh.config import AgentMeshConfig
    from mercury.services.agent_mesh.crypto import load_or_generate_keypair
    from mercury.services.agent_mesh.handlers import MercuryCommandHandler

    config = AgentMeshConfig.from_settings(settings)
    keypair = load_or_generate_keypair(config.key_dir_for(settings), config.agent_id)
    handler = MercuryCommandHandler(settings=settings)
    client = MercuryAgentClient(config=config, keypair=keypair, handlers=handler.handlers)

    print(f"enrolling with Eden at {config.url} as '{config.agent_id}'")
    print(f"public key: {keypair.public_key_b64}")
    try:
        registered = client.enroll_once()
    except Exception as exc:  # noqa: BLE001 — CLI: report and exit nonzero
        print(f"enrollment failed: {exc}", file=sys.stderr)
        sys.exit(1)
    print(f"session: {registered.session_id or '-'}")
    print(f"paired: {str(registered.paired).lower()}")
    print(f"allowed capabilities: {registered.allowed_caps}")
    if registered.rejected_caps:
        print(f"rejected capabilities: {registered.rejected_caps}")
    if not registered.paired:
        print(
            "NOT paired yet — approve this identity on the Eden side once; "
            "trading.* commands will be rejected until then."
        )
        sys.exit(2)


def _cli_backtest(settings, db, args) -> None:
    from mercury.core.validation import Candle
    from mercury.services.backtest.engine import build_strategy_for_backtest, run_backtest
    from mercury.services.data.historical import load_history

    strategy_cfg = next((s for s in settings.strategies.strategies if s.id == args.strategy), None)
    if strategy_cfg is None:
        logger.error("strategy not found", extra={"strategy": args.strategy})
        print(f"strategy not found: {args.strategy}")
        sys.exit(1)
    candles_raw = load_history(settings, strategy_cfg.symbol, strategy_cfg.timeframe, count=args.bars)
    candles = [Candle.model_validate(c) for c in candles_raw]
    strategy = build_strategy_for_backtest(strategy_cfg, settings)
    result = run_backtest(
        strategy,
        candles,
        risk_percent=settings.risk.risk_per_trade_percent,
        contract_size=settings.risk.sizing.contract_size,
    )
    print(json.dumps(result.to_result(), indent=2))


def _cli_proposals(db, args) -> None:
    from sqlalchemy import select

    from mercury.models.orm import ProposalRecord

    with db.session() as session:
        query = select(ProposalRecord).order_by(ProposalRecord.created_at.desc())
        if not args.all:
            query = query.where(ProposalRecord.status == "awaiting_human")
        for p in session.scalars(query).all():
            print(f"#{p.id} [{p.status}] {p.hypothesis[:80]}")


def _cli_approve(db, settings, args) -> None:
    from mercury.core.events import EventBus
    from mercury.services.learning.service import LearningService

    svc = LearningService(bus=EventBus(), settings=settings, db=db)
    stage = "live" if args.live else "paper"
    if svc.approve_proposal(args.proposal_id, stage=stage):
        print(f"proposal #{args.proposal_id} approved for {stage}")
    else:
        logger.error("proposal not found or not awaiting approval", extra={"proposal_id": args.proposal_id})
        print(f"proposal #{args.proposal_id} not found or not awaiting approval")
        sys.exit(1)


def _cli_kill_switch(db, settings, args) -> None:
    from mercury.core.events import EventBus
    from mercury.services.risk.service import RiskManagerService

    svc = RiskManagerService(bus=EventBus(), settings=settings, db=db)
    svc.set_kill_switch(args.state == "on")
    print(f"kill switch: {args.state}")


def _promotion_service(db, settings):
    from mercury.core.events import EventBus
    from mercury.services.promotion.service import PromotionService

    return PromotionService(bus=EventBus(), settings=settings, db=db)


def _cli_promote(db, settings, args) -> None:
    from mercury.services.promotion.service import PromotionError

    svc = _promotion_service(db, settings)
    metrics = json.loads(args.metrics) if args.metrics else None
    try:
        svc.promote(
            args.strategy_id,
            args.to,
            actor=args.actor,
            reason=args.reason,
            metrics=metrics,
            check_gates=args.check_gates,
        )
    except (PromotionError, ValueError) as exc:
        logger.error("promotion failed", extra={"strategy": args.strategy_id, "error": str(exc)})
        print(f"error: {exc}")
        sys.exit(1)
    print(f"{args.strategy_id}: {svc.get_stage(args.strategy_id).value}")


def _cli_demote(db, settings, args) -> None:
    from mercury.services.promotion.service import PromotionError

    svc = _promotion_service(db, settings)
    try:
        svc.demote(args.strategy_id, args.to, actor=args.actor, reason=args.reason)
    except (PromotionError, ValueError) as exc:
        logger.error("demotion failed", extra={"strategy": args.strategy_id, "error": str(exc)})
        print(f"error: {exc}")
        sys.exit(1)
    print(f"{args.strategy_id}: {svc.get_stage(args.strategy_id).value}")


def _cli_stages(db, settings, args) -> None:
    svc = _promotion_service(db, settings)
    env = settings.environment.name
    required = svc.required_stage(env)
    strategies = (
        [args.strategy]
        if args.strategy
        else [s.id for s in settings.strategies.strategies if s.enabled]
    )
    for sid in strategies:
        stage = svc.get_stage(sid).value
        flag = "OK" if svc.may_trade_in_env(sid)[0] else "blocked"
        print(f"{sid}: {stage} (required for '{env}': {required.value}) [{flag}]")
        if args.history:
            for entry in svc.history(sid, limit=10):
                print(f"    {entry['created_at']} {entry['from_stage']} -> {entry['to_stage']} "
                      f"by {entry['actor']} - {entry['reason']}")


if __name__ == "__main__":
    main()
