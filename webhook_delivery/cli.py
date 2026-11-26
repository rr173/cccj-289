"""Command-line operations for topic withdrawal and ticket evidence holds.

This is the supported way to use the new capability on an **existing**
``webhook_delivery.db`` file.  Every command calls ``Store.initialize()``
first, which performs the additive migration (adds ``events.topic_id`` /
``events.status`` / ``attempts.send_started_at`` and creates the new tables).
Old events keep ``topic_id = NULL`` and need no backfill.

Examples
--------
::

    export WEBHOOK_DB=./webhook_delivery.db

    # Stop all use of a topic (idempotent; safe to repeat)
    python -m webhook_delivery withdraw-topic --customer cus_acme --topic gdpr-42

    # Investigation ticket that must preserve specific received events
    python -m webhook_delivery create-ticket --customer cus_acme --title "case 4821"
    python -m webhook_delivery retain --ticket tkt_... --event evt_... --event evt_...
    python -m webhook_delivery show-ticket --ticket tkt_...
    python -m webhook_delivery list-evidence --ticket tkt_...
    python -m webhook_delivery close-ticket --ticket tkt_...

    # Inspection
    python -m webhook_delivery list-events --customer cus_acme --topic gdpr-42
    python -m webhook_delivery list-topics --customer cus_acme
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from typing import Any, Sequence

from .config import Settings
from .store import Store
from .util import DomainError


def _print(value: Any) -> None:
    print(json.dumps(value, indent=2, ensure_ascii=False, sort_keys=False))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m webhook_delivery",
        description="Topic withdrawal and ticket evidence operations.",
    )
    parser.add_argument("--db", default=os.environ.get("WEBHOOK_DB", "webhook_delivery.db"))

    # Lets ``--db`` also appear after the subcommand without overwriting a
    # value given before it (SUPPRESS leaves the parsed namespace alone).
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--db", default=argparse.SUPPRESS, help="SQLite database path")

    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-db", parents=[common], help="Create/migrate the schema and exit")
    p_init.set_defaults(op="init-db")

    p_w = sub.add_parser("withdraw-topic", parents=[common], help="Irreversibly withdraw a customer-scoped topic")
    p_w.add_argument("--customer", required=True)
    p_w.add_argument("--topic", required=True)
    p_w.add_argument("--wait", action="store_true", help="wait for this topic's in-flight sends")
    p_w.add_argument("--wait-timeout", type=float, default=5.0)
    p_w.set_defaults(op="withdraw-topic")

    p_lt = sub.add_parser("list-topics", parents=[common])
    p_lt.add_argument("--customer", required=True)
    p_lt.set_defaults(op="list-topics")

    p_ct = sub.add_parser("create-ticket", parents=[common])
    p_ct.add_argument("--customer", required=True)
    p_ct.add_argument("--title")
    p_ct.add_argument("--reason")
    p_ct.set_defaults(op="create-ticket")

    p_st = sub.add_parser("show-ticket", parents=[common])
    p_st.add_argument("--ticket", required=True)
    p_st.add_argument("--customer")
    p_st.set_defaults(op="show-ticket")

    p_re = sub.add_parser("retain", parents=[common], help="Retain one or more event ids on a ticket")
    p_re.add_argument("--ticket", required=True)
    p_re.add_argument("--customer")
    p_re.add_argument("--event", action="append", required=True, dest="events")
    p_re.set_defaults(op="retain")

    p_le = sub.add_parser("list-evidence", parents=[common])
    p_le.add_argument("--ticket", required=True)
    p_le.add_argument("--customer")
    p_le.set_defaults(op="list-evidence")

    p_cl = sub.add_parser("close-ticket", parents=[common])
    p_cl.add_argument("--ticket", required=True)
    p_cl.add_argument("--customer")
    p_cl.set_defaults(op="close-ticket")

    p_ev = sub.add_parser("list-events", parents=[common])
    p_ev.add_argument("--customer")
    p_ev.add_argument("--flow")
    p_ev.add_argument("--topic")
    p_ev.add_argument("--limit", type=int, default=100)
    p_ev.set_defaults(op="list-events")

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    settings = Settings(database_path=args.db)
    store = Store(settings)
    # Always run the additive migration so the CLI works on an old database.
    store.initialize()

    try:
        if args.op == "init-db":
            _print({"database": args.db, "initialized": True})
        elif args.op == "withdraw-topic":
            _print(
                store.withdraw_topic(
                    customer_id=args.customer,
                    topic_id=args.topic,
                    wait_for_inflight=args.wait,
                    inflight_timeout=args.wait_timeout,
                )
            )
        elif args.op == "list-topics":
            _print(store.list_topics(customer_id=args.customer))
        elif args.op == "create-ticket":
            _print(store.create_ticket(customer_id=args.customer, title=args.title, reason=args.reason))
        elif args.op == "show-ticket":
            _print(store.get_ticket(args.ticket, customer_id=args.customer))
        elif args.op == "retain":
            _print(
                store.retain_events(
                    ticket_id=args.ticket, event_ids=args.events, customer_id=args.customer
                )
            )
        elif args.op == "list-evidence":
            _print(store.list_ticket_events(args.ticket, customer_id=args.customer))
        elif args.op == "close-ticket":
            _print(store.close_ticket(args.ticket, customer_id=args.customer))
        elif args.op == "list-events":
            _print(
                store.list_events(
                    customer_id=args.customer,
                    flow_id=args.flow,
                    topic_id=args.topic,
                    limit=args.limit,
                )
            )
        else:  # pragma: no cover - argparse guarantees a command
            print(f"unknown command: {args.command}", file=sys.stderr)
            return 2
        return 0
    except DomainError as exc:
        print(json.dumps({"error": {"code": exc.code, "message": exc.message}}), file=sys.stderr)
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
