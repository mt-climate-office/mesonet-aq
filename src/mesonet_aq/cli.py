"""`mesonet-aq` entry point (the container's ENTRYPOINT).

    mesonet-aq run [--since YYYY-MM-DD] [--station ID ...]
    mesonet-aq migrate [--station ID ...]
    mesonet-aq rebuild-derived

Exit status is non-zero on any error, and the success heartbeat is only
published on a clean run, so a partial failure still pages via the dead-man
alarm the next morning.
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date

from . import cdn
from .config import Settings
from .migrate import migrate
from .pipeline import rebuild_derived, run


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="mesonet-aq")
    sub = p.add_subparsers(dest="cmd", required=True)
    r = sub.add_parser("run", help="nightly fetch + publish")
    r.add_argument(
        "--since",
        type=date.fromisoformat,
        help="re-fetch from this UTC date (backfill/repair)",
    )
    r.add_argument(
        "--station", action="append", help="limit to station id (repeatable)"
    )
    m = sub.add_parser("migrate", help="convert the legacy station-day tree")
    m.add_argument("--station", action="append")
    sub.add_parser("rebuild-derived", help="recompute hourly/latest/manifest from raw")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s"
    )
    settings = Settings.from_env()
    log = logging.getLogger("mesonet_aq")
    log.info("store=%s prefix=%s", settings.store, settings.prefix)

    if args.cmd == "run":
        res = run(settings, since=args.since, stations=args.station)
        log.info(
            "run: %d day(s), %d row(s), %d error(s)",
            res.fetched_days,
            res.rows,
            len(res.errors),
        )
        ok = not res.errors
    elif args.cmd == "migrate":
        report = migrate(settings, stations=args.station)
        ok = not report["mismatches"]
    else:
        rebuild_derived(settings)
        ok = True

    # Only the scheduled nightly run feeds the dead-man alarm.
    if ok and args.cmd == "run" and not args.station and settings.metric_namespace:
        cdn.heartbeat(settings.metric_namespace)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
