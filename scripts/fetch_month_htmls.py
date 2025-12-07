"""
Fetch all race result HTMLs for a given year/month via past-links traversal.

Usage:
    python scripts/fetch_month_htmls.py 2024 05 [--overwrite]
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import List

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.jra_past_links import get_srl_cnames, get_sde_cnames_from_srl
from scripts.fetch_jra_html import fetch_race_html, cname_to_race_id

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def _append_fail(fail_log: Path | None, cname: str) -> None:
    if fail_log is None:
        return
    fail_log.parent.mkdir(parents=True, exist_ok=True)
    with fail_log.open("a", encoding="utf-8") as f:
        f.write(cname + "\n")


def fetch_month(year: int, month: int, overwrite: bool = False, fail_log: Path | None = None) -> None:
    srl_list = get_srl_cnames(year, month)
    if not srl_list:
        logger.warning("No srl cnames found for %04d-%02d", year, month)
        return

    total_sde = 0
    saved = 0
    for srl in srl_list:
        sde_list: List[str] = get_sde_cnames_from_srl(srl)
        if not sde_list:
            continue
        for sde in sde_list:
            total_sde += 1
            try:
                race_id = cname_to_race_id(sde)
                path = fetch_race_html(race_id=race_id, cname=sde, overwrite=overwrite)
                if path:
                    saved += 1
            except Exception as exc:  # noqa: BLE001
                logger.warning("Failed fetch cname=%s err=%s", sde, exc)
                _append_fail(fail_log, sde)
    logger.info("Month fetch done %04d-%02d: sde_total=%s saved=%s", year, month, total_sde, saved)


def main() -> None:
    ap = argparse.ArgumentParser(description="Fetch all race HTMLs for a given year/month.")
    ap.add_argument("year", type=int, help="Year (e.g., 2024)")
    ap.add_argument("month", type=int, help="Month (1-12)")
    ap.add_argument("--overwrite", action="store_true", help="Overwrite existing race_<race_id>.html")
    ap.add_argument("--fail-log", help="File path to append failed CNAMEs")
    args = ap.parse_args()
    fail_path = Path(args.fail_log) if args.fail_log else None
    fetch_month(args.year, args.month, overwrite=args.overwrite, fail_log=fail_path)


if __name__ == "__main__":
    main()
