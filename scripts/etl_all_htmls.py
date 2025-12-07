"""
Batch ETL for all saved race HTML files (data/raw/jra/race_*.html).

Usage:
    python scripts/etl_all_htmls.py --db data/keiba.db [--limit 100] [--start-after RACEID]
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

from scripts.etl_one_race_v4 import run_etl_for_one_race

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def find_race_ids(start_after: str | None = None) -> List[str]:
    html_dir = Path("data/raw/jra")
    race_ids: List[str] = []
    for path in sorted(html_dir.glob("race_*.html")):
        stem = path.stem  # race_<race_id>
        if not stem.startswith("race_"):
            continue
        race_id = stem.replace("race_", "", 1)
        if start_after and race_id <= start_after:
            continue
        if len(race_id) == 12 and race_id.isdigit():
            race_ids.append(race_id)
    return race_ids


def main() -> None:
    ap = argparse.ArgumentParser(description="Batch ETL all saved race HTMLs.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path (default: data/keiba.db)")
    ap.add_argument("--limit", type=int, help="Process only the first N race_ids")
    ap.add_argument("--start-after", help="Skip race_ids <= this value (useful for resume)")
    ap.add_argument("--fail-log", help="Path to write failed race_ids (appended)", default=None)
    args = ap.parse_args()

    race_ids = find_race_ids(start_after=args.start_after)
    if args.limit:
        race_ids = race_ids[: args.limit]

    total = len(race_ids)
    if total == 0:
        logger.info("No race_*.html files found under data/raw/jra")
        return

    ok = 0
    fail = 0
    failed_ids: List[str] = []
    for idx, race_id in enumerate(race_ids, 1):
        try:
            logger.info("(%s/%s) ETL race_id=%s", idx, total, race_id)
            run_etl_for_one_race(race_id, db_path=args.db)
            ok += 1
        except Exception:
            logger.exception("Failed ETL race_id=%s", race_id)
            fail += 1
            failed_ids.append(race_id)
    if failed_ids and args.fail_log:
        Path(args.fail_log).parent.mkdir(parents=True, exist_ok=True)
        with open(args.fail_log, "a", encoding="utf-8") as f:
            for rid in failed_ids:
                f.write(rid + "\n")
        logger.info("Failed race_ids written to %s", args.fail_log)
    logger.info("Batch ETL done: total=%s success=%s fail=%s", total, ok, fail)
    if fail:
        sys.exit(1)


if __name__ == "__main__":
    main()
