"""
Run ETL for one race HTML (v4 schema).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, List

# add project root and src to sys.path
ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from etl_common import logger  # noqa: E402
from parse_jra_race import parse_race_html  # noqa: E402
from load_to_sqlite_v4 import load_race_to_db  # noqa: E402


def run_etl_for_one_race(html_path: Path | str, db_path: str) -> None:
    """Run ETL for a single race HTML file."""
    html_path = Path(html_path)
    logger.info("ETL start: html=%s", html_path)

    race_dict: Dict[str, Any]
    results_list: List[Dict[str, Any]]
    horses_dict: Dict[str, Any]
    jockeys_dict: Dict[str, Any]
    trainers_dict: Dict[str, Any]
    race_dict, results_list, horses_dict, jockeys_dict, trainers_dict = parse_race_html(html_path)
    logger.info(
        "Transform done: races=1, race_results=%d, horses=%d, jockeys=%d, trainers=%d",
        len(results_list),
        len(horses_dict),
        len(jockeys_dict),
        len(trainers_dict),
    )

    load_race_to_db(
        race_dict,
        results_list,
        horses_dict,
        jockeys_dict,
        trainers_dict,
        db_path=db_path,
    )
    logger.info("Load done: db_path=%s", db_path)
    logger.info("ETL completed successfully: race_id=%s", race_dict.get("race_id"))


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ETL for one JRA race (v4 schema) from HTML file.")
    parser.add_argument("html", help="Path to saved race HTML")
    parser.add_argument(
        "--db",
        type=str,
        default="data/keiba.db",
        help='Path to SQLite DB (default: "data/keiba.db").',
    )
    args = parser.parse_args()

    try:
        run_etl_for_one_race(args.html, db_path=args.db)
    except Exception:
        logger.exception("ETL failed for html=%s", args.html)
        sys.exit(1)


if __name__ == "__main__":
    main()
