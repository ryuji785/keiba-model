"""
Extract-only driver for a given date (YYYYMMDD) using past-links traversal.

Flow:
 1. Given a target date (YYYYMMDD), call get_sde_cnames_for_date(date)
    to obtain all race-result CNAMEs (pw01sde...).
 2. For each CNAME, fetch the race result HTML via fetch_race_html().
 3. Save HTML under data/raw/jra/race_html/.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# add project root to sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from etl_common import logger  # noqa: E402
from scripts.jra_past_links import get_sde_cnames_for_date  # noqa: E402
from scripts.fetch_jra_race_html import fetch_race_html  # noqa: E402


def extract_one_date(date_yyyymmdd: str, overwrite_html: bool = False) -> None:
    """
    Extract all race result HTMLs for a given date (YYYYMMDD).

    Parameters
    ----------
    date_yyyymmdd : str
        Target date (YYYYMMDD).
    overwrite_html : bool, optional
        If True, re-fetch and overwrite existing HTML files.
    """
    print(f"[extract_one_date] === START date={date_yyyymmdd} ===")
    logger.info("extract_one_date START date=%s", date_yyyymmdd)

    # ---- CNAME一覧取得（過去レース結果ページ経由）----
    print(f"[extract_one_date] resolving sde CNAMEs for date={date_yyyymmdd} ...")
    sde_cnames = get_sde_cnames_for_date(date_yyyymmdd)

    if not sde_cnames:
        msg = f"No races found for date={date_yyyymmdd} (probably no JRA meeting)"
        print(f"[extract_one_date] {msg}")
        logger.warning(msg)
        print("[extract_one_date] === END (no races) ===")
        return

    total = len(sde_cnames)
    print(f"[extract_one_date] Found {total} sde CNAMEs for date={date_yyyymmdd}")
    logger.info("Found %d sde CNAMEs for date=%s", total, date_yyyymmdd)

    success = 0
    skip = 0
    fail = 0

    # ---- 各レース HTML 取得 ----
    for idx, cname in enumerate(sde_cnames, start=1):
        print(f"[extract_one_date] [{idx}/{total}] fetching race html cname={cname}")
        logger.info("Fetching race html cname=%s (%d/%d)", cname, idx, total)

        try:
            html_path = fetch_race_html(cname, overwrite=overwrite_html)

            if html_path is None:
                # 既存ファイルがあり overwrite=False の場合など
                print(f"[extract_one_date]  -> SKIP (already exists)")
                logger.info("Skipped (already exists) cname=%s", cname)
                skip += 1
                continue

            print(f"[extract_one_date]  -> OK saved to {html_path}")
            logger.info("Saved HTML for cname=%s path=%s", cname, html_path)
            success += 1

        except Exception:
            fail += 1
            print(f"[extract_one_date]  -> FAIL (see log)")
            logger.exception("Failed to fetch race html cname=%s date=%s", cname, date_yyyymmdd)
            continue

    # ---- サマリー ----
    summary = (
        f"[extract_one_date] === SUMMARY date={date_yyyymmdd} "
        f"total={total} success={success} skip={skip} fail={fail} ==="
    )
    print(summary)
    logger.info(
        "Extract summary date=%s total=%d success=%d skip=%d fail=%d",
        date_yyyymmdd,
        total,
        success,
        skip,
        fail,
    )
    print("[extract_one_date] === END ===")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Extract all race HTMLs for a given date (YYYYMMDD). "
                    "Uses JRA 'Past race results search' navigation."
    )
    parser.add_argument("date", help="YYYYMMDD")
    parser.add_argument(
        "--overwrite-html",
        action="store_true",
        help="Overwrite existing race HTML files",
    )
    args = parser.parse_args()

    extract_one_date(args.date, overwrite_html=args.overwrite_html)


if __name__ == "__main__":
    main()
