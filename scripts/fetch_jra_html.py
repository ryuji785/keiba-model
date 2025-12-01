"""
Fetch JRA race result HTML by CNAME, with Shift_JIS handling and error-page detection.
"""
from __future__ import annotations

import argparse
import logging
import random
import time
from pathlib import Path

from src.common_fetch import fetch_html

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jra.go.jp/JRADB/accessS.html?CNAME="
SAVE_DIR = Path("data/raw/jra/race_html")
ERROR_PHRASES = ["該当ページは存在しません", "ページが存在しません", "エラー"]


def _sleep_random() -> None:
    time.sleep(1.0 + random.random() * 2.0)


def fetch_race_html(cname: str, overwrite: bool = False) -> Path | None:
    """
    Fetch race HTML for given CNAME and save to data/raw/jra/race_html/<cname>.html.
    Skips if error page detected.
    """
    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SAVE_DIR / f"{cname.replace('/', '_')}.html"
    if out_path.exists() and not overwrite:
        logger.info("Skip fetch (exists): %s", out_path)
        return out_path

    url = BASE_URL + cname
    html = fetch_html(url)
    if any(phrase in html for phrase in ERROR_PHRASES):
        logger.warning("Error page detected for cname=%s, skip saving.", cname)
        return None

    out_path.write_text(html, encoding="utf-8")
    logger.info("Saved race html: %s", out_path)
    _sleep_random()
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="Fetch JRA race result HTML by CNAME.")
    ap.add_argument("cname", help="CNAME token")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    fetch_race_html(args.cname, overwrite=args.overwrite)
