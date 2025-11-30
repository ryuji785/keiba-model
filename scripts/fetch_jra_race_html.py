"""
Fetch a single race result HTML using a CNAME token and save it.
"""

from __future__ import annotations

import argparse
from pathlib import Path

from etl_common import http_get, decode_shift_jis, save_text, logger, _polite_sleep

BASE_RESULT_URL = "https://www.jra.go.jp/JRADB/accessS.html"


def fetch_race_html(cname: str, date_yyyymmdd: str, overwrite: bool = False) -> Path:
    """
    Fetch race result page with given CNAME and save under data/raw/jra/YYYYMMDD/.
    """
    out_dir = Path("data/raw/jra") / date_yyyymmdd
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"race_{cname.replace('/', '_')}.html"
    if out_path.exists() and not overwrite:
        logger.info("Skip fetch; file exists: %s", out_path)
        return out_path

    resp = http_get(BASE_RESULT_URL, params={"CNAME": cname})
    html = decode_shift_jis(resp.content)
    save_text(out_path, html, encoding="utf-8")
    logger.info("Saved race HTML: %s", out_path)
    _polite_sleep()
    return out_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch a single race result HTML by CNAME.")
    parser.add_argument("cname", help="CNAME token extracted from race list page")
    parser.add_argument("date", help="YYYYMMDD for storage path")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing file")
    args = parser.parse_args()

    fetch_race_html(args.cname, args.date, overwrite=args.overwrite)
