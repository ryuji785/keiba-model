"""
開催日ページから当日の全レース結果 CNAME を抽出し、HTML を保存する。
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import List

from bs4 import BeautifulSoup

# add project root to sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common_fetch import fetch_html  # noqa: E402
from src.jra_extract_links import extract_cnames_from_soup  # noqa: E402

logger = logging.getLogger(__name__)

RACE_LIST_RAW_DIR = Path("data/raw/jra/race_list")


def crawl_race_list(kaisaibi_cname: str, yyyymmdd: str) -> List[str]:
    """
    開催日 CNAME からレース一覧ページを取得し、CNAME を抽出する。
    HTMLは data/raw/jra/race_list/YYYYMMDD.html に保存。
    """
    url = f"https://www.jra.go.jp/JRADB/accessD.html?CNAME={kaisaibi_cname}"
    html_text = fetch_html(url)
    soup = BeautifulSoup(html_text, "html.parser")
    cnames = extract_cnames_from_soup(soup)
    out = RACE_LIST_RAW_DIR / f"{yyyymmdd}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html_text, encoding="utf-8")
    logger.info("Saved race list page: %s (cnames=%d)", out, len(cnames))
    return cnames


if __name__ == "__main__":
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="Crawl race list from a kaisaibi CNAME.")
    ap.add_argument("cname", help="kaisaibi CNAME (pw01dli00/....)")
    ap.add_argument("date", help="YYYYMMDD")
    args = ap.parse_args()
    crawl_race_list(args.cname, args.date)
