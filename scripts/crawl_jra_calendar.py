"""
カレンダーから開催日ページの CNAME を抽出し、日別 HTML を保存する。
URL 生成は禁止。doAction のパラメータを解析して取得する。
"""
from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup

# add project root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common_fetch import fetch_html  # noqa: E402

logger = logging.getLogger(__name__)

CAL_BASE_URL = "https://www.jra.go.jp/keiba/calendar/"
CAL_RAW_DIR = Path("data/raw/jra/calendar")


def _extract_kaisaibi_cnames(soup: BeautifulSoup) -> Dict[str, str]:
    """
    カレンダーHTMLから開催日 CNAME を抽出する。
    戻り値: {yyyymmdd: cname_string}
    """
    pat = re.compile(r"doAction\(\s*['\"]/JRADB/accessD\.html['\"],\s*['\"]([^'\"]+)['\"]\s*\)")
    results: Dict[str, str] = {}
    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick", "")
        m = pat.search(onclick)
        if not m:
            continue
        cname = m.group(1)
        m_date = re.search(r"(\d{8})", cname)  # YYYYMMDD
        if not m_date:
            continue
        ymd = m_date.group(1)
        if ymd not in results:
            results[ymd] = cname
    return results


def fetch_calendar_and_save(year: int, month: int) -> Dict[str, str]:
    """指定年月のカレンダーを取得し、開催日 CNAME を返す。"""
    url = f"{CAL_BASE_URL}{year:04d}{month:02d}.html"
    cache_path = CAL_RAW_DIR / f"{year:04d}{month:02d}.html"
    if cache_path.exists():
        html = cache_path.read_text(encoding="utf-8")
        logger.info("Loaded calendar from cache: %s", cache_path)
    else:
        html = fetch_html(url)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(html, encoding="utf-8")
        logger.info("Fetched and cached calendar: %s", cache_path)
    soup = BeautifulSoup(html, "html.parser")

    cnames = _extract_kaisaibi_cnames(soup)
    logger.info("Extracted %d kaisaibi cnames for %04d-%02d", len(cnames), year, month)

    for ymd, cname in cnames.items():
        try:
            page_html = fetch_html(f"https://www.jra.go.jp/JRADB/accessD.html?CNAME={cname}")
            out = CAL_RAW_DIR / f"{ymd}.html"
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(page_html, encoding="utf-8")
            logger.info("Saved kaisaibi page: %s", out)
        except Exception:
            logger.exception("Failed to fetch/save kaisaibi page: ymd=%s cname=%s", ymd, cname)
            continue

    return cnames


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="Crawl JRA calendar to extract kaisaibi CNAMEs.")
    ap.add_argument("year", type=int)
    ap.add_argument("month", type=int)
    args = ap.parse_args()
    fetch_calendar_and_save(args.year, args.month)
