"""
Fetch JRA calendar page and extract links for a given date (YYYYMMDD).
Relies on parsing doAction() parameters from HTML; no JavaScript execution.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List

from etl_common import http_get, decode_shift_jis, save_text, logger, _polite_sleep

BASE_CALENDAR_URL = "https://www.jra.go.jp/JRADB/accessD.html"


def fetch_calendar_html(date_yyyymmdd: str) -> str:
    """
    Fetch calendar page and return decoded HTML (Shift_JIS -> UTF-8).
    The calendar page includes doAction links per race day.
    """
    resp = http_get(BASE_CALENDAR_URL)
    html = decode_shift_jis(resp.content)
    out_path = Path("data/raw/jra") / date_yyyymmdd / "calendar.html"
    save_text(out_path, html, encoding="utf-8")
    logger.info("Saved calendar page: %s", out_path)
    _polite_sleep()
    return html


def extract_day_links(calendar_html: str, date_yyyymmdd: str) -> List[str]:
    """
    Extract race-day accessD.html links for the given date.
    Returns list of full URLs to accessD.html with proper parameters (from doAction).
    """
    links: List[str] = []
    # doAction('/JRADB/accessD.html','pw01dli00/20241102/xx')
    pattern = re.compile(r"doAction\(['\"](/JRADB/accessD\.html)['\"],\s*['\"]([^'\"]+)['\"]\)")
    for m in pattern.finditer(calendar_html):
        path = m.group(1)
        param = m.group(2)
        if date_yyyymmdd in param:
            links.append(f"https://www.jra.go.jp{path}?CNAME={param}")
    links = list(dict.fromkeys(links))
    logger.info("Extracted %d day links for %s", len(links), date_yyyymmdd)
    return links


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch JRA calendar and extract day links.")
    parser.add_argument("date", help="YYYYMMDD")
    args = parser.parse_args()

    html = fetch_calendar_html(args.date)
    day_links = extract_day_links(html, args.date)
    for link in day_links:
        print(link)
