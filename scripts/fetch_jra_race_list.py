"""
Fetch race list page for a given race day and extract CNAME tokens for race results.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import List

from etl_common import http_get, decode_shift_jis, save_text, logger, _polite_sleep


def fetch_race_list(day_url: str, date_yyyymmdd: str) -> str:
    """
    Fetch race list HTML for a given day link extracted from calendar.
    """
    resp = http_get(day_url)
    html = decode_shift_jis(resp.content)
    out_path = Path("data/raw/jra") / date_yyyymmdd / "race_list.html"
    save_text(out_path, html, encoding="utf-8")
    logger.info("Saved race list page: %s", out_path)
    _polite_sleep()
    return html


def extract_cnames(race_list_html: str) -> List[str]:
    """
    Extract CNAME tokens from onclick="doAction('/JRADB/accessS.html','pw01sde...')".
    """
    pattern = re.compile(r"doAction\(['\"]/?JRADB/accessS\.html['\"],\s*['\"]([^'\"]+)['\"]\)")
    cnames: List[str] = []
    for m in pattern.finditer(race_list_html):
        cnames.append(m.group(1))
    cnames = list(dict.fromkeys(cnames))
    logger.info("Extracted %d CNAMEs", len(cnames))
    return cnames


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Fetch race list page and extract CNAMEs.")
    parser.add_argument("day_url", help="Full URL to accessD.html?CNAME=... from calendar extraction")
    parser.add_argument("date", help="YYYYMMDD")
    args = parser.parse_args()

    html = fetch_race_list(args.day_url, args.date)
    cnames = extract_cnames(html)
    for c in cnames:
        print(c)
