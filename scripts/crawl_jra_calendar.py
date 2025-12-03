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
from typing import Dict

from bs4 import BeautifulSoup

# add project root
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common_fetch import fetch_html  # noqa: E402

logger = logging.getLogger(__name__)

# 生HTMLの保存先
CAL_RAW_DIR = Path("data/raw/jra/calendar")

# JRAの月別カレンダーURLで使われるスラッグ
MONTH_SLUGS: Dict[int, str] = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "may",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "oct",
    11: "nov",
    12: "dec",
}


def _build_calendar_url(year: int, month: int) -> str:
    """
    年月から JRA レーシングカレンダーの URL を生成する。

    現状の仕様：
      - 2020〜2024年: https://www.jra.go.jp/keiba/calendarYYYY/mon.html
      - 2025年以降 :  https://www.jra.go.jp/keiba/calendar/mon.html
    """
    slug = MONTH_SLUGS[month]

    if year >= 2025:
        # 現在年（2025〜）は /keiba/calendar/xxx.html 形式
        base = "https://www.jra.go.jp/keiba/calendar"
    else:
        # 過去年（〜2024）は /keiba/calendarYYYY/xxx.html 形式
        base = f"https://www.jra.go.jp/keiba/calendar{year}"

    url = f"{base}/{slug}.html"
    logger.info(
        "[crawl_jra_calendar] calendar url=%s (year=%d, month=%d, slug=%s)",
        url,
        year,
        month,
        slug,
    )
    return url


def _extract_kaisaibi_cnames(soup: BeautifulSoup) -> Dict[str, str]:
    """
    カレンダーHTMLから開催日 CNAME を抽出する。
    戻り値: {yyyymmdd: cname_string}
    """
    # doAction('/JRADB/accessD.html', 'pw01dud...YYYYMMDD...')
    pat = re.compile(
        r"doAction\(\s*['\"]/JRADB/accessD\.html['\"],\s*['\"]([^'\"]+)['\"]\s*\)"
    )
    results: Dict[str, str] = {}

    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick", "")
        m = pat.search(onclick)
        if not m:
            continue

        cname = m.group(1)

        # CNAME 内に含まれている YYYYMMDD を拾う
        m_date = re.search(r"(\d{8})", cname)
        if not m_date:
            continue

        ymd = m_date.group(1)
        if ymd not in results:
            results[ymd] = cname

    return results


def fetch_calendar_and_save(year: int, month: int) -> Dict[str, str]:
    """
    指定年月のカレンダーを取得し、開催日 CNAME マップを返す。

    戻り値:
        { 'YYYYMMDD': 'pw01dud....' }
    """
    url = _build_calendar_url(year, month)

    cache_path = CAL_RAW_DIR / f"{year:04d}{month:02d}.html"
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # まずはキャッシュを確認
    if cache_path.exists():
        html = cache_path.read_text(encoding="utf-8")
        logger.info("[crawl_jra_calendar] Loaded calendar from cache: %s", cache_path)
    else:
        logger.info("[crawl_jra_calendar] Fetch calendar HTML: %s", url)
        html = fetch_html(url)
        cache_path.write_text(html, encoding="utf-8")
        logger.info("[crawl_jra_calendar] Fetched and cached calendar: %s", cache_path)

    soup = BeautifulSoup(html, "html.parser")

    cnames = _extract_kaisaibi_cnames(soup)
    logger.info(
        "[crawl_jra_calendar] Extracted %d kaisaibi CNAMEs for %04d-%02d",
        len(cnames),
        year,
        month,
    )

    # 開催日ページ（/JRADB/accessD.html?CNAME=...）も保存しておく
    for ymd, cname in sorted(cnames.items()):
        kaisaibi_url = f"https://www.jra.go.jp/JRADB/accessD.html?CNAME={cname}"
        out = CAL_RAW_DIR / f"{ymd}.html"
        try:
            logger.info(
                "[crawl_jra_calendar] Fetch kaisaibi page: date=%s url=%s",
                ymd,
                kaisaibi_url,
            )
            page_html = fetch_html(kaisaibi_url)
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text(page_html, encoding="utf-8")
            logger.info("[crawl_jra_calendar] Saved kaisaibi page: %s", out)
        except Exception:
            logger.exception(
                "[crawl_jra_calendar] Failed to fetch/save kaisaibi page: ymd=%s cname=%s",
                ymd,
                cname,
            )
            continue

    return cnames


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    ap = argparse.ArgumentParser(
        description="Crawl JRA calendar to extract kaisaibi CNAMEs."
    )
    ap.add_argument("year", type=int)
    ap.add_argument("month", type=int)
    args = ap.parse_args()
    fetch_calendar_and_save(args.year, args.month)
