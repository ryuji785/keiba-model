"""
Utilities for parsing JRA 'Past race results search' page.

Currently provides:
- parse_race_day_entries(html) -> List[Dict[str, str]]

Each entry has:
    {
        "date_yyyymmdd": "20240406",
        "course_name":   "Fukushima",
        "course_label_raw": "福島",
        "srl_cname":     "pw01srl20240406",
    }
"""

from __future__ import annotations

from typing import List, Dict, Optional, Set, Tuple
import logging
import re

from bs4 import BeautifulSoup, Tag

logger = logging.getLogger(__name__)

# 日本語 → 英語コース名マップ
COURSE_NAME_MAP: Dict[str, str] = {
    "東京": "Tokyo",
    "京都": "Kyoto",
    "中山": "Nakayama",
    "阪神": "Hanshin",
    "福島": "Fukushima",
    "札幌": "Sapporo",
    "函館": "Hakodate",
    "新潟": "Niigata",
    "小倉": "Kokura",
    "中京": "Chukyo",
}

# pw01srlYYYYMMDD を拾うためのパターン
SRL_CNAME_RE = re.compile(r"pw01srl(\d{8})")
ONCLICK_SRL_RE = re.compile(r"doAction\([^,]+,\s*'(?P<cname>pw01srl\d{8})'\s*\)")
HREF_SRL_RE = re.compile(r"CNAME=(?P<cname>pw01srl\d{8})")


def _extract_srl_cname_from_tag(tag: Tag) -> Optional[str]:
    """タグの onclick / href から pw01srlYYYYMMDD を取り出す。"""
    onclick = tag.get("onclick")
    if onclick:
        m = ONCLICK_SRL_RE.search(onclick)
        if m:
            return m.group("cname")

    href = tag.get("href")
    if href:
        m = HREF_SRL_RE.search(href)
        if m:
            return m.group("cname")

    return None


def _find_course_label_raw(tag: Tag) -> Optional[str]:
    """
    そのボタン/リンクの「近く」にある日本語コース名（東京/京都/…）を推定する。
    """
    candidates: List[Tag] = []
    if isinstance(tag, Tag):
        candidates.append(tag)
        for ancestor in list(tag.parents)[:5]:
            if isinstance(ancestor, Tag):
                candidates.append(ancestor)

    for node in candidates:
        text = node.get_text(" ", strip=True)
        if not text:
            continue

        for jp_name in COURSE_NAME_MAP.keys():
            if jp_name in text:
                return jp_name

    return None


def parse_race_day_entries(html: str) -> List[Dict[str, str]]:
    """
    '過去の競走成績検索' ページの HTML から race_day_entries を抽出。
    """
    soup = BeautifulSoup(html, "html.parser")

    srl_tags: List[Tag] = []

    for tag in soup.find_all(onclick=True):
        if "pw01srl" in tag.get("onclick", ""):
            srl_tags.append(tag)

    for tag in soup.find_all("a", href=True):
        if "CNAME=pw01srl" in tag.get("href", ""):
            srl_tags.append(tag)

    entries: List[Dict[str, str]] = []
    seen: Set[Tuple[str, str]] = set()

    for tag in srl_tags:
        srl_cname = _extract_srl_cname_from_tag(tag)
        if not srl_cname:
            continue

        m = SRL_CNAME_RE.search(srl_cname)
        if not m:
            continue

        date_yyyymmdd = m.group(1)

        course_label_raw = _find_course_label_raw(tag)
        if not course_label_raw:
            logger.debug("Could not find course_label_raw for %s", srl_cname)
            continue

        course_name = COURSE_NAME_MAP.get(course_label_raw, course_label_raw)

        key = (date_yyyymmdd, course_label_raw)
        if key in seen:
            continue
        seen.add(key)

        entries.append(
            {
                "date_yyyymmdd": date_yyyymmdd,
                "course_name": course_name,
                "course_label_raw": course_label_raw,
                "srl_cname": srl_cname,
            }
        )

    logger.info("Parsed %d race_day_entries from past results HTML", len(entries))
    return entries


if __name__ == "__main__":
    import argparse
    from pathlib import Path

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    ap = argparse.ArgumentParser(description="Parse race_day_entries from saved Past Results HTML.")
    ap.add_argument("html_file", type=Path)
    args = ap.parse_args()

    html_text = args.html_file.read_text(encoding="utf-8", errors="ignore")
    results = parse_race_day_entries(html_text)
    for r in results:
        print(r)
