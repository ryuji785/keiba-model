"""
Fetch JRA race result HTML (Shift_JIS) and save as data/raw/jra/race_<race_id>.html.
The primary key is race_id (12 digits). CNAMEはURLから取得するが、保存名は race_id で統一する。
"""
from __future__ import annotations

import argparse
import logging
import random
import re
import time
from pathlib import Path
from typing import Optional

from src.common_fetch import fetch_html

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jra.go.jp/JRADB/accessS.html?CNAME="
SAVE_DIR = Path("data/raw/jra")
ERROR_PHRASES = ["該当ページは存在しません", "ページが存在しません", "エラー"]


def _sleep_random() -> None:
    time.sleep(5.0 + random.random() * 2.0)
    # 約100回に1回、60秒の長いスリープを追加
    # randint(1, 100) が 1 になる確率は 1/100
    if random.randint(1, 100) == 1:
        time.sleep(60.0)

def cname_to_race_id(cname: str) -> str:
    """
    Extract 12-digit race_id from pw01sde CNAME.
    Pattern example: pw01sde1{jyo2}{yyyy4}{kai2}{nichijo2}{race2}{yyyymmdd}/{chk}
    → race_id = yyyy + jyo + kai + nichijo + race
    """
    m = re.search(
        r"pw01sde10(?P<jyo>\d{2})(?P<yyyy>\d{4})(?P<kai>\d{2})(?P<nichijo>\d{2})(?P<race>\d{2})",
        cname,
    )
    if not m:
        raise ValueError(f"Cannot extract race_id from cname: {cname}")
    race_id = f"{m.group('yyyy')}{m.group('jyo')}{m.group('kai')}{m.group('nichijo')}{m.group('race')}"
    if not re.fullmatch(r"\d{12}", race_id):
        raise ValueError(f"Invalid race_id parsed from cname={cname}: {race_id}")
    return race_id


def build_race_url(cname: str) -> str:
    """Build official result URL from CNAME (race_id単独ではチェックコード不明のためCNAMEを使う)。"""
    return BASE_URL + cname


def fetch_race_html(race_id: Optional[str] = None, *, cname: Optional[str] = None, overwrite: bool = False) -> Optional[Path]:
    """
    Fetch race HTML and save as data/raw/jra/race_<race_id>.html.
    - race_id: 12-digit ID (preferred). If None, derive from cname.
    - cname : CNAME token; required to build URL.
    Returns saved Path or None if error page detected.
    """
    if cname is None and race_id is None:
        raise ValueError("Either race_id or cname must be provided")
    if cname and not race_id:
        race_id = cname_to_race_id(cname)
    if race_id and not cname:
        raise ValueError("cname is required to fetch HTML (race_id alone cannot build URL)")

    assert race_id is not None and cname is not None

    SAVE_DIR.mkdir(parents=True, exist_ok=True)
    out_path = SAVE_DIR / f"race_{race_id}.html"
    if out_path.exists() and not overwrite:
        logger.info("Skip fetch (exists): %s", out_path)
        return out_path

    url = build_race_url(cname)
    html = fetch_html(url)
    if any(phrase in html for phrase in ERROR_PHRASES):
        logger.warning("Error page detected for cname=%s, skip saving.", cname)
        return None

    out_path.write_text(html, encoding="utf-8")
    logger.info("Saved race html (race_id=%s): %s", race_id, out_path)
    _sleep_random()
    return out_path


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    ap = argparse.ArgumentParser(description="Fetch JRA race result HTML by race_id and/or CNAME.")
    ap.add_argument("cname", help="CNAME token")
    ap.add_argument("--race-id", help="12-digit race_id (optional, inferred from CNAME if omitted)")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()
    fetch_race_html(args.race_id, cname=args.cname, overwrite=args.overwrite)
