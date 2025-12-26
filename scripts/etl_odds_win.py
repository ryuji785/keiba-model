"""
Parse odds_*.html (単勝オッズ) and load into odds_win table.

Usage:
  python scripts/etl_odds_win.py --db data/keiba.db --year 2022
  python scripts/etl_odds_win.py --db data/keiba.db --files odds_202201020507.html
"""
from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
ODDS_DIR = ROOT / "data" / "raw" / "jra" / "odds"


def decode_html(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("cp932", "shift_jis", "utf-8"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def parse_odds_file(path: Path) -> List[Dict[str, Optional[str | float | int]]]:
    race_id_match = re.search(r"odds_(\d{12})", path.name)
    if not race_id_match:
        return []
    race_id = race_id_match.group(1)

    html = decode_html(path)
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict[str, Optional[str | float | int]]] = []

    for tr in soup.find_all("tr"):
        num_td = tr.find("td", class_="num")
        if not num_td:
            continue
        try:
            horse_no = int(num_td.get_text(strip=True))
        except Exception:
            continue
        horse_cname = None
        td_horse = tr.find("td", class_="horse")
        if td_horse:
            a = td_horse.find("a", href=True)
            if a:
                m = re.search(r"CNAME=([^&\"']+)", a["href"])
                if m:
                    horse_cname = m.group(1)

        odds_td = tr.find("td", class_="odds_tan")
        if not odds_td:
            continue
        try:
            odds_val = float(odds_td.get_text(strip=True))
        except Exception:
            odds_val = None

        popularity = None  # 単勝ページに人気順がない場合が多い

        horse_id = horse_cname or f"{race_id}_H{horse_no:02d}"
        rows.append(
            {
                "race_id": race_id,
                "horse_id": horse_id,
                "win_odds": odds_val,
                "popularity": popularity,
            }
        )
    return rows


def upsert_odds(conn: sqlite3.Connection, rows: List[Dict[str, Optional[str | float | int]]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO odds_win (race_id, horse_id, win_odds, popularity)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(race_id, horse_id) DO UPDATE SET
            win_odds=excluded.win_odds,
            popularity=COALESCE(excluded.popularity, odds_win.popularity)
    """
    for r in rows:
        conn.execute(sql, (r["race_id"], r["horse_id"], r.get("win_odds"), r.get("popularity")))


def main() -> None:
    ap = argparse.ArgumentParser(description="Load odds_*.html into odds_win table.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    ap.add_argument("--year", help="Filter odds files by year prefix")
    ap.add_argument("--files", help="Comma-separated specific odds_*.html files")
    args = ap.parse_args()

    if args.files:
        targets = [Path(f.strip()) for f in args.files.split(",") if f.strip()]
    else:
        targets = sorted(ODDS_DIR.glob("odds_*.html"))
        if args.year:
            targets = [p for p in targets if p.name.startswith(f"odds_{args.year}")]

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        total = 0
        for p in targets:
            rows = parse_odds_file(p)
            upsert_odds(conn, rows)
            total += len(rows)
        conn.commit()
        print(f"[odds_win] upserted rows: {total} from files: {len(targets)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
