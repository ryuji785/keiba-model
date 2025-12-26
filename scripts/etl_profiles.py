"""
Parse horse/jockey profile HTMLs and update DB (horses/jockeys tables).

Usage examples:
  python scripts/etl_profiles.py --db data/keiba.db --horses
  python scripts/etl_profiles.py --db data/keiba.db --jockeys
  python scripts/etl_profiles.py --db data/keiba.db --horses --files data/raw/jra/horses/horse_xxx.html
"""
from __future__ import annotations

import argparse
import re
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
HORSE_DIR = ROOT / "data" / "raw" / "jra" / "horses"
JOCKEY_DIR = ROOT / "data" / "raw" / "jra" / "jockeys"


def decode_html(path: Path) -> str:
    data = path.read_bytes()
    for enc in ("cp932", "shift_jis", "utf-8"):
        try:
            return data.decode(enc)
        except Exception:
            continue
    return data.decode("utf-8", errors="ignore")


def parse_horse_profile(path: Path) -> Optional[Dict[str, str | int]]:
    html = decode_html(path)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    # name: prefer h1/h2/title text without suffix
    name = None
    for tag in ["h1", "h2", "title"]:
        el = soup.find(tag)
        if el and el.text.strip():
            name = el.text.strip()
            break
    if not name:
        return None

    # sex: 牡/牝/セン
    sex = None
    if re.search(r"牡", text):
        sex = "牡"
    elif re.search(r"牝", text):
        sex = "牝"
    elif re.search(r"セン", text):
        sex = "セン"

    birth_year = None
    m_birth = re.search(r"(\d{4})年\d{1,2}月\d{1,2}日", text)
    if m_birth:
        birth_year = int(m_birth.group(1))

    horse_id = path.stem.replace("horse_", "")
    return {"horse_id": horse_id, "horse_name": name, "sex": sex, "birth_year": birth_year}


def parse_jockey_profile(path: Path) -> Optional[Dict[str, str]]:
    html = decode_html(path)
    soup = BeautifulSoup(html, "html.parser")
    text = soup.get_text(" ", strip=True)

    name = None
    for tag in ["h1", "h2", "title"]:
        el = soup.find(tag)
        if el and el.text.strip():
            name = el.text.strip()
            break
    if not name:
        return None

    jockey_id = path.stem.replace("jockey_", "")
    return {"jockey_id": jockey_id, "jockey_name": name}


def upsert_horses(conn: sqlite3.Connection, rows: List[Dict[str, str | int]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO horses (horse_id, horse_name, sex, birth_year)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(horse_id) DO UPDATE SET
            horse_name=excluded.horse_name,
            sex=COALESCE(excluded.sex, horses.sex),
            birth_year=COALESCE(excluded.birth_year, horses.birth_year)
    """
    for r in rows:
        conn.execute(sql, (r["horse_id"], r["horse_name"], r.get("sex"), r.get("birth_year")))


def upsert_jockeys(conn: sqlite3.Connection, rows: List[Dict[str, str]]) -> None:
    if not rows:
        return
    sql = """
        INSERT INTO jockeys (jockey_id, jockey_name)
        VALUES (?, ?)
        ON CONFLICT(jockey_id) DO UPDATE SET
            jockey_name=excluded.jockey_name
    """
    for r in rows:
        conn.execute(sql, (r["jockey_id"], r["jockey_name"]))


def main() -> None:
    ap = argparse.ArgumentParser(description="Parse horse/jockey profile HTMLs and update DB.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    ap.add_argument("--horses", action="store_true", help="Process horse HTMLs")
    ap.add_argument("--jockeys", action="store_true", help="Process jockey HTMLs")
    ap.add_argument("--files", help="Comma-separated specific HTML files to process")
    args = ap.parse_args()

    if not args.horses and not args.jockeys:
        args.horses = args.jockeys = True

    horse_files: List[Path] = []
    jockey_files: List[Path] = []
    if args.files:
        for f in args.files.split(","):
            p = Path(f.strip())
            if "horse_" in p.name:
                horse_files.append(p)
            elif "jockey_" in p.name:
                jockey_files.append(p)
    else:
        if args.horses:
            horse_files = sorted(HORSE_DIR.glob("horse_*.html"))
        if args.jockeys:
            jockey_files = sorted(JOCKEY_DIR.glob("jockey_*.html"))

    conn = sqlite3.connect(args.db)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        if horse_files:
            parsed = [r for p in horse_files if (r := parse_horse_profile(p))]
            upsert_horses(conn, parsed)
            print(f"[horses] upserted {len(parsed)} rows")
        if jockey_files:
            parsed = [r for p in jockey_files if (r := parse_jockey_profile(p))]
            upsert_jockeys(conn, parsed)
            print(f"[jockeys] upserted {len(parsed)} rows")
        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
