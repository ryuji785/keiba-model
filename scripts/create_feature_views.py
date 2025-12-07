"""
Apply feature view definitions for modeling.

Usage:
    python scripts/create_feature_views.py --db data/keiba.db
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


SQL_FILE = Path(__file__).resolve().parent / "feature_views.sql"


def apply_views(conn: sqlite3.Connection, sql_path: Path) -> None:
    conn.executescript(sql_path.read_text(encoding="utf-8"))


def main() -> None:
    ap = argparse.ArgumentParser(description="Create/update modeling feature views.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    args = ap.parse_args()

    if not SQL_FILE.exists():
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    conn = sqlite3.connect(args.db)
    try:
        apply_views(conn, SQL_FILE)
        conn.commit()
        print("feature views applied")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
