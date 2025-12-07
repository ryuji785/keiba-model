"""
Simple aggregation for class/age_cond/sex_cond completeness.

Usage:
    python scripts/class_age_sex_report.py --db data/keiba.db
"""
from __future__ import annotations

import argparse
import sqlite3


def main() -> None:
    ap = argparse.ArgumentParser(description="Report completeness for class/age_cond/sex_cond.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    cur = conn.cursor()

    total = cur.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    print(f"races={total}")

    for col in ["class", "age_cond", "sex_cond"]:
        nulls = cur.execute(f"SELECT COUNT(*) FROM races WHERE {col} IS NULL").fetchone()[0]
        top5 = cur.execute(
            f"SELECT {col}, COUNT(*) c FROM races GROUP BY {col} ORDER BY c DESC LIMIT 5"
        ).fetchall()
        print(f"{col}: nulls={nulls}")
        for val, cnt in top5:
            print(f"  {val}: {cnt}")

    conn.close()


if __name__ == "__main__":
    main()
