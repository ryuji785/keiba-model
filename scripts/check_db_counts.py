"""
Simple helper script to inspect race_results in the SQLite DB without sqlite3 CLI.

Usage:
    python scripts/check_db_counts.py --db data/keiba.db
    python scripts/check_db_counts.py --db data/keiba.db --race-id 202405020511
"""
from __future__ import annotations

import argparse
import sqlite3
from typing import Iterable, Tuple


def list_counts(conn: sqlite3.Connection) -> Iterable[Tuple[str, int]]:
    """Yield (race_id, count) for all races present in race_results."""
    sql = "SELECT race_id, COUNT(*) FROM race_results GROUP BY race_id ORDER BY race_id"
    return conn.execute(sql).fetchall()


def list_rows(conn: sqlite3.Connection, race_id: str, limit: int = 20) -> Iterable[Tuple]:
    """Yield a few rows for a given race_id."""
    sql = """
        SELECT race_id, horse_id, horse_no, finish_rank, popularity, jockey_id, trainer_id
        FROM race_results
        WHERE race_id = ?
        ORDER BY horse_no
        LIMIT ?
    """
    return conn.execute(sql, (race_id, limit)).fetchall()


def list_unknown_counts(conn: sqlite3.Connection) -> Iterable[Tuple[str, int, int, int]]:
    """
    Yield (race_id, horse_unknown, jockey_unknown, trainer_unknown).
    UNKNOWN_* は parse 時のプレースホルダを想定。
    """
    sql = """
    SELECT
      rr.race_id,
      SUM(CASE WHEN h.horse_name LIKE 'UNKNOWN%%' THEN 1 ELSE 0 END) AS horse_unknown,
      SUM(CASE WHEN j.jockey_name LIKE 'UNKNOWN%%' THEN 1 ELSE 0 END) AS jockey_unknown,
      SUM(CASE WHEN t.trainer_name LIKE 'UNKNOWN%%' THEN 1 ELSE 0 END) AS trainer_unknown
    FROM race_results rr
    LEFT JOIN horses h ON rr.horse_id = h.horse_id
    LEFT JOIN jockeys j ON rr.jockey_id = j.jockey_id
    LEFT JOIN trainers t ON rr.trainer_id = t.trainer_id
    GROUP BY rr.race_id
    ORDER BY rr.race_id
    """
    return conn.execute(sql).fetchall()


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect race_results without sqlite3 CLI.")
    ap.add_argument("--db", default="data/keiba.db", help="Path to SQLite DB (default: data/keiba.db)")
    ap.add_argument("--race-id", help="If set, show rows for this race_id")
    ap.add_argument("--limit", type=int, default=20, help="Limit rows when --race-id is set (default: 20)")
    ap.add_argument("--unknown", action="store_true", help="Show UNKNOWN_* counts per race_id")
    ap.add_argument("--summary", action="store_true", help="Show simple summary (total races, total results)")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)

    if args.unknown:
        rows = list_unknown_counts(conn)
        if not rows:
            print("race_results is empty.")
            return
        print("race_id, horse_unknown, jockey_unknown, trainer_unknown")
        for r in rows:
            print(",".join(str(x) for x in r))
        return

    if args.summary:
        total_races = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
        total_results = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
        print(f"races={total_races}, race_results={total_results}")
        return

    if args.race_id:
        rows = list_rows(conn, args.race_id, args.limit)
        if not rows:
            print(f"No rows found for race_id={args.race_id}")
        else:
            print(f"race_results rows (race_id={args.race_id}, limit={args.limit}):")
            for r in rows:
                print(r)
        return

    counts = list_counts(conn)
    if not counts:
        print("race_results is empty.")
        return
    print("race_id, count")
    for race_id, cnt in counts:
        print(f"{race_id}, {cnt}")


if __name__ == "__main__":
    main()
