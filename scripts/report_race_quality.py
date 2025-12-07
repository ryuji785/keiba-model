"""
Simple quality report for race_results per race_id.

Shows:
- counts (races, race_results)
- basic aggregates (min/max date)
- optional per-race NULL counts for key fields.

Usage:
    python scripts/report_race_quality.py --db data/keiba.db
    python scripts/report_race_quality.py --db data/keiba.db --race-id 202405020511
"""
from __future__ import annotations

import argparse
import sqlite3
from typing import Iterable, Tuple


def summary(conn: sqlite3.Connection) -> None:
    races = conn.execute("SELECT COUNT(*) FROM races").fetchone()[0]
    results = conn.execute("SELECT COUNT(*) FROM race_results").fetchone()[0]
    min_date, max_date = conn.execute("SELECT MIN(date), MAX(date) FROM races").fetchone()
    print(f"races={races}, race_results={results}, date_range=({min_date} .. {max_date})")


def per_race_nulls(conn: sqlite3.Connection, race_id: str | None = None) -> Iterable[Tuple]:
    sql = """
    SELECT
      rr.race_id,
      SUM(CASE WHEN finish_rank IS NULL THEN 1 ELSE 0 END) AS finish_rank_nulls,
      SUM(CASE WHEN corner_pass_order IS NULL THEN 1 ELSE 0 END) AS corner_nulls,
      SUM(CASE WHEN last_3f IS NULL THEN 1 ELSE 0 END) AS last3f_nulls,
      SUM(CASE WHEN margin_sec IS NULL THEN 1 ELSE 0 END) AS margin_nulls,
      COUNT(*) AS total_rows
    FROM race_results rr
    {where}
    GROUP BY rr.race_id
    ORDER BY rr.race_id
    """
    where = ""
    params = ()
    if race_id:
        where = "WHERE rr.race_id = ?"
        params = (race_id,)
    return conn.execute(sql.format(where=where), params).fetchall()


def main() -> None:
    ap = argparse.ArgumentParser(description="Quality report for race_results.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    ap.add_argument(
        "--summary",
        action="store_true",
        help="Show only overall summary (races/results/date range) and exit.",
    )
    ap.add_argument("--race-id", help="If set, show null counts for this race_id")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    summary(conn)

    if args.summary:
        return

    rows = per_race_nulls(conn, args.race_id)
    print("race_id, finish_rank_nulls, corner_nulls, last3f_nulls, margin_nulls, total_rows")
    for r in rows:
        print(",".join(str(x) for x in r))


if __name__ == "__main__":
    main()
