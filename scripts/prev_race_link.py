"""
Populate prev_* fields in race_results by ordering races per horse.

Usage:
    python scripts/prev_race_link.py --db data/keiba.db
"""

from __future__ import annotations

import argparse
import sqlite3


def populate_prev_fields(conn: sqlite3.Connection) -> int:
    """
    Updates race_results.prev_* fields using window functions.
    Returns number of rows updated.
    """
    conn.execute("PRAGMA foreign_keys = ON")
    # Build a temp table with previous race information per horse
    conn.execute("DROP TABLE IF EXISTS tmp_prev")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_prev AS
        SELECT
            rr.race_id,
            rr.horse_id,
            LAG(rr.race_id) OVER w AS prev_race_id,
            LAG(r.date)     OVER w AS prev_date,
            LAG(rr.finish_rank)    OVER w AS prev_finish_rank,
            LAG(rr.margin_sec)     OVER w AS prev_margin_sec,
            LAG(rr.finish_time_sec)OVER w AS prev_time_sec,
            LAG(rr.last_3f)        OVER w AS prev_last_3f
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        WINDOW w AS (PARTITION BY rr.horse_id ORDER BY r.date, rr.race_id)
        """
    )

    # Compute days_since_last separately to avoid reusing window expressions
    conn.execute("DROP TABLE IF EXISTS tmp_prev_days")
    conn.execute(
        """
        CREATE TEMP TABLE tmp_prev_days AS
        SELECT
            rr.race_id,
            rr.horse_id,
            CASE
              WHEN tp.prev_date IS NOT NULL THEN CAST((julianday(r.date) - julianday(tp.prev_date)) AS INT)
              ELSE NULL
            END AS days_since_last
        FROM race_results rr
        JOIN races r ON r.race_id = rr.race_id
        LEFT JOIN tmp_prev tp
          ON tp.race_id = rr.race_id AND tp.horse_id = rr.horse_id
        """
    )

    cur = conn.execute(
        """
        UPDATE race_results AS rr
        SET
          prev_race_id     = (SELECT prev_race_id     FROM tmp_prev      WHERE race_id=rr.race_id AND horse_id=rr.horse_id),
          prev_finish_rank = (SELECT prev_finish_rank FROM tmp_prev      WHERE race_id=rr.race_id AND horse_id=rr.horse_id),
          prev_margin_sec  = (SELECT prev_margin_sec  FROM tmp_prev      WHERE race_id=rr.race_id AND horse_id=rr.horse_id),
          prev_time_sec    = (SELECT prev_time_sec    FROM tmp_prev      WHERE race_id=rr.race_id AND horse_id=rr.horse_id),
          prev_last_3f     = (SELECT prev_last_3f     FROM tmp_prev      WHERE race_id=rr.race_id AND horse_id=rr.horse_id),
          days_since_last  = (SELECT days_since_last  FROM tmp_prev_days WHERE race_id=rr.race_id AND horse_id=rr.horse_id)
        """
    )
    return cur.rowcount


def main() -> None:
    ap = argparse.ArgumentParser(description="Populate prev_* fields in race_results.")
    ap.add_argument("--db", default="data/keiba.db", help="SQLite DB path")
    args = ap.parse_args()

    conn = sqlite3.connect(args.db)
    try:
        conn.execute("BEGIN")
        updated = populate_prev_fields(conn)
        conn.commit()
        print(f"updated rows: {updated}")
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
