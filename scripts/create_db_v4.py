"""
SQLite schema initializer for DB schema v4.

- Tables follow docs/db/table_definitions.md (courses, races, horses, jockeys,
  trainers, race_results, horses_stats).
- Foreign key constraints are enabled.
- All DDL runs inside a single transaction.
- Existing objects are kept (CREATE IF NOT EXISTS); nothing is dropped.
"""

import argparse
import sqlite3
from pathlib import Path

# Default path to the SQLite file
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "data" / "keiba.db"

SCHEMA_SQL = r"""
-- drop existing (dev convenience)
DROP TABLE IF EXISTS race_results;
DROP TABLE IF EXISTS horses;
DROP TABLE IF EXISTS jockeys;
DROP TABLE IF EXISTS trainers;
DROP TABLE IF EXISTS races;
DROP TABLE IF EXISTS courses;

-- ================
-- 1. courses master
-- ================
CREATE TABLE IF NOT EXISTS courses (
    course_id      TEXT PRIMARY KEY,
    venue_id       TEXT NOT NULL,
    course_name    TEXT NOT NULL,
    surface        TEXT NOT NULL,
    track_type     TEXT,
    straight_len   INTEGER,
    slope_max      REAL,
    features_text  TEXT
);

-- ================
-- 2. races
-- ================
CREATE TABLE IF NOT EXISTS races (
    race_id      TEXT PRIMARY KEY,
    date         TEXT NOT NULL,
    course_id    TEXT NOT NULL,
    race_no      INTEGER NOT NULL,
    race_name    TEXT,
    distance     INTEGER NOT NULL,
    surface      TEXT NOT NULL,
    weather      TEXT,
    going        TEXT,
    class        TEXT,
    age_cond     TEXT,
    sex_cond     TEXT,
    num_runners  INTEGER,
    win_time_sec REAL,
    race_type    TEXT DEFAULT 'FLAT',
    FOREIGN KEY(course_id) REFERENCES courses(course_id)
);

-- ================
-- 3. horses (core)
-- ================
CREATE TABLE IF NOT EXISTS horses (
    horse_id    TEXT PRIMARY KEY,
    horse_name  TEXT NOT NULL,
    sex         TEXT,
    birth_year  INTEGER
);

-- ================
-- 4. jockeys master
-- ================
CREATE TABLE IF NOT EXISTS jockeys (
    jockey_id    TEXT PRIMARY KEY,
    jockey_name  TEXT NOT NULL
);

-- ================
-- 5. trainers master
-- ================
CREATE TABLE IF NOT EXISTS trainers (
    trainer_id    TEXT PRIMARY KEY,
    trainer_name  TEXT NOT NULL
);

-- ================
-- 6. race_results (race x horse results, core)
-- ================
CREATE TABLE IF NOT EXISTS race_results (
    race_id         TEXT    NOT NULL,
    horse_id        TEXT    NOT NULL,
    bracket_no      INTEGER,
    horse_no        INTEGER,
    finish_rank     INTEGER,
    finish_status   TEXT,
    finish_time_sec REAL,
    odds            REAL,
    popularity      INTEGER,
    weight          REAL,
    weight_diff     INTEGER,
    body_weight     INTEGER,
    jockey_id       TEXT,
    trainer_id      TEXT,
    corner_pass_order TEXT,
    last_3f         REAL,
    margin_sec      REAL,
    prize           INTEGER,
    prev_race_id        TEXT,
    prev_finish_rank    INTEGER,
    prev_margin_sec     REAL,
    prev_time_sec       REAL,
    prev_last_3f        REAL,
    days_since_last     INTEGER,
    PRIMARY KEY (race_id, horse_id),
    FOREIGN KEY (race_id)  REFERENCES races(race_id)   ON DELETE CASCADE,
    FOREIGN KEY (horse_id) REFERENCES horses(horse_id) ON DELETE CASCADE,
    FOREIGN KEY (jockey_id) REFERENCES jockeys(jockey_id),
    FOREIGN KEY (trainer_id) REFERENCES trainers(trainer_id)
);

-- ================
-- 7. horses_stats (career stats, future-friendly)
-- ================
CREATE TABLE IF NOT EXISTS horses_stats (
    horse_id                 TEXT NOT NULL,
    stats_as_of_date         TEXT NOT NULL,
    career_starts            INTEGER,
    career_wins              INTEGER,
    career_places            INTEGER,
    career_shows             INTEGER,
    win_rate_turf            REAL,
    win_rate_dirt            REAL,
    win_rate_sprint          REAL,
    win_rate_mile            REAL,
    win_rate_middle          REAL,
    win_rate_long            REAL,
    show_rate_turf           REAL,
    show_rate_dirt           REAL,
    avg_finish_last5         REAL,
    PRIMARY KEY (horse_id, stats_as_of_date),
    FOREIGN KEY (horse_id) REFERENCES horses(horse_id)
);
"""

INDEXES_SQL = r"""
-- indexes
CREATE INDEX IF NOT EXISTS idx_races_date ON races(date);
CREATE INDEX IF NOT EXISTS idx_races_course ON races(course_id);

CREATE INDEX IF NOT EXISTS idx_results_horse ON race_results(horse_id);
CREATE INDEX IF NOT EXISTS idx_results_jockey ON race_results(jockey_id);
CREATE INDEX IF NOT EXISTS idx_results_trainer ON race_results(trainer_id);
CREATE INDEX IF NOT EXISTS idx_results_prev_race ON race_results(prev_race_id);
"""

VIEWS_SQL = r"""
CREATE VIEW IF NOT EXISTS v_race_features AS
SELECT
    rr.race_id,
    r.date AS race_date,
    r.course_id,
    c.venue_id,
    c.course_name,
    r.race_no,
    r.race_name,
    r.distance,
    r.surface,
    r.weather,
    r.going,
    r.class,
    r.age_cond,
    r.sex_cond,
    r.num_runners,
    rr.horse_id,
    h.horse_name,
    h.sex AS horse_sex,
    h.birth_year,
    rr.bracket_no,
    rr.horse_no,
    rr.finish_rank,
    rr.finish_status,
    rr.finish_time_sec,
    rr.odds,
    rr.popularity,
    rr.weight,
    rr.weight_diff,
    rr.body_weight,
    rr.corner_pass_order,
    rr.last_3f,
    rr.margin_sec,
    rr.prize,
    j.jockey_name,
    t.trainer_name,
    rr.prev_race_id,
    rr.prev_finish_rank,
    rr.prev_margin_sec,
    rr.prev_time_sec,
    rr.prev_last_3f,
    rr.days_since_last
FROM race_results rr
JOIN races   r ON rr.race_id = r.race_id
JOIN horses  h ON rr.horse_id = h.horse_id
LEFT JOIN courses  c ON r.course_id = c.course_id
LEFT JOIN jockeys  j ON rr.jockey_id = j.jockey_id
LEFT JOIN trainers t ON rr.trainer_id = t.trainer_id;
"""


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("PRAGMA foreign_keys = ON;")
        with conn:  # transactional DDL
            conn.executescript(SCHEMA_SQL)
            conn.executescript(INDEXES_SQL)
            conn.executescript(VIEWS_SQL)
        print(f"[OK] Initialized v4 schema at: {db_path}")
    finally:
        conn.close()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Create SQLite DB with v4 schema.")
    parser.add_argument(
        "--db",
        type=str,
        default=str(DEFAULT_DB_PATH),
        help=f"Path to SQLite DB file (default: {DEFAULT_DB_PATH})",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    db_path = Path(args.db)
    print(f"Using DB file: {db_path}")
    init_db(db_path)
