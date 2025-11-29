# scripts/create_db_v3.py

import sqlite3
from pathlib import Path

# DBファイルのパス
DB_PATH = Path(__file__).resolve().parents[1] / "data" / "keiba.db"

SCHEMA_SQL = r"""
PRAGMA foreign_keys = ON;

-- =========================
-- 1. courses（トラック単位コース情報）
-- =========================
CREATE TABLE IF NOT EXISTS courses (
    course_id      TEXT PRIMARY KEY,
    venue_id       TEXT NOT NULL,
    course_name    TEXT NOT NULL,
    surface        TEXT NOT NULL,
    track_type     TEXT,
    straight_len   INTEGER,
    slope_max      REAL,
    is_turf_start  INTEGER DEFAULT 0,
    features_text  TEXT
);

-- =========================
-- 2. races（レース情報）
-- =========================
CREATE TABLE IF NOT EXISTS races (
    race_id      TEXT PRIMARY KEY,
    date         TEXT NOT NULL,
    course_id    TEXT NOT NULL,
    race_no      INTEGER NOT NULL,
    race_name    TEXT,
    distance     INTEGER NOT NULL,
    surface      TEXT NOT NULL,
    course_ver   TEXT,
    weather      TEXT,
    going        TEXT,
    class        TEXT,
    num_runners  INTEGER,
    win_time_sec REAL,
    FOREIGN KEY(course_id) REFERENCES courses(course_id)
);

-- =========================
-- 3. horses（馬情報・血統）
-- =========================
CREATE TABLE IF NOT EXISTS horses (
    horse_id      TEXT PRIMARY KEY,
    horse_name    TEXT NOT NULL,
    sex           TEXT,
    birth_year    INTEGER,
    coat_color    TEXT,
    breeder_name  TEXT,
    sire_id       TEXT,
    dam_id        TEXT,
    sire_name     TEXT,
    dam_name      TEXT,
    FOREIGN KEY(sire_id) REFERENCES horses(horse_id),
    FOREIGN KEY(dam_id)  REFERENCES horses(horse_id)
);

-- =========================
-- 4. jockeys（騎手マスタ）
-- =========================
CREATE TABLE IF NOT EXISTS jockeys (
    jockey_id   TEXT PRIMARY KEY,
    jockey_name TEXT NOT NULL,
    belonging   TEXT
);

-- =========================
-- 5. trainers（調教師マスタ）
-- =========================
CREATE TABLE IF NOT EXISTS trainers (
    trainer_id   TEXT PRIMARY KEY,
    trainer_name TEXT NOT NULL,
    belonging    TEXT
);

-- =========================
-- 6. race_results（レース×馬の結果：ファクト）
-- =========================
CREATE TABLE IF NOT EXISTS race_results (
    race_id           TEXT NOT NULL,
    horse_id          TEXT NOT NULL,
    bracket_no        INTEGER,
    horse_no          INTEGER,
    finish_rank       INTEGER,
    finish_status     TEXT,
    finish_time_sec   REAL,
    odds              REAL,
    popularity        INTEGER,
    weight            INTEGER,
    weight_diff       INTEGER,
    jockey_id         TEXT,
    trainer_id        TEXT,
    corner_pass_order TEXT,
    last_3f           REAL,
    margin_sec        REAL,
    prize             REAL,
    PRIMARY KEY (race_id, horse_id),
    FOREIGN KEY (race_id)   REFERENCES races(race_id),
    FOREIGN KEY (horse_id)  REFERENCES horses(horse_id),
    FOREIGN KEY (jockey_id) REFERENCES jockeys(jockey_id),
    FOREIGN KEY (trainer_id)REFERENCES trainers(trainer_id)
);

-- =========================
-- 7. race_laps（ラップタイム）
-- =========================
CREATE TABLE IF NOT EXISTS race_laps (
    race_id   TEXT NOT NULL,
    lap_no    INTEGER NOT NULL,
    lap_len   INTEGER,
    lap_time  REAL,
    PRIMARY KEY (race_id, lap_no),
    FOREIGN KEY (race_id) REFERENCES races(race_id)
);
"""

INDEXES_SQL = r"""
-- インデックス定義

-- races
CREATE INDEX IF NOT EXISTS idx_races_date
    ON races(date);
CREATE INDEX IF NOT EXISTS idx_races_course
    ON races(course_id);

-- race_results
CREATE INDEX IF NOT EXISTS idx_results_horse
    ON race_results(horse_id);
CREATE INDEX IF NOT EXISTS idx_results_jockey
    ON race_results(jockey_id);
CREATE INDEX IF NOT EXISTS idx_results_trainer
    ON race_results(trainer_id);

-- horses（血統検索用）
CREATE INDEX IF NOT EXISTS idx_horses_sire
    ON horses(sire_id);
CREATE INDEX IF NOT EXISTS idx_horses_dam
    ON horses(dam_id);
"""

VIEWS_SQL = r"""
-- 分析・学習用ビュー

CREATE VIEW IF NOT EXISTS v_race_features AS
SELECT
    r.race_id,
    r.date,
    r.course_id,
    c.venue_id,
    c.course_name,
    c.surface       AS course_surface,
    c.track_type    AS course_track_type,
    c.straight_len,
    c.slope_max,

    r.race_no,
    r.race_name,
    r.distance,
    r.surface       AS race_surface,
    r.course_ver,
    r.weather,
    r.going,
    r.class,
    r.num_runners,
    r.win_time_sec,

    rr.horse_id,
    h.horse_name,
    h.birth_year,
    h.coat_color,
    h.breeder_name,
    h.sire_name,
    h.dam_name,

    rr.jockey_id,
    j.jockey_name,
    rr.trainer_id,
    t.trainer_name,

    rr.bracket_no,
    rr.horse_no,
    rr.finish_rank,
    rr.finish_status,
    rr.finish_time_sec,
    rr.odds,
    rr.popularity,
    rr.weight,
    rr.weight_diff,
    rr.last_3f,
    rr.margin_sec,
    rr.prize
FROM race_results rr
LEFT JOIN races    r ON rr.race_id   = r.race_id
LEFT JOIN courses  c ON r.course_id  = c.course_id
LEFT JOIN horses   h ON rr.horse_id  = h.horse_id
LEFT JOIN jockeys  j ON rr.jockey_id = j.jockey_id
LEFT JOIN trainers t ON rr.trainer_id= t.trainer_id;
"""


def init_db(db_path: Path) -> None:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    try:
        conn.executescript(SCHEMA_SQL)
        conn.executescript(INDEXES_SQL)
        conn.executescript(VIEWS_SQL)
        conn.commit()
        print(f"[OK] Initialized SQLite DB schema at: {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    print(f"Using DB file: {DB_PATH}")
    init_db(DB_PATH)
