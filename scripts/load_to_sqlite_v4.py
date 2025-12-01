"""
Load layer for v4 schema: races, race_results, horses, jockeys, trainers.
"""
from __future__ import annotations

import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = "data/keiba.db"


def _get_connection(db_path: str) -> sqlite3.Connection:
    db_file = Path(db_path)
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def _ensure_course(conn: sqlite3.Connection, race: Dict[str, Any]) -> None:
    course_id = race.get("course_id")
    if not course_id:
        return
    venue_id = race.get("venue_id") or "UNK"
    surface = race.get("surface") or "unknown"
    distance = race.get("distance") or 0
    course_name = f"{venue_id} {surface} {distance}m"
    sql = """
        INSERT OR IGNORE INTO courses (
            course_id, venue_id, course_name, surface
        ) VALUES (?, ?, ?, ?)
    """
    conn.execute(sql, (course_id, venue_id, course_name, surface))


def _insert_horses(conn: sqlite3.Connection, horses: Dict[str, Dict[str, Any]]) -> None:
    if not horses:
        return
    sql = "INSERT OR IGNORE INTO horses (horse_id, horse_name, sex, birth_year) VALUES (?, ?, ?, ?)"
    for horse_id, info in horses.items():
        conn.execute(sql, (horse_id, info.get("horse_name"), info.get("sex"), info.get("birth_year")))


def _insert_jockeys(conn: sqlite3.Connection, jockeys: Dict[str, Dict[str, Any]]) -> None:
    if not jockeys:
        return
    sql = "INSERT OR IGNORE INTO jockeys (jockey_id, jockey_name) VALUES (?, ?)"
    for jockey_id, info in jockeys.items():
        conn.execute(sql, (jockey_id, info.get("jockey_name")))


def _insert_trainers(conn: sqlite3.Connection, trainers: Dict[str, Dict[str, Any]]) -> None:
    if not trainers:
        return
    sql = "INSERT OR IGNORE INTO trainers (trainer_id, trainer_name) VALUES (?, ?)"
    for trainer_id, info in trainers.items():
        conn.execute(sql, (trainer_id, info.get("trainer_name")))


def _upsert_race(conn: sqlite3.Connection, race: Dict[str, Any]) -> None:
    mandatory = ["race_id", "date", "course_id", "race_no", "distance", "surface"]
    if not all(k in race for k in mandatory):
        logger.warning("race_dict missing keys, skip races insert: %s", race)
        return
    if any(race.get(k) is None for k in mandatory):
        logger.warning("race_dict has None mandatory values, skip races insert: %s", race)
        return

    sql = """
        INSERT OR REPLACE INTO races (
            race_id, date, course_id, race_no, race_name,
            distance, surface, weather, going, class,
            age_cond, sex_cond, num_runners, win_time_sec, race_type
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params = (
        race.get("race_id"),
        race.get("date"),
        race.get("course_id"),
        race.get("race_no"),
        race.get("race_name"),
        race.get("distance"),
        race.get("surface"),
        race.get("weather"),
        race.get("going"),
        race.get("class"),
        race.get("age_cond"),
        race.get("sex_cond"),
        race.get("num_runners"),
        race.get("win_time_sec"),
        race.get("race_type", "FLAT"),
    )
    conn.execute(sql, params)


def _upsert_race_results(conn: sqlite3.Connection, results: List[Dict[str, Any]]) -> None:
    if not results:
        return
    filtered: List[Dict[str, Any]] = []
    skipped_idx: List[int] = []
    for i, r in enumerate(results):
        if r.get("race_id") is None or r.get("horse_id") is None:
            skipped_idx.append(i)
        else:
            filtered.append(r)
    if skipped_idx:
        logger.warning("race_results rows missing PK fields, skipped indexes: %s", skipped_idx)
    results = filtered
    if not results:
        return

    sql = """
        INSERT INTO race_results (
            race_id, horse_id, bracket_no, horse_no,
            finish_rank, finish_status, finish_time_sec,
            odds, popularity, weight, weight_diff, body_weight,
            jockey_id, trainer_id, corner_pass_order,
            last_3f, margin_sec, prize,
            prev_race_id, prev_finish_rank, prev_margin_sec,
            prev_time_sec, prev_last_3f, days_since_last
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(race_id, horse_id) DO UPDATE SET
            bracket_no=excluded.bracket_no,
            horse_no=excluded.horse_no,
            finish_rank=excluded.finish_rank,
            finish_status=excluded.finish_status,
            finish_time_sec=excluded.finish_time_sec,
            odds=excluded.odds,
            popularity=excluded.popularity,
            weight=excluded.weight,
            weight_diff=excluded.weight_diff,
            body_weight=excluded.body_weight,
            jockey_id=excluded.jockey_id,
            trainer_id=excluded.trainer_id,
            corner_pass_order=excluded.corner_pass_order,
            last_3f=excluded.last_3f,
            margin_sec=excluded.margin_sec,
            prize=excluded.prize,
            prev_race_id=excluded.prev_race_id,
            prev_finish_rank=excluded.prev_finish_rank,
            prev_margin_sec=excluded.prev_margin_sec,
            prev_time_sec=excluded.prev_time_sec,
            prev_last_3f=excluded.prev_last_3f,
            days_since_last=excluded.days_since_last
    """
    for row in results:
        params = (
            row.get("race_id"),
            row.get("horse_id"),
            row.get("bracket_no"),
            row.get("horse_no"),
            row.get("finish_rank"),
            row.get("finish_status"),
            row.get("finish_time_sec"),
            row.get("odds"),
            row.get("popularity"),
            row.get("weight"),
            row.get("weight_diff"),
            row.get("body_weight"),
            row.get("jockey_id"),
            row.get("trainer_id"),
            row.get("corner_pass_order"),
            row.get("last_3f"),
            row.get("margin_sec"),
            row.get("prize"),
            row.get("prev_race_id"),
            row.get("prev_finish_rank"),
            row.get("prev_margin_sec"),
            row.get("prev_time_sec"),
            row.get("prev_last_3f"),
            row.get("days_since_last"),
        )
        conn.execute(sql, params)


def load_race_to_db(
    race_dict: Dict[str, Any],
    results_list: List[Dict[str, Any]],
    horses_dict: Dict[str, Dict[str, Any]],
    jockeys_dict: Dict[str, Dict[str, Any]],
    trainers_dict: Dict[str, Dict[str, Any]],
    db_path: str = DEFAULT_DB_PATH,
) -> None:
    logger.info("Start load_race_to_db race_id=%s", race_dict.get("race_id"))
    conn = _get_connection(db_path)
    try:
        conn.execute("BEGIN")
        _ensure_course(conn, race_dict)
        _insert_horses(conn, horses_dict)
        _insert_jockeys(conn, jockeys_dict)
        _insert_trainers(conn, trainers_dict)
        _upsert_race(conn, race_dict)
        _upsert_race_results(conn, results_list)
        conn.commit()
        logger.info("Finished load_race_to_db race_id=%s (results=%d)", race_dict.get("race_id"), len(results_list))
    except Exception:
        conn.rollback()
        logger.exception("Error while loading race_id=%s", race_dict.get("race_id"))
        raise
    finally:
        conn.close()
