# scripts/create_view_v_race_features.py
from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "keiba.db"

print("DB_PATH:", DB_PATH)

sql = """
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

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute(sql)
conn.commit()
conn.close()

print("done: created v_race_features (IF NOT EXISTS)")
