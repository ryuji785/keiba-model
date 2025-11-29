# scripts/insert_course_kyt.py
from pathlib import Path
import sqlite3

# keiba-model のルートを解決
ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "keiba.db"

print("DB_PATH:", DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# KYT_T_OUT がなければ 1件だけ INSERT
cur.execute(
    """
    INSERT OR IGNORE INTO courses (
        course_id, venue_id, course_name,
        surface, track_type,
        straight_len, slope_max, is_turf_start,
        features_text
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    (
        "KYT_T_OUT",          # course_id
        "KYT",                # venue_id（京都）
        "京都 芝 外回り",     # course_name
        "turf",               # surface
        "outer",              # track_type
        None,                 # straight_len（あとで埋めてもOK）
        None,                 # slope_max
        0,                    # is_turf_start
        "京都芝外回りコース（暫定登録）",  # features_text
    ),
)

conn.commit()
conn.close()
print("done: inserted KYT_T_OUT (if not exists)")
