# from pathlib import Path
# import sqlite3
# import pandas as pd

# ROOT = Path(__file__).resolve().parents[1]
# DB_PATH = ROOT / "data" / "keiba.db"

# print("DB_PATH:", DB_PATH)

# conn = sqlite3.connect(DB_PATH)

# # R2025TOK11 だけを DataFrame で見る
# df = pd.read_sql_query(
#     """
#     SELECT race_id,
#            horse_id,
#            horse_no,
#            finish_rank,
#            popularity,
#            weight,
#            weight_diff,
#            corner_pass_order,
#            last_3f
#     FROM race_results
#     WHERE race_id = 'R2025TOK11'
#     ORDER BY finish_rank
#     """,
#     conn,
# )

# print(df)

# conn.close()

from pathlib import Path
import sqlite3

ROOT = Path(__file__).resolve().parents[1]
DB_PATH = ROOT / "data" / "keiba.db"

print("DB_PATH:", DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

print("=== courses ===")
for r in cur.execute("SELECT course_id, venue_id, course_name, surface, track_type FROM courses;"):
    print(r)

print("=== races ===")
for r in cur.execute("SELECT race_id, date, course_id, race_no, race_name, num_runners FROM races WHERE race_id='R2025KYT11';"):
    print(r)

print("=== race_results ===")
for r in cur.execute("SELECT race_id, horse_id, finish_rank, popularity FROM race_results WHERE race_id='R2025KYT11' ORDER BY finish_rank;"):
    print(r)


conn.close()
