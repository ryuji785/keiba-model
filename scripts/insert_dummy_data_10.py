# scripts/insert_dummy_data_10.py
import sqlite3
from pathlib import Path
import random

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "keiba.db"
print("Using DB:", DB_PATH)

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# --------- いったんダミーデータを全削除（実験用） ---------
cur.execute("DELETE FROM race_results")
cur.execute("DELETE FROM race_laps")
cur.execute("DELETE FROM horses")
cur.execute("DELETE FROM jockeys")
cur.execute("DELETE FROM trainers")
cur.execute("DELETE FROM races")
cur.execute("DELETE FROM courses")
conn.commit()

# --------- courses（コースマスタ：東京芝2400mイメージ） ---------
cur.execute("""
    INSERT INTO courses(
        course_id, venue_id, course_name,
        surface, track_type, straight_len,
        slope_max, is_turf_start, features_text
    )
    VALUES (
        'TOK_T', 'TOK', '東京 芝2400m',
        'turf', 'standard', 525,
        2.0, 0, '長い直線と最後の急坂が特徴のコース'
    )
""")

# --------- jockeys / trainers マスタ ---------
jockeys = ["C.ルメール", "川田 将雅", "武 豊", "横山 武史"]
trainers = ["友道 康夫", "中内田 充正", "矢作 芳人"]

for j in jockeys:
    cur.execute(
        "INSERT INTO jockeys(jockey_id, jockey_name, belonging) VALUES (?, ?, ?)",
        (j, j, "JRA")
    )

for t in trainers:
    cur.execute(
        "INSERT INTO trainers(trainer_id, trainer_name, belonging) VALUES (?, ?, ?)",
        (t, t, "栗東")
    )

# --------- horses マスタ（10頭） ---------
horses = [f"ダミーホース{i}" for i in range(1, 11)]

for i, h in enumerate(horses):
    cur.execute("""
        INSERT INTO horses(
            horse_id, horse_name, sex, birth_year,
            coat_color, breeder_name, sire_name, dam_name
        )
        VALUES (?, ?, '牡', 2020, '鹿毛', 'ダミー牧場', 'ダミーサイヤー', 'ダミーダム')
    """, (f"H{i+1}", h))

# --------- races（レース1つ） ---------
race_id = "R2024TOK11"

cur.execute("""
    INSERT INTO races(
        race_id, date, course_id, race_no, race_name,
        distance, surface, course_ver, weather, going,
        class, num_runners, win_time_sec
    )
    VALUES (
        ?, '2024-10-20', 'TOK_T', 11, 'ダミーG1',
        2400, 'turf', 'A', 'fine', 'good',
        'G1', 10, 147.3
    )
""", (race_id,))

# --------- race_results（10頭分） ---------
for i in range(10):
    horse_id = f"H{i+1}"
    jockey = random.choice(jockeys)
    trainer = random.choice(trainers)

    finish_rank = i + 1  # 1〜10着

    cur.execute("""
        INSERT INTO race_results(
            race_id, horse_id,
            bracket_no, horse_no,
            finish_rank, finish_status,
            finish_time_sec, odds, popularity,
            weight, weight_diff,
            jockey_id, trainer_id,
            corner_pass_order, last_3f,
            margin_sec, prize
        )
        VALUES (?, ?, ?, ?, ?, 'OK',
                ?, ?, ?,
                ?, ?,
                ?, ?,
                ?, ?, ?, ?)
    """, (
        race_id,
        horse_id,
        (i % 8) + 1,         # 枠番
        i + 1,               # 馬番
        finish_rank,
        147.3 + finish_rank * 0.3,               # いいかげんなタイム
        round(random.uniform(1.5, 50.0), 1),     # オッズ
        finish_rank,                              # 人気 = 着順（ダミー）
        480 + random.randint(-10, 10),           # 馬体重
        random.randint(-5, 5),                   # 増減
        jockey,
        trainer,
        "1-2-3-4",                                # コーナー通過順ダミー
        round(33 + i * 0.2, 1),                  # 上がり3F
        round(i * 0.2, 1),                       # 着差(秒)
        1000000 if finish_rank <= 3 else 0       # 賞金
    ))

# --------- race_laps（レース全体のラップ：コースごと） ---------
# 200m区切りで12ラップのイメージ
for lap_no in range(1, 13):
    cur.execute("""
        INSERT INTO race_laps(
            race_id, lap_no, lap_len, lap_time
        )
        VALUES (?, ?, ?, ?)
    """, (
        race_id,
        lap_no,
        200,
        round(12.0 + random.uniform(-0.5, 0.5), 2)
    ))

conn.commit()
conn.close()
print("=== Dummy 10 horses inserted (v3 schema) ===")
