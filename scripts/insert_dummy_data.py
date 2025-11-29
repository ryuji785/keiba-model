# scripts/insert_dummy_data.py

from pathlib import Path
import sqlite3

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "keiba.db"


def clear_all(conn: sqlite3.Connection) -> None:
    """再実行しやすいように、既存データを全部消す"""
    cur = conn.cursor()
    # 依存関係の都合で、child → parent の順で消す
    cur.execute("DELETE FROM race_laps;")
    cur.execute("DELETE FROM race_results;")
    cur.execute("DELETE FROM races;")
    cur.execute("DELETE FROM horses;")
    cur.execute("DELETE FROM jockeys;")
    cur.execute("DELETE FROM trainers;")
    cur.execute("DELETE FROM courses;")
    conn.commit()


def insert_dummy_courses(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    courses = [
        (
            "TOK_T",          # course_id
            "TOK",            # venue_id
            "東京 芝",        # course_name
            "turf",           # surface
            "standard",       # track_type
            525,              # straight_len
            2.0,              # slope_max
            0,                # is_turf_start
            "長い直線とゴール前の坂が特徴の東京芝コース。瞬発力が重要。",  # features_text
        ),
        (
            "TOK_D",
            "TOK",
            "東京 ダート",
            "dirt",
            "standard",
            501,
            1.5,
            1,
            "スタートのみ芝を走り、その後ダートに入るコース。先行有利と言われる。",
        ),
    ]
    cur.executemany(
        """
        INSERT INTO courses (
            course_id, venue_id, course_name,
            surface, track_type, straight_len,
            slope_max, is_turf_start, features_text
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        courses,
    )
    conn.commit()


def insert_dummy_people(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    jockeys = [
        ("J001", "C.ルメール", "栗東"),
        ("J002", "川田 将雅", "栗東"),
    ]
    cur.executemany(
        "INSERT INTO jockeys (jockey_id, jockey_name, belonging) VALUES (?, ?, ?);",
        jockeys,
    )

    trainers = [
        ("T001", "藤沢 和雄", "美浦"),
        ("T002", "友道 康夫", "栗東"),
    ]
    cur.executemany(
        "INSERT INTO trainers (trainer_id, trainer_name, belonging) VALUES (?, ?, ?);",
        trainers,
    )

    conn.commit()


def insert_dummy_horses(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    horses = [
        # sire_id / dam_id はとりあえず None（本番データで埋めていける）
        ("H001", "サンプルホースA", "牡", 2019, "鹿毛", "ノーザンファーム", None, None, "ディープインパクト", "シーザリオ"),
        ("H002", "サンプルホースB", "牝", 2020, "黒鹿毛", "社台ファーム", None, None, "キングカメハメハ", "ブエナビスタ"),
        ("H003", "サンプルホースC", "牡", 2018, "芦毛", "ビッグレッドF", None, None, "クロフネ", "マイネテンプレス"),
    ]
    cur.executemany(
        """
        INSERT INTO horses (
            horse_id, horse_name, sex, birth_year,
            coat_color, breeder_name,
            sire_id, dam_id, sire_name, dam_name
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        horses,
    )
    conn.commit()


def insert_dummy_race_and_results(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()

    # 1レースだけ作る（東京芝2400の重賞をイメージ）
    race = (
        "R2024TOK11",   # race_id
        "2024-10-20",   # date
        "TOK_T",        # course_id
        11,             # race_no
        "テスト重賞（ダミー）",  # race_name
        2400,           # distance
        "turf",         # surface
        "A",            # course_ver
        "fine",         # weather
        "good",         # going
        "G2",           # class
        3,              # num_runners
        144.3,          # win_time_sec (2:24.3 相当)
    )
    cur.execute(
        """
        INSERT INTO races (
            race_id, date, course_id, race_no, race_name,
            distance, surface, course_ver, weather, going,
            class, num_runners, win_time_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        race,
    )

    # 3頭分の結果
    results = [
        (
            "R2024TOK11",  # race_id
            "H001",        # horse_id
            3,             # bracket_no
            5,             # horse_no
            1,             # finish_rank
            "OK",          # finish_status
            144.3,         # finish_time_sec
            2.1,           # odds
            1,             # popularity
            480,           # weight
            +2,            # weight_diff
            "J001",        # jockey_id
            "T001",        # trainer_id
            "4-4-3-1",     # corner_pass_order
            33.5,          # last_3f
            0.0,           # margin_sec
            5500_000,      # prize
        ),
        (
            "R2024TOK11",
            "H002",
            5,
            9,
            2,
            "OK",
            144.5,
            3.8,
            2,
            460,
            0,
            "J002",
            "T002",
            "5-5-4-2",
            33.6,
            0.2,
            2200_000,
        ),
        (
            "R2024TOK11",
            "H003",
            1,
            1,
            3,
            "OK",
            145.0,
            8.7,
            3,
            500,
            -4,
            "J002",
            "T002",
            "1-1-1-3",
            34.2,
            0.7,
            1400_000,
        ),
    ]
    cur.executemany(
        """
        INSERT INTO race_results (
            race_id, horse_id,
            bracket_no, horse_no,
            finish_rank, finish_status, finish_time_sec,
            odds, popularity,
            weight, weight_diff,
            jockey_id, trainer_id,
            corner_pass_order, last_3f, margin_sec,
            prize
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        results,
    )

    conn.commit()


def insert_dummy_laps(conn: sqlite3.Connection) -> None:
    """2400m を 200m ごとに12ラップとして適当に作る"""
    cur = conn.cursor()
    race_id = "R2024TOK11"
    lap_len = 200

    # とりあえず緩→締まり気味のラップを適当に置く
    lap_times = [13.0, 12.5, 12.4, 12.3, 12.2, 12.1,
                 12.0, 12.0, 11.9, 11.8, 11.7, 11.6]

    laps = []
    for i, t in enumerate(lap_times, start=1):
        laps.append((race_id, i, lap_len, t))

    cur.executemany(
        "INSERT INTO race_laps (race_id, lap_no, lap_len, lap_time) VALUES (?, ?, ?, ?);",
        laps,
    )
    conn.commit()


def show_summary(conn: sqlite3.Connection) -> None:
    cur = conn.cursor()
    print("=== Row counts ===")
    for table in ["courses", "races", "horses", "jockeys", "trainers", "race_results", "race_laps"]:
        cur.execute(f"SELECT COUNT(*) FROM {table};")
        (cnt,) = cur.fetchone()
        print(f"{table:12}: {cnt}")

    # v_race_features の一部だけ確認
    print("\n=== Sample from v_race_features ===")
    cur.execute(
        "SELECT race_id, horse_name, jockey_name, finish_rank, odds "
        "FROM v_race_features ORDER BY finish_rank LIMIT 5;"
    )
    for row in cur.fetchall():
        print(row)


def main() -> None:
    print("Using DB:", DB_PATH)
    conn = sqlite3.connect(DB_PATH)
    try:
        clear_all(conn)
        insert_dummy_courses(conn)
        insert_dummy_people(conn)
        insert_dummy_horses(conn)
        insert_dummy_race_and_results(conn)
        insert_dummy_laps(conn)
        show_summary(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
