# scripts/etl_one_race.py
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
import sys
from typing import Dict, Any
import re
import pandas as pd

# プロジェクトルートを解決して src を import path に追加
ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT / "src"))

from jra_parser import parse_race_page, parse_time_to_sec  # type: ignore

def upsert_race(
    conn: sqlite3.Connection,
    race_meta: Dict[str, Any],
    num_runners: int,
    win_time_sec: float | None,
) -> None:
    cur = conn.cursor()

    race_id = race_meta.get("race_id")

    # ① race_meta から取れなかった場合に備えてフォールバック
    race_no = race_meta.get("race_no")
    if race_no is None and isinstance(race_id, str):
        m = re.search(r"(\d{1,2})$", race_id)
        if m:
            race_no = int(m.group(1))

    # 念のため、それでも None なら 0 を入れておく（NOT NULL 回避用）
    if race_no is None:
        race_no = 0

    cur.execute(
        """
        INSERT INTO races (
            race_id, date, course_id, race_no, race_name,
            distance, surface, course_ver, weather, going,
            class, num_runners, win_time_sec
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(race_id) DO UPDATE SET
            date         = excluded.date,
            course_id    = excluded.course_id,
            race_no      = excluded.race_no,
            race_name    = excluded.race_name,
            distance     = excluded.distance,
            surface      = excluded.surface,
            course_ver   = excluded.course_ver,
            weather      = excluded.weather,
            going        = excluded.going,
            class        = excluded.class,
            num_runners  = excluded.num_runners,
            win_time_sec = excluded.win_time_sec
        """,
        (
            race_id,
            race_meta.get("date"),
            race_meta.get("course_id"),
            race_no,  # ← ここが race_meta["race_no"] ではなくフォールバック付き race_no
            race_meta.get("race_name"),
            race_meta.get("distance"),
            race_meta.get("surface"),
            race_meta.get("course_ver"),
            race_meta.get("weather"),
            race_meta.get("going"),
            race_meta.get("race_class"),
            num_runners,
            win_time_sec,
        ),
    )
    conn.commit()


def upsert_jockeys(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """jockeys テーブルに名前ベースで upsert"""
    cur = conn.cursor()
    jockeys = sorted(set(df["jockey_name"].dropna().astype(str)))
    for name in jockeys:
        jockey_id = name  # 現状は名前をそのままIDとして使う
        cur.execute(
            """
            INSERT INTO jockeys (jockey_id, jockey_name)
            VALUES (?, ?)
            ON CONFLICT(jockey_id) DO UPDATE SET
                jockey_name = excluded.jockey_name
            """,
            (jockey_id, name),
        )
    conn.commit()


def upsert_trainers(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    """trainers テーブルに名前ベースで upsert"""
    cur = conn.cursor()
    trainers = sorted(set(df["trainer_name"].dropna().astype(str)))
    for name in trainers:
        trainer_id = name
        cur.execute(
            """
            INSERT INTO trainers (trainer_id, trainer_name)
            VALUES (?, ?)
            ON CONFLICT(trainer_id) DO UPDATE SET
                trainer_name = excluded.trainer_name
            """,
            (trainer_id, name),
        )
    conn.commit()


def upsert_horses(conn: sqlite3.Connection, race_id: str, df: pd.DataFrame) -> None:
    """
    horses テーブルへの upsert。
    いったん「race_id + 馬番」で固有IDを振る（例: R2025KYT11_H01）。
    """
    cur = conn.cursor()
    for _, row in df.iterrows():
        try:
            horse_no = int(row["horse_no"])
        except Exception:
            continue

        horse_id = f"{race_id}_H{horse_no:02d}"
        horse_name = row.get("horse_name")
        sex = row.get("sex")

        cur.execute(
            """
            INSERT INTO horses (
                horse_id, horse_name, sex, birth_year,
                coat_color, breeder_name, sire_id, dam_id,
                sire_name, dam_name
            )
            VALUES (?, ?, ?, NULL, NULL, NULL, NULL, NULL, NULL, NULL)
            ON CONFLICT(horse_id) DO UPDATE SET
                horse_name = excluded.horse_name,
                sex        = excluded.sex
            """,
            (horse_id, horse_name, sex),
        )
    conn.commit()


def upsert_race_results(conn: sqlite3.Connection, race_id: str, df: pd.DataFrame) -> None:
    """race_results テーブルへの upsert（v3 定義準拠）"""
    cur = conn.cursor()

    for _, row in df.iterrows():
        try:
            horse_no = int(row["horse_no"])
        except Exception:
            continue

        horse_id = f"{race_id}_H{horse_no:02d}"

        # 着順
        finish_rank = row.get("finish_rank")
        try:
            finish_rank = int(finish_rank)
        except Exception:
            finish_rank = None

        # 人気
        popularity = row.get("popularity")
        try:
            popularity = int(popularity)
        except Exception:
            popularity = None

        # 体重
        weight = row.get("body_weight")
        weight_diff = row.get("body_weight_diff")

        # 上がり3F
        last_3f = row.get("last_3f_est")
        try:
            last_3f = float(last_3f)
        except Exception:
            last_3f = None

        # 走破タイム
        time_str = row.get("time_str")
        finish_time_sec = parse_time_to_sec(time_str)

        # 1着との差（今は未実装）
        margin_sec = None

        # ステータス（とりあえず完走扱い）
        finish_status = "OK" if finish_rank is not None else None

        jockey_name = row.get("jockey_name")
        trainer_name = row.get("trainer_name")
        jockey_id = jockey_name if pd.notna(jockey_name) else None
        trainer_id = trainer_name if pd.notna(trainer_name) else None

        corner_pass_order = row.get("corner_order")

        cur.execute(
            """
            INSERT INTO race_results (
                race_id, horse_id,
                bracket_no, horse_no,
                finish_rank, finish_status,
                finish_time_sec,
                odds, popularity,
                weight, weight_diff,
                jockey_id, trainer_id,
                corner_pass_order,
                last_3f, margin_sec,
                prize
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(race_id, horse_id) DO UPDATE SET
                bracket_no        = excluded.bracket_no,
                horse_no          = excluded.horse_no,
                finish_rank       = excluded.finish_rank,
                finish_status     = excluded.finish_status,
                finish_time_sec   = excluded.finish_time_sec,
                odds              = excluded.odds,
                popularity        = excluded.popularity,
                weight            = excluded.weight,
                weight_diff       = excluded.weight_diff,
                jockey_id         = excluded.jockey_id,
                trainer_id        = excluded.trainer_id,
                corner_pass_order = excluded.corner_pass_order,
                last_3f           = excluded.last_3f,
                margin_sec        = excluded.margin_sec,
                prize             = excluded.prize
            """,
            (
                race_id,
                horse_id,
                row.get("bracket_no"),
                horse_no,
                finish_rank,
                finish_status,
                finish_time_sec,
                None,  # odds は別ページから取るまでは NULL
                popularity,
                weight,
                weight_diff,
                jockey_id,
                trainer_id,
                corner_pass_order,
                last_3f,
                margin_sec,
                None,  # prize も現時点では NULL
            ),
        )
    conn.commit()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("html_path", type=str, help="保存したJRAレース結果HTMLのパス")
    parser.add_argument(
        "--race-id",
        required=True,
        help="DB上で使う race_id（例: R2025KYT11）",
    )
    args = parser.parse_args()

    html_path = Path(args.html_path)
    race_id = args.race_id

    print(f"[INFO] parsing {html_path}")
    race_meta, df = parse_race_page(html_path, race_id)

    # DB 接続
    db_path = ROOT / "data" / "keiba.db"
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")

    # 1着タイム → races.win_time_sec
    win_time_sec = None
    if not df.empty:
        first_time = df.loc[df["finish_rank"] == df["finish_rank"].min(), "time_str"].iloc[0]
        win_time_sec = parse_time_to_sec(first_time)

    # races
    upsert_race(conn, race_meta, num_runners=len(df), win_time_sec=win_time_sec)
    # jockeys / trainers / horses
    upsert_jockeys(conn, df)
    upsert_trainers(conn, df)
    upsert_horses(conn, race_id, df)
    # race_results
    upsert_race_results(conn, race_id, df)

    conn.close()
    print(f"[INFO] done. race_id={race_id}, num_rows={len(df)}")


if __name__ == "__main__":
    main()
