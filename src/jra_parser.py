# src/jra_parser.py
from __future__ import annotations

from pathlib import Path
from typing import Tuple, Dict, Any

import pandas as pd
from bs4 import BeautifulSoup
import re


def parse_time_to_sec(time_str: str | None) -> float | None:
    """'1:34.2' みたいなタイム文字列 → 秒(float)。パースできなければ None。"""
    if not isinstance(time_str, str):
        return None
    s = time_str.strip()
    if not s:
        return None
    m = re.match(r"(?:(\d+):)?(\d+(?:\.\d+)?)$", s)
    if not m:
        return None
    minutes = m.group(1)
    seconds = float(m.group(2))
    return (int(minutes) * 60 + seconds) if minutes is not None else seconds


def _parse_race_header(soup: BeautifulSoup) -> Dict[str, Any]:
    """race_header ブロックから日付・開催・天候・馬場・コース情報を抜き出す。"""
    race_header = soup.find("div", "race_header")
    info: Dict[str, Any] = {}
    if not race_header:
        return info

    lines = [ln.strip() for ln in race_header.text.splitlines()]
    lines = [ln for ln in lines if ln]

    # 1行目: 2025年10月26日（日曜） 3回京都9日
    if lines:
        line0 = lines[0]
        m = re.search(r"(\d{4})年(\d{1,2})月(\d{1,2})日", line0)
        if m:
            y, mn, d = m.groups()
            info["date"] = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
        m2 = re.search(r"\d回([^\d]+)\d日", line0)
        if m2:
            info["venue_jp"] = m2.group(1).strip()

    # 例: 天候小雨芝稍重
    weather_line = next((ln for ln in lines if ln.startswith("天候")), None)
    if weather_line:
        rest = weather_line.replace("天候", "", 1)
        m = re.match(r"(.+?)(芝|ダート)(.+)", rest)
        if m:
            info["weather_jp"] = m.group(1)
            info["surface_jp"] = m.group(2)
            info["going_jp"] = m.group(3)

    # 例: コース：3,000メートル（芝・右 外）
    m3 = re.search(r"コース：([^\n]+)", race_header.text)
    if m3:
        course_text = m3.group(1).strip()
        info["course_text"] = course_text
        m4 = re.search(r"([\d,]+)メートル", course_text)
        if m4:
            info["distance"] = int(m4.group(1).replace(",", ""))
        m5 = re.search(r"（(.+?)）", course_text)
        if m5:
            info["course_inside"] = m5.group(1)

    return info


def _find_race_name(soup: BeautifulSoup) -> str | None:
    """h2タグから '第86回菊花賞' のようなレース名を取得。"""
    for h2 in soup.find_all("h2"):
        txt = h2.text.strip()
        if re.match(r"^第\d+回", txt):
            return txt
    return None


def parse_race_meta(soup: BeautifulSoup, race_id: str) -> Dict[str, Any]:
    """レース全体のメタ情報を辞書で返す（races テーブル用）。"""
    header = _parse_race_header(soup)
    meta: Dict[str, Any] = {
        "race_id": race_id,
        "date": header.get("date"),
        "distance": header.get("distance"),
        "weather": header.get("weather_jp"),
        "going": header.get("going_jp"),
    }

    # レース名
    race_name = _find_race_name(soup)
    meta["race_name"] = race_name

    # レース番号: h1 の「11レース」から取得
    h1 = soup.find("h1", string=re.compile("レース結果"))
    if h1:
        m = re.search(r"(\d+)レース", h1.text)
        if m:
            meta["race_no"] = int(m.group(1))

    # 競馬場 → venue_id, course_id
    VENUE_MAP = {
        "東京": "TOK",
        "京都": "KYT",
        "阪神": "HAN",
        "中山": "NAK",
        "新潟": "NII",
        "小倉": "KOK",
        "札幌": "SAP",
        "函館": "HAK",
        "中京": "CKY",
        "福島": "FKS",
    }
    venue_jp = header.get("venue_jp")
    venue_id = VENUE_MAP.get(venue_jp) if venue_jp else None

    surface_jp = header.get("surface_jp")
    surface_en = None
    if surface_jp == "芝":
        surface_en = "turf"
    elif surface_jp == "ダート":
        surface_en = "dirt"

    # 内回り/外回り/直線
    track_type = None
    inside = header.get("course_inside") or ""
    if "直線" in inside:
        track_type = "straight"
    elif "内" in inside:
        track_type = "inner"
    elif "外" in inside:
        track_type = "outer"

    suffix = ""
    if surface_en == "turf":
        suffix = "T"
    elif surface_en == "dirt":
        suffix = "D"

    if track_type == "inner":
        suffix += "_IN"
    elif track_type == "outer":
        suffix += "_OUT"
    elif track_type == "straight":
        suffix += "_STR"

    course_id = f"{venue_id}_{suffix}" if venue_id and suffix else None
    meta["course_id"] = course_id
    meta["surface"] = surface_en

    # レースクラス（例: "3歳 オープン GⅠ" など）を header からざっくり拾う
    race_header = soup.find("div", "race_header")
    if race_header and race_name:
        lines = [ln.strip() for ln in race_header.text.splitlines()]
        lines = [ln for ln in lines if ln]
        try:
            idx = lines.index(race_name)
            class_tokens = []
            for l in lines[idx + 1 : idx + 5]:
                if any(kw in l for kw in ["サラ", "歳", "オープン", "G", "グレード", "混合", "指定"]):
                    class_tokens.append(l.strip())
            if class_tokens:
                meta["race_class"] = " ".join(class_tokens)
        except ValueError:
            pass

    # course_ver（A/B/Cコース）は今は取得しない
    meta["course_ver"] = None

    return meta


def parse_race_results(soup: BeautifulSoup) -> pd.DataFrame:
    """
    着順テーブルを DataFrame に整形。
    race_results / horses / jockeys / trainers の元データになる。
    """
    th_place = soup.find("th", class_="place")
    if th_place is None:
        raise RuntimeError("着順テーブル(th.place)が見つかりませんでした")
    result_table = th_place.find_parent("table")

    df = pd.read_html(str(result_table))[0]

    # 列名を英語ベースにリネーム
    rename_map = {
        "着順": "finish_rank",
        "枠": "bracket_no",
        "馬番": "horse_no",
        "馬 番": "horse_no",
        "馬名": "horse_name",
        "性齢": "sex_age",
        "負担重量": "carried_weight",
        "負担 重量": "carried_weight",
        "騎手名": "jockey_name",
        "タイム": "time_str",
        "差": "margin_str",
        "着差": "margin_str",
        "コーナー通過順位": "corner_order",
        "コーナー 通過順位": "corner_order",
        "推定上り": "last_3f_est",
        "馬体重（増減）": "body_weight_raw",
        "馬体重 （増減）": "body_weight_raw",
        "馬体重(増減)": "body_weight_raw",
        "調教師名": "trainer_name",
        "単勝人気": "popularity",
        "単勝 人気": "popularity",
        "人気": "popularity",
        "Rt": "rating",
    }
    df = df.rename(columns=rename_map)

    # 性齢 → sex, age
    if "sex_age" in df.columns:
        sex_age = df["sex_age"].astype(str)
        se = sex_age.str.extract(r"([牡牝セ騙])(\d+)")
        df["sex"] = se[0]
        df["age"] = pd.to_numeric(se[1], errors="coerce").astype("Int64")

    # 馬体重（本体重・増減）
    weight_cols = [c for c in df.columns if c == "body_weight_raw" or "馬体重" in str(c)]
    if weight_cols:
        src = weight_cols[0]
        df["body_weight_raw"] = df[src].astype(str)
        pattern = r"(\d+)\s*\(([+\-−]?\d+)\)"
        w = df["body_weight_raw"].str.extract(pattern)
        df["body_weight"] = pd.to_numeric(w[0], errors="coerce").astype("Int64")
        df["body_weight_diff"] = pd.to_numeric(w[1], errors="coerce").astype("Int64")

    # コーナー通過順位 → corner_1〜corner_4
    corner_cols = [c for c in df.columns if c == "corner_order" or "コーナー" in str(c)]
    if corner_cols:
        corner_src = corner_cols[0]
        corners = (
            df[corner_src]
            .astype(str)
            .str.replace("\u3000", " ", regex=False)
            .str.strip()
            .str.split(r"\s+", expand=True)
            .iloc[:, :4]
        )
        corners.columns = ["corner_1", "corner_2", "corner_3", "corner_4"]
        for c in corners.columns:
            corners[c] = pd.to_numeric(corners[c], errors="coerce").astype("Int64")
        df = pd.concat([df, corners], axis=1)

    return df


def parse_race_page(html_path: Path, race_id: str) -> Tuple[Dict[str, Any], pd.DataFrame]:
    """HTMLファイル1件 → (レースメタ情報, 整形済み着順DataFrame)"""
    html = Path(html_path).read_text(encoding="utf-8")
    soup = BeautifulSoup(html, "lxml")

    race_meta = parse_race_meta(soup, race_id)
    df_results = parse_race_results(soup)

    return race_meta, df_results
