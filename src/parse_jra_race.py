"""
Parse JRA race result HTML into v4 schema dicts.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Patterns
JP_DATE_PATTERN = r"(\d{4})\u5e74\s*(\d{1,2})\u6708\s*(\d{1,2})\u65e5"
JP_VENUE_PATTERN = r"\d+\u56de([^\d\s]+)\d+\u65e5"
JP_METER = r"\u30e1\u30fc\u30c8\u30eb"
JP_TURF = "\u829d"
JP_DIRT = "\u30c0\u30fc\u30c8"
JP_WEATHER = "\u5929\u5019"
JP_GOING = "\u99ac\u5834"  # not used directly, but kept for reference


def _read_html_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="shift_jis", errors="ignore")


def _infer_race_id(html_path: Path) -> str:
    m = re.search(r"(\d{12})", html_path.stem)
    return m.group(1) if m else html_path.stem


def _safe_int(val: Any) -> int | None:
    try:
        if pd.isna(val):
            return None
        return int(str(val).strip().replace(",", ""))
    except Exception:
        return None


def _safe_float(val: Any) -> float | None:
    try:
        if pd.isna(val):
            return None
        return float(str(val).strip().replace(",", ""))
    except Exception:
        return None


def _parse_time_to_sec(time_str: str | None) -> float | None:
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


def _parse_margin_to_sec(margin: str | None) -> float | None:
    if not isinstance(margin, str):
        return None
    txt = margin.strip()
    if not txt or txt in ["-", ""]:
        return None
    # simple numeric
    try:
        return float(txt)
    except ValueError:
        pass
    # common Japanese lengths (very rough approximations in seconds)
    mapping = {
        "\u30cf\u30ca": 0.1,  # ハナ
        "\u30af\u30d3": 0.2,  # クビ
        "\u534a\u99ac\u8eab": 0.3,
        "1/2": 0.3,
    }
    for k, v in mapping.items():
        if k in txt:
            return v
    return None


def _slugify(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text.strip()).strip("_")


def _make_id(prefix: str, name: str | None, fallback: str) -> str:
    if name:
        return f"{prefix}_{_slugify(name)}"
    return fallback


def _parse_race_overview(soup: BeautifulSoup, race_id: str) -> Dict[str, Any]:
    info: Dict[str, Any] = {
        "race_id": race_id,
        "date": None,
        "race_name": None,
        "distance": None,
        "surface": None,
        "weather": None,
        "going": None,
        "class": None,
        "age_cond": None,
        "sex_cond": None,
        "race_no": None,
        "num_runners": None,
    }

    header_text = soup.get_text("\n", strip=True)

    m_date = re.search(JP_DATE_PATTERN, header_text)
    if m_date:
        y, mn, d = m_date.groups()
        info["date"] = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    else:
        m_fallback = re.match(r"(\d{4})(\d{2})(\d{2})", race_id)
        if m_fallback:
            y, mn, d = m_fallback.groups()
            info["date"] = f"{y}-{mn}-{d}"

    # race name
    h2 = soup.find("h2")
    if h2 and h2.text.strip():
        info["race_name"] = h2.text.strip()

    # race number: look for "11R" or similar
    m_r = re.search(r"(\d+)\s*R", header_text)
    if m_r:
        info["race_no"] = int(m_r.group(1))

    # distance / surface: look for "芝1600m", "ダート1800m"
    m_dist = re.search(r"([\u829d\u30c0\u30fc\u30c8])\s*([\d,]+)m", header_text)
    if m_dist:
        surf_jp, dist_str = m_dist.groups()
        info["distance"] = int(dist_str.replace(",", ""))
        info["surface"] = "turf" if surf_jp == JP_TURF else "dirt"

    # weather / going: "天候 : 晴　芝 : 良"
    m_weather = re.search(r"\u5929\u5019[:\uFF1A]\s*([^\s\u3000]+)", header_text)
    if m_weather:
        info["weather"] = m_weather.group(1)
    m_going = re.search(r"(?:\u829d|\u30c0\u30fc\u30c8)[:\uFF1A]\s*([^\s\u3000]+)", header_text)
    if m_going:
        info["going"] = m_going.group(1)

    return info


def _find_results_table(soup: BeautifulSoup):
    # prioritize th.place (JRA PC page)
    th_place = soup.find("th", class_="place")
    if th_place:
        return th_place.find_parent("table")
    # fallback: first table containing 着順
    for table in soup.find_all("table"):
        if table.find(string=re.compile("\u7740\u9806")):
            return table
    tables = soup.find_all("table")
    return tables[0] if tables else None


def _parse_results_table(soup: BeautifulSoup) -> pd.DataFrame:
    table = _find_results_table(soup)
    if table is None:
        logger.warning("Result table not found")
        return pd.DataFrame()
    try:
        df = pd.read_html(str(table))[0]
    except ValueError:
        logger.warning("pandas.read_html failed on result table")
        return pd.DataFrame()

    rename_map = {
        "着順": "finish_rank",
        "着順\n(P)": "finish_rank",
        "着": "finish_rank",
        "枠": "bracket_no",
        "枠番": "bracket_no",
        "馬番": "horse_no",
        "馬 番": "horse_no",
        "馬名": "horse_name",
        "馬　名": "horse_name",
        "性齢": "sex_age",
        "斤量": "weight",
        "斤　量": "weight",
        "騎手": "jockey_name",
        "騎手名": "jockey_name",
        "タイム": "time_str",
        "着差": "margin_str",
        "通過": "corner_pass_order",
        "上り": "last_3f",
        "推定上り": "last_3f",
        "単勝": "odds",
        "単　勝": "odds",
        "人気": "popularity",
        "馬体重(増減)": "body_weight_raw",
        "馬体重": "body_weight_raw",
        "調教師": "trainer_name",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # sex / age
    if "sex_age" in df.columns:
        se = df["sex_age"].astype(str).str.extract(r"([\u725b\u725d\u30bb\u9a19])\s*(\d+)")
        df["sex"] = se[0]
        df["age"] = pd.to_numeric(se[1], errors="coerce").astype("Int64")

    # body weight
    if "body_weight_raw" in df.columns:
        w = df["body_weight_raw"].astype(str).str.extract(r"(\d+)\s*\(([+\-\u2212]?\d+)\)")
        df["body_weight"] = pd.to_numeric(w[0], errors="coerce").astype("Int64")
        df["body_weight_diff"] = pd.to_numeric(w[1], errors="coerce").astype("Int64")

    return df


def parse_jra_race(html_path: Union[str, Path]) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    path = Path(html_path)
    if not path.exists():
        raise FileNotFoundError(f"HTML not found: {path}")
    race_id = _infer_race_id(path)
    html = _read_html_text(path)
    soup = BeautifulSoup(html, "lxml")

    race_info = _parse_race_overview(soup, race_id)
    df = _parse_results_table(soup)

    # fill defaults
    distance = race_info.get("distance") or 0
    surface = race_info.get("surface") or "unknown"
    course_id = f"UNK_{surface}_{distance}"

    race_dict: Dict[str, Any] = {
        "race_id": race_id,
        "date": race_info.get("date"),
        "course_id": course_id,
        "race_no": race_info.get("race_no") or 0,
        "race_name": race_info.get("race_name"),
        "distance": distance,
        "surface": surface,
        "weather": race_info.get("weather"),
        "going": race_info.get("going"),
        "class": race_info.get("class"),
        "age_cond": race_info.get("age_cond"),
        "sex_cond": race_info.get("sex_cond"),
        "num_runners": len(df) if not df.empty else None,
        "win_time_sec": None,
        "race_type": "FLAT",
        "venue_id": None,
    }

    results_list: List[Dict[str, Any]] = []
    for idx, row in df.iterrows():
        horse_name = str(row.get("horse_name", "")).strip()
        jockey_name = str(row.get("jockey_name", "")).strip()
        trainer_name = str(row.get("trainer_name", "")).strip()

        horse_no = _safe_int(row.get("horse_no"))
        horse_id = _make_id("HORSE", horse_name, f"{race_id}_H{idx:02d}")
        jockey_id = _make_id("JOCKEY", jockey_name, f"{race_id}_J{idx:02d}")
        trainer_id = _make_id("TRAINER", trainer_name, f"{race_id}_T{idx:02d}")

        finish_time = _parse_time_to_sec(row.get("time_str"))
        margin_sec = _parse_margin_to_sec(row.get("margin_str"))

        result = {
            "race_id": race_id,
            "horse_id": horse_id,
            "bracket_no": _safe_int(row.get("bracket_no")),
            "horse_no": horse_no,
            "finish_rank": _safe_int(row.get("finish_rank")),
            "finish_status": "OK" if _safe_int(row.get("finish_rank")) is not None else None,
            "finish_time_sec": finish_time,
            "odds": _safe_float(row.get("odds")),
            "popularity": _safe_int(row.get("popularity")),
            "weight": _safe_float(row.get("weight")),
            "body_weight": _safe_int(row.get("body_weight")),
            "weight_diff": _safe_int(row.get("body_weight_diff")),
            "jockey_id": jockey_id,
            "trainer_id": trainer_id,
            "corner_pass_order": row.get("corner_pass_order") if "corner_pass_order" in df.columns else None,
            "last_3f": _safe_float(row.get("last_3f")),
            "margin_sec": margin_sec,
            "prize": None,
            "prev_race_id": None,
            "prev_finish_rank": None,
            "prev_margin_sec": None,
            "prev_time_sec": None,
            "prev_last_3f": None,
            "days_since_last": None,
        }
        results_list.append(result)

    if results_list and results_list[0].get("finish_time_sec") is not None:
        race_dict["win_time_sec"] = results_list[0]["finish_time_sec"]

    return race_dict, results_list
