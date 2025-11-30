"""
Transform: Parse a JRA race-result HTML into v4 schema-friendly dicts.
Focus: populate races and race_results with required v4 fields; masters for horses/jockeys/trainers.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# Japanese patterns
JP_DATE_PATTERN = r"(\d{4})\u5e74\s*(\d{1,2})\u6708\s*(\d{1,2})\u65e5"
JP_VENUE_PATTERN = r"\d+\u56de([^\d\s]+)\d+\u65e5"
JP_METER = r"\u30e1\u30fc\u30c8\u30eb"
JP_WEATHER = "\u5929\u5019"
JP_TURF = "\u829d"
JP_DIRT = "\u30c0\u30fc\u30c8"
JP_GOING_SEP = r"[:\uFF1A]\s*([^\s\u3000]+)"
JP_RACE_RESULT = r"\u30ec\u30fc\u30b9\u7d50\u679c"
JP_STAKES = r"\u30b9\u30c6\u30fc\u30af\u30b9"
JP_SHINBA = r"\u65b0\u99ac"


def _infer_race_id(html_path: Path) -> str:
    name = html_path.stem
    m = re.search(r"(\d{12})", name)
    if m:
        return m.group(1)
    return name


def _read_html_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="shift_jis", errors="ignore")


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


def _slugify(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text.strip()).strip("_")


def _make_id(prefix: str, name: str | None) -> str | None:
    if not name:
        return None
    return f"{prefix}_{_slugify(name)}"


def _parse_race_header(soup: BeautifulSoup) -> Dict[str, Any]:
    info: Dict[str, Any] = {}
    header = soup.find("div", class_="race_header") or soup.find("div", id="race_header")
    text = header.get_text("\n", strip=True) if header else soup.get_text("\n", strip=True)

    m = re.search(JP_DATE_PATTERN, text)
    if m:
        y, mn, d = m.groups()
        info["date"] = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"

    m = re.search(JP_VENUE_PATTERN, text)
    if m:
        info["venue_jp"] = m.group(1).strip()

    m = re.search(rf"(?:([\d,]+)\s*{JP_METER}|([\d,]+)\s*m)", text)
    if m:
        dist = m.group(1) or m.group(2)
        if dist:
            info["distance"] = int(dist.replace(",", ""))

    wx_line = None
    for line in text.split("\n"):
        if JP_WEATHER in line and (JP_TURF in line or JP_DIRT in line):
            wx_line = line
            break
    if wx_line:
        mw = re.search(rf"{JP_WEATHER}{JP_GOING_SEP}", wx_line)
        if mw:
            info["weather_jp"] = mw.group(1)
        ms = re.search(rf"({JP_TURF}|{JP_DIRT}){JP_GOING_SEP}", wx_line)
        if ms:
            info["surface_jp"] = ms.group(1)
            info["going_jp"] = ms.group(2)

    return info


def _find_race_name(soup: BeautifulSoup) -> str | None:
    for tag in soup.find_all(["h1", "h2", "p"]):
        txt = tag.get_text(strip=True)
        if not txt:
            continue
        if re.search(rf"(?:\u56de.+\u8cde|{JP_RACE_RESULT}|{JP_STAKES}|{JP_SHINBA})", txt):
            return txt
    return None


def _parse_race_meta(soup: BeautifulSoup, race_id: str) -> Dict[str, Any]:
    header = _parse_race_header(soup)
    meta: Dict[str, Any] = {
        "race_id": race_id,
        "date": None,
        "distance": header.get("distance"),
        "weather": header.get("weather_jp"),
        "going": header.get("going_jp"),
        "age_cond": None,
        "sex_cond": None,
        "race_type": "FLAT",
    }
    m_date = re.match(r"(\d{4})(\d{2})(\d{2})", race_id)
    if m_date:
        y, mn, d = m_date.groups()
        meta["date"] = f"{y}-{mn}-{d}"
    elif header.get("date"):
        meta["date"] = header.get("date")

    race_name = _find_race_name(soup)
    meta["race_name"] = race_name

    text = soup.get_text(" ", strip=True)
    m = re.search(r"(\d+)\s*R", text)
    if not m:
        m = re.search(r"(\d+)\s*\u30ec\u30fc\u30b9", text)
    if m:
        meta["race_no"] = int(m.group(1))

    VENUE_MAP = {
        "\u6771\u4eac": "TOK",
        "\u4eac\u90fd": "KYT",
        "\u962a\u795e": "HAN",
        "\u4e2d\u5c71": "NAK",
        "\u65b0\u6f5f": "NII",
        "\u5c0f\u5009": "KOK",
        "\u672d\u5e4c": "SAP",
        "\u51f0\u9928": "HAK",
        "\u4e2d\u4eac": "CKY",
        "\u798f\u5cf6": "FKS",
    }
    venue_id = VENUE_MAP.get(header.get("venue_jp")) if header.get("venue_jp") else None
    surface_en = None
    if header.get("surface_jp") == JP_TURF:
        surface_en = "turf"
    elif header.get("surface_jp") == JP_DIRT:
        surface_en = "dirt"
    meta["surface"] = surface_en

    meta["course_id"] = None
    if venue_id and surface_en and meta.get("distance"):
        meta["course_id"] = f"{venue_id}_{surface_en}_{meta['distance']}"

    if race_name:
        tokens = []
        for kw in ["G", "\u30aa\u30fc\u30d7\u30f3", "\u65b0\u99ac", "\u672a\u52dd\u5229", "1\u52dd", "2\u52dd", "3\u52dd", "\u30ea\u30b9\u30c6\u30c3\u30c9", "\u91cd\u8cde"]:
            if kw in race_name:
                tokens.append(kw)
        if tokens:
            meta["class"] = " ".join(tokens)

    meta["venue_id"] = venue_id
    return meta


def _parse_results_table(soup: BeautifulSoup) -> pd.DataFrame:
    target_table = _find_results_table(soup)
    if target_table is None:
        logger.warning("Result table not found in HTML")
        return pd.DataFrame()
    try:
        df = pd.read_html(str(target_table))[0]
    except ValueError:
        logger.warning("pandas.read_html failed to parse results table")
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
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "sex_age" in df.columns:
        se = df["sex_age"].astype(str).str.extract(r"([\u725b\u725d\u30bb\u9a19])\s*(\d+)")
        df["sex"] = se[0]
        df["age"] = pd.to_numeric(se[1], errors="coerce").astype("Int64")

    if "body_weight_raw" in df.columns:
        w = df["body_weight_raw"].astype(str).str.extract(r"(\d+)\s*\(([+\-\u2212]?\d+)\)")
        df["body_weight"] = pd.to_numeric(w[0], errors="coerce").astype("Int64")
        df["body_weight_diff"] = pd.to_numeric(w[1], errors="coerce").astype("Int64")

    return df


def _find_results_table(soup: BeautifulSoup):
    th_place = soup.find("th", class_="place")
    if th_place:
        return th_place.find_parent("table")
    for table in soup.find_all("table"):
        if table.find(string=re.compile("\u7740\u9806")):
            return table
    tables = soup.find_all("table")
    return tables[0] if tables else None


def _extract_row_ids(soup: BeautifulSoup) -> List[Dict[str, str | None]]:
    table = _find_results_table(soup)
    if table is None:
        return []
    rows = table.find_all("tr")
    data_rows = [r for r in rows if r.find_all("td")]
    extracted: List[Dict[str, str | None]] = []
    for r in data_rows:
        horse_id = _extract_id_from_href(r.find("a", href=re.compile(r"/datafile/horse/")))
        jockey_id = _extract_id_from_href(r.find("a", href=re.compile(r"/datafile/jockey/")))
        trainer_id = _extract_id_from_href(r.find("a", href=re.compile(r"/datafile/trainer/")))
        extracted.append({"horse_id": horse_id, "jockey_id": jockey_id, "trainer_id": trainer_id})
    return extracted


def _extract_id_from_href(tag: Any, pattern: str) -> str | None:
    if not tag:
        return None
    href = tag.get("href", "")
    m = re.search(pattern, href)
    if m:
        return m.group(1)
    return None


def _build_results_list(
    df: pd.DataFrame,
    race_id: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    results_list: List[Dict[str, Any]] = []
    horses_dict: Dict[str, Dict[str, Any]] = {}
    jockeys_dict: Dict[str, Dict[str, Any]] = {}
    trainers_dict: Dict[str, Dict[str, Any]] = {}

    for idx, row in df.iterrows():
        horse_name = str(row.get("horse_name", "")).strip()
        jockey_name = str(row.get("jockey_name", "")).strip()
        trainer_name = str(row.get("trainer_name", "")).strip()

        horse_no = _safe_int(row.get("horse_no"))
        horse_id = _make_id("HORSE", horse_name)
        if horse_id is None:
            if horse_no is not None:
                horse_id = f"{race_id}_H{horse_no:02d}"
            else:
                horse_id = f"{race_id}_H{idx:02d}"
        jockey_id = _make_id("JOCKEY", jockey_name) or jockey_name or None
        trainer_id = _make_id("TRAINER", trainer_name) or trainer_name or None

        sex_val = row.get("sex")
        if isinstance(sex_val, str):
            sex_val = sex_val.strip()
        elif pd.isna(sex_val):
            sex_val = None

        horses_dict[horse_id] = {"horse_name": horse_name or None, "sex": sex_val, "birth_year": None}
        if jockey_id:
            jockeys_dict[jockey_id] = {"jockey_name": jockey_name or None}
        if trainer_id:
            trainers_dict[trainer_id] = {"trainer_name": trainer_name or None}

        finish_rank = _safe_int(row.get("finish_rank"))
        finish_status = "OK" if finish_rank is not None else None
        finish_raw = str(row.get("finish_rank") or "").strip()
        if finish_rank is None and finish_raw:
            if any(k in finish_raw for k in ["中止", "落馬"]):
                finish_status = "DNF"
            elif any(k in finish_raw for k in ["取消", "除外"]):
                finish_status = "SCR"
            elif "失格" in finish_raw:
                finish_status = "DQ"

        result = {
            "race_id": race_id,
            "horse_id": horse_id,
            "bracket_no": _safe_int(row.get("bracket_no")),
            "horse_no": horse_no,
            "finish_rank": finish_rank,
            "finish_status": finish_status,
            "finish_time_sec": _parse_time_to_sec(row.get("time_str") if isinstance(row.get("time_str"), str) else None),
            "odds": _safe_float(row.get("odds")),
            "popularity": _safe_int(row.get("popularity")),
            "weight": _safe_float(row.get("weight")),
            "body_weight": _safe_int(row.get("body_weight")),
            "weight_diff": _safe_int(row.get("body_weight_diff")),
            "jockey_id": jockey_id,
            "trainer_id": trainer_id,
            "corner_pass_order": row.get("corner_pass_order") if "corner_pass_order" in df.columns else None,
            "last_3f": _safe_float(row.get("last_3f")),
            "margin_sec": None,
            "prize": None,
            "prev_race_id": None,
            "prev_finish_rank": None,
            "prev_margin_sec": None,
            "prev_time_sec": None,
            "prev_last_3f": None,
            "days_since_last": None,
        }
        results_list.append(result)

    return results_list, horses_dict, jockeys_dict, trainers_dict


def parse_race_html(html_path: Union[str, Path]) -> Tuple[Dict, List[Dict], Dict, Dict, Dict]:
    path = Path(html_path)
    if not path.exists():
        raise FileNotFoundError(f"HTML file not found: {path}")

    race_id = _infer_race_id(path)
    html = _read_html_text(path)
    soup = BeautifulSoup(html, "html.parser")

    race_meta = _parse_race_meta(soup, race_id)
    df = _parse_results_table(soup)

    win_time_sec = None
    if not df.empty and "time_str" in df.columns:
        first_time = df["time_str"].iloc[0]
        win_time_sec = _parse_time_to_sec(first_time if isinstance(first_time, str) else None)

    m_rno = re.search(r"(\d{2})$", race_id)
    fallback_race_no = int(m_rno.group(1)) if m_rno else None

    distance_val = race_meta.get("distance") if race_meta.get("distance") is not None else 0
    surface_val = race_meta.get("surface") if race_meta.get("surface") is not None else "unknown"
    course_id_val = race_meta.get("course_id")
    if course_id_val is None:
        venue_part = race_meta.get("venue_id") or "UNK"
        course_id_val = f"{venue_part}_{surface_val}_{distance_val}"

    race_dict: Dict[str, Any] = {
        "race_id": race_id,
        "date": race_meta.get("date"),
        "course_id": course_id_val,
        "race_no": race_meta.get("race_no") or fallback_race_no or 0,
        "race_name": race_meta.get("race_name"),
        "distance": distance_val,
        "surface": surface_val,
        "venue_id": race_meta.get("venue_id"),
        "weather": race_meta.get("weather"),
        "going": race_meta.get("going"),
        "class": race_meta.get("class"),
        "age_cond": race_meta.get("age_cond"),
        "sex_cond": race_meta.get("sex_cond"),
        "num_runners": len(df) if not df.empty else None,
        "win_time_sec": win_time_sec,
        "race_type": race_meta.get("race_type", "FLAT"),
    }

    results_list, horses_dict, jockeys_dict, trainers_dict = _build_results_list(df, race_id)

    logger.info(
        "Transform summary: race_id=%s races=1 race_results=%d horses=%d jockeys=%d trainers=%d",
        race_id,
        len(results_list),
        len(horses_dict),
        len(jockeys_dict),
        len(trainers_dict),
    )

    return race_dict, results_list, horses_dict, jockeys_dict, trainers_dict
