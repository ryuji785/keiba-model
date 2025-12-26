"""
Parse JRA race result HTML into v4 schema dicts (races, race_results, masters, payouts).
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Tuple, Union

import pandas as pd
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

JP_DATE_PATTERN = r"(\d{4})年\s*(\d{1,2})月\s*(\d{1,2})日"
JP_TURF = "芝"
JP_DIRT = "ダート"


def _read_html_text(path: Path) -> str:
    data = path.read_bytes()
    # Most saved files are UTF-8; fall back to CP932/Shift_JIS for safety.
    for enc in ("utf-8", "cp932", "shift_jis"):
        try:
            return data.decode(enc)
        except UnicodeDecodeError:
            continue
    return data.decode("utf-8", errors="ignore")


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
    if not txt or txt in {"-", ""}:
        return None

    trans = str.maketrans(
        {
            "０": "0",
            "１": "1",
            "２": "2",
            "３": "3",
            "４": "4",
            "５": "5",
            "６": "6",
            "７": "7",
            "８": "8",
            "９": "9",
            "．": ".",
            "－": "-",
            "−": "-",
            "／": "/",
            "＋": "+",
            "　": "",
            " ": "",
        }
    )
    txt_norm = txt.translate(trans)

    base = 0.2  # rough seconds per 1馬身
    mapping = {
        "ハナ": 0.05,
        "鼻": 0.05,
        "アタマ": 0.15,
        "クビ": 0.1,
        "半馬身": base * 0.5,
        "1/4": base * 0.25,
        "1/2": base * 0.5,
        "3/4": base * 0.75,
        "大差": None,
    }
    for k, v in mapping.items():
        if k in txt_norm:
            return v

    m_mixed = re.match(r"(\d+(?:\.\d+)?)\s*(1/2|3/4)", txt_norm)
    if m_mixed:
        lengths = float(m_mixed.group(1)) + (0.5 if m_mixed.group(2) == "1/2" else 0.75)
        return lengths * base

    m_len = re.match(r"(\d+(?:\.\d+)?)\s*馬身", txt_norm)
    if m_len:
        return float(m_len.group(1)) * base

    try:
        return float(txt_norm)
    except ValueError:
        pass
    return None


def _slugify(text: str) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "_", text.strip()).strip("_")


def _make_id(prefix: str, name: str | None, fallback: str) -> str:
    if name:
        slug = _slugify(name)
        if slug:
            return f"{prefix}_{slug}"
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
        "venue_id": None,
    }

    header_div = (
        soup.find("div", id="race_result")
        or soup.find("div", class_="race_result_unit")
        or soup.find("div", class_="race_header")
    )
    header_text = header_div.get_text(" ", strip=True) if header_div else soup.get_text(" ", strip=True)

    title_div = soup.find("div", class_="race_title")
    if title_div and title_div.text.strip():
        info["race_name"] = title_div.get_text(strip=True)
    else:
        h2 = soup.find("h2")
        if h2 and h2.text.strip():
            info["race_name"] = h2.get_text(strip=True)

    h1 = soup.find("h1")
    h1_text = h1.get_text(" ", strip=True) if h1 else ""

    m_date = re.search(JP_DATE_PATTERN, header_text)
    if m_date:
        y, mn, d = m_date.groups()
        info["date"] = f"{int(y):04d}-{int(mn):02d}-{int(d):02d}"
    else:
        m_fallback = re.match(r"(\d{4})(\d{2})(\d{2})", race_id)
        if m_fallback:
            y, mn, d = m_fallback.groups()
            info["date"] = f"{y}-{mn}-{d}"

    m_rno = re.search(r"(\d+)\s*レース", h1_text) or re.search(r"(\d+)\s*R", header_text)
    if m_rno:
        info["race_no"] = int(m_rno.group(1))

    m_venue = re.search(r"\d+回([^\s\d]+)", header_text)
    if m_venue:
        info["venue_id"] = m_venue.group(1)

    m_weather = re.search(r"天候[:：]?\s*([^\s]+)", header_text)
    if m_weather:
        info["weather"] = m_weather.group(1)

    # コース情報: 距離・芝/ダート/障害・内外など
    m_course = re.search(r"コース[:：]\s*([\d,]+)メートル（([^）]+)）", header_text)
    if m_course:
        info["distance"] = int(m_course.group(1).replace(",", ""))
        course_desc = m_course.group(2)
        if "ダート" in course_desc:
            info["surface"] = "ダート"
        elif "芝" in course_desc:
            info["surface"] = "芝"
        elif "障害" in header_text:
            info["surface"] = "障害"
        info["course_layout"] = course_desc
    else:
        m_dist = re.search(r"(芝|ダート|障害)[^\d]*([\d,]+)m", header_text)
        if m_dist:
            info["surface"] = m_dist.group(1)
            info["distance"] = int(m_dist.group(2).replace(",", ""))
    if info.get("surface") is None:
        m_surface = re.search(r"(芝|ダート|障害)", header_text)
        if m_surface:
            info["surface"] = m_surface.group(1)

    m_going = re.search(r"(芝|ダート)\s*([^\s\(]+)", header_text)
    if m_going and m_going.group(2):
        info["going"] = m_going.group(2)

    cls_candidates = [
        ("グレード1", "G1"),
        ("グレード2", "G2"),
        ("グレード3", "G3"),
        ("G1", "G1"),
        ("G2", "G2"),
        ("G3", "G3"),
        ("オープン特別", "OPEN"),
        ("オープン", "OPEN"),
        ("特別", "OP"),
        ("新馬", "NEW"),
        ("未勝利", "MAIDEN"),
        ("1勝クラス", "1-WIN"),
        ("2勝クラス", "2-WIN"),
        ("3勝クラス", "3-WIN"),
    ]
    for pat, label in cls_candidates:
        if pat in header_text:
            info["class"] = label
            break
    if info["class"] is None:
        m_cls = re.search(r"(\d)勝", header_text)
        if m_cls:
            info["class"] = f"{m_cls.group(1)}-WIN"

    m_age = re.search(r"(\d+)[歳才]\s*以上", header_text)
    if m_age:
        info["age_cond"] = f"{m_age.group(1)}YO+"
    else:
        m_age2 = re.search(r"(\d+)[歳才]", header_text)
        if m_age2:
            info["age_cond"] = f"{m_age2.group(1)}YO"

    return info


def _find_results_table(soup: BeautifulSoup):
    th_place = soup.find("th", class_="place")
    if th_place:
        return th_place.find_parent("table")
    for table in soup.find_all("table"):
        if table.find(string=re.compile("着順")):
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

    def _norm_col(col: Any) -> str:
        s = str(col).strip()
        for ch in [" ", "　", "\xa0"]:
            s = s.replace(ch, "")
        return s

    rename_by_norm = {
        "着順": "finish_rank",
        "枠": "bracket_no",
        "馬番": "horse_no",
        "馬名": "horse_name",
        "性齢": "sex_age",
        "負担重量": "weight",
        "斤量": "weight",
        "騎手名": "jockey_name",
        "調教師名": "trainer_name",
        "タイム": "time_str",
        "着差": "margin_str",
        "コーナー通過順位": "corner_pass_order",
        "推定上り": "last_3f",
        "平均1F": "avg_1f",
        "馬体重（増減）": "body_weight_raw",
        "馬体重(増減)": "body_weight_raw",
        "単勝人気": "popularity",
        "オッズ": "odds",
        "Rt": "rt",
    }

    col_map: Dict[Any, str] = {}
    unknown_cols: List[str] = []
    for col in df.columns:
        norm = _norm_col(col)
        dest = rename_by_norm.get(norm)
        if dest:
            col_map[col] = dest
        else:
            unknown_cols.append(norm)
    if col_map:
        df = df.rename(columns=col_map)
    if unknown_cols:
        logger.debug("Unknown result columns (ignored): %s", unknown_cols)

    if "bracket_no" not in df.columns or df["bracket_no"].isna().all():
        brackets: List[int | None] = []
        for tr in table.find_all("tr"):
            td = tr.find("td", class_="waku")
            if not td:
                continue
            img = td.find("img")
            alt = img["alt"] if img and img.has_attr("alt") else None
            num = None
            if alt:
                m = re.search(r"枠(\d+)", alt)
                if m:
                    num = int(m.group(1))
            brackets.append(num)
        if brackets:
            df["bracket_no"] = pd.Series(brackets[: len(df)])

    def _clean_corner(val: Any) -> str | None:
        if pd.isna(val):
            return None
        s = str(val).strip()
        if not s or s.lower() == "nan":
            return None
        s = s.replace(" ", "").replace("　", "")
        s = s.replace("/", "-")
        s = re.sub(r"[^0-9\-]", "", s)
        s = re.sub(r"-+", "-", s)
        return s or None

    if "corner_pass_order" in df.columns:
        df["corner_pass_order"] = df["corner_pass_order"].apply(_clean_corner)

    if "sex_age" in df.columns:
        se = df["sex_age"].astype(str).str.extract(r"([牡牝騸セせ])\s*(\d+)")
        df["sex"] = se[0]
        df["age"] = pd.to_numeric(se[1], errors="coerce").astype("Int64")

    if "body_weight_raw" in df.columns:
        w = df["body_weight_raw"].astype(str).str.extract(r"(\d+)\s*\(([+\-\u2212\uFF0D]?\d+)\)")
        diff_norm = w[1].replace({"−": "-", "－": "-"}, regex=False)
        df["body_weight"] = pd.to_numeric(w[0], errors="coerce").astype("Int64")
        df["body_weight_diff"] = pd.to_numeric(diff_norm, errors="coerce").astype("Int64")

    if "last_3f" in df.columns:
        def _clean_last(val: Any) -> float | None:
            if pd.isna(val):
                return None
            s = str(val).strip()
            if not s or s.lower() == "nan":
                return None
            s = s.translate(str.maketrans("０１２３４５６７８９．", "0123456789."))
            s = re.sub(r"[^0-9\.]", "", s)
            return float(s) if s else None
        df["last_3f"] = df["last_3f"].apply(_clean_last)

    if "avg_1f" in df.columns and "last_3f" not in df.columns:
        df["last_3f"] = pd.to_numeric(df["avg_1f"], errors="coerce")

    return df


def _extract_cnames_from_table(soup: BeautifulSoup) -> List[Dict[str, str | None]]:
    """
    Extract CNAME tokens for horses/jockeys from result table rows to build stable IDs.
    Order of rows matches pandas.read_html output.
    """
    table = _find_results_table(soup)
    cnames: List[Dict[str, str | None]] = []
    if not table:
        return cnames

    for tr in table.find_all("tr"):
        horse_c = None
        jockey_c = None

        td_horse = tr.find("td", class_="horse")
        if td_horse:
            a = td_horse.find("a", href=True)
            if a:
                m = re.search(r"CNAME=([^&\"']+)", a["href"])
                if m:
                    horse_c = m.group(1)

        td_j = tr.find("td", class_="jockey")
        if td_j:
            a = td_j.find("a")
            if a:
                onclick = a.get("onclick") or a.get("onClick")
                if isinstance(onclick, str):
                    m = re.search(r"accessK\\.html'\\s*,\\s*'([^']+)'", onclick)
                    if m:
                        jockey_c = m.group(1)

        cnames.append({"horse": horse_c, "jockey": jockey_c})

    return cnames


def _parse_payouts(soup: BeautifulSoup, race_id: str) -> List[Dict[str, Any]]:
    bet_map = {
        "win": "単勝",
        "place": "複勝",
        "wakuren": "枠連",
        "wide": "ワイド",
        "umaren": "馬連",
        "umatan": "馬単",
        "trio": "三連複",
        "tierce": "三連単",
    }
    payouts: List[Dict[str, Any]] = []
    for li in soup.find_all("li"):
        classes = li.get("class") or []
        cls_match = next((c for c in classes if c in bet_map), None)
        if not cls_match:
            continue
        bet_type = bet_map[cls_match]
        lines = li.find_all("div", class_="line")
        for idx, line in enumerate(lines):
            num = line.find("div", class_="num")
            yen = line.find("div", class_="yen")
            pop = line.find("div", class_="pop")
            combination = num.get_text(strip=True) if num else None
            payout_yen = None
            popularity = None
            if yen:
                payout_str = re.sub(r"[^\d]", "", yen.get_text())
                payout_yen = int(payout_str) if payout_str else None
            if pop:
                pop_str = re.sub(r"[^\d]", "", pop.get_text())
                popularity = int(pop_str) if pop_str else None
            odds = payout_yen / 100.0 if payout_yen is not None else None
            payouts.append(
                {
                    "race_id": race_id,
                    "bet_type": bet_type,
                    "combination": combination,
                    "payout_yen": payout_yen,
                    "popularity": popularity,
                    "odds": odds,
                    "line_no": idx,
                }
            )
    return payouts


def _apply_odds_from_payouts(results_list: List[Dict[str, Any]], payouts: List[Dict[str, Any]]) -> None:
    """Fill odds/popularity on race_results for WIN/PLACE combinations where possible."""
    if not payouts:
        return
    # Map horse_no -> result dict
    by_no = {r.get("horse_no"): r for r in results_list if r.get("horse_no") is not None}
    for p in payouts:
        bet_type = p.get("bet_type")
        if bet_type not in {"単勝", "複勝"}:
            continue
        combo = p.get("combination")
        try:
            horse_no = int(str(combo).split("-")[0])
        except Exception:
            continue
        res = by_no.get(horse_no)
        if not res:
            continue
        if res.get("odds") is None and p.get("odds") is not None:
            res["odds"] = p["odds"]
        if res.get("popularity") is None and p.get("popularity") is not None:
            res["popularity"] = p["popularity"]


def parse_jra_race(html_path: Union[str, Path]) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    List[Dict[str, Any]],
]:
    path = Path(html_path)
    if not path.exists():
        raise FileNotFoundError(f"HTML not found: {path}")
    race_id = _infer_race_id(path)
    html = _read_html_text(path)
    soup = BeautifulSoup(html, "html.parser")

    race_info = _parse_race_overview(soup, race_id)
    df = _parse_results_table(soup)
    cname_rows = _extract_cnames_from_table(soup)
    payouts_list = _parse_payouts(soup, race_id)

    venue = race_info.get("venue_id") or "UNK"
    surface = race_info.get("surface") or "不明"
    distance = race_info.get("distance") or 0
    course_id = f"{venue}_{surface}_{distance}"

    is_jump = ("avg_1f" in df.columns) or ("障害" in soup.get_text())

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
        "race_type": "JUMP" if is_jump else "FLAT",
        "venue_id": race_info.get("venue_id"),
    }

    results_list: List[Dict[str, Any]] = []
    horses_dict: Dict[str, Dict[str, Any]] = {}
    jockeys_dict: Dict[str, Dict[str, Any]] = {}
    trainers_dict: Dict[str, Dict[str, Any]] = {}
    unknown_counters = {"horse": 0, "jockey": 0, "trainer": 0}

    for idx, row in df.iterrows():
        horse_name = str(row.get("horse_name", "")).strip() or f"UNKNOWN_HORSE_{idx:02d}"
        jockey_name = str(row.get("jockey_name", "")).strip() or f"UNKNOWN_JOCKEY_{idx:02d}"
        trainer_name = str(row.get("trainer_name", "")).strip() or f"UNKNOWN_TRAINER_{idx:02d}"

        if horse_name.startswith("UNKNOWN_HORSE"):
            unknown_counters["horse"] += 1
        if jockey_name.startswith("UNKNOWN_JOCKEY"):
            unknown_counters["jockey"] += 1
        if trainer_name.startswith("UNKNOWN_TRAINER"):
            unknown_counters["trainer"] += 1

        horse_no = _safe_int(row.get("horse_no")) or idx + 1

        # stable IDs: prefer CNAME extracted from table; fallback to slugged name -> race-specific
        horse_cname = cname_rows[idx]["horse"] if idx < len(cname_rows) else None
        jockey_cname = cname_rows[idx]["jockey"] if idx < len(cname_rows) else None

        horse_id = horse_cname or _make_id("HORSE", horse_name, f"{race_id}_H{horse_no:02d}")
        jockey_id = jockey_cname or _make_id("JOCKEY", jockey_name, f"{race_id}_J{idx:02d}")
        trainer_id = _make_id("TRAINER", trainer_name, f"{race_id}_T{idx:02d}")

        finish_time = _parse_time_to_sec(row.get("time_str"))
        margin_sec = _parse_margin_to_sec(row.get("margin_str"))

        birth_year = None
        race_date = race_dict.get("date")
        age_val = row.get("age")
        try:
            if race_date and age_val and not pd.isna(age_val):
                birth_year = int(str(race_date)[:4]) - int(age_val)
        except Exception:
            birth_year = None

        horses_dict[horse_id] = {"horse_name": horse_name or None, "sex": row.get("sex"), "birth_year": birth_year}
        jockeys_dict[jockey_id] = {"jockey_name": jockey_name or None}
        trainers_dict[trainer_id] = {"trainer_name": trainer_name or None}

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

    # set win_time_sec as best available (prefer rank=1 time, else min time)
    win_time = None
    for r in results_list:
        if r.get("finish_rank") == 1 and r.get("finish_time_sec") is not None:
            win_time = r["finish_time_sec"]
            break
    if win_time is None:
        times = [r.get("finish_time_sec") for r in results_list if r.get("finish_time_sec") is not None]
        if times:
            win_time = min(times)
    if win_time is not None:
        race_dict["win_time_sec"] = win_time

    # Fill odds/popularity for WIN/PLACE from payouts when available
    _apply_odds_from_payouts(results_list, payouts_list)

    if any(unknown_counters.values()):
        logger.info(
            "Unknown placeholders used (race_id=%s): horse=%s, jockey=%s, trainer=%s",
            race_id,
            unknown_counters["horse"],
            unknown_counters["jockey"],
            unknown_counters["trainer"],
        )

    return race_dict, results_list, horses_dict, jockeys_dict, trainers_dict, payouts_list


def parse_race_html(html_path: Union[str, Path]) -> Tuple[
    Dict[str, Any],
    List[Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    Dict[str, Dict[str, Any]],
    List[Dict[str, Any]],
]:
    """Wrapper to match legacy API."""
    return parse_jra_race(html_path)
