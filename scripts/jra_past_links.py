"""
Traverse past race search to obtain pw01srl and pw01sde CNAME lists.
"""
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Dict, List

from bs4 import BeautifulSoup

from scripts.etl_common import decode_shift_jis, http_post, logger

BASE_URL = "https://www.jra.go.jp/JRADB/accessS.html"
CHECKDIGIT_CACHE = Path("data/raw/jra/checkdigits.json")


def fetch_yymm_checkdigit_dict() -> Dict[str, str]:
    """
    Fetch yymm -> checkdigit mapping by posting to pw01skl00999999/B3.
    Cached locally to avoid repeated network calls.
    """
    if CHECKDIGIT_CACHE.exists():
        try:
            return json.loads(CHECKDIGIT_CACHE.read_text(encoding="utf-8"))
        except Exception:
            logger.warning("Failed to load cache: %s", CHECKDIGIT_CACHE)

    resp = http_post(BASE_URL, data={"cname": "pw01skl00999999/B3"})
    html = decode_shift_jis(resp.content)
    # Example in the page:
    # var objParam = new Array();objParam["2501"]="3F";objParam["2502"]="0D"; ...
    pattern = re.compile(r'objParam\["(\d{4})"\]\s*=\s*"([0-9A-Z]{1,2})"')
    mapping: Dict[str, str] = {}
    for m in pattern.finditer(html):
        yymm, chk = m.group(1), m.group(2)
        mapping[yymm] = chk

    if mapping:
        CHECKDIGIT_CACHE.parent.mkdir(parents=True, exist_ok=True)
        CHECKDIGIT_CACHE.write_text(json.dumps(mapping, ensure_ascii=False), encoding="utf-8")
        logger.info("Saved checkdigit cache: %s", CHECKDIGIT_CACHE)
        return mapping

    # fallback: keep empty but log snippet for debugging
    dump_path = CHECKDIGIT_CACHE.with_suffix(".html")
    dump_path.write_text(html, encoding="utf-8", errors="ignore")
    logger.warning("No yymm/checkdigit mapping found; dumped response to %s", dump_path)
    return {}


def get_srl_cnames(year: int, month: int) -> List[str]:
    """
    Build pw01skl10YYYYMM/<chk> and extract pw01srl... cnames.
    """
    mapping = fetch_yymm_checkdigit_dict()
    yyyymm = f"{year}{month:02d}"
    yymm = yyyymm[2:]
    chk = mapping.get(yymm)
    if not chk:
        logger.warning("No checkdigit found for yymm=%s", yymm)
        return []

    cname = f"pw01skl10{yyyymm}/{chk}"
    resp = http_post(BASE_URL, data={"cname": cname})
    html = decode_shift_jis(resp.content)
    soup = BeautifulSoup(html, "html.parser")

    cnames: List[str] = []
    for a in soup.find_all("a", href=True):
        if "pw01srl" in a["href"]:
            cnames.append(a["href"].split("CNAME=")[-1])

    onclick_pat = re.compile(r"pw01srl[^'\" >]+")
    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick", "")
        for m in onclick_pat.finditer(onclick):
            cnames.append(m.group(0))

    seen = set()
    uniq: List[str] = []
    for c in cnames:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def get_sde_cnames_for_date(date_yyyymmdd: str) -> List[str]:
    """
    For a specific date (YYYYMMDD), traverse srl -> sde and return sde CNAME list.
    """
    year = int(date_yyyymmdd[:4])
    month = int(date_yyyymmdd[4:6])
    srl_list = get_srl_cnames(year, month)
    if not srl_list:
        return []

    sde_cnames: List[str] = []
    for srl in srl_list:
        matches = re.findall(r"(?=(\d{8}))", srl)  # overlapping to capture trailing date
        target_in_srl = matches[-1] if matches else None
        if target_in_srl != date_yyyymmdd:
            continue
        resp = http_post(BASE_URL, data={"cname": srl})
        html = decode_shift_jis(resp.content)
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            if "pw01sde" in a["href"]:
                sde_cnames.append(a["href"].split("CNAME=")[-1])
        onclick_pat = re.compile(r"pw01sde[^'\" >]+")
        for tag in soup.find_all(onclick=True):
            onclick = tag.get("onclick", "")
            for m2 in onclick_pat.finditer(onclick):
                sde_cnames.append(m2.group(0))

    seen = set()
    uniq: List[str] = []
    for c in sde_cnames:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq


def get_sde_cnames_from_srl(srl: str) -> List[str]:
    """
    Given one srl CNAME (pw01srl...), fetch its page and extract pw01sde... CNAMEs.
    """
    resp = http_post(BASE_URL, data={"cname": srl})
    html = decode_shift_jis(resp.content)
    soup = BeautifulSoup(html, "html.parser")
    sde_cnames: List[str] = []
    for a in soup.find_all("a", href=True):
        if "pw01sde" in a["href"]:
            sde_cnames.append(a["href"].split("CNAME=")[-1])
    onclick_pat = re.compile(r"pw01sde[^'\" >]+")
    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick", "")
        for m2 in onclick_pat.finditer(onclick):
            sde_cnames.append(m2.group(0))
    seen = set()
    uniq: List[str] = []
    for c in sde_cnames:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq
