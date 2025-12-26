"""
Fetch related HTMLs (odds, horse profiles, jockey profiles) from saved race result HTMLs.
Also fetch racecourse information pages (direct access).

Features:
- Filter by year (race_id prefix) or specific files.
- Categories selectable (odds / horses / jockeys / courses).
- Parallel download with polite sleeps (2-4s per request, 1% chance of +60s).
- Progress bars per category.
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set

import requests
from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "raw" / "jra"
ODDS_DIR = RAW_DIR / "odds"
HORSE_DIR = RAW_DIR / "horses"
JOCKEY_DIR = RAW_DIR / "jockeys"
COURSE_DIR = RAW_DIR / "courses"

COURSE_SLUGS = [
    "sapporo",
    "hakodate",
    "fukushima",
    "niigata",
    "tokyo",
    "nakayama",
    "chukyo",
    "kyoto",
    "hanshin",
    "kokura",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    )
}


def polite_sleep() -> None:
    time.sleep(2 + random.random() * 2)  # 2-4秒
    if random.random() < 0.01:
        time.sleep(60)


def decode_html(content: bytes) -> str:
    for enc in ("cp932", "shift_jis", "utf-8"):
        try:
            return content.decode(enc)
        except Exception:
            continue
    return content.decode("utf-8", errors="ignore")


def extract_cnames_from_result(path: Path) -> Dict[str, Set[str]]:
    """Extract CNAME tokens from a race result HTML for odds, horses, jockeys."""
    text = path.read_bytes()
    html = decode_html(text)
    soup = BeautifulSoup(html, "html.parser")

    odds_cnames: Set[str] = set()
    horse_cnames: Set[str] = set()
    jockey_cnames: Set[str] = set()

    # 1) odds: collect all accessO onclick tokens, then pick best (race_id含む/長いもの)
    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick") or tag.get("onClick")
        if not isinstance(onclick, str):
            continue
        for m in re.finditer(r"accessO\.html'\s*,\s*'([^']+)'", onclick):
            odds_cnames.add(m.group(1))
    # fallback: anchors containing "オッズ" text may have onclick
    for a in soup.find_all("a"):
        if not a.get_text(strip=True):
            continue
        if "オッズ" not in a.get_text():
            continue
        onclick = a.get("onclick") or a.get("onClick")
        if not isinstance(onclick, str):
            continue
        m = re.search(r"accessO\.html'\s*,\s*'([^']+)'", onclick)
        if m:
            odds_cnames.add(m.group(1))

    def _odds_rank(token: str) -> tuple[int, int]:
        has_race = 1 if re.search(r"\d{12}", token) else 0
        return (has_race, len(token))
    if odds_cnames:
        best = sorted(odds_cnames, key=_odds_rank, reverse=True)
        odds_cnames = {best[0]}

    # 2) horses: only from result table cells (td.horse)
    for td in soup.find_all("td", class_="horse"):
        a = td.find("a", href=True)
        if not a:
            continue
        href = a["href"]
        m_horse = re.search(r"accessU\.html\?CNAME=([^&\"']+)", href)
        if m_horse:
            horse_cnames.add(m_horse.group(1))

    # 3) jockeys: only from result table cells (td.jockey) onclick accessK
    for td in soup.find_all("td", class_="jockey"):
        a = td.find("a")
        if not a:
            continue
        onclick = a.get("onclick") or a.get("onClick")
        if not isinstance(onclick, str):
            continue
        m_jockey = re.search(r"accessK\.html'\s*,\s*'([^']+)'", onclick)
        if m_jockey:
            jockey_cnames.add(m_jockey.group(1))

    return {"odds": odds_cnames, "horses": horse_cnames, "jockeys": jockey_cnames}


def sanitize(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)


def race_id_from_name(path: Path) -> Optional[str]:
    m = re.search(r"race_(\d{12})", path.name)
    return m.group(1) if m else None


@dataclass
class Task:
    category: str  # odds/horses/jockeys
    url: str
    method: str
    payload: Optional[Dict[str, str]]
    out_path: Path


def build_tasks(
    files: Iterable[Path],
    categories: Set[str],
    overwrite: bool,
    progress_interval: int = 200,
    show_spinner: bool = True,
) -> List[Task]:
    files_list = list(files)
    total = len(files_list)
    spinner = "|/-\\"
    seen: Dict[str, Set[str]] = {"odds": set(), "horses": set(), "jockeys": set()}
    tasks: List[Task] = []
    for idx, path in enumerate(files_list, 1):
        race_id = race_id_from_name(path)
        cnames = extract_cnames_from_result(path)

        if "odds" in categories and race_id and cnames["odds"]:
            # pick first odds CNAME
            cname = sorted(cnames["odds"])[0]
            if cname in seen["odds"]:
                continue
            out = ODDS_DIR / f"odds_{race_id}.html"
            if overwrite or not out.exists():
                # odds pages require POST with cname
                url = "https://www.jra.go.jp/JRADB/accessO.html"
                tasks.append(Task("odds", url, "POST", {"cname": cname}, out))
            seen["odds"].add(cname)

        if "horses" in categories:
            for cname in cnames["horses"]:
                if cname in seen["horses"]:
                    continue
                out = HORSE_DIR / f"horse_{sanitize(cname)}.html"
                if overwrite or not out.exists():
                    url = f"https://www.jra.go.jp/JRADB/accessU.html?CNAME={cname}"
                    tasks.append(Task("horses", url, "GET", None, out))
                seen["horses"].add(cname)

        if "jockeys" in categories:
            for cname in cnames["jockeys"]:
                if cname in seen["jockeys"]:
                    continue
                out = JOCKEY_DIR / f"jockey_{sanitize(cname)}.html"
                if overwrite or not out.exists():
                    # jockey pages also require POST with cname
                    url = "https://www.jra.go.jp/JRADB/accessK.html"
                    tasks.append(Task("jockeys", url, "POST", {"cname": cname}, out))
                seen["jockeys"].add(cname)

        if progress_interval > 0 and idx % progress_interval == 0:
            print(f"[info] build tasks progress: {idx}/{total} race htmls processed", end="\r", flush=True)
        if show_spinner:
            frame = spinner[(idx - 1) % len(spinner)]
            print(f"\r[build] parsing race htmls {idx}/{total} {frame}", end="", flush=True)

    if total > 0:
        print()  # newline after progress/spinner
    return tasks


def build_course_tasks(overwrite: bool) -> List[Task]:
    tasks: List[Task] = []
    # Top facilities page
    top_out = COURSE_DIR / "facilities.html"
    if overwrite or not top_out.exists():
        tasks.append(
            Task(
                category="courses",
                url="https://www.jra.go.jp/facilities/",
                method="GET",
                payload=None,
                out_path=top_out,
            )
        )
    # Each course page
    for slug in COURSE_SLUGS:
        out = COURSE_DIR / f"course_{slug}.html"
        if overwrite or not out.exists():
            url = f"https://www.jra.go.jp/facilities/race/{slug}/course/index.html"
            tasks.append(
                Task(
                    category="courses",
                    url=url,
                    method="GET",
                    payload=None,
                    out_path=out,
                )
            )
    return tasks


def fetch_one(task: Task) -> tuple[str, bool, str]:
    try:
        if task.method.upper() == "POST":
            resp = requests.post(task.url, data=task.payload or {}, headers=HEADERS, timeout=15)
        else:
            resp = requests.get(task.url, headers=HEADERS, timeout=15)
        if resp.status_code != 200:
            return task.category, False, f"HTTP {resp.status_code} {task.url}"
        html = decode_html(resp.content)
        task.out_path.parent.mkdir(parents=True, exist_ok=True)
        task.out_path.write_text(html, encoding="utf-8")
        polite_sleep()
        return task.category, True, ""
    except Exception as exc:  # noqa: BLE001
        return task.category, False, str(exc)


def progress_bar(done: int, total: int, width: int = 12) -> str:
    if total == 0:
        return "|------------| 0/0 0%"
    ratio = done / total
    filled = int(width * ratio)
    bar = "#" * filled + "-" * (width - filled)
    return f"|{bar}| {done}/{total} {int(ratio*100)}%"


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch related HTMLs (odds, horses, jockeys, courses) from saved race results.")
    parser.add_argument("--max-workers", type=int, default=4, help="Max concurrent downloads (default: 4)")
    parser.add_argument(
        "--categories",
        default="odds,horses,jockeys,courses",
        help="Comma-separated categories to fetch (odds,horses,jockeys,courses)",
    )
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("--year", help="Process only race files whose race_id starts with this year (YYYY)")
    parser.add_argument(
        "--files",
        help="Comma-separated specific race HTML filenames (e.g., race_202101010101.html)",
    )
    parser.add_argument("--fail-log", help="Path to write failures (append mode)")
    args = parser.parse_args()

    cats = {c.strip() for c in args.categories.split(",") if c.strip()}
    valid = {"odds", "horses", "jockeys", "courses"}
    cats = cats & valid
    if not cats:
        print("No valid categories specified.")
        sys.exit(1)

    # ensure output dirs exist for selected categories
    if "odds" in cats:
        ODDS_DIR.mkdir(parents=True, exist_ok=True)
    if "horses" in cats:
        HORSE_DIR.mkdir(parents=True, exist_ok=True)
    if "jockeys" in cats:
        JOCKEY_DIR.mkdir(parents=True, exist_ok=True)
    if "courses" in cats:
        COURSE_DIR.mkdir(parents=True, exist_ok=True)

    tasks: List[Task] = []

    race_cats = cats & {"odds", "horses", "jockeys"}
    if race_cats:
        if args.files:
            target_files = [RAW_DIR / f for f in args.files.split(",") if f.strip()]
        else:
            target_files = list(RAW_DIR.glob("race_*.html"))
            if args.year:
                target_files = [p for p in target_files if p.name.startswith(f"race_{args.year}")]
        target_files = sorted(target_files)
        if not target_files:
            print("No race_*.html files matched the filter for race-based categories.")
        else:
            tasks.extend(build_tasks(target_files, race_cats, args.overwrite))

    if "courses" in cats:
        tasks.extend(build_course_tasks(args.overwrite))

    if not tasks:
        print("No tasks to download (maybe files already exist).")
        return

    totals = {c: 0 for c in valid}
    done = {c: 0 for c in valid}
    for t in tasks:
        totals[t.category] += 1

    print(
        "Total tasks: "
        f"{len(tasks)} "
        f"(odds={totals['odds']} horses={totals['horses']} jockeys={totals['jockeys']} courses={totals['courses']})"
    )

    # track active categories in a fixed order for stable progress display
    active_cats = [c for c in ["odds", "horses", "jockeys", "courses"] if totals[c] > 0]

    def render_progress() -> str:
        parts = []
        for c in active_cats:
            parts.append(f"[{c}] {progress_bar(done[c], totals[c])}")
        return "  ".join(parts)

    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        future_to_task = {ex.submit(fetch_one, t): t for t in tasks}
        for fut in as_completed(future_to_task):
            t = future_to_task[fut]
            cat, ok, msg = fut.result()
            done[cat] += 1
            if not ok:
                print(f"[WARN] {cat} failed: {msg}")
                if args.fail_log:
                    fail_path = Path(args.fail_log)
                    fail_path.parent.mkdir(parents=True, exist_ok=True)
                    with fail_path.open("a", encoding="utf-8") as f:
                        f.write(f"{cat}\t{t.url}\t{msg}\n")
            # overwrite single line with aggregated progress per category
            sys.stdout.write("\r" + render_progress())
            sys.stdout.flush()

    if active_cats:
        print()  # newline after progress
    print("All done.")


if __name__ == "__main__":
    main()
