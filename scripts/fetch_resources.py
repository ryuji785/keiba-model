"""
Unified fetch script with interactive menu and CLI subcommands.

Menu order reflects dependency:
1) レース結果HTML取得 (race-results)
2) オッズ・馬・騎手ページ取得 (related; derived from race_*.html)
3) 競馬場・コース情報取得 (courses; fixed 11 pages)
4) まとめて実行 (1 -> 2 -> 3)
q) 終了

Shared options (CLIでもメニューでも入力可):
- --year YYYY
- --month MM or YYYYMM (race-resultsで使用)
- --files comma-separated (race_*.html for related; CNAME or race_*.html for race-results if提供されていれば)
- --categories odds,horses,jockeys (relatedのみ; 省略で全部)
- --overwrite
- --max-workers N (HTTP並列; related/coursesのみ)
- --fail-log path (失敗を追記)
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter
from datetime import datetime
import logging
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import fetch_related_htmls as rel  # noqa: E402
from scripts.fetch_month_htmls import fetch_month  # noqa: E402
from scripts.fetch_year_htmls import fetch_year  # noqa: E402
from scripts.fetch_jra_html import fetch_race_html  # noqa: E402


def _describe_task(task: rel.Task) -> str:
    """Return human-friendly identifier for the task (race/horse/jockey)."""
    name = task.out_path.name
    if task.category == "odds" and name.startswith("odds_"):
        race_id = name[len("odds_") :].replace(".html", "")
        return f"オッズ race_id={race_id}"
    if task.category == "horses" and name.startswith("horse_"):
        return f"馬 {name[len('horse_') :].replace('.html', '')}"
    if task.category == "jockeys" and name.startswith("jockey_"):
        return f"ジョッキー {name[len('jockey_') :].replace('.html', '')}"
    if task.category == "courses":
        return f"コース {name.replace('.html', '')}"
    return f"{task.category} {name}"


def _parse_years(year_str: Optional[str]) -> Optional[Set[str]]:
    if not year_str:
        return None
    years = {y.strip() for y in year_str.split(",") if y.strip()}
    return years or None


def _filter_race_files(
    years: Optional[Set[str]], files: Optional[Sequence[str]]
) -> List[Path]:
    """Select local race_*.html files by year prefix (multiple allowed) or explicit filenames."""
    if files:
        targets = [Path(f) if Path(f).is_absolute() else rel.RAW_DIR / f for f in files]
    else:
        targets = list(rel.RAW_DIR.glob("race_*.html"))
        if years:
            targets = [p for p in targets if any(p.name.startswith(f"race_{y}") for y in years)]
    return sorted(targets)


def _run_tasks(tasks: List[rel.Task], max_workers: int, fail_log: Optional[Path]) -> None:
    """Run download tasks with category-wise progress."""
    if not tasks:
        print("No tasks to download.")
        return

    totals = Counter(t.category for t in tasks)
    done = {c: 0 for c in totals}
    active_cats = [c for c in ["race-results", "odds", "horses", "jockeys", "courses"] if c in totals]

    def render() -> str:
        parts = []
        for c in active_cats:
            parts.append(f"[{c}] {rel.progress_bar(done[c], totals[c])}")
        return "  ".join(parts)

    total_msg = " ".join(f"{c}={totals.get(c,0)}" for c in active_cats)
    print(f"[info] run tasks: total={len(tasks)} ({total_msg})")

    from concurrent.futures import ThreadPoolExecutor, as_completed

    last_len = 0
    last_ticks: dict[str, int] = {}

    def _print_progress(last_task: Optional[rel.Task]) -> None:
        nonlocal last_len, last_ticks
        # update only when bar (#) moves
        changed = False
        cur_ticks: dict[str, int] = {}
        for c in active_cats:
            total_c = totals[c]
            tick = int(12 * done[c] / total_c) if total_c else 0
            cur_ticks[c] = tick
            if last_ticks.get(c) != tick:
                changed = True
        if not changed:
            return
        last_ticks = cur_ticks

        status = ""
        if last_task:
            ts = datetime.now().strftime("%H:%M:%S")
            status = f"  [{ts}] { _describe_task(last_task) }"

        line = render() + status
        max_len = 120
        if len(line) > max_len:
            line = line[: max_len - 3] + "..."
        # pad to clear residual characters from previous longer line
        padding = " " * max(0, last_len - len(line))
        sys.stdout.write("\r" + line + padding)
        sys.stdout.flush()
        last_len = len(line)

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        future_to_task = {ex.submit(rel.fetch_one, t): t for t in tasks}
        for fut in as_completed(future_to_task):
            t = future_to_task[fut]
            cat, ok, msg = fut.result()
            done[cat] += 1
            if not ok:
                print(f"\n[WARN] {cat} failed: {msg}")
                if fail_log:
                    fail_log.parent.mkdir(parents=True, exist_ok=True)
                    with fail_log.open("a", encoding="utf-8") as f:
                        f.write(f"{cat}\t{t.url}\t{msg}\n")
            _print_progress(t)
    if active_cats:
        sys.stdout.write("\n")
    print("All done.")


def fetch_related(
    years: Optional[Set[str]],
    files: Optional[Sequence[str]],
    categories: Set[str],
    overwrite: bool,
    max_workers: int,
    fail_log: Optional[Path],
) -> None:
    cats = categories & {"odds", "horses", "jockeys", "courses"}
    race_files = _filter_race_files(years, files)
    print(
        f"[info] race_*.html candidates: {len(race_files)} "
        f"(filter year={','.join(sorted(years)) if years else 'ALL'}, files={'specified' if files else 'none'})"
    )
    print(f"[info] target categories: {sorted(cats)} overwrite={overwrite} max_workers={max_workers}")
    tasks: List[rel.Task] = []

    race_cats = cats & {"odds", "horses", "jockeys"}
    if race_cats:
        if not race_files:
            print("No race_*.html matched the filter; skip related downloads.")
        else:
            tasks.extend(rel.build_tasks(race_files, race_cats, overwrite))

    if "courses" in cats:
        tasks.extend(rel.build_course_tasks(overwrite))

    odds_n = sum(1 for t in tasks if t.category == "odds")
    horses_n = sum(1 for t in tasks if t.category == "horses")
    jockeys_n = sum(1 for t in tasks if t.category == "jockeys")
    courses_n = sum(1 for t in tasks if t.category == "courses")
    print(
        f"[info] Total tasks: {len(tasks)} "
        f"(odds={odds_n} horses={horses_n} jockeys={jockeys_n} courses={courses_n})"
    )
    _run_tasks(tasks, max_workers=max_workers, fail_log=fail_log)


def fetch_courses(overwrite: bool, max_workers: int, fail_log: Optional[Path]) -> None:
    tasks = rel.build_course_tasks(overwrite)
    print(f"[info] courses tasks: {len(tasks)} (overwrite={overwrite} max_workers={max_workers})")
    _run_tasks(tasks, max_workers=max_workers, fail_log=fail_log)


def fetch_race_results(
    year: int,
    month: Optional[int],
    files: Optional[Sequence[str]],
    overwrite: bool,
    fail_log: Optional[Path],
) -> None:
    if files:
        print(f"[info] race-results: explicit files/CNAMEs={len(files)} overwrite={overwrite}")
        for token in files:
            cname = token
            if token.endswith(".html"):
                cname = Path(token).stem.replace("race_", "")
            try:
                fetch_race_html(cname=cname, race_id=None, overwrite=overwrite)
            except Exception as exc:  # noqa: BLE001
                print(f"[WARN] failed to fetch {token}: {exc}")
                if fail_log:
                    fail_log.parent.mkdir(parents=True, exist_ok=True)
                    with fail_log.open("a", encoding="utf-8") as f:
                        f.write(f"race-results\t{token}\t{exc}\n")
        return

    if month:
        print(f"[info] race-results: fetch month {year}-{month:02d} overwrite={overwrite}")
        fetch_month(year, month, overwrite=overwrite, fail_log=fail_log)
    else:
        print(f"[info] race-results: fetch year {year} overwrite={overwrite}")
        fetch_year(year, overwrite=overwrite, fail_log=fail_log)


def _prompt(text: str, default: Optional[str] = None) -> str:
    suffix = f" [{default}]" if default is not None else ""
    return input(f"{text}{suffix}: ").strip()


def interactive_menu() -> None:
    menu = [
        "1) レース結果HTML取得（年/月/ファイル指定可）",
        "2) オッズ・馬・騎手ページ取得（race_*.htmlから派生）",
        "3) 競馬場・コース情報取得（固定11ページ）",
        "4) まとめて実行（1→2→3）",
        "q) 終了",
    ]
    while True:
        print("\n=== JRA fetch menu ===")
        for line in menu:
            print(line)
        choice = input("選択を入力してください: ").strip().lower()
        if choice == "q":
            print("Bye.")
            return
        elif choice == "1":
            year = _prompt("year (YYYY)", None)
            if not year:
                print("yearは必須です")
                continue
            month_in = _prompt("month (MM or YYYYMM, 空なら年全体)", "")
            month = int(month_in[-2:]) if month_in else None
            files_in = _prompt("CNAMEまたはrace_*.htmlをカンマ区切り指定（空ならなし）", "")
            files = [f.strip() for f in files_in.split(",") if f.strip()] if files_in else None
            overwrite = _prompt("overwrite? (y/N)", "N").lower().startswith("y")
            fail_log = _prompt("fail-log path (空なら未記録)", "")
            fetch_race_results(
                year=int(year),
                month=month,
                files=files,
                overwrite=overwrite,
                fail_log=Path(fail_log) if fail_log else None,
            )
        elif choice == "2":
            year_in = _prompt("year filter for race_*.html (YYYY,カンマ区切り可, 空なら全件)", "")
            files_in = _prompt("race_*.htmlをカンマ区切り指定（空ならなし）", "")
            files = [f.strip() for f in files_in.split(",") if f.strip()] if files_in else None
            cats_in = _prompt("categories (odds,horses,jockeys, 空なら全部)", "")
            cats = {c.strip() for c in cats_in.split(",")} if cats_in else {"odds", "horses", "jockeys"}
            overwrite = _prompt("overwrite? (y/N)", "N").lower().startswith("y")
            mw = _prompt("max-workers", "4")
            fail_log = _prompt("fail-log path (空なら未記録)", "")
            fetch_related(
                years=_parse_years(year_in),
                files=files,
                categories=cats,
                overwrite=overwrite,
                max_workers=int(mw or 4),
                fail_log=Path(fail_log) if fail_log else None,
            )
        elif choice == "3":
            overwrite = _prompt("overwrite? (y/N)", "N").lower().startswith("y")
            mw = _prompt("max-workers", "4")
            fail_log = _prompt("fail-log path (空なら未記録)", "")
            fetch_courses(
                overwrite=overwrite,
                max_workers=int(mw or 4),
                fail_log=Path(fail_log) if fail_log else None,
            )
        elif choice == "4":
            # gather race-results params
            year = _prompt("year (YYYY)", None)
            if not year:
                print("yearは必須です")
                continue
            month_in = _prompt("month (MM or YYYYMM, 空なら年全体)", "")
            month = int(month_in[-2:]) if month_in else None
            overwrite = _prompt("overwrite? (y/N)", "N").lower().startswith("y")
            fail_log = _prompt("fail-log path (空なら未記録)", "")
            mw = _prompt("max-workers (related/courses用)", "4")

            fetch_race_results(
                year=int(year),
                month=month,
                files=None,
                overwrite=overwrite,
                fail_log=Path(fail_log) if fail_log else None,
            )
            cats_in = _prompt("categories for related (odds,horses,jockeys, 空なら全部)", "")
            cats = {c.strip() for c in cats_in.split(",")} if cats_in else {"odds", "horses", "jockeys"}
            fetch_related(
                year=year,
                files=None,
                categories=cats,
                overwrite=overwrite,
                max_workers=int(mw or 4),
                fail_log=Path(fail_log) if fail_log else None,
            )
            fetch_courses(
                overwrite=overwrite,
                max_workers=int(mw or 4),
                fail_log=Path(fail_log) if fail_log else None,
            )
        else:
            print("不正な入力です")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Unified fetch menu for JRA race-related HTMLs.")
    sub = ap.add_subparsers(dest="cmd")

    def common_related(p: argparse.ArgumentParser) -> None:
        p.add_argument("--year", help="Filter race_*.html by year prefix (related only)")
        p.add_argument("--files", help="Comma-separated race_*.html files (related) or CNAME/race files (race-results)")
        p.add_argument("--categories", default="odds,horses,jockeys", help="For related; comma-separated")
        p.add_argument("--overwrite", action="store_true")
        p.add_argument("--max-workers", type=int, default=4)
        p.add_argument("--fail-log")

    p_rel = sub.add_parser("related", help="Fetch odds/horses/jockeys from race_*.html")
    common_related(p_rel)

    p_courses = sub.add_parser("courses", help="Fetch racecourse info pages")
    common_related(p_courses)

    p_rr = sub.add_parser("race-results", help="Fetch race result HTMLs")
    p_rr.add_argument("--year", type=int, required=True, help="Year (YYYY)")
    p_rr.add_argument("--month", type=int, help="Month (1-12, optional)")
    p_rr.add_argument("--files", help="Comma-separated CNAME tokens or race_*.html paths")
    p_rr.add_argument("--overwrite", action="store_true")
    p_rr.add_argument("--fail-log")

    p_all = sub.add_parser("all", help="Run race-results -> related -> courses in order")
    p_all.add_argument("--year", type=int, required=True)
    p_all.add_argument("--month", type=int, help="Month (1-12, optional)")
    p_all.add_argument("--categories", default="odds,horses,jockeys")
    p_all.add_argument("--overwrite", action="store_true")
    p_all.add_argument("--max-workers", type=int, default=4)
    p_all.add_argument("--fail-log")

    return ap.parse_args()


def cli_entry() -> None:
    # default info logging (avoid duplicate handlers)
    if not logging.getLogger().handlers:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    args = parse_args()
    if not args.cmd:
        interactive_menu()
        return

    if args.cmd == "related":
        files = [f.strip() for f in (args.files or "").split(",") if f.strip()] or None
        cats = {c.strip() for c in args.categories.split(",") if c.strip()}
        fetch_related(
            years=_parse_years(args.year),
            files=files,
            categories=cats,
            overwrite=args.overwrite,
            max_workers=args.max_workers,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
    elif args.cmd == "courses":
        fetch_courses(
            overwrite=args.overwrite,
            max_workers=args.max_workers,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
    elif args.cmd == "race-results":
        files = [f.strip() for f in (args.files or "").split(",") if f.strip()] or None
        fetch_race_results(
            year=args.year,
            month=args.month,
            files=files,
            overwrite=args.overwrite,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
    elif args.cmd == "all":
        cats = {c.strip() for c in args.categories.split(",") if c.strip()}
        fetch_race_results(
            year=args.year,
            month=args.month,
            files=None,
            overwrite=args.overwrite,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
        fetch_related(
            year=str(args.year),
            files=None,
            categories=cats,
            overwrite=args.overwrite,
            max_workers=args.max_workers,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
        fetch_courses(
            overwrite=args.overwrite,
            max_workers=args.max_workers,
            fail_log=Path(args.fail_log) if args.fail_log else None,
        )
    else:
        interactive_menu()


if __name__ == "__main__":
    cli_entry()
