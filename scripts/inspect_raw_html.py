"""
JRA の生 HTML (data/raw/jra 配下) を「読むだけ・分析だけ」するスクリプト。
既存 ETL の修正や書き込みは一切行わず、構造バリエーションの概要を標準出力に出す。

Usage:
    python scripts/inspect_raw_html.py

Codex に使わせたい場合:
  1. このファイルをリポジトリに保存
  2. Codex に「まずこのスクリプトを実行して出力を見てから、
     jra_parser の直し方を提案して」と指示する
"""
from __future__ import annotations

from collections import Counter, defaultdict
from pathlib import Path
import argparse
import concurrent.futures as futures
import re

from bs4 import BeautifulSoup

# 必要に応じてここだけ書き換えれば OK
RAW_DIR = Path("data/raw/jra")


def guess_year_from_filename(path: Path) -> int | None:
    """
    race_202405020509.html のようなファイル名から 2024 を推定する。
    """
    m = re.search(r"(\d{4})", path.name)
    return int(m.group(1)) if m else None


def find_result_table(soup: BeautifulSoup):
    """
    着順テーブルを特定する。
    ヘッダに '着順' が含まれている <table> を 1 つ返す。
    """
    for table in soup.find_all("table"):
        thead = table.find("thead")
        if not thead:
            continue
        th_texts = [th.get_text(strip=True) for th in thead.find_all("th")]
        if "着順" in th_texts:
            return table, th_texts
    return None, []


def analyze_one_file(path: Path) -> dict:
    """
    1 つの HTML ファイルを読み込んで結果テーブルの構造情報を返す。
    """
    # JRA は Shift_JIS (cp932) なので、まずそれで読む
    data = path.read_bytes()
    try:
        html = data.decode("cp932")
    except UnicodeDecodeError:
        # 念のため fallback
        html = data.decode("utf-8", errors="ignore")

    soup = BeautifulSoup(html, "lxml")

    table, header = find_result_table(soup)
    if table is None:
        return {
            "file": str(path),
            "year": guess_year_from_filename(path),
            "has_result_table": False,
            "header": [],
            "row_count": 0,
            "td_min": 0,
            "td_max": 0,
            "td_classes": set(),
            "has_rat": False,
            "variant_key": "NO_RESULT_TABLE",
        }

    tbody = table.find("tbody")
    rows = tbody.find_all("tr") if tbody else []

    td_counts: list[int] = []
    class_set: set[str] = set()
    has_rat = False

    for row in rows:
        tds = row.find_all("td")
        td_counts.append(len(tds))
        for td in tds:
            classes = td.get("class") or []
            for cls in classes:
                class_set.add(cls)
                if cls == "rat":
                    has_rat = True

    if td_counts:
        td_min = min(td_counts)
        td_max = max(td_counts)
    else:
        td_min = td_max = 0

    # バリアントキー: ヘッダ + 列数範囲 + class セット
    header_str = "|".join(header)
    classes_str = "|".join(sorted(class_set))
    variant_key = f"HEAD:{header_str}||TD:{td_min}-{td_max}||CLS:{classes_str}"

    return {
        "file": str(path),
        "year": guess_year_from_filename(path),
        "has_result_table": True,
        "header": header,
        "row_count": len(rows),
        "td_min": td_min,
        "td_max": td_max,
        "td_classes": class_set,
        "has_rat": has_rat,
        "variant_key": variant_key,
    }


def main() -> None:
    ap = argparse.ArgumentParser(description="Inspect raw JRA HTML structures (read-only).")
    ap.add_argument("--jobs", type=int, default=1, help="Parallel workers (default: 1). Use 0 for cpu_count().")
    args = ap.parse_args()

    html_files = sorted(RAW_DIR.rglob("*.html"))
    if not html_files:
        print(f"[WARN] No html files under {RAW_DIR}")
        return

    print(f"[INFO] Found {len(html_files)} html files under {RAW_DIR}")
    jobs = None if args.jobs == 0 else max(1, args.jobs)

    records: list[dict] = []
    variant_counter = Counter()
    variant_example: dict[str, str] = defaultdict(str)

    def _handle_info(info: dict) -> None:
        records.append(info)
        key = info["variant_key"]
        variant_counter[key] += 1
        if not variant_example[key]:
            variant_example[key] = info["file"]

    if jobs == 1:
        for path in html_files:
            _handle_info(analyze_one_file(path))
    else:
        with futures.ProcessPoolExecutor(max_workers=jobs) as ex:
            for info in ex.map(analyze_one_file, html_files):
                _handle_info(info)

    # 1) 年ごとの概要
    print("\n==== Summary by year ====")
    per_year = Counter(r["year"] for r in records)
    for year, cnt in sorted(per_year.items(), key=lambda kv: (kv[0] is None, kv[0] or 0)):
        print(f"  {year}: {cnt} files")

    # 2) バリアントごとの概要
    print("\n==== HTML structure variants ====")
    for key, cnt in variant_counter.most_common():
        example = variant_example[key]
        print(f"\n--- Variant ({cnt} files) ---")
        print(f"variant_key : {key}")
        print(f"example_file: {example}")

    # 3) rat 列を持つファイル一覧
    print("\n==== Files that have <td class='rat'> (rating column?) ====")
    rat_files = [r for r in records if r["has_rat"]]
    if not rat_files:
        print("  (none)")
    else:
        for r in rat_files:
            print(f"  {r['file']} (year={r['year']}, td_min={r['td_min']}, td_max={r['td_max']})")

    # 4) 小規模ファイルを抜き出して目視しやすくする
    print("\n==== Small files (row_count <= 5) for manual inspection ====")
    small = [r for r in records if r["row_count"] <= 5 and r["has_result_table"]]
    for r in small[:20]:
        print(
            f"  {r['file']}: rows={r['row_count']}, "
            f"td_min={r['td_min']}, td_max={r['td_max']}, "
            f"classes={sorted(r['td_classes'])}"
        )


if __name__ == "__main__":
    main()
