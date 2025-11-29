# scripts/fetch_races_2024.py
from __future__ import annotations

import time
import csv
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parents[1]

RACE_LIST_CSV = ROOT / "data" / "master" / "race_list_2024.csv"
SAVE_DIR = ROOT / "data" / "raw" / "jra" / "races_2024"

SAVE_DIR.mkdir(parents=True, exist_ok=True)

def fetch_one_race_html(race_id: str, url: str) -> None:
    """1レースぶんのHTMLをダウンロードして保存"""
    print(f"[INFO] fetching {race_id} from {url}")

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0 Safari/537.36"
        )
    }

    resp = requests.get(url, headers=headers, timeout=10)
    resp.raise_for_status()

    # JRAは基本 Shift_JIS なので encoding を明示
    resp.encoding = "shift_jis"

    html = resp.text

    save_path = SAVE_DIR / f"{race_id}.html"
    save_path.write_text(html, encoding="utf-8")  # 保存はUTF-8に統一

    print(f"[OK] saved to {save_path}")

def main() -> None:
    if not RACE_LIST_CSV.exists():
        print(f"[ERROR] race list not found: {RACE_LIST_CSV}")
        return

    with RACE_LIST_CSV.open(encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    print(f"[INFO] total races in list: {len(rows)}")

    for i, row in enumerate(rows, start=1):
        race_id = row["race_id"].strip()
        url = row["url"].strip()

        try:
            fetch_one_race_html(race_id, url)
        except Exception as e:
            print(f"[ERROR] failed: {race_id} ({e})")

        # アクセス間隔（マナーとして 1〜2秒程度あける）
        time.sleep(1.5)

    print("[DONE] all races fetched")

if __name__ == "__main__":
    main()
