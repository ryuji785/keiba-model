# scripts/netkeiba_fetch_html_2024.py
from __future__ import annotations

import csv
import time
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]

RACE_LIST_CSV = ROOT / "data" / "master" / "netkeiba_race_list_2024.csv"
SAVE_DIR = ROOT / "data" / "raw" / "netkeiba" / "races_2024"

SAVE_DIR.mkdir(parents=True, exist_ok=True)

SLEEP_SEC = 3  # アクセス間隔（短くしすぎない）


# ---- 共通 HTTP セッション（Cookie保持 + User-Agent付与） ----
session = requests.Session()
session.headers.update(
    {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/118.0 Safari/537.36"
        )
    }
)


def fetch_one_html(race_id: str, url: str) -> None:
    """1レースHTMLを取得して保存"""
    print(f"[INFO] fetching {race_id} from {url}")

    res = session.get(url, timeout=10)

    # netkeiba 側の charset を優先し、なければ EUC-JP で読む
    encoding = res.encoding or "euc_jp"
    try:
        html = res.content.decode(encoding, errors="ignore")
    except Exception:
        html = res.content.decode("euc_jp", errors="ignore")

    save_path = SAVE_DIR / f"{race_id}.html"
    save_path.write_text(html, encoding="utf-8")
    print(f"[OK] saved -> {save_path}")


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

        save_path = SAVE_DIR / f"{race_id}.html"
        if save_path.exists():
            print(f"[SKIP] already exists: {save_path}")
            continue

        try:
            fetch_one_html(race_id, url)
        except Exception as e:
            print(f"[ERROR] failed: {race_id} ({e})")

        time.sleep(SLEEP_SEC)

    print("[DONE] all html fetched")


if __name__ == "__main__":
    main()
