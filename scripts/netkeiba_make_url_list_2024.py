# scripts/netkeiba_make_url_list_2024.py
from __future__ import annotations

import csv
import time
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

SLEEP_SEC = 5  # 月ごとのアクセス間隔
ROOT = Path(__file__).resolve().parents[1]
OUT_CSV = ROOT / "data" / "master" / "netkeiba_race_list_2024.csv"

BASE_CAL_URL = "https://db.netkeiba.com/race/list/{ymd}/"

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


def fetch_html(url: str) -> str:
    """netkeiba 用の HTML 取得（encoding も考慮）"""
    res = session.get(url, timeout=10)
    # netkeiba はヘッダ的には EUC-JP 系。encoding が入っていればそれを、なければ euc_jp。
    encoding = res.encoding or "euc_jp"
    try:
        return res.content.decode(encoding, errors="ignore")
    except Exception:
        # 念のためフォールバック
        return res.content.decode("euc_jp", errors="ignore")


def get_one_month_urls(year: int, month: int) -> list[str]:
    """指定年/月の全レースURL一覧を取得"""
    ymd = f"{year}{month:02d}01"
    url = BASE_CAL_URL.format(ymd=ymd)

    print(f"[INFO] get race list: {year}/{month:02d} -> {url}")

    html = fetch_html(url)
    soup = BeautifulSoup(html, "html.parser")

    race_date_list: list[str] = []
    date_div = soup.find("div", class_="race_calendar")
    if not date_div:
        print("[WARN] race_calendar not found")
        return []

    # カレンダーからその月の「開催日ページ」URLを集める
    for td in date_div.find_all("td"):
        a = td.find("a")
        if a and a.get("href"):
            race_date_list.append("https://db.netkeiba.com" + a["href"])

    race_urls: list[str] = []

    # 各開催日ページから、その日の各レースの URL を取得
    for race_date_url in race_date_list:
        print(f"  [INFO] day page: {race_date_url}")
        date_html = fetch_html(race_date_url)
        date_soup = BeautifulSoup(date_html, "html.parser")

        # ページ内の各レースへのリンク（例：/race/202405010101/）
        for dl in date_soup.find_all("dl", class_="race_top_data_info fc"):
            a = dl.find("a")
            if a and a.get("href"):
                race_url = "https://db.netkeiba.com" + a["href"]
                race_urls.append(race_url)

        # 開催日ページのアクセス間隔（1秒）
        time.sleep(1.0)

    return race_urls


def main() -> None:
    year = 2024

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)

    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["race_id", "url"])

        for month in range(1, 13):
            urls = get_one_month_urls(year, month)

            for u in urls:
                race_key = u.rstrip("/").split("/")[-1]  # 202405010101 など
                writer.writerow([race_key, u])

            # 月ごとのアクセス間隔
            time.sleep(SLEEP_SEC)

    print(f"[DONE] saved: {OUT_CSV}")


if __name__ == "__main__":
    main()
