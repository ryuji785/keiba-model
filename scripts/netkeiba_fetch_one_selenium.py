# scripts/netkeiba_fetch_one_selenium.py
from __future__ import annotations

from pathlib import Path
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def fetch_one(url: str, save_path: Path) -> None:
    """Selenium で 1 レースページを取得して保存"""

    # Chrome を headless で起動
    options = Options()
    options.add_argument("--headless=new")  # 画面を出さない
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0 Safari/537.36"
    )

    driver = webdriver.Chrome(ChromeDriverManager().install(), options=options)

    try:
        print(f"[INFO] open {url}")
        driver.get(url)

        # JS 実行待ち（とりあえず固定で 5 秒）
        time.sleep(5)

        html = driver.page_source
        save_path.write_text(html, encoding="utf-8")
        print(f"[OK] saved -> {save_path}")

    finally:
        driver.quit()


def main() -> None:
    # ★ 試しに 1 レースだけ（ここは好きなURLで差し替えてOK）
    url = "https://db.netkeiba.com/race/202406010101/"
    root = Path(__file__).resolve().parents[1]
    save_dir = root / "data" / "raw" / "netkeiba" / "selenium_test"
    save_dir.mkdir(parents=True, exist_ok=True)

    race_id = url.rstrip("/").split("/")[-1]
    save_path = save_dir / f"{race_id}.html"

    fetch_one(url, save_path)


if __name__ == "__main__":
    main()
