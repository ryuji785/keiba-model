# scripts/fetch_jra_html.py

import requests
from pathlib import Path
import time

def fetch_race_html(cname: str, save_dir: Path):
    """
    JRA のレースページ（Shift_JIS）を取得し、
    UTF-8 に変換して保存する。
    """
    url = f"https://www.jra.go.jp/JRADB/accessS.html?CNAME={cname}"
    print("Fetching:", url)

    resp = requests.get(url)
    if resp.status_code != 200:
        raise Exception(f"Failed to fetch {url} status={resp.status_code}")

    # --- ここが重要 ---
    # JRAはShift_JIS（CP932）で配信しているので、一度decodeする
    html_text = resp.content.decode("shift_jis", errors="ignore")

    # ファイル名に使えない「/」を置き換える
    safe_id = cname.replace("/", "_")

    # UTF-8で保存
    save_path = save_dir / f"race_{safe_id}.html"
    save_path.write_text(html_text, encoding="utf-8")

    print(f"Saved HTML (UTF-8) to: {save_path}")


def main():
    base_dir = Path(__file__).resolve().parents[1]
    save_dir = base_dir / "data" / "raw" / "jra"
    save_dir.mkdir(parents=True, exist_ok=True)

    cname = "pw01sde1008202503091120251026/59"
    fetch_race_html(cname, save_dir)
    time.sleep(3)


if __name__ == "__main__":
    main()
