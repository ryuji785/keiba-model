"""
Extract JRA race-result HTML and save to data/raw/jra/.

Usage:
    python fetch_jra_html.py 202405020411 [--overwrite] [--sleep 1.0]
"""

import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Optional

import requests

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "raw" / "jra"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)

logger = logging.getLogger(__name__)


def build_jra_result_url(race_id: str) -> str:
    """
    与えられた race_id から JRA レース結果ページの URL を構築して返す。
    TODO: 正確なURLパターンは後で調整する。
    """
    # 仮の構築ロジック。実際のパラメータは後日更新予定。
    # 例: https://www.jra.go.jp/JRADB/accessU.html?CNAME=pw01dud{race_id}
    return f"https://www.jra.go.jp/JRADB/accessU.html?CNAME=pw01dud{race_id}"


def _request_with_retries(url: str, max_attempts: int = 3, timeout: int = 10) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    last_error: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code != 200:
                response.raise_for_status()
            return response
        except Exception as exc:
            last_error = exc
            logger.warning("Request failed (attempt %s/%s): %s", attempt, max_attempts, exc)
            if attempt < max_attempts:
                time.sleep(2)  # fixed backoff
    assert last_error is not None
    logger.error("All retry attempts failed for URL: %s", url)
    raise last_error


def fetch_and_save_race_html(race_id: str, overwrite: bool = False, sleep_sec: float = 1.0) -> Path:
    """
    race_id の HTML を JRA から取得して data/raw/jra/ に保存し、
    保存したファイルの Path を返す。
    既存ファイルがあり overwrite=False の場合は HTTP 取得を行わず、
    既存ファイルの Path をそのまま返す。
    何らかの致命的エラーがあれば例外を送出する。
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"race_{race_id}.html"

    if output_path.exists() and not overwrite:
        logger.info("既存ファイルがあるためスキップ: %s", output_path)
        return output_path

    url = build_jra_result_url(race_id)
    logger.info("取得開始: race_id=%s url=%s 保存先=%s", race_id, url, output_path)

    if sleep_sec > 0:
        time.sleep(sleep_sec)

    response = _request_with_retries(url)

    if sleep_sec > 0:
        time.sleep(sleep_sec)

    if not response.encoding:
        response.encoding = response.apparent_encoding or "utf-8"

    output_path.write_text(response.text, encoding=response.encoding or "utf-8")
    logger.info("取得・保存完了: %s", output_path)
    return output_path


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch JRA race result HTML and save locally.")
    parser.add_argument("race_id", type=str, help="JRA race ID (e.g., 202405020411)")
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the file even if it already exists.",
    )
    parser.add_argument(
        "--sleep",
        dest="sleep_sec",
        type=float,
        default=1.0,
        help="Seconds to sleep before and after the request (for access etiquette).",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    args = _parse_args()
    try:
        fetch_and_save_race_html(args.race_id, overwrite=args.overwrite, sleep_sec=args.sleep_sec)
    except Exception:
        logger.exception("JRAレース結果の取得に失敗しました")
        sys.exit(1)


if __name__ == "__main__":
    main()
