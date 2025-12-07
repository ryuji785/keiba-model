"""
Common helpers for ETL: logging, HTTP with retry/backoff, filesystem utilities.
"""

import logging
import random
import time
from pathlib import Path
from typing import Optional

import requests


LOG_PATH = Path(__file__).resolve().parents[1] / "logs" / "etl.log"
LOG_PATH.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Referer": "https://www.jra.go.jp/",
}


def http_get(
    url: str,
    *,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: int = 15,
    retries: int = 3,
) -> requests.Response:
    """GET with simple retry and polite sleep."""
    merged_headers = DEFAULT_HEADERS.copy()
    if headers:
        merged_headers.update(headers)

    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(url, params=params, headers=merged_headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("GET failed (attempt %s/%s) url=%s err=%s", attempt, retries, url, exc)
            if attempt < retries:
                _polite_sleep()
    assert last_exc is not None
    raise last_exc


def _polite_sleep() -> None:
    time.sleep(1.0 + random.random())


def save_text(path: Path, text: str, encoding: str = "utf-8") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding=encoding)


def decode_shift_jis(content: bytes) -> str:
    try:
        return content.decode("shift_jis")
    except Exception:
        return content.decode("shift_jis", errors="ignore")


def http_post(
    url: str,
    *,
    data: dict,
    headers: Optional[dict] = None,
    timeout: int = 15,
    retries: int = 3,
) -> requests.Response:
    """POST with simple retry and polite sleep."""
    merged_headers = DEFAULT_HEADERS.copy()
    if headers:
        merged_headers.update(headers)

    last_exc: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, data=data, headers=merged_headers, timeout=timeout)
            if resp.status_code == 200:
                return resp
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            logger.warning("POST failed (attempt %s/%s) url=%s err=%s", attempt, retries, url, exc)
            if attempt < retries:
                _polite_sleep()
    assert last_exc is not None
    raise last_exc
