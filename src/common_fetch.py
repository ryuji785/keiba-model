"""
Common HTTP fetch helper for JRA pages (encoding-safe + BeautifulSoup).
"""

from __future__ import annotations

import logging
from typing import Optional, Dict, Any

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# 共通 User-Agent（ブラウザっぽく見せる）
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0 Safari/537.36"
)

# 追加ヘッダ
EXTRA_HEADERS: Dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en-US;q=0.7,en;q=0.3",
    "Referer": "https://www.jra.go.jp/",
    "Connection": "keep-alive",
}

# セッション（コネクション再利用）
_SESSION = requests.Session()


def _choose_encoding(resp: requests.Response, explicit: Optional[str]) -> str:
    """
    Decide encoding with JRA-friendly defaults.
    - explicit が指定されていればそれを優先
    - そうでなければ cp932 (Shift_JIS) を最優先で試す
    - それでも不明なら apparent_encoding → encoding の順
    """
    if explicit:
        return explicit

    # 多くの JRA ページは Shift_JIS/CP932 前提
    if resp.encoding and resp.encoding.lower() in {"shift_jis", "cp932", "shift-jis"}:
        return resp.encoding
    return resp.apparent_encoding or resp.encoding or "cp932"


def _decode_response(resp: requests.Response, encoding: Optional[str]) -> str:
    enc = _choose_encoding(resp, encoding)
    logger.info("[common_fetch] decode with encoding=%s url=%s", enc, resp.url)

    try:
        return resp.content.decode(enc, errors="ignore")
    except Exception as e:
        logger.error(
            "[common_fetch] decode error encoding=%s url=%s error=%s",
            enc,
            resp.url,
            e,
        )
        # 最悪 resp.text にフォールバック
        return resp.text


def fetch_html(
    url: str,
    *,
    timeout: int = 15,
    headers: Optional[dict] = None,
    encoding: Optional[str] = None,
) -> str:
    """
    Fetch URL and return decoded HTML text.

    Parameters
    ----------
    url : str
        取得対象のURL
    timeout : int, optional
        タイムアウト秒数（デフォルト 15）
    headers : dict, optional
        追加ヘッダ（User-Agent / Accept 等は共通で付与される）
    encoding : str | None, optional
        明示的に使用するエンコーディング。
        None の場合は apparent_encoding → encoding → 'cp932' の順で決める。

    Notes
    -----
    - 4xx / 5xx は resp.raise_for_status() で例外を投げる
    - decode時は errors='ignore' で落ちないようにする
    """
    merged_headers: Dict[str, Any] = {
        "User-Agent": USER_AGENT,
        **EXTRA_HEADERS,
    }
    if headers:
        merged_headers.update(headers)

    logger.info("[common_fetch] GET %s", url)

    try:
        resp = _SESSION.get(url, timeout=timeout, headers=merged_headers)
    except Exception as e:
        logger.error("[common_fetch] request error url=%s error=%s", url, e)
        raise

    logger.info(
        "[common_fetch] status=%s url=%s resp.encoding=%s apparent=%s",
        resp.status_code,
        url,
        resp.encoding,
        resp.apparent_encoding,
    )

    resp.raise_for_status()

    return _decode_response(resp, encoding)


def get_soup(
    url: str,
    *,
    timeout: int = 15,
    headers: Optional[dict] = None,
    encoding: Optional[str] = None,
) -> BeautifulSoup:
    """
    Fetch URL and return BeautifulSoup parsed with html.parser.

    - 内部的には fetch_html() を使う
    - encoding の扱いは fetch_html と同じ
    """
    html_text = fetch_html(url, timeout=timeout, headers=headers, encoding=encoding)
    return BeautifulSoup(html_text, "html.parser")
