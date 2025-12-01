"""
Common HTTP fetch helper for JRA pages (Shift_JIS handling + BeautifulSoup).
"""

from __future__ import annotations

import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36"
)


def _decode_response(resp: requests.Response) -> str:
    if resp.encoding:
        try:
            return resp.content.decode(resp.encoding, errors="ignore")
        except Exception:
            return resp.text
    enc = resp.apparent_encoding or "shift_jis"
    try:
        return resp.content.decode(enc, errors="ignore")
    except Exception:
        return resp.text


def get_soup(url: str, *, timeout: int = 15, headers: Optional[dict] = None) -> BeautifulSoup:
    """
    Fetch URL and return BeautifulSoup parsed with html.parser (no lxml dependency).

    - Sets User-Agent
    - Tries response.encoding, falls back to apparent_encoding, then Shift_JIS
    - Uses errors='ignore' to avoid decode errors
    - Raises for non-200 status codes
    """
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)

    resp = requests.get(url, timeout=timeout, headers=h)
    if resp.status_code != 200:
        resp.raise_for_status()

    html_text = _decode_response(resp)
    return BeautifulSoup(html_text, "html.parser")


def fetch_html(url: str, *, timeout: int = 15, headers: Optional[dict] = None) -> str:
    """
    Fetch URL and return decoded HTML text (Shift_JIS-safe).
    """
    h = {"User-Agent": USER_AGENT}
    if headers:
        h.update(headers)
    resp = requests.get(url, timeout=timeout, headers=h)
    if resp.status_code != 200:
        resp.raise_for_status()
    return _decode_response(resp)
