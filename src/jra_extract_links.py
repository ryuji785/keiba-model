"""
Extract JRA doAction CNAME parameters from onclick attributes.
"""

from __future__ import annotations

import re
from typing import List

from bs4 import BeautifulSoup


def extract_cnames_from_soup(soup: BeautifulSoup) -> List[str]:
    """
    Given a BeautifulSoup, extract all CNAME (second argument of doAction) from onclick.

    Example: onclick="doAction('/JRADB/accessS.html','pw01sde10....')"
    """
    pattern = re.compile(r"doAction\(\s*['\"][^'\"]+['\"],\s*['\"]([^'\"]+)['\"]\s*\)")
    cnames = []
    for tag in soup.find_all(onclick=True):
        onclick = tag.get("onclick", "")
        m = pattern.search(onclick)
        if m:
            cnames.append(m.group(1))
    # remove duplicates preserving order
    seen = set()
    uniq = []
    for c in cnames:
        if c not in seen:
            seen.add(c)
            uniq.append(c)
    return uniq
