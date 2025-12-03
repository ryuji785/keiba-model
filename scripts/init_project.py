"""
Initialize project directory structure and basic .gitignore for JRA ETL.

Creates:
  - scripts/
  - data/raw/jra/
  - data/master/
  - data/db/
  - docs/
and ensures .gitignore contains common ignores.
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


DEFAULT_DIRS = [
    Path("scripts"),
    Path("data/raw/jra"),
    Path("data/master"),
    Path("data/db"),
    Path("docs"),
]

GITIGNORE_ENTRIES = [
    "__pycache__/",
    "*.pyc",
    "*.pyo",
    "*.pyd",
    ".Python",
    "env/",
    "venv/",
    ".venv/",
    "build/",
    "dist/",
    "data/raw/",
    "*.html",
    ".DS_Store",
]


def ensure_dirs(base: Path) -> None:
    for d in DEFAULT_DIRS:
        target = base / d
        target.mkdir(parents=True, exist_ok=True)
        logger.info("Ensured directory: %s", target)


def ensure_gitignore(base: Path) -> None:
    gitignore_path = base / ".gitignore"
    if gitignore_path.exists():
        existing = set(gitignore_path.read_text(encoding="utf-8").splitlines())
    else:
        existing = set()
    updated = False
    lines = list(existing)
    for entry in GITIGNORE_ENTRIES:
        if entry not in existing:
            lines.append(entry)
            updated = True
    if updated or not gitignore_path.exists():
        gitignore_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        logger.info("Updated .gitignore: %s", gitignore_path)
    else:
        logger.info(".gitignore already contains expected entries")


def main() -> None:
    parser = argparse.ArgumentParser(description="Initialize JRA ETL project structure.")
    parser.add_argument(
        "--base",
        type=Path,
        default=Path("."),
        help="Project root (default: current directory).",
    )
    args = parser.parse_args()
    base = args.base.resolve()

    ensure_dirs(base)
    ensure_gitignore(base)
    logger.info("Project initialization completed at %s", base)


if __name__ == "__main__":
    main()
