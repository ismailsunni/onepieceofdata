"""Fail if the scraped data is incomplete, so a partial scrape never gets released or exported.

- data/chapters.json must contain every chapter 1..OP_LAST_CHAPTER.
- Other scraped files must not shrink by more than 2% vs. the last commit (git HEAD).
"""

import json
import subprocess
import sys
from pathlib import Path

from onepieceofdata.config.settings import get_settings

TOLERANCE = 0.98
FILES = ["arcs.json", "sagas.json", "volumes.json", "characters_detail.json"]

errors = []

last = get_settings().last_chapter
chapters = {c["chapter_number"] for c in json.loads(Path("data/chapters.json").read_text())}
missing = sorted(set(range(1, last + 1)) - chapters)
if missing:
    errors.append(f"chapters.json: {len(missing)} of {last} chapters missing (e.g. {missing[:10]})")

for name in FILES:
    path = f"data/{name}"
    new = len(json.loads(Path(path).read_text()))
    try:
        old = len(json.loads(subprocess.check_output(["git", "show", f"HEAD:{path}"])))
    except subprocess.CalledProcessError:
        continue
    print(f"{name}: {new} (last commit: {old})")
    if new < old * TOLERANCE:
        errors.append(f"{name}: {new} entries, down from {old}")

if errors:
    print("❌ Scrape looks incomplete:\n  " + "\n  ".join(errors))
    sys.exit(1)
print(f"✅ Scrape complete: chapters 1..{last}")
