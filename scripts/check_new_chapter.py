"""Find the newest chapter that is released and filled in on the Fandom wiki.

Starts from the last chapter in data/chapters.json and probes the next ones.
A chapter counts as ready when its release date has passed and its wiki page
has the infobox filled in, a populated character list and a real Long Summary.

Prints KEY=VALUE lines (and appends them to $GITHUB_OUTPUT when set):
  ready=true|false, chapter=<newest ready chapter>, volume=<last volume>
"""

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from onepieceofdata.scrapers.chapter_api import ChapterScraperAPI

MIN_CHARACTERS = 10
MIN_LONG_SUMMARY = 1000  # unfilled pages carry a ~50-char placeholder
REQUIRED_FIELDS = ("title", "japanese_title", "pages", "jump_info")


def section_length(wikitext: str, name: str) -> int:
    m = re.search(rf"==\s*{name}\s*==(.*?)(?=\n==[^=]|\Z)", wikitext, re.S)
    return len(m.group(1).strip()) if m else 0


def is_ready(scraper: ChapterScraperAPI, number: int) -> bool:
    result = scraper.scrape_chapter(number)
    data = result.data or {}
    try:
        released = datetime.strptime(data.get("release_date", ""), "%B %d, %Y").date()
    except ValueError:
        released = None
    checks = {
        "fields": all(data.get(f) for f in REQUIRED_FIELDS),
        "released": released is not None and released <= datetime.now(timezone.utc).date(),
        "characters": len(data.get("characters", [])) >= MIN_CHARACTERS,
        "long_summary": section_length(scraper._fetch_chapter_wikitext(number) or "", "Long Summary")
        >= MIN_LONG_SUMMARY,
    }
    ok = result.success and all(checks.values())
    print(f"Chapter {number}: released={released} {checks} ready={ok}")
    return ok


def main() -> None:
    chapters = json.loads(Path("data/chapters.json").read_text())
    current = max(c["chapter_number"] for c in chapters)
    volume = max(int(c["volume"]) for c in chapters if str(c.get("volume", "")).isdigit())

    scraper = ChapterScraperAPI()
    latest = current
    while is_ready(scraper, latest + 1):
        latest += 1

    out = f"ready={'true' if latest > current else 'false'}\nchapter={latest}\nvolume={volume}\n"
    print(out, end="")
    if os.environ.get("GITHUB_OUTPUT"):
        with open(os.environ["GITHUB_OUTPUT"], "a") as f:
            f.write(out)


if __name__ == "__main__":
    main()
