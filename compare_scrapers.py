#!/usr/bin/env python3
"""Compare old HTML scraper vs new API scraper results."""

import json
import sys
from pathlib import Path
from typing import Dict, List, Any

from loguru import logger


def load_json(file_path: Path) -> List[Dict[str, Any]]:
    """Load JSON data from file."""
    with open(file_path) as f:
        return json.load(f)


def compare_chapter_data(old_data: Dict, new_data: Dict, chapter_num: int) -> Dict[str, Any]:
    """Compare a single chapter's data between old and new scrapers.

    Returns:
        Dictionary with comparison results
    """
    results = {
        "chapter": chapter_num,
        "matches": [],
        "differences": [],
        "missing_in_old": [],
        "missing_in_new": [],
        "improvements": [],
    }

    # Compare titles
    old_title = old_data.get("title", "")
    new_title = new_data.get("title", "")

    if old_title and new_title:
        if old_title in new_title or new_title in old_title:
            results["matches"].append(f"Title matches (similar)")
        else:
            results["differences"].append(f"Title differs: '{old_title}' vs '{new_title}'")
    elif new_title and not old_title:
        results["improvements"].append(f"API found title: '{new_title}'")
        results["missing_in_old"].append("title")
    elif old_title and not new_title:
        results["missing_in_new"].append("title")

    # Compare character counts
    old_chars = old_data.get("characters", [])
    new_chars = new_data.get("characters", [])

    old_char_count = len(old_chars)
    new_char_count = len(new_chars)

    if old_char_count == new_char_count:
        results["matches"].append(f"Character count matches ({old_char_count})")
    else:
        diff = new_char_count - old_char_count
        if diff > 0:
            results["improvements"].append(
                f"API found {diff} more characters ({new_char_count} vs {old_char_count})"
            )
        else:
            results["differences"].append(
                f"API found {abs(diff)} fewer characters ({new_char_count} vs {old_char_count})"
            )

    # Compare character names
    old_names = {char.get("name", "") for char in old_chars if char.get("name")}
    new_names = {char.get("name", "") for char in new_chars if char.get("name")}

    common_names = old_names & new_names
    only_in_old = old_names - new_names
    only_in_new = new_names - old_names

    if common_names:
        results["matches"].append(f"{len(common_names)} common character names")

    if only_in_new:
        results["improvements"].append(
            f"API found {len(only_in_new)} additional characters: {', '.join(list(only_in_new)[:5])}"
        )

    if only_in_old:
        results["differences"].append(
            f"{len(only_in_old)} characters only in old scraper: {', '.join(list(only_in_old)[:5])}"
        )

    # Compare other fields
    comparable_fields = ["volume", "pages", "release_date", "jump_info"]
    for field in comparable_fields:
        old_val = old_data.get(field)
        new_val = new_data.get(field)

        if old_val and new_val:
            if str(old_val) == str(new_val):
                results["matches"].append(f"{field} matches")
            else:
                results["differences"].append(f"{field} differs: '{old_val}' vs '{new_val}'")
        elif old_val and not new_val:
            results["missing_in_new"].append(field)
        elif new_val and not old_val:
            results["improvements"].append(f"API found {field}: '{new_val}'")

    return results


def main():
    """Compare old and new scraper outputs."""
    logger.info("=" * 70)
    logger.info("Comparing Old HTML Scraper vs New API Scraper")
    logger.info("=" * 70)

    # Check if we have existing data
    existing_file = Path("data/chapters.json")
    if not existing_file.exists():
        logger.error("No existing chapters.json found")
        logger.info("Please run the old scraper first to generate baseline data")
        return 1

    # Scrape a sample with the new API scraper
    logger.info("\nStep 1: Scraping chapters 1-10 with API scraper...")
    import subprocess

    result = subprocess.run(
        [
            "uv",
            "run",
            "onepieceofdata",
            "scrape-chapters-api",
            "--start-chapter",
            "1",
            "--end-chapter",
            "10",
            "--output",
            "/tmp/chapters_api_comparison.json",
        ],
        capture_output=True,
        text=True,
    )

    if result.returncode != 0:
        logger.error(f"API scraping failed: {result.stderr}")
        return 1

    logger.success("✓ API scraping completed")

    # Load both datasets
    logger.info("\nStep 2: Loading data...")
    old_data = load_json(existing_file)
    new_data = load_json(Path("/tmp/chapters_api_comparison.json"))

    # Filter old data to same range
    old_data_filtered = [c for c in old_data if 1 <= c.get("chapter_number", 0) <= 10]

    logger.info(f"Old scraper data: {len(old_data_filtered)} chapters")
    logger.info(f"New API data: {len(new_data)} chapters")

    # Compare each chapter
    logger.info("\nStep 3: Comparing chapters...")
    logger.info("=" * 70)

    all_matches = []
    all_differences = []
    all_improvements = []
    all_missing_new = []

    for new_chapter in new_data:
        chapter_num = new_chapter.get("chapter_number")
        old_chapter = next(
            (c for c in old_data_filtered if c.get("chapter_number") == chapter_num), None
        )

        if not old_chapter:
            logger.warning(f"Chapter {chapter_num} not found in old data")
            continue

        comparison = compare_chapter_data(old_chapter, new_chapter, chapter_num)

        # Print summary for each chapter
        logger.info(f"\n📖 Chapter {chapter_num}:")

        if comparison["matches"]:
            for match in comparison["matches"]:
                logger.success(f"  ✓ {match}")
                all_matches.append(match)

        if comparison["improvements"]:
            for improvement in comparison["improvements"]:
                logger.info(f"  ⬆️  {improvement}")
                all_improvements.append(improvement)

        if comparison["differences"]:
            for diff in comparison["differences"]:
                logger.warning(f"  ⚠️  {diff}")
                all_differences.append(diff)

        if comparison["missing_in_new"]:
            for missing in comparison["missing_in_new"]:
                logger.error(f"  ❌ Missing in API: {missing}")
                all_missing_new.append(missing)

    # Final summary
    logger.info("\n" + "=" * 70)
    logger.info("FINAL SUMMARY")
    logger.info("=" * 70)

    logger.success(f"✓ Matches: {len(all_matches)}")
    logger.info(f"⬆️  Improvements: {len(all_improvements)}")
    logger.warning(f"⚠️  Differences: {len(all_differences)}")
    logger.error(f"❌ Missing in API: {len(all_missing_new)}")

    # Verdict
    logger.info("\n" + "=" * 70)
    logger.info("VERDICT")
    logger.info("=" * 70)

    if len(all_missing_new) > 5:
        logger.error("❌ API scraper is missing significant data")
        logger.error("   Migration NOT recommended yet")
        return 1
    elif len(all_improvements) > len(all_differences):
        logger.success("✅ API scraper provides better or equivalent data")
        logger.success("   Migration is RECOMMENDED")
        return 0
    else:
        logger.warning("⚠️  API scraper has some differences")
        logger.warning("   Review differences before migration")
        return 0


if __name__ == "__main__":
    sys.exit(main())
