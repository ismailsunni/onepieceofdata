# Convert JSON to CSV
import pandas as pd
import json
from datetime import datetime


def convert_chapters_to_csv(
    chapters_json_file: str, characters_csv: str, chapters_csv: str, coc_csv: str
):
    print("### Converting Chapters JSON to CSV (chapters, characters, coc) ###")
    # Chapters
    with open(chapters_json_file) as json_file:
        chapter_list = json.load(json_file)

    characters = {}

    chapter_numbers = []
    chapter_volume = []
    chapter_name = []
    chapter_page = []
    chapter_date = []
    chapter_jump = []

    character_ids = []
    character_names = []
    character_urls = []

    # character on chapter
    coc_chapters = []
    coc_characters = []
    coc_notes = []

    for chapter in chapter_list:
        # Chapters
        chapter_numbers.append(int(chapter["chapter"]))
        try:
            chapter_volume.append(int(chapter["vol"]))
        except ValueError:
            chapter_volume.append(None)
        chapter_name.append(chapter["ename"])
        chapter_page.append(int(chapter["page"][:2]))
        chapter_date.append(datetime.strptime(chapter["date2"], "%B %d, %Y"))
        chapter_jump.append(chapter["jump"])

        # Characters
        for character in chapter["characters"]:
            if character["url"]:
                char_id = character["url"].split("/")[-1]
                if "#" in char_id:
                    char_id = char_id.split("#")[-1]
            else:
                char_id = character["name"].replace(" ", "_")

            if char_id not in character_ids:
                character_ids.append(char_id)
                character_names.append(character["name"])
                character_urls.append(character["url"])

            # Characters on Chapter
            coc_chapters.append(int(chapter["chapter"]))
            coc_characters.append(char_id)
            coc_notes.append(character["note"] if character["note"] else "")

    chapters = {
        "chapter": chapter_numbers,
        "volume": chapter_volume,
        "name": chapter_name,
        "page": chapter_page,
        "date": chapter_date,
        "jump": chapter_jump,
    }

    # Create DataFrames
    chapters_df = pd.DataFrame(
        chapters, columns=["chapter", "volume", "name", "page", "date", "jump"]
    )
    characters = {"id": character_ids, "name": character_names, "url": character_urls}
    characters_df = pd.DataFrame(characters, columns=["id", "name", "url"])

    coc = {"chapter": coc_chapters, "character": coc_characters, "note": coc_notes}
    coc_df = pd.DataFrame(coc, columns=["chapter", "character", "note"])

    # Write to CSV
    chapters_df.to_csv(chapters_csv, index=False)
    characters_df.to_csv(characters_csv, index=False)
    coc_df.to_csv(coc_csv, index=False)

    print("Summary:")
    print("Number of chapters", len(chapters_df))
    print("Number of characters", len(characters_df))
    print("Number of Character on Chapter", len(coc_df))


if __name__ == "__main__":
    chapters_json_file = "./data/chapters.json"
    characters_csv = "./data/characters.csv"
    chapters_csv = "./data/chapters.csv"
    coc_csv = "./data/coc.csv"

    convert_chapters_to_csv(chapters_json_file, characters_csv, chapters_csv, coc_csv)

    # Read back from CSV
    chapters_from_csv = pd.read_csv(chapters_csv, parse_dates=["date"])
    characters_from_csv = pd.read_csv(characters_csv)
    coc_from_csv = pd.read_csv(coc_csv, keep_default_na=False)

    # Check
    print(chapters_from_csv)
    print(characters_from_csv)
    print(coc_from_csv)
