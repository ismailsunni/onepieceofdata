from scrapper.chapter import scrap_chapters
from scrapper.to_csv import convert_chapters_to_csv

if __name__ == "__main__":
    last_chapter = 1132
    chapters_json_file = "./data/chapters.json"
    scrap_chapters(last_chapter, chapters_json_file)

    characters_csv = "./data/characters.csv"
    chapters_csv = "./data/chapters.csv"
    coc_csv = "./data/coc.csv"

    convert_chapters_to_csv(chapters_json_file, characters_csv, chapters_csv, coc_csv)

    print("fin")
