from scrapper.chapter import scrap_chapters
from scrapper.to_csv import convert_chapters_to_csv
from scrapper.volume import scrap_volumes

if __name__ == "__main__":
    # Last chapter and volume
    last_chapter = 1132
    last_volume = 110

    # JSON files
    chapters_json_path = "./data/chapters.json"
    volumes_json_path = "./data/volumes.json"

    # CSV files
    characters_csv = "./data/characters.csv"
    chapters_csv = "./data/chapters.csv"
    coc_csv = "./data/coc.csv"

    # Scrap all chapters and volumes
    scrap_chapters(last_chapter, chapters_json_path)
    convert_chapters_to_csv(chapters_json_path, characters_csv, chapters_csv, coc_csv)
    scrap_volumes(last_volume, volumes_json_path)

    print("fin")
