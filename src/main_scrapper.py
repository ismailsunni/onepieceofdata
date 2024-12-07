import configs
from scrapper.chapter import scrap_chapters
from scrapper.to_csv import convert_chapters_to_csv
from scrapper.volume import scrap_volumes
from scrapper.character import scrap_characters

if __name__ == "__main__":
    print("###### Start scraping data ######")
    # Scrap all chapters and volumes
    scrap_chapters(configs.last_chapter, configs.chapters_json_path)
    convert_chapters_to_csv(
        configs.chapters_json_path,
        configs.characters_csv,
        configs.chapters_csv,
        configs.coc_csv,
    )
    scrap_volumes(configs.last_volume, configs.volumes_json_path)
    scrap_characters(configs.characters_csv, configs.characters_json_path)

    print("Finish scrapping")
