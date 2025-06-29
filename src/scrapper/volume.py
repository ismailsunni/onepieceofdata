import urllib3
from bs4 import BeautifulSoup
import json
from utils import timing_decorator

base_url = "https://onepiece.fandom.com/wiki/Chapters_and_Volumes/Volumes"


def parse_volume_table(soup: BeautifulSoup, volume_number: int):
    print(f"Parsing volume {volume_number}")
    volume_table = soup.find("table", id=f"Volume_{volume_number}")

    english_title = ""
    cover_characters = []

    if volume_table:
        rows = volume_table.findAll("tr")
        if len(rows) >= 4:
            row = rows[3]
            cells = row.findAll(["td", "th"])
            title = cells[1].get_text()
            english_title = title.strip()
        if len(rows) >= 5:
            row = rows[4]
            cells = row.findAll(["td", "th"])
            characters_cells = cells[1]
            chars_href = characters_cells.findAll("li")
            for c in chars_href:
                a_tags = c.findAll("a")
                if len(a_tags) == 0:
                    # continue
                    character_slug = c.get_text().strip()
                    print("No a tag for", c)
                else:
                    href = a_tags[0]["href"]
                    character_slug = href.split("/")[-1]
                cover_characters.append(
                    {"name": c.get_text().strip(), "slug": character_slug}
                )

    return {
        "volume_number": volume_number,
        "english_title": english_title,
        "cover_characters": cover_characters,
    }


@timing_decorator
def scrap_volumes(last_volume: int, volumes_json_path: str):
    """Scrap all volume in one go"""
    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen("GET", base_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, "html.parser")

    # Make it parallel
    all_volume = []
    for volume in range(1, last_volume + 1):
        volume_data = parse_volume_table(soup, volume)
        all_volume.append(volume_data)

    with open(volumes_json_path, "w") as f:
        json.dump(all_volume, f)


if __name__ == "__main__":
    last_volume = 111
    volumes_json_path = "./data/volumes.json"
    scrap_volumes(last_volume, volumes_json_path)
    print("fin")
