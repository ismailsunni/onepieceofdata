import urllib3
from bs4 import BeautifulSoup
import json
import multiprocessing

from utils import timing_decorator

base_url = "https://onepiece.fandom.com/wiki/Chapter_"


def scrap_chapter(chapter):
    """Scrap chapter."""
    print(f"Scrap chapter {chapter}")
    chapter_info = {}

    chapter_url = base_url + str(chapter)

    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen("GET", chapter_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, "html.parser")
    chapter_section = soup.findAll(
        "section", {"class": "pi-item pi-group pi-border-color"}
    )[0]
    items = ["vol", "chapter", "ename", "page", "date2", "jump"]
    for item in items:
        try:
            chapter_info[item] = (
                chapter_section.findAll("div", {"data-source": item})[0]
                .findAll("div", {"class": "pi-data-value"})[0]
                .text
            )
            if "[ref]" in chapter_info[item]:
                chapter_info[item] = chapter_info[item].strip("[ref]")
        except IndexError as e:
            print(item, e)
    # Characters
    characters = []
    character_table = soup.findAll("table", {"class": "CharTable"})[0]
    char_items = character_table.findAll("li")
    for char_item in char_items:
        full_text = char_item.text.rstrip("\n")
        note = ""
        if "(" in full_text and ")" in full_text:
            note = full_text[full_text.find("(") + 1 : full_text.find(")")]
        if char_item.findAll("a"):
            char_name = char_item.findAll("a")[0].text
            char_url = char_item.findAll("a")[0]["href"]
        else:
            print("No URL", char_item)
            char_name = full_text
            char_url = ""
        characters.append(
            {"name": char_name, "url": char_url, "note": note, "full_text": full_text}
        )
        if note:
            print("Note for %s: %s" % (char_name, note))

    chapter_info["characters"] = characters
    return chapter_info


@timing_decorator
def scrap_all_chapter(last_chapter):
    chapters = []
    for chapter in range(1, last_chapter + 1):
        # print("chapter", chapter)
        try:
            result = scrap_chapter(chapter)
            chapters.append(result)
        except Exception as e:
            print(e)

        if chapter % 100 == 0:
            with open("./cache/chapters_{}.json".format(chapter), "w") as fp:
                json.dump(chapters, fp)
    with open("./data/chapters.json", "w") as fp:
        json.dump(chapters, fp, indent=2)


@timing_decorator
def scrap_chapter_parallel(last_chapter):
    list_of_chapter = range(1, last_chapter + 1)

    # Number of processes to use
    num_processes = multiprocessing.cpu_count() - 1

    # Process rows in parallel using multiprocessing.Pool
    with multiprocessing.Pool(num_processes) as pool:
        chapters = pool.map(scrap_chapter, list_of_chapter)

    print(f"Success to scrap {len(chapters)} chapters")

    with open("./data/chapter_parallel.json", "w") as fp:
        json.dump(chapters, fp, indent=2)


if __name__ == "__main__":
    last_chapter = 1132
    # scrap_all_chapter(last_chapter)
    # print("-------------------")
    scrap_chapter_parallel(last_chapter)
