import urllib3
from bs4 import BeautifulSoup

base_url = "https://onepiece.fandom.com/wiki/Volume_"


def scrap_volume(volume):
    """Scrap volume."""
    volume_info = {}
    volume_url = base_url + str(volume)
    print(volume_url)
    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen("GET", volume_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, "html.parser")
    volume_section = soup.findAll(
        "section", {"class": "pi-item pi-group pi-border-color"}
    )[0]
    # Chapter
    chapter = (
        volume_section.findAll("div", {"data-source": "chapters"})[0]
        .findAll("div", {"class": "pi-data-value"})[0]
        .text
    )
    volume_info["chapter"] = chapter
    # Chapter
    ename = (
        volume_section.findAll("div", {"data-source": "ename"})[0]
        .findAll("div", {"class": "pi-data-value"})[0]
        .text
    )
    volume_info["ename"] = ename
    return html_page


# last_chapter = 10
# for i in range(1, last_chapter):
#     chapter_url = base_url + str(i)
#     print(chapter_url)

if __name__ == "__main__":
    scrap_volume(1)
