import urllib3
from bs4 import BeautifulSoup

base_url = 'https://onepiece.fandom.com/wiki/Chapter_'

def scrap_chapter(chapter):
    """Scrap chapter."""
    chapter_info = {}

    chapter_url = base_url + str(chapter)
    print(chapter_url)

    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen('GET',chapter_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, 'html.parser')
    chapter_section = soup.findAll("section", {"class": "pi-item pi-group pi-border-color"})[0] 
    items = ['vol', 'chapter', 'ename', 'page', 'date2']
    for item in items:
        try:
            chapter_info[item] = chapter_section.findAll("div", {"data-source": item})[0].findAll('div', {'class': 'pi-data-value'})[0].text 
            if '[ref]' in chapter_info[item]:
                chapter_info[item] = chapter_info[item].strip('[ref]')
        except IndexError as e:
            print(item, e)
    return chapter_info

if __name__ == "__main__":
    last_chapter = 10
    for chapter in range(1, last_chapter):
        print(scrap_chapter(chapter))