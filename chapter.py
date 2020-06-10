import urllib3
from bs4 import BeautifulSoup
from pprint import pprint

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
    # Characters
    characters = []
    character_table = soup.findAll('table', {"class": 'CharTable'})[0]
    char_items = character_table.findAll('li')
    for char_item in char_items:
        if char_item.findAll('a'):
            pass
            char_name = char_item.findAll('a')[0].text
            char_url = char_item.findAll('a')[0]['href']
            print(char_item.text)
            full_text = char_item.text
            note = ''
            if '(' in full_text and ')' in full_text:
                note = full_text[full_text.find("(")+1:full_text.find(")")]
            characters.append({
                'name': char_name,
                'url': char_url,
                'note': note,
                'full_text': full_text
            })
        else:
            print('No URL', char_item)
        # print(char_item)

    chapter_info['characters'] = characters
    return chapter_info

if __name__ == "__main__":
    chapters = []
    last_chapter = 2
    for chapter in range(1, last_chapter + 1):
        print(chapter)
        result = scrap_chapter(chapter)
        pprint(result)