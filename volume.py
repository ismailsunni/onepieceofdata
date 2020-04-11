import urllib3
from bs4 import BeautifulSoup

base_url = 'https://onepiece.fandom.com/wiki/Volume_'


def scrap_volume(volume):
    """Scrap volume."""
    volume_info = {}
    volume_url = base_url + str(volume)
    print(volume_url)
    http_pool = urllib3.PoolManager()
    r = http_pool.urlopen('GET',volume_url)
    html_page = r.data
    soup = BeautifulSoup(html_page, 'html.parser')
    volume_section = soup.findAll("section", {"class": "pi-item pi-group pi-border-color"})[0] 
    items = ['chapters', 'ename', 'page']
    for item in items:
        try:
            volume_info[item] = volume_section.findAll("div", {"data-source": item})[0].findAll('div', {'class': 'pi-data-value'})[0].text 
        except IndexError as e:
            print(item, e)
    return volume_info

if __name__ == "__main__":
    last_volume = 10
    for volume in range(1, last_volume + 1):
        print(scrap_volume(volume))
