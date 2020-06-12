# Convert JSON to CSV
import pandas as pd
import json
from datetime import datetime

# Chapters
chapters_json_file = 'data/chapters.json' 
with open(chapters_json_file) as json_file:
    chapter_list = json.load(json_file)

characters = {}
character_on_chapter = {}

chapter_numbers = []
chapter_volume = []
chapter_name = []
chapter_page = []
chapter_date = []

character_ids = []
character_names = []
character_urls = []

for chapter in chapter_list:
    # Chapters
    chapter_numbers.append(int(chapter['chapter']))
    try:
        chapter_volume.append(int(chapter['vol']))
    except ValueError:
        chapter_volume.append(None)
    chapter_name.append(chapter['ename'])
    chapter_page.append(int(chapter['page']))
    chapter_date.append(datetime.strptime(chapter['date2'], "%B %d, %Y"))

    # Characters
    for character in chapter['characters']:
        if (character['url']):
            char_id = character['url'].split('/')[-1]
            if '#' in char_id:
                char_id = char_id.split('#')[-1]
        else:
            char_id = character['name'].replace(' ', '_')
        
        if char_id not in character_ids:
            character_ids.append(char_id)
            character_names.append(character['name'])
            character_urls.append(character['url'])

chapters = {
    'chapter': chapter_numbers,
    'volume': chapter_volume,
    'name': chapter_name,
    'page': chapter_page,
    'date': chapter_date
}

chapters_df = pd.DataFrame(chapters, columns = ['chapter', 'volume', 'name', 'page', 'date'])
print(chapters_df)

characters = {
    'id': character_ids,
    'name': character_names,
    'url': character_urls
}

character_df = pd.DataFrame(characters, columns = ['id', 'name', 'url'])
print(character_df)

# Write to CSV
chapters_df.to_csv('data/chapters.csv', index = False)
character_df.to_csv('data/characters.csv', index = False)

# Read back from CSV
chapters_from_csv = pd.read_csv('data/chapters.csv', parse_dates=['date']) 
characters_from_csv = pd.read_csv('data/characters.csv') 

# Check
print(chapters_from_csv)
print(characters_from_csv)