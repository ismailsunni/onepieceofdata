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
for chapter in chapter_list:
    chapter_numbers.append(int(chapter['chapter']))
    try:
        chapter_volume.append(int(chapter['vol']))
    except ValueError:
        chapter_volume.append(None)
    chapter_name.append(chapter['ename'])
    chapter_page.append(int(chapter['page']))
    chapter_date.append(datetime.strptime(chapter['date2'], "%B %d, %Y"))

chapters = {
    'chapter': chapter_numbers,
    'volume': chapter_volume,
    'name': chapter_name,
    'page': chapter_page,
    'date': chapter_date
}

chapters_df = pd.DataFrame(chapters, columns = ['chapter', 'volume', 'name', 'page', 'date'])
print(chapters_df)

# Write to CSV
chapters_df.to_csv('data/chapters.csv', index = False)

# Read back from CSV
csv_pd = pd.read_csv('data/chapters.csv', parse_dates=['date']) 

# Check
print(csv_pd)
print(chapters_df.dtypes)
print(csv_pd.dtypes)