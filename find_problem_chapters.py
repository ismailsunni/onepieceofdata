#!/usr/bin/env python3
"""Find the problematic chapters in JSON data."""

import json

def find_chapters_with_issues():
    with open('data/chapters.json', 'r', encoding='utf-8') as f:
        chapters = json.load(f)
    
    problem_chapters = [12, 999, 1024, 1145]
    
    for chapter_data in chapters:
        chapter_num = chapter_data.get('chapter_number')
        if chapter_num:
            try:
                num = int(chapter_num)
                if num in problem_chapters:
                    print(f"\n📖 Chapter {num}:")
                    print(f"  Title: {chapter_data.get('title', 'N/A')}")
                    print(f"  Pages: '{chapter_data.get('pages', 'N/A')}'")
                    print(f"  Release Date: {chapter_data.get('release_date', 'N/A')}")
            except ValueError:
                pass

if __name__ == "__main__":
    find_chapters_with_issues()
