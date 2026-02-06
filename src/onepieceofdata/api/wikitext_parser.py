"""Utilities for parsing MediaWiki wikitext."""

import re
from typing import Dict, List, Optional, Any
from loguru import logger


class WikitextParser:
    """Parser for MediaWiki wikitext format.

    This class provides utilities to extract structured data from wikitext,
    particularly from templates like {{Chapter Box}} and {{Infobox Character}}.
    """

    @staticmethod
    def extract_template(wikitext: str, template_name: str) -> Optional[str]:
        """Extract a template block from wikitext.

        Args:
            wikitext: Raw wikitext content
            template_name: Name of the template (e.g., "Chapter Box")

        Returns:
            Template content (without {{ }}) or None if not found
        """
        # Handle templates case-insensitively
        pattern = r'\{\{\s*' + re.escape(template_name) + r'\s*(.*?)\}\}'
        match = re.search(pattern, wikitext, re.DOTALL | re.IGNORECASE)

        if match:
            return match.group(1).strip()

        return None

    @staticmethod
    def parse_template_params(template_content: str) -> Dict[str, str]:
        """Parse template parameters into a dictionary.

        Args:
            template_content: Content of a template (without {{ }})

        Returns:
            Dictionary mapping parameter names to values
        """
        params = {}

        # Split by | but handle nested templates
        parts = []
        current = []
        depth = 0

        for char in template_content:
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
            elif char == '|' and depth == 0:
                parts.append(''.join(current))
                current = []
                continue
            current.append(char)

        if current:
            parts.append(''.join(current))

        # Parse each part as key=value
        for part in parts:
            part = part.strip()
            if '=' in part:
                key, value = part.split('=', 1)
                params[key.strip()] = value.strip()

        return params

    @staticmethod
    def parse_chapter_box(wikitext: str) -> Optional[Dict[str, Any]]:
        """Parse {{Chapter Box}} template from wikitext.

        Args:
            wikitext: Raw wikitext content

        Returns:
            Dictionary with chapter information or None if not found
        """
        template = WikitextParser.extract_template(wikitext, "Chapter Box")
        if not template:
            logger.debug("Chapter Box template not found")
            return None

        params = WikitextParser.parse_template_params(template)

        # Extract relevant fields
        chapter_info = {}

        if 'title' in params:
            chapter_info['title'] = WikitextParser.clean_text(params['title'])

        if 'ename' in params:
            chapter_info['english_title'] = WikitextParser.clean_text(params['ename'])

        if 'jname' in params:
            chapter_info['japanese_title'] = WikitextParser.clean_text(params['jname'])

        # Additional fields that might be in infoboxes
        for key in ['vol', 'chapter', 'page', 'date2', 'jump']:
            if key in params:
                chapter_info[key] = WikitextParser.clean_text(params[key])

        return chapter_info

    @staticmethod
    def parse_character_table(wikitext: str) -> List[Dict[str, str]]:
        """Parse character tables from wikitext.

        Args:
            wikitext: Raw wikitext content

        Returns:
            List of character dictionaries with name and note
        """
        characters = []

        # Find CharTable sections
        table_pattern = r'\{\|\s*class="CharTable"(.*?)\|\}'
        matches = re.finditer(table_pattern, wikitext, re.DOTALL | re.IGNORECASE)

        for match in matches:
            table_content = match.group(1)

            # Extract character links
            # Pattern for [[Character Name]] or [[Character Name|Display Name]]
            link_pattern = r'\[\[([^\]|]+?)(?:\|([^\]]+?))?\]\]'

            for link_match in re.finditer(link_pattern, table_content):
                character_name = link_match.group(2) if link_match.group(2) else link_match.group(1)
                character_link = link_match.group(1)

                # Extract notes (text in parentheses after the link)
                note = ""
                # Look for text after this match until next list item or link
                remaining_text = table_content[link_match.end():link_match.end()+100]
                note_match = re.search(r'\(([^)]+)\)', remaining_text)
                if note_match:
                    note = note_match.group(1)

                characters.append({
                    "name": character_name.strip(),
                    "url": f"/wiki/{character_link.replace(' ', '_')}",
                    "note": note.strip(),
                })

        return characters

    @staticmethod
    def parse_infobox(wikitext: str, infobox_type: str = "Infobox") -> Optional[Dict[str, Any]]:
        """Parse an infobox template from wikitext.

        Args:
            wikitext: Raw wikitext content
            infobox_type: Type of infobox (e.g., "Infobox Character")

        Returns:
            Dictionary with infobox data or None if not found
        """
        template = WikitextParser.extract_template(wikitext, infobox_type)
        if not template:
            return None

        params = WikitextParser.parse_template_params(template)

        # Clean all values
        return {k: WikitextParser.clean_text(v) for k, v in params.items()}

    @staticmethod
    def clean_text(text: str) -> str:
        """Clean wikitext formatting from text.

        Removes:
        - Wiki links: [[Link|Display]] -> Display
        - Templates: {{Template}} -> ""
        - Formatting: '''bold''' -> bold
        - HTML comments
        - References: <ref>...</ref>

        Args:
            text: Raw wikitext

        Returns:
            Cleaned plain text
        """
        if not text:
            return ""

        # Remove HTML comments
        text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

        # Remove references
        text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<ref[^>]*?/>', '', text, flags=re.IGNORECASE)

        # Remove simple templates like {{Ruby|...}}
        # This is a simplified approach; full template parsing is complex
        text = re.sub(r'\{\{[^}]+\}\}', '', text)

        # Convert wiki links [[Target|Display]] to Display
        text = re.sub(r'\[\[(?:[^|\]]+\|)?([^\]]+)\]\]', r'\1', text)

        # Remove formatting
        text = re.sub(r"'''([^']+)'''", r'\1', text)  # Bold
        text = re.sub(r"''([^']+)''", r'\1', text)    # Italic

        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)

        return text.strip()

    @staticmethod
    def extract_chapter_number_from_title(title: str) -> Optional[int]:
        """Extract chapter number from page title.

        Args:
            title: Page title (e.g., "Chapter 1", "Chapter 1165")

        Returns:
            Chapter number or None if not found
        """
        match = re.search(r'Chapter\s+(\d+)', title, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def extract_volume_number_from_title(title: str) -> Optional[int]:
        """Extract volume number from page title.

        Args:
            title: Page title (e.g., "Volume 1", "Volume 113")

        Returns:
            Volume number or None if not found
        """
        match = re.search(r'Volume\s+(\d+)', title, re.IGNORECASE)
        if match:
            return int(match.group(1))
        return None
