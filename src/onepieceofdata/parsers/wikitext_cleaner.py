"""Clean raw wikitext into structured plaintext with sections."""

import re
from typing import Dict, Tuple


def _remove_templates(text: str) -> str:
    """Remove nested {{templates}} using a depth counter."""
    result = []
    depth = 0
    i = 0
    while i < len(text):
        if text[i:i+2] == '{{':
            depth += 1
            i += 2
        elif text[i:i+2] == '}}':
            if depth > 0:
                depth -= 1
            else:
                # Unmatched closing brace — keep it
                result.append('}}')
            i += 2
        else:
            if depth == 0:
                result.append(text[i])
            i += 1
    return ''.join(result)


def _remove_tables(text: str) -> str:
    """Remove {| ... |} wiki table markup."""
    result = []
    depth = 0
    i = 0
    while i < len(text):
        if text[i:i+2] == '{|':
            depth += 1
            i += 2
        elif text[i:i+2] == '|}':
            if depth > 0:
                depth -= 1
            i += 2
        else:
            if depth == 0:
                result.append(text[i])
            i += 1
    return ''.join(result)


def clean_wikitext(raw: str) -> str:
    """Convert raw wikitext to clean plaintext.

    Steps:
    1. Remove HTML comments
    2. Remove <ref>...</ref> and <ref.../>
    3. Remove category/file/image links
    4. Remove nested {{templates}}
    5. Remove {| ... |} table markup
    6. Convert [[link|display]] → display, [[link]] → link
    7. Remove wiki formatting (bold/italic)
    8. Remove remaining HTML tags
    9. Collapse whitespace
    """
    if not raw:
        return ""

    # 1. HTML comments
    text = re.sub(r'<!--.*?-->', '', raw, flags=re.DOTALL)

    # 2. Ref tags
    text = re.sub(r'<ref[^>]*>.*?</ref>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<ref[^>]*/>', '', text, flags=re.IGNORECASE)

    # 3. Category / File / Image links (remove entirely)
    text = re.sub(r'\[\[\s*(?:Category|File|Image)\s*:[^\]]*\]\]', '', text, flags=re.IGNORECASE)

    # 4. Remove nested {{...}} templates
    text = _remove_templates(text)

    # 5. Remove wiki table markup {| ... |}
    text = _remove_tables(text)

    # 6. Convert wiki links
    # [[Target|Display]] → Display
    text = re.sub(r'\[\[(?:[^|\]]+\|)([^\]]+)\]\]', r'\1', text)
    # [[Target]] → Target
    text = re.sub(r'\[\[([^\]]+)\]\]', r'\1', text)

    # 7. Wiki formatting
    text = re.sub(r"'{3}([^']+)'{3}", r'\1', text)   # '''bold'''
    text = re.sub(r"'{2}([^']+)'{2}", r'\1', text)   # ''italic''

    # 8. HTML tags (strip tags, keep content)
    text = re.sub(r'<[^>]+>', ' ', text)

    # 9. Collapse whitespace — preserve single newlines between paragraphs
    # First collapse multiple blank lines to two newlines
    text = re.sub(r'\n{3,}', '\n\n', text)
    # Collapse spaces/tabs on a single line
    text = re.sub(r'[ \t]+', ' ', text)
    # Strip leading/trailing space on each line
    text = '\n'.join(line.strip() for line in text.split('\n'))
    # Remove leading/trailing blank lines
    text = text.strip()

    return text


def extract_sections(raw: str) -> Dict[str, str]:
    """Split wikitext into {section_name: clean_text} dict.

    Handles == Level 2 == and === Level 3 === headers.
    First section (before any header) is keyed as "intro".
    Each section's text is cleaned via clean_wikitext().
    """
    sections: Dict[str, str] = {}

    # Split on level-2 or level-3 headers
    header_pattern = re.compile(r'^={2,3}\s*(.+?)\s*={2,3}\s*$', re.MULTILINE)

    parts = header_pattern.split(raw)
    # parts alternates: [pre-header-text, header1, section1-text, header2, section2-text, ...]

    # First element is intro text
    intro_text = clean_wikitext(parts[0])
    sections['intro'] = intro_text

    # Remaining parts come in pairs: (header_name, section_text)
    i = 1
    while i + 1 < len(parts):
        header_name = parts[i].strip()
        section_text = clean_wikitext(parts[i + 1])
        # If duplicate header names, append an index
        if header_name in sections:
            idx = 2
            while f"{header_name}_{idx}" in sections:
                idx += 1
            header_name = f"{header_name}_{idx}"
        sections[header_name] = section_text
        i += 2

    return sections


def parse_wiki_page(raw_wikitext: str) -> Tuple[str, str, Dict[str, str]]:
    """Parse a full wiki page.

    Returns: (intro_text, full_text, sections_dict)
    - intro_text: clean text of the first section (before any headers)
    - full_text: all sections concatenated with newlines
    - sections_dict: {"intro": "...", "Appearance": "...", ...}
    """
    sections = extract_sections(raw_wikitext)

    intro_text = sections.get('intro', '')

    # Concatenate all section texts in order
    parts = []
    for section_name, section_text in sections.items():
        if section_text:
            if section_name != 'intro':
                parts.append(f"{section_name}\n{section_text}")
            else:
                parts.append(section_text)
    full_text = '\n\n'.join(parts)

    return intro_text, full_text, sections
