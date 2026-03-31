"""Tests for the wikitext_cleaner module."""

import pytest
from onepieceofdata.parsers.wikitext_cleaner import (
    clean_wikitext,
    extract_sections,
    parse_wiki_page,
    _remove_templates,
)


# ---------------------------------------------------------------------------
# _remove_templates
# ---------------------------------------------------------------------------

class TestRemoveTemplates:
    def test_simple_template(self):
        assert _remove_templates("Hello {{Bold|world}} there") == "Hello  there"

    def test_nested_templates(self):
        assert _remove_templates("{{A|{{B|C}}}}") == ""

    def test_deeply_nested(self):
        assert _remove_templates("{{A|{{B|{{C|D}}}}}}") == ""

    def test_no_template(self):
        assert _remove_templates("plain text") == "plain text"

    def test_multiple_templates(self):
        result = _remove_templates("a {{T1}} b {{T2|x}} c")
        assert result == "a  b  c"

    def test_template_in_middle_of_text(self):
        result = _remove_templates("start {{Ruby|海賊|かいぞく}} end")
        assert result == "start  end"


# ---------------------------------------------------------------------------
# clean_wikitext
# ---------------------------------------------------------------------------

class TestCleanWikitext:
    def test_simple_link(self):
        assert clean_wikitext("[[Luffy]]") == "Luffy"

    def test_piped_link(self):
        assert clean_wikitext("[[Monkey D. Luffy|Luffy]]") == "Luffy"

    def test_multiple_links(self):
        result = clean_wikitext("[[Luffy]] and [[Zoro]]")
        assert result == "Luffy and Zoro"

    def test_bold_formatting(self):
        assert clean_wikitext("'''bold text'''") == "bold text"

    def test_italic_formatting(self):
        assert clean_wikitext("''italic text''") == "italic text"

    def test_ref_tag_removal(self):
        result = clean_wikitext("text<ref>footnote</ref> more")
        assert "footnote" not in result
        assert "text" in result
        assert "more" in result

    def test_self_closing_ref(self):
        result = clean_wikitext("text<ref name=\"src\"/> more")
        assert "<ref" not in result

    def test_html_comment_removal(self):
        result = clean_wikitext("text<!-- hidden comment --> visible")
        assert "hidden comment" not in result
        assert "visible" in result

    def test_category_link_removal(self):
        result = clean_wikitext("text [[Category:Characters]] more")
        assert "Category" not in result
        assert "text" in result

    def test_file_link_removal(self):
        result = clean_wikitext("text [[File:Luffy.png|thumb]] more")
        assert "File:" not in result

    def test_image_link_removal(self):
        result = clean_wikitext("text [[Image:Zoro.png]] more")
        assert "Image:" not in result

    def test_template_removal(self):
        result = clean_wikitext("text {{Ruby|海賊|かいぞく}} more")
        assert "{{" not in result
        assert "text" in result

    def test_nested_template_removal(self):
        result = clean_wikitext("text {{A|{{B|nested}}}} more")
        assert "{{" not in result
        assert "}}" not in result

    def test_html_tags(self):
        result = clean_wikitext("text<small>small</small> more")
        assert "<small>" not in result

    def test_empty_string(self):
        assert clean_wikitext("") == ""

    def test_whitespace_collapse(self):
        result = clean_wikitext("word1    word2\t\tword3")
        assert "  " not in result

    def test_table_removal(self):
        table = "{|\n| cell1 || cell2\n|-\n| cell3\n|}"
        result = clean_wikitext(table)
        assert "{|" not in result
        assert "|}" not in result

    def test_piped_link_display_over_target(self):
        # Display text (after |) should be used, not the target
        result = clean_wikitext("[[Roronoa Zoro|Zoro]]")
        assert result == "Zoro"
        assert "Roronoa" not in result


# ---------------------------------------------------------------------------
# extract_sections
# ---------------------------------------------------------------------------

SAMPLE_WIKITEXT = """\
This is the intro text about Luffy.
He is the main character.

== Appearance ==
Luffy wears a [[straw hat]].
He has a [[scar]] under his eye.

== Personality ==
Luffy is cheerful and loves [[meat]].

=== Fighting Style ===
He uses [[Gomu Gomu no Mi|Devil Fruit]] powers.

== History ==
Luffy grew up in [[Foosha Village]].
"""


class TestExtractSections:
    def test_intro_key_exists(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        assert "intro" in sections

    def test_intro_content(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        assert "Luffy" in sections["intro"]
        assert "main character" in sections["intro"]

    def test_level2_sections(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        assert "Appearance" in sections
        assert "Personality" in sections
        assert "History" in sections

    def test_level3_sections(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        assert "Fighting Style" in sections

    def test_section_content_cleaned(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        # Links should be resolved
        assert "straw hat" in sections["Appearance"]
        assert "[[" not in sections["Appearance"]

    def test_links_resolved_in_sections(self):
        sections = extract_sections(SAMPLE_WIKITEXT)
        assert "Devil Fruit" in sections["Fighting Style"]
        assert "Gomu Gomu no Mi" not in sections["Fighting Style"]

    def test_no_header_wikitext(self):
        sections = extract_sections("Just some plain text.")
        assert "intro" in sections
        assert len(sections) == 1

    def test_duplicate_section_names(self):
        wikitext = "== Notes ==\nfirst\n== Notes ==\nsecond"
        sections = extract_sections(wikitext)
        assert "Notes" in sections
        assert "Notes_2" in sections


# ---------------------------------------------------------------------------
# parse_wiki_page
# ---------------------------------------------------------------------------

class TestParseWikiPage:
    def test_returns_three_values(self):
        result = parse_wiki_page(SAMPLE_WIKITEXT)
        assert len(result) == 3

    def test_intro_text(self):
        intro_text, _, _ = parse_wiki_page(SAMPLE_WIKITEXT)
        assert "Luffy" in intro_text
        assert "main character" in intro_text

    def test_full_text_contains_all_sections(self):
        _, full_text, _ = parse_wiki_page(SAMPLE_WIKITEXT)
        assert "straw hat" in full_text
        assert "cheerful" in full_text
        assert "Foosha Village" in full_text

    def test_sections_dict(self):
        _, _, sections = parse_wiki_page(SAMPLE_WIKITEXT)
        assert "intro" in sections
        assert "Appearance" in sections
        assert "Personality" in sections
        assert "History" in sections

    def test_no_raw_wikitext_in_output(self):
        _, full_text, _ = parse_wiki_page(SAMPLE_WIKITEXT)
        assert "[[" not in full_text
        assert "]]" not in full_text
        assert "{{" not in full_text

    def test_empty_input(self):
        intro_text, full_text, sections = parse_wiki_page("")
        assert intro_text == ""
        assert "intro" in sections

    def test_complex_nested_templates(self):
        wikitext = "Luffy ate the {{color|red|{{w|Gomu Gomu no Mi}}}} fruit."
        intro_text, _, _ = parse_wiki_page(wikitext)
        assert "{{" not in intro_text
        assert "}}" not in intro_text
        assert "Luffy" in intro_text
