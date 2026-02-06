"""Tests for WikitextParser."""

import pytest
from onepieceofdata.api import WikitextParser


class TestWikitextParser:
    """Test suite for WikitextParser."""

    def test_extract_template_chapter_box(self):
        """Test extracting Chapter Box template."""
        wikitext = """
{{Chapter Box
| title = Test Chapter
| ename = English Name
}}

Some other content.
"""
        result = WikitextParser.extract_template(wikitext, "Chapter Box")

        assert result is not None
        assert "title" in result
        assert "ename" in result

    def test_extract_template_not_found(self):
        """Test template extraction when template doesn't exist."""
        wikitext = "Just some regular text without templates."
        result = WikitextParser.extract_template(wikitext, "Chapter Box")

        assert result is None

    def test_parse_template_params(self):
        """Test parsing template parameters."""
        template_content = """
| title = Romance Dawn
| ename = English Title
| vol = 1
"""
        result = WikitextParser.parse_template_params(template_content)

        assert "title" in result
        assert result["title"] == "Romance Dawn"
        assert result["ename"] == "English Title"
        assert result["vol"] == "1"

    def test_parse_chapter_box(self):
        """Test parsing complete Chapter Box."""
        wikitext = """
{{Chapter Box
| title = Romance Dawn —The Dawn of the Adventure—
| jname = ロマンスドーン
| ename = Romance Dawn
}}
"""
        result = WikitextParser.parse_chapter_box(wikitext)

        assert result is not None
        assert "title" in result
        assert "Romance Dawn" in result["title"]
        assert "english_title" in result or "ename" in result

    def test_parse_chapter_box_not_found(self):
        """Test parsing when Chapter Box doesn't exist."""
        wikitext = "No chapter box here."
        result = WikitextParser.parse_chapter_box(wikitext)

        assert result is None

    def test_parse_character_table(self):
        """Test parsing character tables."""
        wikitext = """
{| class="CharTable"
! Characters
|-
|
*[[Monkey D. Luffy]]
*[[Roronoa Zoro]]
*[[Nami]]
|}
"""
        result = WikitextParser.parse_character_table(wikitext)

        assert isinstance(result, list)
        assert len(result) >= 3

        # Check that characters were extracted
        char_names = [c["name"] for c in result]
        assert "Monkey D. Luffy" in char_names or "Luffy" in str(char_names)

    def test_clean_text_basic(self):
        """Test basic text cleaning."""
        text = "'''Bold text''' and ''italic text''"
        result = WikitextParser.clean_text(text)

        assert result == "Bold text and italic text"

    def test_clean_text_wiki_links(self):
        """Test cleaning wiki links."""
        text = "[[Monkey D. Luffy|Luffy]] is the [[Pirate|pirate]]"
        result = WikitextParser.clean_text(text)

        assert "[[" not in result
        assert "]]" not in result
        assert "Luffy" in result
        assert "pirate" in result

    def test_clean_text_references(self):
        """Test removing references."""
        text = "Some text<ref>Reference content</ref> more text"
        result = WikitextParser.clean_text(text)

        assert "<ref>" not in result
        assert "Reference content" not in result
        assert "Some text" in result
        assert "more text" in result

    def test_clean_text_html_comments(self):
        """Test removing HTML comments."""
        text = "Visible text<!-- Hidden comment --> more visible"
        result = WikitextParser.clean_text(text)

        assert "<!--" not in result
        assert "Hidden comment" not in result
        assert "Visible text" in result

    def test_extract_chapter_number_from_title(self):
        """Test extracting chapter number from title."""
        assert WikitextParser.extract_chapter_number_from_title("Chapter 1") == 1
        assert WikitextParser.extract_chapter_number_from_title("Chapter 123") == 123
        assert WikitextParser.extract_chapter_number_from_title("Chapter 1165") == 1165
        assert WikitextParser.extract_chapter_number_from_title("Not a chapter") is None

    def test_extract_volume_number_from_title(self):
        """Test extracting volume number from title."""
        assert WikitextParser.extract_volume_number_from_title("Volume 1") == 1
        assert WikitextParser.extract_volume_number_from_title("Volume 113") == 113
        assert WikitextParser.extract_volume_number_from_title("Not a volume") is None

    def test_parse_infobox(self):
        """Test parsing generic infobox."""
        wikitext = """
{{Infobox Character
| name = Luffy
| age = 19
| occupation = Pirate
}}
"""
        result = WikitextParser.parse_infobox(wikitext, "Infobox Character")

        assert result is not None
        assert "name" in result
        assert "age" in result
        assert result["name"] == "Luffy"


class TestWikitextParserIntegration:
    """Integration tests with real wikitext examples."""

    @pytest.mark.integration
    def test_parse_real_chapter_1(self):
        """Test parsing actual Chapter 1 wikitext."""
        from onepieceofdata.api import FandomAPIClient

        client = FandomAPIClient(wiki="onepiece")
        wikitext = client.get_page_wikitext("Chapter 1")

        if wikitext:
            chapter_info = WikitextParser.parse_chapter_box(wikitext)
            assert chapter_info is not None
            assert "title" in chapter_info

            characters = WikitextParser.parse_character_table(wikitext)
            assert isinstance(characters, list)
            assert len(characters) > 0
