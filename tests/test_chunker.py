"""Tests for the wiki page chunker."""

import json
import pytest

from onepieceofdata.embeddings.chunker import (
    chunk_wiki_page,
    _MAX_CHARS,
    _OVERLAP_CHARS,
    _MIN_SECTION_CHARS,
)


class TestSingleSection:
    def test_single_section_produces_one_chunk(self):
        sections = {"intro": "Monkey D. Luffy is the main protagonist of One Piece."}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) == 1

    def test_chunk_id_format(self):
        sections = {"intro": "Luffy is a pirate."}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert chunks[0]["chunk_id"] == "Luffy::intro"

    def test_chunk_fields_are_populated(self):
        sections = {"Appearance": "Zoro has green hair and three swords."}
        chunks = chunk_wiki_page("Zoro", "character", "Roronoa Zoro", sections)
        c = chunks[0]
        assert c["page_id"] == "Zoro"
        assert c["page_type"] == "character"
        assert c["title"] == "Roronoa Zoro"
        assert c["section_name"] == "Appearance"
        assert c["text"] == "Zoro has green hair and three swords."
        assert c["chunk_id"] == "Zoro::Appearance"


class TestLongSections:
    def _long_text(self, char_count):
        """Build a text of roughly char_count characters using whole words."""
        word = "word "
        return (word * (char_count // len(word) + 1))[:char_count]

    def test_long_section_is_split(self):
        text = self._long_text(_MAX_CHARS + 500)
        sections = {"History": text}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) > 1

    def test_split_chunk_ids_have_part_suffix(self):
        text = self._long_text(_MAX_CHARS + 500)
        sections = {"History": text}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert chunks[0]["chunk_id"] == "Luffy::History::part_0"
        assert chunks[1]["chunk_id"] == "Luffy::History::part_1"

    def test_split_chunks_share_section_name(self):
        text = self._long_text(_MAX_CHARS + 500)
        sections = {"History": text}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        for chunk in chunks:
            assert chunk["section_name"] == "History"

    def test_split_chunks_overlap(self):
        """The last OVERLAP_CHARS of part_N equal the first OVERLAP_CHARS of part_N+1."""
        text = self._long_text(_MAX_CHARS * 2 + 100)
        sections = {"History": text}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) >= 2
        tail_of_first = chunks[0]["text"][-_OVERLAP_CHARS:]
        head_of_second = chunks[1]["text"][:_OVERLAP_CHARS]
        assert tail_of_first == head_of_second

    def test_normal_section_not_split(self):
        text = self._long_text(_MAX_CHARS - 10)
        sections = {"Personality": text}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) == 1
        assert chunks[0]["chunk_id"] == "Luffy::Personality"


class TestShortSectionMerging:
    def test_short_intro_merged_with_next(self):
        sections = {
            "intro": "Short.",  # < _MIN_SECTION_CHARS
            "History": "Luffy ate the Gomu Gomu no Mi as a child and became a rubber man.",
        }
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) == 1
        assert "Short." in chunks[0]["text"]
        assert "Luffy ate" in chunks[0]["text"]

    def test_short_middle_section_merged_with_following(self):
        sections = {
            # 52 chars — above _MIN_SECTION_CHARS so it stays as its own chunk
            "intro": "Nami is the navigator of the Straw Hat Pirates crew.",
            "Note": "See also.",  # short, should merge into next
            "Personality": "Nami is cunning, greedy, and deeply loyal to her friends.",
        }
        chunks = chunk_wiki_page("Nami", "character", "Nami", sections)
        # intro stays (>= 50 chars), "Note" merges into "Personality"
        assert len(chunks) == 2
        merged_chunk = chunks[1]
        assert "See also." in merged_chunk["text"]
        assert "Nami is cunning" in merged_chunk["text"]

    def test_short_last_section_kept(self):
        """A short section with no following section is kept as-is."""
        sections = {
            "History": "Luffy grew up in Foosha Village and trained under Shanks for years.",
            "Trivia": "Ok.",  # short, no next section to merge with
        }
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        trivia_chunks = [c for c in chunks if "Ok." in c["text"]]
        assert len(trivia_chunks) == 1

    def test_section_exactly_at_min_not_merged(self):
        """A section with text length == _MIN_SECTION_CHARS is not merged."""
        text = "x" * _MIN_SECTION_CHARS
        sections = {
            "intro": text,
            "History": "Some longer history that we do not want merged.",
        }
        chunks = chunk_wiki_page("X", "character", "X", sections)
        assert len(chunks) == 2


class TestChunkIdFormat:
    def test_page_id_with_underscores(self):
        sections = {"intro": "Some text about this character in the One Piece world."}
        chunks = chunk_wiki_page("Monkey_D_Luffy", "character", "Monkey D. Luffy", sections)
        assert chunks[0]["chunk_id"] == "Monkey_D_Luffy::intro"

    def test_section_name_preserved_in_chunk_id(self):
        sections = {"Powers and Abilities": "Luffy has rubber powers."}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert chunks[0]["chunk_id"] == "Luffy::Powers and Abilities"

    def test_split_part_indices_are_sequential(self):
        text = "w " * 2500  # enough for 3+ parts
        sections = {"History": text.strip()}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        for i, chunk in enumerate(chunks):
            assert chunk["chunk_id"] == f"Luffy::History::part_{i}"


class TestEdgeCases:
    def test_empty_sections_returns_empty(self):
        chunks = chunk_wiki_page("X", "character", "X", {})
        assert chunks == []

    def test_none_sections_returns_empty(self):
        chunks = chunk_wiki_page("X", "character", "X", None)
        assert chunks == []

    def test_sections_as_json_string(self):
        sections_dict = {"intro": "Nami is the navigator of the Straw Hat Pirates."}
        sections_json = json.dumps(sections_dict)
        chunks = chunk_wiki_page("Nami", "character", "Nami", sections_json)
        assert len(chunks) == 1
        assert chunks[0]["text"] == sections_dict["intro"]

    def test_invalid_json_string_returns_empty(self):
        chunks = chunk_wiki_page("X", "character", "X", "not valid json {{{")
        assert chunks == []

    def test_sections_with_empty_values_skipped(self):
        sections = {"intro": "", "History": "Luffy grew up in Foosha Village and trained for years."}
        chunks = chunk_wiki_page("Luffy", "character", "Monkey D. Luffy", sections)
        assert len(chunks) == 1
        assert chunks[0]["section_name"] == "History"

    def test_arc_page_type(self):
        sections = {"intro": "The East Blue Saga is the first saga of the One Piece series."}
        chunks = chunk_wiki_page("East_Blue_Saga", "saga", "East Blue Saga", sections)
        assert chunks[0]["page_type"] == "saga"
        assert chunks[0]["chunk_id"] == "East_Blue_Saga::intro"
