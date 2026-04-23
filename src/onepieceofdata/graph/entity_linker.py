"""Map surface strings in text to graph_nodes IDs via alias matching.

Simple whole-word case-insensitive matching. Good enough as a pre-filter
for the extraction stage; quality improvements come from curating aliases
in graph_nodes, not from smarter matching here.
"""

import re
from dataclasses import dataclass

import duckdb


@dataclass(frozen=True)
class _Alias:
    node_id: int
    alias: str
    length: int


class EntityLinker:
    """Resolves mentions of entities in free text to graph_nodes.id values."""

    def __init__(self, aliases: list[_Alias]):
        # Sort descending by length so "Straw Hat Pirates" wins over "Straw Hat".
        self._aliases = sorted(aliases, key=lambda a: -a.length)
        self._patterns = [
            (a.node_id, re.compile(rf"\b{re.escape(a.alias)}\b", re.IGNORECASE))
            for a in self._aliases
        ]

    @classmethod
    def from_db(cls, db_path: str) -> "EntityLinker":
        conn = duckdb.connect(db_path, read_only=True)
        try:
            rows = conn.execute(
                "SELECT id, aliases FROM graph_nodes WHERE aliases IS NOT NULL"
            ).fetchall()
        finally:
            conn.close()

        aliases: list[_Alias] = []
        seen: set[tuple[int, str]] = set()
        for node_id, alias_list in rows:
            for alias in alias_list or []:
                if not alias or len(alias) < 2:
                    continue
                key = (node_id, alias.lower())
                if key in seen:
                    continue
                seen.add(key)
                aliases.append(_Alias(node_id=node_id, alias=alias, length=len(alias)))
        return cls(aliases)

    def match(self, text: str) -> list[int]:
        """Return node IDs whose aliases appear in text (de-duplicated, order preserved)."""
        if not text:
            return []
        matched: list[int] = []
        seen: set[int] = set()
        for node_id, pattern in self._patterns:
            if node_id in seen:
                continue
            if pattern.search(text):
                matched.append(node_id)
                seen.add(node_id)
        return matched

    def __len__(self) -> int:
        return len(self._aliases)
