"""Post-processor to parse character occupations into structured data.

Parses the raw ``occupation`` string from ``characters_detail.json`` (which
has the same grammar as ``affiliation`` — semicolon-delimited entries with
optional parenthetical status qualifiers) into a ``character_occupation``
table.

Differences vs. :mod:`parse_affiliations`:

* Occupations are flat roles (e.g. "Fleet Admiral", "Pirate Captain"), with
  no meaningful sub-group hierarchy, so the output schema is simpler:
  ``(character_id, role, status)``.
* A pre-cleaning pass strips Japanese/romaji/VIZ footnote parentheticals
  that leak into occupation for the Gorosei (e.g.
  ``"Warrior God of Science (科学防衛武神, Kagaku Bōei Bushin ?, VIZ: 'Godhead of…')"``).
* A small manual-override table handles three source-scrape cases where
  individual roles were concatenated without a delimiter (``Z``, ``Camie``,
  ``Rebecca``). Fix those at the scraper layer eventually and the override
  becomes a no-op.

Examples of raw input:
  ``"Fleet Admiral ; Admiral (former); Vice Admiral (former)"``
  ``"Pirate Captain ; Emperor ; Figurehead President of Cross Guild; ..."``
  ``"Pirate Captain (acting, former); Battle Convoy leader (former); ..."``
"""

from __future__ import annotations

import json
import re
from typing import Callable, Optional

import duckdb
from loguru import logger


# Status qualifiers recognized inside parens. Anything not in this set is
# kept as-is in a free-form "note" (we currently drop it — roles with odd
# qualifiers like Brook's "(acting, former)" keep only the recognized piece).
RELATIONSHIP_STATUSES: set[str] = {
    "former", "formerly", "current",
    "acting", "promoted",
    "temporary", "temporarily",
    "disbanded", "defected", "revoked", "resigned",
    "retired", "semi-retired",
    "secret", "double agent", "undercover", "clandestine",
    "ruse", "espionage",
    "post mortem", "descended", "illegitimate",
    "dissolved", "unknown status",
    # Non-canon markers
    "filler", "movie", "anime only", "non-canon",
}


# Source-scrape fixes: raw occupation field contained multiple roles crushed
# together without a delimiter. These overrides replace the raw string with a
# properly delimited version before parsing. Confirmed manually against the
# wiki (2026-04).
MANUAL_OVERRIDES: dict[str, str] = {
    "Z": "Neo Marines Supreme Commander ; Marine Instructor (former) ; Marine Admiral (former)",
    "Camie": "Fashion designing apprentice ; Takoyaki seller ; Waitress at Mermaid Cafe",
    # Rebecca already works via the comma-fallback splitter, but normalize to
    # semicolons for consistency.
    "Rebecca": "Lady-in-waiting ; Gladiator (former) ; Princess Of Dressrosa (former)",
    # Film Gold villain — source has space-concatenated roles, no delimiter.
    "Gild_Tesoro": "Proprietor (former) ; Pirate (former) ; Slave (former)",
    # Vinsmoke Family advisor — source uses ", " instead of ";" without status
    # qualifiers so the role-list splitter doesn't trigger.
    "Vito": "Pirate ; Advisor",
}


# Matches a parenthetical group that looks like a Japanese/romaji footnote
# rather than a status qualifier. Examples this strips (but _parse_single
# keeps real status parens like "(former)"):
#   ( 科学防衛武神 , Kagaku Bōei Bushin ? , VIZ: "Godhead of Science & Defense" )
#   ( 農務武神 , Nōmu Bushin ? , English versions: "Godhead of Agriculture" )
_CJK_RE = re.compile(r"[　-鿿＀-￯]")  # CJK + full-width
_FOOTNOTE_MARKERS = ("viz:", "english versions:", "english version:", "4kids:")


def _strip_footnote_parens(raw: str) -> str:
    """Remove parenthetical groups that contain CJK characters or footnote
    markers like "VIZ:" / "English versions:" — these are translations, not
    status qualifiers.
    """
    out: list[str] = []
    i = 0
    while i < len(raw):
        ch = raw[i]
        if ch == "(":
            # Find the matching close paren, respecting nesting.
            depth = 1
            j = i + 1
            while j < len(raw) and depth > 0:
                if raw[j] == "(":
                    depth += 1
                elif raw[j] == ")":
                    depth -= 1
                j += 1
            inner = raw[i + 1 : j - 1] if depth == 0 else raw[i + 1 :]
            lower = inner.lower()
            is_footnote = (
                _CJK_RE.search(inner) is not None
                or any(m in lower for m in _FOOTNOTE_MARKERS)
            )
            if is_footnote:
                i = j
                continue
            # Keep this paren group verbatim.
            out.append(raw[i:j])
            i = j
        else:
            out.append(ch)
            i += 1
    cleaned = "".join(out)
    # Collapse whitespace introduced by stripping.
    return re.sub(r"\s{2,}", " ", cleaned).strip()


def _split_entries(raw: str) -> list[str]:
    """Split occupation list into entries.

    Primary delimiter: ``;``. Falls back to top-level ``,`` when no
    semicolons are present (Rebecca's format). Empty parts are dropped
    (handles trailing ``;`` cases like Shanks, Alvida).
    """
    if ";" in raw:
        return [p.strip() for p in raw.split(";") if p.strip()]

    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in raw:
        if ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            part = "".join(current).strip()
            if part:
                parts.append(part)
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)
    return parts


def _pick_primary_status(statuses: list[str]) -> str:
    """Pick the most significant status for an occupation entry.

    Priority favors clearly-terminated states (former, defected, resigned)
    over ongoing-but-qualified ones (acting, temporary, undercover), and
    defaults to ``current`` when no recognized qualifier is present.
    """
    priority = [
        "defected", "former", "resigned", "retired", "disbanded", "dissolved",
        "revoked", "post mortem",
        "acting", "promoted", "temporary",
        "secret", "double agent", "undercover", "clandestine",
        "ruse", "espionage", "semi-retired",
        "descended", "illegitimate", "unknown status",
        "filler", "movie", "anime only", "non-canon",
    ]
    for p in priority:
        if p in statuses:
            return p
    return "current"


def _trim_trailing(raw: str) -> str:
    """Strip trailing ``?``, ``≠`` and whitespace that leak in from source."""
    return raw.strip().rstrip("?≠").strip()


def _extract_trailing_parens(raw: str) -> tuple[str, list[str]]:
    """Peel any number of trailing ``(...)`` groups off ``raw``.

    Returns ``(role, [paren_content, ...])`` with paren contents in the order
    they appeared. Handles double-wrapped cases like
    ``"Marine Commander (former) (Non-Canon)"`` which collapse to
    ``("Marine Commander", ["former", "Non-Canon"])``.
    """
    parens: list[str] = []
    s = _trim_trailing(raw)
    while s.endswith(")"):
        # Find matching "(" for this last ")".
        depth = 0
        open_idx = -1
        for i in range(len(s) - 1, -1, -1):
            if s[i] == ")":
                depth += 1
            elif s[i] == "(":
                depth -= 1
                if depth == 0:
                    open_idx = i
                    break
        if open_idx < 0:
            break  # unbalanced — bail out, keep as-is
        parens.insert(0, s[open_idx + 1 : -1].strip())
        s = _trim_trailing(s[:open_idx])
    return s, parens


def _split_inner_role_list(raw: str) -> list[str]:
    """Split a single ``;``-entry that contains multiple ``role (status)`` items
    separated by commas (e.g. Jango, Figarland_Garling). If the pattern
    doesn't look like multiple role-status pairs, return ``[raw]``.
    """
    # Must have at least one ", " separator AND at least one "(...)" group,
    # otherwise it's a plain role that happens to contain commas.
    if "," not in raw or "(" not in raw:
        return [raw]

    # Split on top-level commas (outside parens).
    parts: list[str] = []
    current: list[str] = []
    depth = 0
    for ch in raw:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    tail = "".join(current).strip()
    if tail:
        parts.append(tail)

    # Only treat as a role-list if most parts look like "<words> (<status>)".
    if len(parts) < 2:
        return [raw]
    status_re = re.compile(r"\(([^)]+)\)\s*$")
    role_like = sum(1 for p in parts if status_re.search(p))
    if role_like < len(parts) - 1:
        # Not a role-list pattern — keep original.
        return [raw]
    return [p for p in parts if p]


def _parse_single_occupation(raw: str) -> Optional[dict]:
    """Parse one entry into ``{role, status}`` or ``None`` if empty."""
    raw = _trim_trailing(raw)
    if not raw:
        return None

    role, paren_groups = _extract_trailing_parens(raw)
    role = role.strip()
    if not role:
        return None

    # Flatten all paren contents, then keep only recognized statuses.
    statuses: list[str] = []
    for grp in paren_groups:
        for piece in grp.split(","):
            s = piece.strip().lower()
            if s == "formerly":
                s = "former"
            elif s == "temporarily":
                s = "temporary"
            if s in RELATIONSHIP_STATUSES:
                statuses.append(s)

    return {"role": role, "status": _pick_primary_status(statuses)}


def _load_alias_mapping(alias_path: str = "data/character_aliases.json") -> dict:
    try:
        with open(alias_path) as f:
            return json.load(f)
    except FileNotFoundError:
        logger.warning(f"Alias file not found: {alias_path}, skipping deduplication")
        return {}


def parse_occupations(
    db_path: str,
    *,
    characters_json: str = "data/characters_detail.json",
    alias_file: str = "data/character_aliases.json",
    dry_run: bool = False,
    progress_callback: Optional[Callable] = None,
) -> dict:
    """Parse character occupations and save to ``character_occupation`` table.

    Args:
        db_path: Path to the DuckDB database file.
        characters_json: Path to ``characters_detail.json``.
        alias_file: Path to character alias mapping JSON file.
        dry_run: If True, parse but do not write.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Dict with entry/role/character counts and cleanup stats.
    """
    with open(characters_json) as f:
        characters = json.load(f)

    alias_map = _load_alias_mapping(alias_file)

    conn = duckdb.connect(str(db_path), read_only=True)
    valid_ids = {r[0] for r in conn.execute("SELECT id FROM character").fetchall()}
    conn.close()

    merged_count = 0
    skipped_invalid = 0
    footnote_stripped = 0
    overridden = 0

    rows: list[tuple[str, str, str]] = []
    roles: set[str] = set()
    characters_with_occ: set[str] = set()

    for char in characters:
        cid = char.get("id")
        occ_raw = char.get("occupation")
        if not cid or not isinstance(occ_raw, str) or not occ_raw.strip():
            continue

        if cid in alias_map:
            cid = alias_map[cid]
            merged_count += 1

        if cid not in valid_ids:
            skipped_invalid += 1
            continue

        if cid in MANUAL_OVERRIDES:
            occ_raw = MANUAL_OVERRIDES[cid]
            overridden += 1
        else:
            cleaned = _strip_footnote_parens(occ_raw)
            if cleaned != occ_raw:
                footnote_stripped += 1
                occ_raw = cleaned

        for part in _split_entries(occ_raw):
            for sub in _split_inner_role_list(part):
                parsed = _parse_single_occupation(sub)
                if not parsed:
                    continue
                rows.append((cid, parsed["role"], parsed["status"]))
                roles.add(parsed["role"])
                characters_with_occ.add(cid)

    # Deduplicate on (character_id, role) — keep first seen.
    seen: set[tuple[str, str]] = set()
    deduped: list[tuple[str, str, str]] = []
    for row in rows:
        key = (row[0], row[1])
        if key in seen:
            continue
        seen.add(key)
        deduped.append(row)
    duplicates_removed = len(rows) - len(deduped)
    rows = deduped

    if merged_count:
        logger.info(
            f"Merged {merged_count} alias IDs, removed {duplicates_removed} duplicates"
        )
    if skipped_invalid:
        logger.info(f"Skipped {skipped_invalid} non-character entries")
    if footnote_stripped:
        logger.info(f"Stripped footnote parentheticals from {footnote_stripped} entries")
    if overridden:
        logger.info(f"Applied manual overrides for {overridden} characters")

    logger.info(
        f"Parsed {len(rows)} occupation entries "
        f"({len(characters_with_occ)} characters, {len(roles)} distinct roles)"
    )

    if progress_callback:
        progress_callback(1, 2, "parsed")

    if not dry_run:
        conn = duckdb.connect(str(db_path))
        try:
            conn.execute("DROP TABLE IF EXISTS character_occupation CASCADE")
            conn.execute(
                """
                CREATE TABLE character_occupation (
                    character_id TEXT NOT NULL,
                    role TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'current',
                    PRIMARY KEY (character_id, role)
                )
                """
            )
            conn.executemany(
                """INSERT INTO character_occupation
                   (character_id, role, status) VALUES (?, ?, ?)
                   ON CONFLICT (character_id, role) DO UPDATE SET
                       status = EXCLUDED.status""",
                rows,
            )
            logger.success(
                f"Saved {len(rows)} occupation entries to character_occupation"
            )
        finally:
            conn.close()

    if progress_callback:
        progress_callback(2, 2, "done")

    return {
        "entry_count": len(rows),
        "role_count": len(roles),
        "character_count": len(characters_with_occ),
        "aliases_merged": merged_count,
        "duplicates_removed": duplicates_removed,
        "non_characters_skipped": skipped_invalid,
        "footnote_stripped": footnote_stripped,
        "manual_overrides": overridden,
    }
