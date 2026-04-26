"""Build graph_edges from graph_extractions.

Pure deterministic pipeline:
  1. Read extractions at the current PROMPT_VERSION
  2. Resolve subject/object strings to graph_nodes.id (canonical_name or aliases)
  3. Drop triples with unlinkable subject/object (logged + counted)
  4. Filter confidence < CONFIDENCE_THRESHOLD
  5. Dedup on (subject_id, relation, object_id):
       - keep highest-confidence row's confidence
       - concatenate up to MAX_EVIDENCE_QUOTES distinct evidence quotes
       - keep one source_extraction_id (highest-confidence)
  6. Truncate graph_edges and reinsert
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Optional

import duckdb
from loguru import logger

from .relations import PROMPT_VERSION, RELATIONS
from .schema import create_graph_tables


CONFIDENCE_THRESHOLD = 0.6
MAX_EVIDENCE_QUOTES = 3
MAX_EVIDENCE_LEN = 500


@dataclass
class BuildStats:
    extractions_read: int = 0
    triples_total: int = 0
    triples_dropped_low_conf: int = 0
    triples_dropped_unknown_relation: int = 0
    triples_dropped_unlinkable: int = 0
    edges_after_dedup: int = 0
    unlinkable_subjects: dict[str, int] = field(default_factory=dict)
    unlinkable_objects: dict[str, int] = field(default_factory=dict)


@dataclass
class _Edge:
    subject_id: int
    relation: str
    object_id: int
    confidence: float
    evidence_chapter: Optional[int]
    evidence_text: str
    source_extraction_id: int


def _build_name_index(conn: duckdb.DuckDBPyConnection) -> dict[str, int]:
    """Lowercased canonical_name and alias → node_id."""
    rows = conn.execute(
        "SELECT id, canonical_name, aliases FROM graph_nodes"
    ).fetchall()
    index: dict[str, int] = {}
    for node_id, canon, aliases in rows:
        if canon:
            index.setdefault(canon.strip().lower(), node_id)
        for alias in aliases or []:
            if alias:
                index.setdefault(alias.strip().lower(), node_id)
    return index


def _resolve(name: object, index: dict[str, int]) -> Optional[int]:
    if not isinstance(name, str):
        return None
    key = name.strip().lower()
    if not key:
        return None
    return index.get(key)


def build_graph(
    db_path: str,
    confidence_threshold: float = CONFIDENCE_THRESHOLD,
    prompt_version: int = PROMPT_VERSION,
) -> BuildStats:
    """Materialize graph_edges from raw extractions. Returns BuildStats."""
    create_graph_tables(db_path)
    conn = duckdb.connect(db_path)
    stats = BuildStats()
    try:
        logger.info(f"Building graph_edges from prompt_version={prompt_version}")

        name_to_id = _build_name_index(conn)
        logger.info(f"Loaded {len(name_to_id):,} entity name → id mappings")

        # Pull all extractions at the target prompt_version. Join to active
        # source_text rows so deleted/superseded sources don't contribute.
        rows = conn.execute(
            """
            SELECT ex.id, ex.raw_triples
            FROM graph_extractions ex
            JOIN graph_source_text st ON st.id = ex.source_text_id
            WHERE ex.prompt_version = ?
              AND st.superseded_at IS NULL
            """,
            [prompt_version],
        ).fetchall()
        stats.extractions_read = len(rows)
        logger.info(f"Read {len(rows):,} extractions")

        # Bucket into deduped edges.
        bucket: dict[tuple[int, str, int], list[_Edge]] = {}
        for ex_id, raw in rows:
            triples = (
                json.loads(raw) if isinstance(raw, str) else (raw or [])
            )
            if not isinstance(triples, list):
                continue
            for t in triples:
                if not isinstance(t, dict):
                    continue
                stats.triples_total += 1

                rel = t.get("relation")
                if rel not in RELATIONS:
                    stats.triples_dropped_unknown_relation += 1
                    continue

                conf = t.get("confidence")
                try:
                    conf_f = float(conf) if conf is not None else 0.0
                except (TypeError, ValueError):
                    stats.triples_dropped_low_conf += 1
                    continue
                if conf_f < confidence_threshold:
                    stats.triples_dropped_low_conf += 1
                    continue

                subj_str = t.get("subject")
                obj_str = t.get("object")
                subj_id = _resolve(subj_str, name_to_id)
                obj_id = _resolve(obj_str, name_to_id)
                if subj_id is None or obj_id is None or subj_id == obj_id:
                    stats.triples_dropped_unlinkable += 1
                    if subj_id is None and isinstance(subj_str, str):
                        stats.unlinkable_subjects[subj_str] = (
                            stats.unlinkable_subjects.get(subj_str, 0) + 1
                        )
                    if obj_id is None and isinstance(obj_str, str):
                        stats.unlinkable_objects[obj_str] = (
                            stats.unlinkable_objects.get(obj_str, 0) + 1
                        )
                    continue

                ev_text = t.get("evidence_text") or ""
                if not isinstance(ev_text, str):
                    ev_text = str(ev_text)
                ev_ch = t.get("evidence_chapter")
                if not isinstance(ev_ch, int):
                    ev_ch = None

                edge = _Edge(
                    subject_id=subj_id,
                    relation=rel,
                    object_id=obj_id,
                    confidence=conf_f,
                    evidence_chapter=ev_ch,
                    evidence_text=ev_text,
                    source_extraction_id=ex_id,
                )
                bucket.setdefault((subj_id, rel, obj_id), []).append(edge)

        # Dedup: keep highest-confidence row, concat evidence quotes.
        deduped: list[_Edge] = []
        for (subj_id, rel, obj_id), group in bucket.items():
            group.sort(key=lambda e: e.confidence, reverse=True)
            top = group[0]
            seen_quotes: set[str] = set()
            quotes: list[str] = []
            chapter = top.evidence_chapter
            for e in group:
                if e.evidence_text and e.evidence_text not in seen_quotes:
                    seen_quotes.add(e.evidence_text)
                    quotes.append(e.evidence_text)
                    if len(quotes) >= MAX_EVIDENCE_QUOTES:
                        break
                if chapter is None and e.evidence_chapter is not None:
                    chapter = e.evidence_chapter
            evidence_text = " | ".join(quotes)[:MAX_EVIDENCE_LEN]
            deduped.append(
                _Edge(
                    subject_id=subj_id,
                    relation=rel,
                    object_id=obj_id,
                    confidence=top.confidence,
                    evidence_chapter=chapter,
                    evidence_text=evidence_text,
                    source_extraction_id=top.source_extraction_id,
                )
            )
        stats.edges_after_dedup = len(deduped)

        # Replace graph_edges atomically-ish.
        conn.execute("BEGIN")
        try:
            conn.execute("DELETE FROM graph_edges")
            conn.executemany(
                """
                INSERT INTO graph_edges
                    (subject_id, relation, object_id, evidence_chapter,
                     evidence_text, confidence, source_extraction_id)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        e.subject_id,
                        e.relation,
                        e.object_id,
                        e.evidence_chapter,
                        e.evidence_text,
                        e.confidence,
                        e.source_extraction_id,
                    )
                    for e in deduped
                ],
            )
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise

        logger.success(
            f"Build done — extractions={stats.extractions_read:,} "
            f"triples={stats.triples_total:,} "
            f"dropped(low_conf)={stats.triples_dropped_low_conf:,} "
            f"dropped(unlinkable)={stats.triples_dropped_unlinkable:,} "
            f"dropped(unknown_rel)={stats.triples_dropped_unknown_relation:,} "
            f"edges={stats.edges_after_dedup:,}"
        )
        return stats
    finally:
        conn.close()
