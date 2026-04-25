"""Driver that walks graph_source_text and writes graph_extractions.

Skips any row that already has an extraction at the current PROMPT_VERSION
unless --force is passed. Respects a simple rate limit for Groq free tier.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass

import duckdb
from dotenv import load_dotenv
from groq import Groq
from loguru import logger

from .llm_extract import DEFAULT_MODEL, extract_triples
from .relations import PROMPT_VERSION
from .schema import create_graph_tables


@dataclass
class ExtractStats:
    candidates: int = 0
    extracted: int = 0
    skipped_cached: int = 0
    skipped_no_entities: int = 0
    failed: int = 0
    input_tokens: int = 0
    output_tokens: int = 0
    total_triples: int = 0


def run_extraction(
    db_path: str,
    limit: int | None = None,
    force: bool = False,
    source_id: str | None = None,
    model: str = DEFAULT_MODEL,
    rate_limit_rpm: int = 25,
) -> ExtractStats:
    """Extract triples from pending source rows, writing to graph_extractions."""
    load_dotenv()
    if not os.environ.get("GROQ_API_KEY"):
        raise RuntimeError(
            "GROQ_API_KEY is not set. Add it to .env or export it before running."
        )
    create_graph_tables(db_path)
    client = Groq()

    conn = duckdb.connect(db_path)
    stats = ExtractStats()
    min_interval = 60.0 / rate_limit_rpm if rate_limit_rpm > 0 else 0.0

    try:
        id_to_name = dict(
            conn.execute(
                "SELECT id, canonical_name FROM graph_nodes"
            ).fetchall()
        )

        candidates = _select_candidates(conn, limit, force, source_id)
        stats.candidates = len(candidates)
        if not force and limit is None:
            eligible_total = _count_eligible(conn, source_id)
            stats.skipped_cached = max(eligible_total - len(candidates), 0)
        logger.info(
            f"Found {len(candidates):,} source rows to extract "
            f"(prompt_version={PROMPT_VERSION}, model={model}, force={force})"
        )

        last_call = 0.0
        for row_num, (src_id, text, entity_ids) in enumerate(candidates, start=1):
            entity_names = [
                id_to_name[i] for i in (entity_ids or []) if i in id_to_name
            ]
            if len(entity_names) < 2:
                stats.skipped_no_entities += 1
                continue

            wait = min_interval - (time.monotonic() - last_call)
            if wait > 0:
                time.sleep(wait)

            try:
                result = extract_triples(client, text, entity_names, model=model)
                last_call = time.monotonic()
            except Exception as e:
                logger.warning(f"Extraction failed for source_text_id={src_id}: {e}")
                stats.failed += 1
                continue

            stats.extracted += 1
            stats.input_tokens += result.input_tokens
            stats.output_tokens += result.output_tokens
            stats.total_triples += len(result.triples)

            conn.execute(
                """
                INSERT INTO graph_extractions
                    (source_text_id, model, prompt_version, raw_triples,
                     input_tokens, output_tokens)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    src_id,
                    result.model,
                    PROMPT_VERSION,
                    json.dumps(result.triples),
                    result.input_tokens,
                    result.output_tokens,
                ],
            )

            if row_num % 25 == 0 or row_num == len(candidates):
                logger.info(
                    f"  progress {row_num}/{len(candidates)} "
                    f"triples={stats.total_triples} "
                    f"tokens_in={stats.input_tokens} out={stats.output_tokens}"
                )

        logger.success(
            f"Extract done — extracted={stats.extracted} "
            f"triples={stats.total_triples} failed={stats.failed} "
            f"tokens_in={stats.input_tokens} tokens_out={stats.output_tokens}"
        )
        return stats
    finally:
        conn.close()


def _select_candidates(
    conn: duckdb.DuckDBPyConnection,
    limit: int | None,
    force: bool,
    source_id: str | None,
) -> list[tuple[int, str, list[int]]]:
    """Return (source_text_id, text, entities_found) rows needing extraction."""
    where = ["st.superseded_at IS NULL", "array_length(st.entities_found) >= 2"]
    params: list = []

    if source_id is not None:
        where.append("st.source_id = ?")
        params.append(source_id)

    if not force:
        # Skip sources that already have an extraction at the current PROMPT_VERSION
        where.append(
            "NOT EXISTS ("
            "  SELECT 1 FROM graph_extractions ex"
            "  WHERE ex.source_text_id = st.id"
            "    AND ex.prompt_version = ?"
            ")"
        )
        params.append(PROMPT_VERSION)

    limit_clause = f" LIMIT {int(limit)}" if limit else ""
    query = (
        "SELECT st.id, st.text, st.entities_found "
        "FROM graph_source_text st "
        f"WHERE {' AND '.join(where)} "
        f"ORDER BY st.id{limit_clause}"
    )
    return conn.execute(query, params).fetchall()


def _count_eligible(
    conn: duckdb.DuckDBPyConnection,
    source_id: str | None,
) -> int:
    """Count source rows eligible for extraction, ignoring the cache filter."""
    where = ["superseded_at IS NULL", "array_length(entities_found) >= 2"]
    params: list = []
    if source_id is not None:
        where.append("source_id = ?")
        params.append(source_id)
    query = (
        "SELECT COUNT(*) FROM graph_source_text "
        f"WHERE {' AND '.join(where)}"
    )
    return conn.execute(query, params).fetchone()[0]
