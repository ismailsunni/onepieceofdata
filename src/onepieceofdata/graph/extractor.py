"""Driver that walks graph_source_text and writes graph_extractions.

Skips any row that already has an extraction at the current PROMPT_VERSION
unless --force is passed. Supports two providers (Groq, Anthropic) and
optional thread-pool concurrency for Anthropic's higher rate limits.
"""

from __future__ import annotations

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass

import duckdb
from dotenv import load_dotenv
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
    model: str | None = None,
    rate_limit_rpm: int = 25,
    scope: str = "all",
    provider: str = "groq",
    concurrency: int = 1,
) -> ExtractStats:
    """Extract triples from pending source rows, writing to graph_extractions."""
    load_dotenv()
    client, extract_fn, resolved_model = _build_client(provider, model)

    create_graph_tables(db_path)
    conn = duckdb.connect(db_path)
    stats = ExtractStats()
    min_interval = (
        60.0 / rate_limit_rpm if rate_limit_rpm > 0 and concurrency == 1 else 0.0
    )
    model = resolved_model

    try:
        id_to_name = dict(
            conn.execute(
                "SELECT id, canonical_name FROM graph_nodes"
            ).fetchall()
        )

        candidates = _select_candidates(conn, limit, force, source_id, scope)
        stats.candidates = len(candidates)
        if not force and limit is None:
            eligible_total = _count_eligible(conn, source_id, scope)
            stats.skipped_cached = max(eligible_total - len(candidates), 0)
        logger.info(
            f"Found {len(candidates):,} source rows to extract "
            f"(provider={provider}, model={model}, prompt_version={PROMPT_VERSION}, "
            f"concurrency={concurrency}, force={force})"
        )

        # Resolve entity names up front; skip rows with <2 known entities.
        jobs: list[tuple[int, str, list[str]]] = []
        for src_id, text, entity_ids in candidates:
            entity_names = [
                id_to_name[i] for i in (entity_ids or []) if i in id_to_name
            ]
            if len(entity_names) < 2:
                stats.skipped_no_entities += 1
                continue
            jobs.append((src_id, text, entity_names))

        if concurrency > 1:
            _run_concurrent(
                jobs, conn, stats, client, extract_fn, model, concurrency
            )
        else:
            _run_sequential(
                jobs, conn, stats, client, extract_fn, model, min_interval
            )

        logger.success(
            f"Extract done — extracted={stats.extracted} "
            f"triples={stats.total_triples} failed={stats.failed} "
            f"tokens_in={stats.input_tokens} tokens_out={stats.output_tokens}"
        )
        return stats
    finally:
        conn.close()


# --- provider plumbing ------------------------------------------------------


def _build_client(provider: str, model: str | None):
    """Return (client, extract_fn, resolved_model) for the chosen provider."""
    if provider == "groq":
        if not os.environ.get("GROQ_API_KEY"):
            raise RuntimeError("GROQ_API_KEY is not set.")
        from groq import Groq
        return Groq(), extract_triples, model or DEFAULT_MODEL

    if provider == "anthropic":
        if not os.environ.get("ANTHROPIC_API_KEY"):
            raise RuntimeError("ANTHROPIC_API_KEY is not set.")
        from anthropic import Anthropic

        from .anthropic_extract import (
            DEFAULT_ANTHROPIC_MODEL,
            extract_triples_anthropic,
        )
        return Anthropic(), extract_triples_anthropic, model or DEFAULT_ANTHROPIC_MODEL

    raise ValueError(f"Unknown provider: {provider!r}")


def _insert_extraction(conn, src_id, result) -> None:
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


def _run_sequential(jobs, conn, stats, client, extract_fn, model, min_interval):
    last_call = 0.0
    total = len(jobs)
    for row_num, (src_id, text, entity_names) in enumerate(jobs, start=1):
        wait = min_interval - (time.monotonic() - last_call)
        if wait > 0:
            time.sleep(wait)
        try:
            result = extract_fn(client, text, entity_names, model=model)
            last_call = time.monotonic()
        except Exception as e:
            logger.warning(f"Extraction failed for source_text_id={src_id}: {e}")
            stats.failed += 1
            continue

        stats.extracted += 1
        stats.input_tokens += result.input_tokens
        stats.output_tokens += result.output_tokens
        stats.total_triples += len(result.triples)
        _insert_extraction(conn, src_id, result)

        if row_num % 25 == 0 or row_num == total:
            logger.info(
                f"  progress {row_num}/{total} "
                f"triples={stats.total_triples} "
                f"tokens_in={stats.input_tokens} out={stats.output_tokens}"
            )


def _run_concurrent(jobs, conn, stats, client, extract_fn, model, concurrency):
    """Thread-pool fan-out. Workers do API calls; main thread writes to DuckDB."""
    total = len(jobs)
    done = 0

    def _call(job):
        src_id, text, entity_names = job
        try:
            return src_id, extract_fn(client, text, entity_names, model=model), None
        except Exception as e:  # noqa: BLE001
            return src_id, None, e

    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(_call, job) for job in jobs]
        for fut in as_completed(futures):
            src_id, result, err = fut.result()
            done += 1
            if err is not None:
                logger.warning(
                    f"Extraction failed for source_text_id={src_id}: {err}"
                )
                stats.failed += 1
            else:
                stats.extracted += 1
                stats.input_tokens += result.input_tokens
                stats.output_tokens += result.output_tokens
                stats.total_triples += len(result.triples)
                _insert_extraction(conn, src_id, result)

            if done % 25 == 0 or done == total:
                logger.info(
                    f"  progress {done}/{total} "
                    f"triples={stats.total_triples} "
                    f"tokens_in={stats.input_tokens} out={stats.output_tokens}"
                )


# --- scope filtering --------------------------------------------------------
#
# scope="important" restricts the candidate pool to:
#   - all arc and saga wiki pages
#   - top 500 characters by appearance_count
#   - top 500 characters by wiki page length
#   - characters with bounty > 100M
#   - characters with last_appearance >= 1100 AND appearance_count >= 3
# This is the recommended default for paid-API runs to keep cost bounded
# while still catching the long tail of recently-introduced major characters.

SCOPE_TOP_CHARS = 500
SCOPE_MIN_BOUNTY = 100_000_000
SCOPE_RECENT_FROM = 1100
SCOPE_RECENT_MIN_APPEARANCES = 3


def _scope_clause(scope: str) -> tuple[str, list]:
    """Return (sql_fragment, params) restricting st.source_id by scope."""
    if scope == "all":
        return "", []
    if scope != "important":
        raise ValueError(f"Unknown scope: {scope!r}")

    sql = """(
      st.source_table = 'wiki_text' AND st.source_id IN (
        SELECT page_id FROM wiki_text WHERE page_type IN ('arc', 'saga')
        UNION
        SELECT wt.page_id FROM wiki_text wt
        JOIN (
          SELECT name FROM character
          WHERE appearance_count IS NOT NULL
          ORDER BY appearance_count DESC LIMIT ?
        ) top_app ON top_app.name = wt.title
        WHERE wt.page_type = 'character'
        UNION
        SELECT page_id FROM (
          SELECT page_id FROM wiki_text WHERE page_type = 'character'
          ORDER BY length(full_text) DESC LIMIT ?
        )
        UNION
        SELECT wt.page_id FROM wiki_text wt
        JOIN character c ON c.name = wt.title
        WHERE wt.page_type = 'character' AND c.bounty > ?
        UNION
        SELECT wt.page_id FROM wiki_text wt
        JOIN character c ON c.name = wt.title
        WHERE wt.page_type = 'character'
          AND c.last_appearance >= ?
          AND c.appearance_count >= ?
      )
    )"""
    params = [
        SCOPE_TOP_CHARS,
        SCOPE_TOP_CHARS,
        SCOPE_MIN_BOUNTY,
        SCOPE_RECENT_FROM,
        SCOPE_RECENT_MIN_APPEARANCES,
    ]
    return sql, params


def _select_candidates(
    conn: duckdb.DuckDBPyConnection,
    limit: int | None,
    force: bool,
    source_id: str | None,
    scope: str = "all",
) -> list[tuple[int, str, list[int]]]:
    """Return (source_text_id, text, entities_found) rows needing extraction."""
    where = ["st.superseded_at IS NULL", "array_length(st.entities_found) >= 2"]
    params: list = []

    if source_id is not None:
        where.append("st.source_id = ?")
        params.append(source_id)

    scope_sql, scope_params = _scope_clause(scope)
    if scope_sql:
        where.append(scope_sql)
        params.extend(scope_params)

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
    scope: str = "all",
) -> int:
    """Count source rows eligible for extraction, ignoring the cache filter."""
    where = ["st.superseded_at IS NULL", "array_length(st.entities_found) >= 2"]
    params: list = []
    if source_id is not None:
        where.append("st.source_id = ?")
        params.append(source_id)
    scope_sql, scope_params = _scope_clause(scope)
    if scope_sql:
        where.append(scope_sql)
        params.extend(scope_params)
    query = (
        "SELECT COUNT(*) FROM graph_source_text st "
        f"WHERE {' AND '.join(where)}"
    )
    return conn.execute(query, params).fetchone()[0]
