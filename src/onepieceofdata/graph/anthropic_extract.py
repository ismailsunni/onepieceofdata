"""Anthropic backend for triple extraction (Claude Haiku 4.5 / Sonnet 4.6).

Mirrors the Groq path in llm_extract.py — same SYSTEM_PROMPT, same triple
shape, same _valid_triple gate. Returns ExtractionResult identical to the
Groq path so extractor.py is provider-agnostic.

The system prompt (~500 tokens) is below the 2048-token minimum needed for
Sonnet 4.6 cache reads (and 4096 for Haiku 4.5), so prompt caching would
silently no-op; not worth the cache-write premium.
"""

from __future__ import annotations

import json

from anthropic import Anthropic
from loguru import logger

from .llm_extract import (
    MAX_ENTITIES_IN_PROMPT,
    MAX_OUTPUT_TOKENS,
    MAX_TEXT_CHARS,
    SYSTEM_PROMPT,
    ExtractionResult,
    _valid_triple,
)


DEFAULT_ANTHROPIC_MODEL = "claude-haiku-4-5"


_TRIPLES_SCHEMA = {
    "type": "object",
    "properties": {
        "triples": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "subject": {"type": "string"},
                    "relation": {"type": "string"},
                    "object": {"type": "string"},
                    "evidence_chapter": {"type": ["integer", "null"]},
                    "evidence_text": {"type": "string"},
                    "confidence": {"type": "number"},
                },
                "required": [
                    "subject",
                    "relation",
                    "object",
                    "evidence_chapter",
                    "evidence_text",
                    "confidence",
                ],
                "additionalProperties": False,
            },
        }
    },
    "required": ["triples"],
    "additionalProperties": False,
}


def extract_triples_anthropic(
    client: Anthropic,
    text: str,
    entity_names: list[str],
    model: str = DEFAULT_ANTHROPIC_MODEL,
) -> ExtractionResult:
    """Single Anthropic call to extract triples from one section."""
    if not entity_names:
        return ExtractionResult(
            triples=[], input_tokens=0, output_tokens=0, model=model
        )

    truncated_text = text[:MAX_TEXT_CHARS]
    capped_entities = entity_names[:MAX_ENTITIES_IN_PROMPT]
    entity_block = "\n".join(f"- {n}" for n in capped_entities)
    user_msg = (
        f"ENTITIES:\n{entity_block}\n\n"
        f"TEXT:\n{truncated_text}\n\n"
        "Return the JSON object now."
    )

    response = client.messages.create(
        model=model,
        max_tokens=MAX_OUTPUT_TOKENS,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
        output_config={
            "format": {"type": "json_schema", "schema": _TRIPLES_SCHEMA}
        },
    )

    text_out = next(
        (b.text for b in response.content if b.type == "text"), "{}"
    )
    if response.stop_reason == "max_tokens":
        logger.warning(
            f"Anthropic response truncated by max_tokens={MAX_OUTPUT_TOKENS}; "
            "triples after the truncation point will be lost"
        )
    try:
        parsed = json.loads(text_out)
    except json.JSONDecodeError:
        parsed = {}

    raw = parsed.get("triples", []) if isinstance(parsed, dict) else []
    if not isinstance(raw, list):
        raw = []
    valid = [t for t in raw if _valid_triple(t)]

    usage = response.usage
    return ExtractionResult(
        triples=valid,
        input_tokens=getattr(usage, "input_tokens", 0) or 0,
        output_tokens=getattr(usage, "output_tokens", 0) or 0,
        model=model,
    )
