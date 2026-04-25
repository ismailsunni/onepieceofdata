"""LLM extraction of (subject, relation, object) triples from section text.

One call per source section. The system prompt holds the relation vocabulary;
the user message holds the section text plus the list of known entities we've
already detected via string match. Model is instructed to use only those
entity names (strict vocabulary) and only the listed relations.

Keep the prompt and the JSON schema stable — bump graph.relations.PROMPT_VERSION
whenever either changes, so stale extractions can be re-run selectively.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from groq import Groq

from .relations import RELATIONS


MAX_TEXT_CHARS = 3000          # truncate to keep cost predictable
MAX_ENTITIES_IN_PROMPT = 60    # upper bound on entity list size
MAX_OUTPUT_TOKENS = 1024
DEFAULT_MODEL = "llama-3.3-70b-versatile"


def _relations_block() -> str:
    return "\n".join(f"- {name}: {desc}" for name, desc in RELATIONS.items())


SYSTEM_PROMPT = f"""\
You extract typed relationships between One Piece entities from wiki text.

You will be given:
1. A list of ENTITIES known to appear in the text (by their canonical names).
2. A TEXT passage.

Your task: emit a JSON object {{"triples": [...]}} listing relationships that
the TEXT explicitly supports.

Rules:
- Use ONLY the entity names listed in ENTITIES, verbatim, for subject and object.
- Use ONLY the relation names from this vocabulary:
{_relations_block()}
- Do NOT invent relations or entities.
- If the text does not clearly support a relation, skip it.
- confidence is 0.0–1.0; use 0.9+ only for claims stated directly.
- evidence_chapter is an integer chapter number if the text mentions one,
  otherwise null.
- evidence_text is a short (≤120 chars) quote or paraphrase from the text.

Output strictly this JSON shape (no prose, no markdown):
{{
  "triples": [
    {{
      "subject": "<entity name>",
      "relation": "<relation name>",
      "object": "<entity name>",
      "evidence_chapter": <int or null>,
      "evidence_text": "<short quote>",
      "confidence": <float 0..1>
    }}
  ]
}}
If nothing qualifies, return {{"triples": []}}."""


@dataclass
class ExtractionResult:
    triples: list[dict]
    input_tokens: int
    output_tokens: int
    model: str


def extract_triples(
    client: Groq,
    text: str,
    entity_names: list[str],
    model: str = DEFAULT_MODEL,
) -> ExtractionResult:
    """Call the LLM once and parse its JSON response."""
    if not entity_names:
        return ExtractionResult(triples=[], input_tokens=0, output_tokens=0, model=model)

    truncated_text = text[:MAX_TEXT_CHARS]
    capped_entities = entity_names[:MAX_ENTITIES_IN_PROMPT]

    entity_block = "\n".join(f"- {n}" for n in capped_entities)
    user = (
        f"ENTITIES:\n{entity_block}\n\n"
        f"TEXT:\n{truncated_text}\n\n"
        "Return the JSON object now."
    )

    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user},
        ],
        response_format={"type": "json_object"},
        max_tokens=MAX_OUTPUT_TOKENS,
        temperature=0.0,
    )

    content = response.choices[0].message.content or "{}"
    try:
        parsed = json.loads(content)
    except json.JSONDecodeError:
        parsed = {}

    triples = parsed.get("triples", []) if isinstance(parsed, dict) else []
    if not isinstance(triples, list):
        triples = []

    valid = [t for t in triples if _valid_triple(t)]

    usage = getattr(response, "usage", None)
    return ExtractionResult(
        triples=valid,
        input_tokens=getattr(usage, "prompt_tokens", 0) or 0,
        output_tokens=getattr(usage, "completion_tokens", 0) or 0,
        model=model,
    )


def _valid_triple(t: object) -> bool:
    if not isinstance(t, dict):
        return False
    subj = t.get("subject")
    rel = t.get("relation")
    obj = t.get("object")
    if not (isinstance(subj, str) and isinstance(rel, str) and isinstance(obj, str)):
        return False
    if rel not in RELATIONS:
        return False
    if subj == obj:
        return False
    return True
