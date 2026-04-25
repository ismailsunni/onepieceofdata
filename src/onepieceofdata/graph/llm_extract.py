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
MAX_OUTPUT_TOKENS = 4096
DEFAULT_MODEL = "llama-3.3-70b-versatile"


def _relations_block() -> str:
    return "\n".join(f"- {name}: {desc}" for name, desc in RELATIONS.items())


SYSTEM_PROMPT = f"""\
You extract typed relationships between One Piece entities from wiki text.

You will be given:
1. A list of ENTITIES known to appear in the text (by their canonical names).
2. A TEXT passage.

Your task: emit a JSON object {{"triples": [...]}} listing ONLY relationships
that the TEXT itself explicitly supports. Do NOT use prior knowledge.

Hard rules — violations cause the triple to be rejected:
- evidence_text MUST be a verbatim quote of at least 5 words copied from
  the TEXT. Do NOT write "none", "n/a", paraphrases, or empty strings.
  If you cannot quote ≥5 supporting words from the TEXT, DO NOT emit the triple.
- The quote must directly justify the relation. A passing mention of the
  object alongside the subject is NOT enough (e.g. "Nami wore the hat" does
  NOT justify ally_of between Luffy and Nami).
- Use ONLY the entity names listed in ENTITIES, verbatim, for subject and object.
- Use ONLY the relation names from this vocabulary:
{_relations_block()}
- confidence ∈ [0.6, 1.0] only. Anything you would rate below 0.6, OMIT entirely.
- Do not emit duplicate (subject, relation, object) triples within one response.
- evidence_chapter: integer if the TEXT mentions a chapter number, else null.

Output strictly this JSON shape (no prose, no markdown):
{{
  "triples": [
    {{
      "subject": "<entity name>",
      "relation": "<relation name>",
      "object": "<entity name>",
      "evidence_chapter": <int or null>,
      "evidence_text": "<verbatim quote, ≥5 words, ≤200 chars>",
      "confidence": <float 0.6..1.0>
    }}
  ]
}}
If nothing qualifies, return {{"triples": []}}. An empty result is correct
and expected for sections that do not describe relationships."""


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


_BAD_EVIDENCE = {"", "none", "n/a", "na", "null"}


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

    evidence = t.get("evidence_text")
    if not isinstance(evidence, str):
        return False
    cleaned = evidence.strip().strip("'\"").lower()
    if cleaned in _BAD_EVIDENCE:
        return False
    if len(evidence.split()) < 5:
        return False

    conf = t.get("confidence")
    try:
        conf_f = float(conf) if conf is not None else 0.0
    except (TypeError, ValueError):
        return False
    if conf_f < 0.6:
        return False

    return True
