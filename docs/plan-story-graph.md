# Plan: Typed Relationship Graph (Story Graph)

Build a typed knowledge graph of One Piece entities (characters, crews, organizations, devil fruits, locations) and their relationships, extracted from wiki text via LLM, with aggressive caching so weekly re-runs cost almost nothing.

## Goals

- Typed relationships (not just co-occurrence): `captain_of`, `ally_of`, `fought`, `ate_devil_fruit`, etc.
- Re-runnable without re-hitting the LLM when text is unchanged.
- Re-runnable without re-scraping when source text is cached.
- Weekly updates on new chapter release should cost pennies and complete in minutes.

## Architecture

Four decoupled stages, each reading from the previous stage's DuckDB table. Any stage can be re-run without touching upstream stages or the network.

```
wiki_text ──▶ graph_source_text ──▶ graph_extractions ──▶ graph_edges
  (scrape)     (sync+snapshot)         (LLM extract)         (build)
```

Plus a separate one-time layer:

```
character, wiki scrape ──▶ graph_nodes  (entities)
```

---

## Schema

```sql
-- Entities (characters, crews, orgs, fruits, locations)
graph_nodes (
  id INTEGER PRIMARY KEY,
  type VARCHAR,                       -- character|crew|organization|devil_fruit|location
  canonical_name VARCHAR,
  aliases VARCHAR[],
  source_table VARCHAR,               -- e.g. 'character', NULL for hand-seeded
  source_id INTEGER,
  UNIQUE (type, canonical_name)
);

-- Immutable snapshot of text fed to the LLM
graph_source_text (
  id INTEGER PRIMARY KEY,
  source_table VARCHAR,               -- 'wiki_text'
  source_id INTEGER,
  section_name VARCHAR,
  text TEXT,
  normalized_hash VARCHAR,
  text_length INTEGER,
  entities_found INTEGER[],           -- cached node_ids appearing in text
  first_seen_at TIMESTAMP,
  last_seen_at TIMESTAMP,
  superseded_at TIMESTAMP
);

-- LLM output, one row per (source_text, prompt_version)
graph_extractions (
  id INTEGER PRIMARY KEY,
  source_text_id INTEGER,
  model VARCHAR,
  prompt_version INTEGER,
  extracted_at TIMESTAMP,
  raw_triples JSON,                   -- [{subject, relation, object, evidence_chapter, confidence}, ...]
  input_tokens INTEGER,
  output_tokens INTEGER
);

-- Final graph, rebuilt from extractions
graph_edges (
  id INTEGER PRIMARY KEY,
  subject_id INTEGER,
  relation VARCHAR,
  object_id INTEGER,
  evidence_chapter INTEGER,
  evidence_text TEXT,
  confidence FLOAT,
  source_extraction_id INTEGER
);
```

---

## Phase 1 — Foundation (one-time, ~half day)

**1.1** Relation vocabulary in `src/onepieceofdata/graph/relations.py`:

`member_of_crew`, `captain_of`, `ally_of`, `enemy_of`, `fought`, `defeated_by`, `mentor_of`, `family_of`, `ate_devil_fruit`, `affiliated_with`, `has_bounty_of`, `originates_from`.

Each with a one-line description. Export `PROMPT_VERSION = 1`.

**1.2** DuckDB migration for the four tables above.

## Phase 2 — Entity Setup (one-time, ~half day)

**2.1** Populate `graph_nodes`:
- All `character` rows → nodes.
- Hand-seed ~20 organizations (Marines, Yonko, Shichibukai, CP0, Revolutionary Army, etc.).
- Crews: extract from character infoboxes or scrape the crew list page once.
- Devil fruits: parse from `character.devil_fruit` column; fallback scrape the fruit index page.

**2.2** Alias index in `graph/entity_linker.py`. Build from character names, nicknames, epithets. Manual curation file for edge cases. This is the single biggest quality lever — budget time to iterate.

## Phase 3 — Source Snapshot (~half day)

`graph/source_sync.py` implements `graph-sync-sources`:

```python
for wiki_row in wiki_text:
    for section_name, text in wiki_row.sections.items():
        hash_ = normalize_hash(text)
        existing = latest_source(wiki_row.id, section_name)

        if existing is None:
            entities = match_entities(text)         # string match vs aliases
            insert_new_source(..., entities_found=entities)
        elif existing.normalized_hash == hash_:
            touch(existing.last_seen_at)
        elif similarity(existing.text, text) > 0.95:
            touch(existing.last_seen_at)
        else:
            mark_superseded(existing)
            insert_new_source(...)
```

Entities are pre-filtered here, once per text version. Cached in `entities_found`.

## Phase 4 — Extraction (~1-2 days)

`graph/extractor.py` implements `graph-extract`:

```python
for src in graph_source_text where superseded_at IS NULL:
    if len(src.entities_found) < 2: continue
    prev = latest_extraction(src.id)
    if prev and prev.prompt_version == CURRENT_VERSION: continue
    triples = llm_extract(src.text, src.entities_found, RELATIONS)
    insert_extraction(src.id, CURRENT_VERSION, triples, tokens)
```

Prompt: text + list of entities in text + relation vocabulary + JSON schema. Groq llama-3.3-70b. Permissive: extract anything plausible, filter in build phase.

Flags: `--force` (ignore cache), `--limit N` (test run), `--source-id X` (one page).

## Phase 5 — Build (~half day)

`graph/builder.py` implements `graph-build`. Pure deterministic:

1. Read all latest extractions.
2. Entity-link subject/object strings → `graph_nodes.id`. Drop unlinkable (or park in `graph_unlinked` for review).
3. Normalize relations (merge synonyms by lookup table).
4. Filter `confidence < 0.6`.
5. Dedupe on `(subject_id, relation, object_id)`, keep highest confidence, concatenate evidence.
6. Truncate `graph_edges` and reinsert.

## Phase 6 — CLI & Makefile (~2 hours)

```bash
make graph-init-nodes        # Phase 2
make graph-sync-sources      # Phase 3
make graph-extract           # Phase 4 (incremental)
make graph-extract-force     # Phase 4 (ignore cache)
make graph-build             # Phase 5
make graph-status            # counts, pending, token spend
make graph-export-graphml    # optional visualization export
```

---

## Weekly Run: New Chapter Released

Extend `update-new-chapter` target:

```makefile
update-new-chapter-with-graph: update-new-chapter graph-sync-sources graph-extract graph-build
```

What runs:

1. `update-new-chapter` — existing flow, re-scrapes wiki text.
2. `graph-sync-sources` — snapshots new/changed sections only.
3. `graph-extract` — LLM runs only on rows where `prev is None` or prompt version bumped. Typical week: 5-50 calls.
4. `graph-build` — seconds, no network.

Prompt iteration (no new chapter): `make graph-extract graph-build` after bumping `PROMPT_VERSION`.

Build logic iteration only: `make graph-build`. No LLM, no network.

---

# Savings Analysis

## Less API Hits (Wiki)

Weekly bottleneck today: re-scraping hundreds of wiki pages even when most are unchanged.

| Saving | How | Impact |
|---|---|---|
| **HTTP conditional requests** | Send `If-Modified-Since` or `If-None-Match` headers to MediaWiki API; 304 responses skip body transfer. Fandom supports this. | 60-80% bandwidth cut weekly |
| **Use `revisions` endpoint with `rvprop=timestamp` first** | One cheap API call returns last-edited timestamps for many pages. Skip full refetch if timestamp unchanged since `wiki_text.scraped_at`. | Eliminates most page fetches |
| **Changed-pages feed** | MediaWiki exposes `list=recentchanges` — pull last week's changed pages and only refetch those, instead of iterating the full character list. | 95%+ cut on steady-state weeks |
| **Scope by new chapter** | A new chapter usually affects ~5-20 character pages + the new chapter/arc/volume page. Derive the affected page set from the chapter diff instead of re-scraping everything. | Extreme weeks: 10-20 fetches total |

Recommended order: implement `recentchanges` feed first — biggest win, ~half day of work. Conditional headers are nice but redundant once recentchanges is in place.

## Less LLM Usage

| Saving | How | Impact |
|---|---|---|
| **Section-level caching** (in plan) | `graph_source_text` hash + similarity. Only new/meaningfully-changed sections hit the LLM. | Steady-state: 95%+ cut |
| **Skip sections with <2 entities** (in plan) | No pair, no relation possible. Cheap pre-filter. | ~30-50% cut on first run |
| **Skip stable sections entirely** | Early-life/backstory sections of established characters almost never change meaningfully. Mark sections as `stable=true` after N weeks of no changes; raise the similarity threshold for these from 0.95 to 0.85. | Further ~20% cut |
| **Batch by page, not per-section** | If several sections of one character page are small, concatenate them into one LLM call. Fewer overhead tokens, fewer rate-limit hits. | 2-3x token efficiency |
| **Use a smaller model for easy cases** | If a section has exactly 2 entities and is short, llama-3.1-8b is usually sufficient. Route by entity count + length. Reserve 70b for complex sections. | 5-10x cost cut on the easy slice |
| **Two-pass: cheap extractor, LLM only for disagreement** | Rule-based extractor handles obvious patterns ("X is the captain of Y", "X is the father of Y") with regex on known aliases. LLM only runs on text the rule-based pass couldn't explain. | 30-50% of easy triples free |
| **Don't re-extract on build-phase changes** | Relation merging, confidence thresholds, entity linking fixes: all live in the builder. LLM output is permissive and stable. | Zero LLM cost for most iteration |
| **Version the prompt; re-extract selectively** | When improving the prompt, re-run only on a stratified sample first (e.g. 50 sections). Commit the new `prompt_version` only if sample looks better. | Avoids wasted full-corpus re-runs |

Biggest wins: section caching (already in plan) + rule-based first pass + section-level stability flags. Together, weekly cost drops to near-zero.

## Less DB Size (git-lfs)

Assuming `onepiece.duckdb` is committed via git-lfs, each table addition grows every snapshot.

| Saving | How | Impact |
|---|---|---|
| **Don't commit derived tables** | `graph_extractions` and `graph_edges` are reproducible from `graph_source_text` + code. Add a `make db-slim` target that `DROP`s derived tables before commit, then `make db-rebuild` restores them. | Biggest single win |
| **Split into multiple DuckDB files** | Core data (`onepiece.duckdb`, checked in) vs derived (`onepiece_graph.duckdb`, gitignored + built by CI or on-demand). DuckDB can `ATTACH` multiple DBs and query across them. | Cleanest long-term |
| **Truncate evidence text** | `graph_extractions.raw_triples` and `graph_edges.evidence_text` can hold long quotes. Cap evidence at 200 chars; keep `source_text_id` for full lookup. | 30-50% reduction on graph tables |
| **Drop superseded source snapshots** | History is nice but optional. `graph-prune-sources --keep-latest-only` collapses the snapshot table to current-only when size becomes a concern. | Bounded growth |
| **Compress JSON** | `raw_triples` as JSON is verbose. Option: store as a child `graph_triples` table (normalized rows) instead of JSON blobs. Smaller, also queryable without `json_extract`. | 40% on extractions table |
| **Don't store `source_text` in `graph_extractions`** (already in plan via `source_text_id`) | Single copy in `graph_source_text`. | Already captured |
| **Wiki text deduplication** | `wiki_text.full_text` and `wiki_text.sections` overlap heavily (sections are slices of full_text). Store only `sections`; derive `full_text` on read. | 40-50% on wiki_text |
| **Vector embeddings: quantize or externalize** | `wiki_chunks` embeddings at FLOAT32 × 384 × N chunks is probably the largest single thing in the DB. Options: quantize to int8 (4x smaller, minor quality loss), or move embeddings to a separate non-LFS'd file rebuilt from wiki_chunks text. | Potentially the single biggest overall DB size win — measure first |

Recommended order: measure current table sizes with `PRAGMA database_size`; attack the biggest one first. Likely ranking: embeddings > wiki_text > everything else. Moving `wiki_chunks` embeddings out of the committed DB is likely the highest-leverage single change, and it's orthogonal to the graph work.

---

## Effort Totals

| Phase | Effort | One-time LLM cost |
|---|---|---|
| 1. Schema & vocab | 0.5d | — |
| 2. Entity setup | 0.5d | — |
| 3. Source snapshot | 0.5d | — |
| 4. Extraction | 1-2d | ~$5-20 first run |
| 5. Build | 0.5d | — |
| 6. CLI/Makefile | 2h | — |
| **Core plan** | **~3-4d** | **one-time** |
| Savings: recentchanges feed | 0.5d | — |
| Savings: rule-based first pass | 1d | — |
| Savings: DB size audit + fixes | 0.5-1d | — |

Savings work is additive — ship the core plan first, add savings incrementally once weekly cost can be measured.

## Risk Notes

- **Entity linking is where quality lives or dies.** Budget time for iterating on the alias list — this is the thing tweaked most after launch.
- **Keep the LLM permissive, the builder strict.** Extract liberally, filter in the build phase. Makes iteration cheap.
- **Version the prompt.** Without `prompt_version`, stale extractions can't be distinguished from fresh ones.
- **Spot-check 50-100 edges manually after the first full run** before trusting the graph.
