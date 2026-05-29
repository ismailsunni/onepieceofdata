# One Piece of Data

![One Piece of Data](./onepieceofdata-header.png)

**A modern Python data pipeline + RAG chatbot for One Piece manga data.**

One Piece of Data scrapes, validates, enriches, and stores information about One Piece
manga chapters, characters, volumes, arcs, and sagas from the
[One Piece Fandom Wiki](https://onepiece.fandom.com/). The structured data lives in
[DuckDB](https://duckdb.org/) and can be exported to PostgreSQL/Supabase. On top of it
sits a Retrieval-Augmented Generation (RAG) chatbot and an LLM-extracted story knowledge
graph.

---

## ✨ What's inside

| Capability | Description |
|------------|-------------|
| 🕷️ **Scraping** | Parallel scrapers for chapters, volumes, characters, arcs, sagas + raw wiki text |
| 📊 **Parsing & validation** | Pydantic models validate every record before it lands in DuckDB |
| 🔧 **Post-processing** | 14 enrichment steps: birth dates, affiliations, devil fruits, haki, occupations, appearance analytics, importance scoring |
| 🗄️ **Storage** | DuckDB as the analytical source of truth; PostgreSQL / Supabase as export targets |
| 🤖 **RAG chatbot** | Groq LLM with tool-calling over SQL + vector search of embedded wiki text |
| 🕸️ **Knowledge graph** | LLM-extracted entity/relationship triples (`graph_nodes`, `graph_edges`) |
| 🌐 **Network explorer** | Local interactive web app for the character co-appearance network |
| ⚙️ **Tooling** | `uv` for deps, `make` for orchestration, `ruff`/`black` for quality, `pytest` for tests |

---

## 🗺️ Project flow

The project has three layers: a **structured data pipeline**, an **export layer**, and an
**AI layer** (RAG + knowledge graph). They all read from / write to a single DuckDB file.

```mermaid
flowchart TD
    subgraph Source["🌐 Source"]
        WIKI[One Piece Fandom Wiki]
    end

    subgraph Pipeline["📦 Structured Data Pipeline"]
        direction TB
        SCRAPE["1. Scrape<br/>chapters · volumes · characters · arcs · sagas"]
        PARSE["2. Parse + validate<br/>Pydantic models → DuckDB"]
        POST["3. Post-process<br/>birth dates · affiliations · devil fruits ·<br/>haki · appearance analytics · importance"]
        SCRAPE --> PARSE --> POST
    end

    subgraph Store["🗄️ Storage"]
        DUCK[(DuckDB<br/>onepiece.duckdb)]
    end

    subgraph AI["🤖 AI Layer"]
        direction TB
        WTEXT["Wiki text scrape<br/>+ clean"]
        EMBED["Chunk + embed<br/>all-MiniLM-L6-v2"]
        GRAPH["LLM triple extraction<br/>→ knowledge graph"]
        WTEXT --> EMBED
    end

    subgraph Export["📤 Export Targets"]
        CSV[CSV files]
        PG[(PostgreSQL)]
        SUPA[(Supabase + FTS)]
    end

    subgraph Consume["🎯 Consumers"]
        CHAT[RAG Chatbot]
        NET[Network Explorer]
    end

    WIKI --> SCRAPE
    WIKI --> WTEXT
    POST --> DUCK
    EMBED --> DUCK
    GRAPH --> DUCK
    DUCK --> GRAPH

    DUCK --> CSV
    DUCK --> PG
    DUCK --> SUPA

    DUCK --> CHAT
    SUPA --> CHAT
    DUCK --> NET

    style DUCK fill:#fef3c7,stroke:#d97706,color:#000
    style WIKI fill:#dbeafe,stroke:#2563eb,color:#000
    style CHAT fill:#dcfce7,stroke:#16a34a,color:#000
```

### Data pipeline stages (detail)

The structured pipeline is driven entirely by `make`. `run-data-pipeline` chains the first
three stages; `run-all-exports` handles the fourth.

```mermaid
flowchart LR
    subgraph S1["Stage 1 — Scrape"]
        direction TB
        A1[scrape-chapters] --> A2[scrape-volumes]
        A2 --> A3[extract-characters]
        A3 --> A4[scrape-characters]
        A4 --> A5[scrape-story-structure]
    end

    subgraph S2["Stage 2 — Parse"]
        direction TB
        B1[parse basic data] --> B2[parse story structure<br/>auto-link arcs → sagas]
    end

    subgraph S3["Stage 3 — Post-process"]
        direction TB
        C1[filter non-characters] --> C2[birth dates]
        C2 --> C3[load CoV / merge dupes]
        C3 --> C4[appearance analytics]
        C4 --> C5[affiliations · devil fruits ·<br/>haki · occupations]
        C5 --> C6[bios · importance]
    end

    subgraph S4["Stage 4 — Export"]
        direction TB
        D1[CSV export] --> D2[PostgreSQL full sync]
    end

    S1 --> S2 --> S3 --> S4
```

### RAG chatbot flow

```mermaid
flowchart TD
    Q[User question] --> LLM["Groq LLM<br/>(llama-3.3-70b)"]
    LLM -->|tool call| T{Which tool?}
    T -->|structured query| T1["query_database<br/>(SQL over DuckDB)"]
    T -->|semantic search| T2["search_wiki<br/>(vector similarity)"]
    T -->|character lookup| T3["get_character_profile<br/>(structured + intro text)"]
    T1 --> CTX[Retrieved context]
    T2 --> CTX
    T3 --> CTX
    CTX --> LLM
    LLM --> ANS[Grounded answer]
```

---

## 🚀 Quick start

### Prerequisites

Install [uv](https://github.com/astral-sh/uv):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

### Installation

```bash
git clone https://github.com/ismailsunni/onepieceofdata.git
cd onepieceofdata
make setup            # installs deps, creates dirs, sets up the dev environment
cp .env.example .env  # then edit as needed
```

### Run the pipeline

```bash
# Recommended two-command workflow
make run-data-pipeline   # Stages 1–3: scrape → parse → post-process (into DuckDB)
make run-all-exports     # Stage 4: export to CSV + PostgreSQL

# Or all-in-one (parallel)
make run-full-pipeline-parallel
```

Quick checks before a full run:

```bash
make status               # pipeline + config status
make test-scrape-parallel # scrape chapters 1–10 only
make config               # view current configuration
```

---

## 🗄️ Database schema

DuckDB is the source of truth. Core tables and their relationships:

```mermaid
erDiagram
    SAGA ||--o{ ARC : contains
    ARC ||--o{ CHAPTER : spans
    VOLUME ||--o{ CHAPTER : contains
    CHAPTER ||--o{ COC : appears_in
    CHARACTER ||--o{ COC : appears_in
    VOLUME ||--o{ COV : cover_of
    CHARACTER ||--o{ COV : on_cover
```

| Table | Description | Key fields |
|-------|-------------|------------|
| `saga` | Major story sagas | `saga_id`, `title`, `start_chapter`, `end_chapter` |
| `arc` | Story arcs (auto-linked to sagas via chapter range) | `arc_id`, `title`, `saga_id`, `start_chapter`, `end_chapter` |
| `volume` | Manga volumes | `number`, `title` |
| `chapter` | Individual chapters | `number`, `title`, `volume`, `num_page`, `date` |
| `character` | Character details + denormalized appearance analytics | `id`, `name`, `bounty`, `status`, `chapter_list`, `arc_list`, … |
| `coc` | Character-of-chapter (appearances) | `chapter`, `character`, `note` |
| `cov` | Character-on-volume (cover appearances) | `volume`, `character` |

**RAG / graph tables:** `wiki_text` (cleaned wikitext), `wiki_chunks` (embeddings),
`graph_nodes`, `graph_edges`, `graph_source_text`.

> **Notes**
> - Arc→saga linking is **computed** from chapter-range containment, not scraped.
> - The PostgreSQL export **excludes `coc`/`cov`** — appearance data is denormalized into
>   `character` for join-free analytics.
> - `SchemaMapper` handles DuckDB→PostgreSQL type conversion (including `INTEGER[]` arrays).

📖 Full schema: [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md)

### Example queries

```sql
-- Arcs in the East Blue saga
SELECT title, start_chapter, end_chapter FROM arc WHERE saga_id = 'east_blue';

-- Highest bounties
SELECT name, bounty FROM character
WHERE bounty IS NOT NULL ORDER BY bounty DESC LIMIT 10;

-- Characters appearing in a chapter
SELECT character FROM coc WHERE chapter = 1000;
```

---

## 🎮 Command-line interface

All commands run via `uv run onepieceofdata <command>` or the corresponding `make` target.

### Scraping

```bash
uv run onepieceofdata scrape-chapters --parallel
uv run onepieceofdata scrape-chapters --start-chapter 1 --end-chapter 100
uv run onepieceofdata scrape-volumes
uv run onepieceofdata scrape-characters --parallel
uv run onepieceofdata scrape-story-structure   # arcs + sagas
```

### Parsing & export

```bash
uv run onepieceofdata parse --create-tables
uv run onepieceofdata export --output-dir exports/          # CSV
uv run onepieceofdata export-postgres --mode full           # full sync
uv run onepieceofdata export-postgres --mode incremental    # changed tables only
uv run onepieceofdata sync-status
```

### Character management

```bash
make run-character-workflow         # extract → scrape → parse → merge → sync
uv run onepieceofdata extract-characters
uv run onepieceofdata merge-characters --dry-run   # preview dedupe
uv run onepieceofdata merge-characters             # apply (uses data/character_aliases.json)
uv run onepieceofdata sync-character-appearances
```

Characters may appear under multiple IDs (code names like *Mr. 1 / Daz Bonez*, epithets
like *Akainu / Sakazuki*, disguises like *Lucy / Sabo*). `merge-characters` consolidates
them.

### Modular pipeline targets

```bash
make run-all-scrapers          # Stage 1
make run-all-parsers           # Stage 2
make run-all-postprocessors    # Stage 3 (14 enrichment steps)
make run-all-exports           # Stage 4
```

---

## 🤖 RAG chatbot

A Groq-backed chatbot that answers One Piece questions, grounded in your data via
tool-calling (SQL, vector search, character profiles).

```bash
make wiki-scrape          # 1. fetch + clean wikitext into wiki_text
make embed-wiki           # 2. chunk + embed into wiki_chunks (all-MiniLM-L6-v2, 384-dim)
make chat                 # 3. start the chatbot
make search Q="gear 5"    # ad-hoc semantic search
make export-supabase-fts  # export wiki_text + wiki_chunks to Supabase w/ FTS indexes
```

**Tools available to the LLM:** `query_database` (SQL), `search_wiki` (vector search),
`get_character_profile` (structured fields + intro text).

Key files: `rag/chat.py`, `rag/tools.py`, `rag/tool_handlers.py`,
`embeddings/{chunker,embedder,vector_store}.py`.

---

## 🕸️ Story knowledge graph

Extracts entity/relationship triples from wiki text using an LLM (Groq or Anthropic),
then deterministically builds graph edges.

```bash
make graph-init-schema     # create graph tables
make graph-init-nodes      # populate graph_nodes from existing entities
make graph-sync-sources    # snapshot wiki sections into graph_source_text
make graph-extract         # extract triples (Groq); --scope important / --force available
make graph-build           # build graph_edges (deterministic, links extracted entities)
make export-postgres-graph # export graph tables to PostgreSQL (slow; run on demand)
```

Provider/model overrides exist for Claude Haiku/Sonnet (`graph-extract-haiku`,
`graph-extract-sonnet`).

---

## 🌐 Character network explorer

A local interactive web app for the chapter co-appearance network.

```bash
make run-network-explorer
# then open http://127.0.0.1:8765/web/network_explorer/index.html
```

Requires `exports/network_analysis/character_network_nodes_gt10.csv` and
`character_coappearance_edges_gt10.csv`. Filter by minimum appearances / co-appearance
weight, search and focus a node, zoom/pan/drag.

---

## 📅 Weekly update (new chapter released)

When a new chapter drops, bump `OP_LAST_CHAPTER` (and `OP_LAST_VOLUME` if needed) in
`.env`, then run a single command:

```bash
make update-new-chapter
```

This runs all five stages in order:

```mermaid
flowchart LR
    S1[1. run-data-pipeline<br/>scrape · parse · post-process] --> S2[2. run-all-exports<br/>CSV + PostgreSQL]
    S2 --> S3[3. wiki-scrape] --> S4[4. embed-wiki] --> S5[5. export-supabase-fts]
```

---

## ⚙️ Configuration

Managed via `.env` + `pydantic-settings` (`src/onepieceofdata/config/settings.py`).
See [.env.example](.env.example) for the full list.

```bash
# Content bounds
OP_LAST_CHAPTER=1183
OP_LAST_VOLUME=113

# Scraping
OP_MAX_WORKERS=4
OP_SCRAPING_DELAY=1.0
OP_MAX_RETRIES=3
OP_REQUEST_TIMEOUT=30

# Storage
OP_DATABASE_PATH=./data/onepiece.duckdb

# PostgreSQL (local or Supabase)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=onepiece
POSTGRES_USER=postgres
POSTGRES_PASSWORD=onepiece
# Or: POSTGRES_URL=postgresql://user:pass@host:port/database
```

### Local PostgreSQL via Docker

```bash
make postgres-init     # start PostgreSQL + pgAdmin and run a full export
make postgres-start    # just start the containers
make postgres-status   # sync status
make postgres-stop
```

- PostgreSQL: `localhost:5432` (`postgres` / `onepiece`)
- pgAdmin: `http://localhost:5050` (`admin@onepiece.com` / `admin`)

---

## 📁 Project structure

```
onepieceofdata/
├── src/onepieceofdata/
│   ├── cli/             # Click commands (commands.py, chat.py, embed.py, wiki_scrape.py)
│   ├── config/          # pydantic-settings configuration
│   ├── models/          # Pydantic data models
│   ├── scrapers/        # Chapter/Volume/Character/Arc/Saga + wiki-text scrapers
│   ├── parsers/         # Arc/Saga parsers + wikitext cleaner
│   ├── postprocessors/  # Enrichment steps (affiliations, devil fruits, haki, …)
│   ├── database/        # DuckDB ops, PostgreSQL export, schema mapper
│   ├── embeddings/      # Chunker, embedder, vector store
│   ├── rag/             # Chat loop + tool definitions/handlers
│   ├── graph/           # Knowledge-graph schema, extraction, builder
│   ├── api/             # Fandom API client + wikitext parser
│   └── utils/           # Logging, helpers, birth-date parsing
├── scripts/             # Supabase export, network explorer, comparisons
├── web/                 # Network explorer frontend
├── tests/               # pytest suite
├── docs/                # Schema, features, plans, PRD
├── Makefile             # All orchestration targets
└── pyproject.toml
```

---

## 🛠️ Development

```bash
make setup     # install deps + dev environment
make test      # pytest
make lint      # ruff
make format    # black
make check     # lint + test
make clean     # remove generated files
```

Tests use pytest markers `@pytest.mark.slow` and `@pytest.mark.integration`:

```bash
uv run pytest -m "not slow"
uv run pytest --cov=src/onepieceofdata
uv run pytest tests/test_config.py -v
```

---

## 📚 Documentation

- [docs/DATABASE_SCHEMA.md](docs/DATABASE_SCHEMA.md) — complete schema with examples
- [docs/FEATURES.md](docs/FEATURES.md) — feature overview & CLI reference
- [docs/PIPELINE.md](docs/PIPELINE.md) — pipeline details
- [docs/PLAN-RAG.md](docs/PLAN-RAG.md) — RAG design
- [docs/plan-story-graph.md](docs/plan-story-graph.md) — knowledge graph design

---

## 🤝 Contributing

1. Fork the repo and create a feature branch.
2. Make your changes.
3. Run `make check`.
4. Open a pull request.

---

## 📜 License

MIT — see [LICENSE](LICENSE).

## 🙏 Acknowledgments

- Data sourced from the [One Piece Fandom Wiki](https://onepiece.fandom.com/).
- Built with [uv](https://github.com/astral-sh/uv), [Pydantic](https://pydantic.dev/),
  [DuckDB](https://duckdb.org/), [Groq](https://groq.com/), and
  [sentence-transformers](https://www.sbert.net/).
- Header generated with [Font Generator](https://www.textstudio.com/).

---

### 🏴‍☠️ Sail into the world of One Piece data
