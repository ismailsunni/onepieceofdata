# One Piece of Data — RAG Chatbot Plan

**Goal:** A One Piece knowledge chatbot that answers questions using structured data + wiki narrative.
**Stack:** 100% free — Supabase pgvector, local embeddings, Groq LLM, existing Fandom API client.
**Date:** 2026-03-31

---

## Architecture

```
User: "What is Gear 5?"
         │
         ▼
┌──────────────────┐
│  Groq LLM        │  ← free tier (llama3-70b)
│  (tool-calling)   │
└────┬────┬────┬───┘
     │    │    │
     ▼    ▼    ▼
  Tool 1  Tool 2  Tool 3
  SQL DB  Vector  Character
  Query   Search  Lookup
     │    │    │
     ▼    ▼    ▼
  Supabase Supabase Supabase
  (tables) (pgvector)(both)
```

**Flow:**
1. User asks a question
2. LLM decides which tool(s) to call
3. Tools query Supabase (structured data and/or vector search)
4. LLM generates answer from retrieved context

---

## Tech Stack (all free)

| Component | Choice | Cost | Notes |
|---|---|---|---|
| **Database** | Supabase PostgreSQL | Free (500MB) | Already set up, has character/chapter/arc data |
| **Vector store** | Supabase pgvector | Free (included) | Same DB, enable `vector` extension |
| **Embeddings** | `all-MiniLM-L6-v2` | Free (local) | 384 dims, runs on CPU, ~2GB RAM |
| **LLM** | Groq free tier | Free (30 RPM) | llama3-70b or llama3.1-8b |
| **Wiki API** | Fandom MediaWiki API | Free | Existing `FandomAPIClient` in codebase |
| **Hosting** | None (CLI first) | Free | React app later if wanted |

**Estimated Supabase usage:**
- Existing tables: ~5MB
- Wiki text (1,553 pages × ~5KB avg): ~8MB
- Embeddings (15K chunks × 384 floats × 4 bytes): ~23MB
- **Total: ~36MB** (well within 500MB free tier)

---

## Existing Code to Reuse

Already in `src/onepieceofdata/`:
- `api/fandom_client.py` — `FandomAPIClient` with rate limiting, retry logic
- `api/wikitext_parser.py` — `WikitextParser` with template extraction
- `scrapers/character_api.py` — pattern for parallel API scraping
- `config/settings.py` — settings with `scraping_delay`
- DuckDB with all structured data (to be synced to Supabase)

---

## Phases

### Phase 1: Wiki Text Scraping & Storage

**Goal:** Scrape wiki pages and store clean text in Supabase.

#### 1.1 New Supabase table

```sql
-- Enable vector extension first
CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA extensions;

CREATE TABLE wiki_text (
  page_id TEXT PRIMARY KEY,          -- e.g. 'Monkey_D._Luffy'
  page_type TEXT NOT NULL,           -- 'character', 'arc', 'saga', 'devil_fruit'
  title TEXT NOT NULL,
  intro_text TEXT,                   -- first section (summary paragraph)
  full_text TEXT,                    -- all sections concatenated
  sections JSONB,                    -- {"Appearance": "...", "Personality": "...", ...}
  scraped_at TIMESTAMPTZ DEFAULT NOW()
);

-- RLS: public read
ALTER TABLE wiki_text ENABLE ROW LEVEL SECURITY;
CREATE POLICY "Public read" ON wiki_text FOR SELECT USING (true);
```

#### 1.2 Wikitext → plaintext parser

Extend existing `WikitextParser` with new methods:

```python
# src/onepieceofdata/parsers/wikitext_cleaner.py

def clean_wikitext(raw: str) -> str:
    """Convert raw wikitext to clean plaintext."""
    # Strip {{template}} blocks (infoboxes, navboxes, etc.)
    # Convert [[link|display]] → display
    # Convert [[link]] → link
    # Strip <ref>...</ref>
    # Strip HTML tags
    # Convert ==Section== → section breaks
    # Collapse whitespace
    
def extract_sections(raw: str) -> dict[str, str]:
    """Split wikitext into {section_name: clean_text} dict."""
    # Split on == headers ==
    # Clean each section
    # Return {"intro": "...", "Appearance": "...", "History": "...", ...}
```

#### 1.3 Wiki scraper

```python
# src/onepieceofdata/scrapers/wiki_text_scraper.py

class WikiTextScraper:
    """Scrape full wiki page text for characters, arcs, sagas."""
    
    def scrape_page(self, page_slug: str) -> dict:
        """Fetch and parse a single wiki page."""
        # Use existing FandomAPIClient
        # action=parse&page={slug}&prop=wikitext
        # Parse with wikitext_cleaner
        # Return {page_id, title, intro_text, full_text, sections}
    
    def scrape_all_characters(self):
        """Scrape all 1,509 character pages."""
        # Read character IDs from DB
        # Sequential (respect rate limit): 1 req/sec
        # ~25 min for all characters
        # Save to Supabase wiki_text table
    
    def scrape_arcs_and_sagas(self):
        """Scrape 33 arc + 11 saga pages."""
        # ~1 min
```

#### 1.4 CLI commands

```bash
make wiki-scrape              # Scrape all (characters + arcs + sagas)
make wiki-scrape-characters   # Characters only
make wiki-scrape-arcs         # Arcs and sagas only
make wiki-status              # Show scraping progress
```

**Scope:** 1,509 characters + 33 arcs + 11 sagas = 1,553 pages
**Time:** ~26 minutes at 1 req/sec
**Output:** `wiki_text` table populated in Supabase

---

### Phase 2: Embeddings & Vector Search

**Goal:** Chunk wiki text, generate embeddings, enable semantic search.

#### 2.1 Chunking

```python
# src/onepieceofdata/embeddings/chunker.py

def chunk_wiki_page(page: dict) -> list[dict]:
    """Split a wiki page into searchable chunks."""
    chunks = []
    for section_name, text in page['sections'].items():
        # If section > 800 tokens, split with 100-token overlap
        # Each chunk gets metadata: {page_id, page_type, title, section_name}
        chunks.append({
            'chunk_id': f"{page['page_id']}:{section_name}",
            'page_id': page['page_id'],
            'page_type': page['page_type'],
            'title': page['title'],
            'section_name': section_name,
            'text': text,
        })
    return chunks
```

**Estimated chunks:** ~10,000–15,000 (avg 7 sections × 1,553 pages)

#### 2.2 Embedding generation

```python
# src/onepieceofdata/embeddings/embedder.py

from sentence_transformers import SentenceTransformer

model = SentenceTransformer('all-MiniLM-L6-v2')  # 384 dims, ~80MB download

def embed_chunks(chunks: list[dict]) -> list[dict]:
    """Generate embeddings for all chunks."""
    texts = [c['text'] for c in chunks]
    embeddings = model.encode(texts, batch_size=64, show_progress_bar=True)
    for chunk, emb in zip(chunks, embeddings):
        chunk['embedding'] = emb.tolist()
    return chunks
```

**Performance on Intel UHD / 16GB RAM:**
- Model download: ~80MB (one-time)
- RAM usage: ~2GB during encoding
- Speed: ~100-200 chunks/sec on CPU
- Total time: ~1-2 minutes for 15K chunks

#### 2.3 Supabase vector table

```sql
CREATE TABLE wiki_chunks (
  chunk_id TEXT PRIMARY KEY,
  page_id TEXT REFERENCES wiki_text(page_id),
  page_type TEXT,
  title TEXT,
  section_name TEXT,
  chunk_text TEXT,
  embedding extensions.vector(384),
  metadata JSONB
);

-- Index for fast similarity search
CREATE INDEX wiki_chunks_embedding_idx ON wiki_chunks
  USING ivfflat (embedding extensions.vector_cosine_ops) WITH (lists = 50);
```

#### 2.4 Similarity search function

```sql
CREATE OR REPLACE FUNCTION search_wiki(
  query_embedding extensions.vector(384),
  match_count INT DEFAULT 5,
  filter_type TEXT DEFAULT NULL
)
RETURNS TABLE (
  chunk_id TEXT,
  page_id TEXT,
  title TEXT,
  section_name TEXT,
  chunk_text TEXT,
  similarity FLOAT
)
LANGUAGE sql STABLE
AS $$
  SELECT chunk_id, page_id, title, section_name, chunk_text,
    1 - (embedding <=> query_embedding) AS similarity
  FROM wiki_chunks
  WHERE (filter_type IS NULL OR page_type = filter_type)
  ORDER BY embedding <=> query_embedding
  LIMIT match_count;
$$;
```

#### 2.5 CLI commands

```bash
make embed-wiki           # Chunk + embed + upload to Supabase
make embed-status         # Show embedding progress
make search "gear 5"      # Test search from CLI
```

---

### Phase 3: RAG Chat

**Goal:** LLM answers One Piece questions using tools.

#### 3.1 Tool definitions

```python
# src/onepieceofdata/rag/tools.py

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": "Query the One Piece structured database via SQL. Use for: bounties, ages, appearance counts, chapter numbers, arc/saga info, character lists, rankings, statistics.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "PostgreSQL query. Tables: character (id, name, bounty, age, status, origin, first_appearance, last_appearance, appearance_count), chapter (number, title, volume, num_page, date), arc (arc_id, title, start_chapter, end_chapter, saga_id), saga (saga_id, title, start_chapter, end_chapter), volume (number, title)"
                    }
                },
                "required": ["sql"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "search_wiki",
            "description": "Semantic search over One Piece wiki articles. Use for: character descriptions, abilities, devil fruits, backstories, relationships, plot summaries, personality traits.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "Natural language search query"},
                    "limit": {"type": "integer", "description": "Number of results (default 5, max 10)"}
                },
                "required": ["query"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_character_profile",
            "description": "Get a complete character profile: structured data + wiki summary. Use when user asks about a specific character.",
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Character name (e.g., 'Luffy', 'Roronoa Zoro')"}
                },
                "required": ["name"]
            }
        }
    }
]
```

#### 3.2 Tool implementations

```python
# src/onepieceofdata/rag/tool_handlers.py

class ToolHandler:
    def __init__(self, supabase_client, embedding_model):
        self.supabase = supabase_client
        self.model = embedding_model
    
    def query_database(self, sql: str) -> str:
        """Execute SQL against Supabase (read-only, with guardrails)."""
        # Validate: only SELECT allowed
        # Execute via supabase.rpc or direct postgres
        # Return formatted results
    
    def search_wiki(self, query: str, limit: int = 5) -> str:
        """Embed query → vector search → return relevant chunks."""
        # Embed the query
        # Call search_wiki RPC
        # Format results with source attribution
    
    def get_character_profile(self, name: str) -> str:
        """Fuzzy match name → fetch structured data + wiki intro."""
        # Fuzzy match against character table
        # Fetch character row + wiki_text intro
        # Return combined profile
```

#### 3.3 Chat loop

```python
# src/onepieceofdata/rag/chat.py

from groq import Groq

SYSTEM_PROMPT = """You are a One Piece expert assistant. You have access to:
1. A structured database with character stats, chapter info, arcs, and sagas
2. Wiki articles with descriptions, abilities, backstories, and plot details

Use the tools to find accurate information before answering.
Always cite your sources (database query or wiki article).
Data sourced from One Piece Wiki (CC-BY-SA)."""

class OnePieceChat:
    def __init__(self):
        self.client = Groq()  # uses GROQ_API_KEY env var
        self.tools = ToolHandler(...)
        self.history = []
    
    def chat(self, user_message: str) -> str:
        """Process a user message and return an answer."""
        # Add to history
        # Call Groq with tools
        # Handle tool calls
        # Return final response
```

#### 3.4 CLI chat interface

```bash
make chat                 # Start interactive chat
# or
uv run opod chat          # Same thing

> What is Luffy's current bounty?
🔍 Querying database...
Monkey D. Luffy's current bounty is ₿3,000,000,000 (3 billion berries).
He is ranked 10th among all known bounties.

> Explain Gear 5
🔍 Searching wiki...
Gear 5 is the awakened form of Luffy's Devil Fruit, the Hito Hito no Mi...
```

#### 3.5 Dependencies

```toml
# Add to pyproject.toml
[project.optional-dependencies]
rag = [
    "sentence-transformers>=2.2.0",
    "groq>=0.4.0",
    "supabase>=2.0.0",
]
```

---

### Phase 4: React Chat UI (Optional, Future)

**Goal:** Add chat to the existing React app.

#### 4.1 Architecture options

**Option A: Supabase Edge Function** (recommended)
- Deploy the RAG logic as a Supabase Edge Function (Deno)
- React app calls the function directly
- No separate server needed
- Free on Supabase (500K invocations/month)

**Option B: FastAPI on fly.io**
- Deploy Python RAG as a FastAPI app
- Free tier on fly.io (3 shared VMs)
- More flexible but another service to manage

#### 4.2 Chat UI

- New page in React app: `/chat`
- Simple chat interface: message input, conversation history
- Streaming responses (if using Option B)
- Show tool calls inline ("🔍 Searching wiki..." "📊 Querying database...")

---

## File Structure

```
src/onepieceofdata/
  api/
    fandom_client.py      # EXISTING — reuse
    wikitext_parser.py     # EXISTING — reuse
  parsers/
    wikitext_cleaner.py    # NEW — wikitext → plaintext + sections
  scrapers/
    wiki_text_scraper.py   # NEW — scrape full pages
  embeddings/
    chunker.py             # NEW — split text into chunks
    embedder.py            # NEW — generate embeddings (sentence-transformers)
    uploader.py            # NEW — upload to Supabase pgvector
  rag/
    tools.py               # NEW — tool definitions
    tool_handlers.py       # NEW — tool implementations
    chat.py                # NEW — chat loop with Groq
    
supabase/
  migrations/
    002_wiki_text.sql      # NEW — wiki_text + wiki_chunks tables
    
Makefile                   # ADD targets: wiki-scrape, embed-wiki, chat
```

---

## Execution Order

```
Phase 1 (wiki scraping)     Phase 2 (embeddings)     Phase 3 (chat)
┌─────────────────────┐     ┌──────────────────┐     ┌──────────────┐
│ 1. wikitext_cleaner │     │ 1. chunker       │     │ 1. tools     │
│ 2. wiki_text_scraper│────►│ 2. embedder      │────►│ 2. handlers  │
│ 3. Supabase upload  │     │ 3. Supabase      │     │ 3. chat loop │
│ 4. CLI commands     │     │    pgvector       │     │ 4. CLI       │
└─────────────────────┘     └──────────────────┘     └──────────────┘
      ~2 hours                   ~2 hours                ~3 hours
```

---

## Estimated Resources

| Step | Time | RAM | Disk | Network |
|---|---|---|---|---|
| Wiki scraping | ~26 min | Minimal | ~8MB in Supabase | 1,553 API calls |
| Embedding model download | 1 min | ~2GB | 80MB | One-time |
| Embedding generation | ~2 min | ~2GB | - | - |
| Vector upload | ~1 min | Minimal | ~23MB in Supabase | Upload |
| **Total Supabase** | - | - | **~36MB / 500MB** | - |

---

## Setup Instructions (for later)

```bash
# 1. Get a free Groq API key
# → https://console.groq.com/keys

# 2. Enable pgvector in Supabase
# → Dashboard → Database → Extensions → search "vector" → enable

# 3. Install RAG dependencies
uv pip install -e ".[rag]"

# 4. Set env vars
echo "GROQ_API_KEY=gsk_..." >> .env

# 5. Run the pipeline
make wiki-scrape          # ~26 min
make embed-wiki           # ~3 min
make chat                 # start chatting!
```

---

## License & Attribution

- Wiki text: **CC-BY-SA** (One Piece Fandom Wiki)
- Include in app: "Data sourced from One Piece Wiki (CC-BY-SA)"
- Groq: free tier, 30 requests/minute, 6000 tokens/minute for llama3-70b
- All code: MIT (same as existing project)
