"""Export wiki data to Supabase with full-text search for RAG deployment."""

import os
import json
import duckdb
import psycopg2
from dotenv import load_dotenv

from onepieceofdata.config.settings import get_settings

load_dotenv('.env')


def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        dbname=os.getenv('POSTGRES_DB'),
        user=os.getenv('POSTGRES_USER'),
        password=os.getenv('POSTGRES_PASSWORD'),
        sslmode='require',
    )


def create_tables(cur):
    """Create wiki tables with full-text search indexes."""
    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_text (
            page_id TEXT PRIMARY KEY,
            page_type TEXT NOT NULL,
            title TEXT NOT NULL,
            intro_text TEXT,
            full_text TEXT,
            sections JSONB,
            scraped_at TIMESTAMPTZ DEFAULT NOW()
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS wiki_chunks (
            chunk_id TEXT PRIMARY KEY,
            page_id TEXT,
            page_type TEXT,
            title TEXT,
            section_name TEXT,
            chunk_text TEXT,
            metadata JSONB
        );
    """)

    # Full-text search indexes
    cur.execute("""
        CREATE INDEX IF NOT EXISTS wiki_text_fts_idx ON wiki_text
            USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(full_text, '')));
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS wiki_chunks_fts_idx ON wiki_chunks
            USING gin(to_tsvector('english', coalesce(title, '') || ' ' || coalesce(section_name, '') || ' ' || coalesce(chunk_text, '')));
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS wiki_chunks_page_type_idx ON wiki_chunks(page_type);")
    cur.execute("CREATE INDEX IF NOT EXISTS wiki_text_page_type_idx ON wiki_text(page_type);")

    # Search function
    cur.execute("""
        CREATE OR REPLACE FUNCTION search_wiki_fts(
            query_text TEXT,
            match_count INT DEFAULT 5,
            filter_type TEXT DEFAULT NULL
        )
        RETURNS TABLE (
            chunk_id TEXT,
            page_id TEXT,
            title TEXT,
            section_name TEXT,
            chunk_text TEXT,
            rank REAL
        )
        LANGUAGE sql STABLE
        AS $$
            SELECT chunk_id, page_id, title, section_name, chunk_text,
                ts_rank(
                    to_tsvector('english', coalesce(title, '') || ' ' || coalesce(section_name, '') || ' ' || coalesce(chunk_text, '')),
                    websearch_to_tsquery('english', query_text)
                ) AS rank
            FROM wiki_chunks
            WHERE
                to_tsvector('english', coalesce(title, '') || ' ' || coalesce(section_name, '') || ' ' || coalesce(chunk_text, ''))
                @@ websearch_to_tsquery('english', query_text)
                AND (filter_type IS NULL OR page_type = filter_type)
            ORDER BY rank DESC
            LIMIT match_count;
        $$;
    """)

    # RLS
    cur.execute("ALTER TABLE wiki_text ENABLE ROW LEVEL SECURITY;")
    cur.execute("DROP POLICY IF EXISTS \"Public read wiki_text\" ON wiki_text;")
    cur.execute("CREATE POLICY \"Public read wiki_text\" ON wiki_text FOR SELECT USING (true);")
    cur.execute("ALTER TABLE wiki_chunks ENABLE ROW LEVEL SECURITY;")
    cur.execute("DROP POLICY IF EXISTS \"Public read wiki_chunks\" ON wiki_chunks;")
    cur.execute("CREATE POLICY \"Public read wiki_chunks\" ON wiki_chunks FOR SELECT USING (true);")

    print("✅ Tables, indexes, search function, and RLS created")


def export_wiki_text(cur, duck):
    rows = duck.execute(
        "SELECT page_id, page_type, title, intro_text, full_text, sections, scraped_at FROM wiki_text"
    ).fetchall()
    print(f"Exporting {len(rows)} wiki_text rows...")

    for r in rows:
        sections_json = json.dumps(r[5]) if isinstance(r[5], dict) else r[5]
        cur.execute(
            """INSERT INTO wiki_text (page_id, page_type, title, intro_text, full_text, sections, scraped_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (page_id) DO UPDATE SET
                   page_type=EXCLUDED.page_type, title=EXCLUDED.title,
                   intro_text=EXCLUDED.intro_text, full_text=EXCLUDED.full_text,
                   sections=EXCLUDED.sections, scraped_at=EXCLUDED.scraped_at""",
            (r[0], r[1], r[2], r[3], r[4], sections_json, r[6]),
        )

    print(f"✅ wiki_text: {len(rows)} rows exported")


def export_wiki_chunks(cur, duck):
    rows = duck.execute(
        "SELECT chunk_id, page_id, page_type, title, section_name, chunk_text FROM wiki_chunks"
    ).fetchall()
    print(f"Exporting {len(rows)} wiki_chunks rows...")

    batch = []
    for i, r in enumerate(rows):
        batch.append(r)
        if len(batch) >= 500 or i == len(rows) - 1:
            args = ",".join(
                cur.mogrify("(%s,%s,%s,%s,%s,%s)", row).decode() for row in batch
            )
            cur.execute(
                f"""INSERT INTO wiki_chunks (chunk_id, page_id, page_type, title, section_name, chunk_text)
                    VALUES {args}
                    ON CONFLICT (chunk_id) DO UPDATE SET
                        page_id=EXCLUDED.page_id, page_type=EXCLUDED.page_type,
                        title=EXCLUDED.title, section_name=EXCLUDED.section_name,
                        chunk_text=EXCLUDED.chunk_text"""
            )
            batch = []
            if (i + 1) % 5000 == 0 or i == len(rows) - 1:
                print(f"  {i+1}/{len(rows)}")

    print(f"✅ wiki_chunks: {len(rows)} rows exported")


def main():
    pg = get_pg_conn()
    pg.autocommit = True
    cur = pg.cursor()

    create_tables(cur)

    duck = duckdb.connect(str(get_settings().database_path), read_only=True)
    export_wiki_text(cur, duck)
    export_wiki_chunks(cur, duck)
    duck.close()

    # Verify
    cur.execute("SELECT count(*) FROM wiki_text")
    print(f"\nSupabase wiki_text: {cur.fetchone()[0]} rows")
    cur.execute("SELECT count(*) FROM wiki_chunks")
    print(f"Supabase wiki_chunks: {cur.fetchone()[0]} rows")

    # Test FTS
    cur.execute("SELECT * FROM search_wiki_fts('Luffy devil fruit', 3)")
    results = cur.fetchall()
    print(f"\nFTS test 'Luffy devil fruit': {len(results)} results")
    for r in results:
        print(f"  [{r[5]:.4f}] {r[2]} :: {r[3]} — {r[4][:80]}...")

    pg.close()
    print("\n✅ Export complete!")


if __name__ == "__main__":
    main()
