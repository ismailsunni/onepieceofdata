"""Tool handlers for RAG chat — execute tool calls against DuckDB."""

import duckdb


class ToolHandler:
    """Routes and executes tool calls against the One Piece DuckDB database."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._model = None  # lazy load embedding model

    @property
    def model(self):
        """Lazy-load the sentence-transformers embedding model."""
        if self._model is None:
            from ..embeddings.embedder import load_model

            self._model = load_model()
        return self._model

    def query_database(self, sql: str) -> str:
        """Execute a read-only SQL query. Only SELECT is allowed.

        Returns formatted results as a string, limited to 50 rows.
        """
        stripped = sql.strip().rstrip(";").strip()
        if not stripped.upper().startswith("SELECT"):
            return "Error: Only SELECT queries are allowed."

        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            result = conn.execute(stripped).fetchdf()
            if result.empty:
                return "Query returned no results."
            if len(result) > 50:
                result = result.head(50)
                truncated = True
            else:
                truncated = False
            text = result.to_string(index=False)
            if truncated:
                text += "\n... (truncated to 50 rows)"
            return text
        except Exception as e:
            return f"Query error: {e}"
        finally:
            conn.close()

    def search_wiki(self, query: str, limit: int = 5) -> str:
        """Embed query and vector search wiki_chunks.

        Returns formatted results with title, section, and snippet.
        """
        limit = max(1, min(limit, 10))

        from ..embeddings.vector_store import search_similar

        query_embedding = self.model.encode(query).tolist()
        results = search_similar(query_embedding, self.db_path, limit=limit)

        if not results:
            return "No wiki results found."

        lines = []
        for i, r in enumerate(results, 1):
            snippet = r["chunk_text"][:500]
            sim = f"{r['similarity']:.3f}"
            lines.append(
                f"{i}. [{sim}] {r['title']} :: {r['section_name']}\n   {snippet}"
            )
        return "\n\n".join(lines)

    def get_character_profile(self, name: str) -> str:
        """Fuzzy match a character name and return structured data + wiki intro."""
        conn = duckdb.connect(self.db_path, read_only=True)
        try:
            rows = conn.execute(
                "SELECT * FROM character WHERE name ILIKE ? LIMIT 5",
                [f"%{name}%"],
            ).fetchdf()

            if rows.empty:
                return f"Character '{name}' not found."

            # Take the best match (first result)
            char = rows.iloc[0]
            parts = [f"# {char.get('name', name)}"]

            fields = [
                ("Origin", "origin"),
                ("Status", "status"),
                ("Age", "age"),
                ("Birth", "birth"),
                ("Blood Type", "blood_type"),
                ("Bounty", "bounty"),
                ("Appearance Count", "appearance_count"),
                ("First Appearance", "first_appearance"),
                ("Last Appearance", "last_appearance"),
            ]
            for label, key in fields:
                val = char.get(key)
                if val is not None and str(val) not in ("", "None", "nan", "<NA>"):
                    parts.append(f"- **{label}**: {val}")

            # Arc and saga lists
            for label, key in [("Arcs", "arc_list"), ("Sagas", "saga_list")]:
                val = char.get(key)
                if val is not None and str(val) not in ("", "None", "nan", "<NA>"):
                    parts.append(f"- **{label}**: {val}")

            # Try to get wiki intro
            char_id = char.get("id", "")
            if char_id:
                try:
                    intro_row = conn.execute(
                        "SELECT intro_text FROM wiki_text WHERE page_id = ?",
                        [char_id],
                    ).fetchone()
                    if intro_row and intro_row[0]:
                        intro = intro_row[0][:1000]
                        parts.append(f"\n## Wiki Summary\n{intro}")
                except Exception:
                    pass  # wiki_text table may not exist

            if len(rows) > 1:
                other_names = ", ".join(rows["name"].iloc[1:].tolist())
                parts.append(f"\n_Other matches: {other_names}_")

            return "\n".join(parts)
        except Exception as e:
            return f"Error looking up character: {e}"
        finally:
            conn.close()

    def handle_tool_call(self, tool_name: str, arguments: dict) -> str:
        """Route a tool call to the right handler."""
        handlers = {
            "query_database": lambda args: self.query_database(args["sql"]),
            "search_wiki": lambda args: self.search_wiki(
                args["query"], args.get("limit", 5)
            ),
            "get_character_profile": lambda args: self.get_character_profile(
                args["name"]
            ),
        }
        handler = handlers.get(tool_name)
        if handler is None:
            return f"Unknown tool: {tool_name}"
        try:
            return handler(arguments)
        except Exception as e:
            return f"Tool error ({tool_name}): {e}"
