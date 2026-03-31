"""Tool definitions for Groq/OpenAI function-calling format."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "query_database",
            "description": (
                "Execute a read-only SQL SELECT query against the One Piece database. "
                "Available tables: character (id, name, origin, status, birth, blood_type, "
                "bounty, age, appearance_count, first_appearance, last_appearance, "
                "chapter_list, volume_list, arc_list, saga_list, cover_volume_list, "
                "cover_appearance_count), chapter (number, volume, title, num_page, date), "
                "arc (arc_id, title, start_chapter, end_chapter, saga_id), "
                "saga (saga_id, title, start_chapter, end_chapter), "
                "volume (number, title), coc (chapter, character, note), "
                "cov (volume, character). Only SELECT queries are allowed."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "A read-only SQL SELECT query.",
                    }
                },
                "required": ["sql"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "search_wiki",
            "description": (
                "Semantic search over 19,000+ wiki article chunks from the One Piece Wiki. "
                "Use this for descriptions, backstory, abilities, events, and explanations. "
                "Returns the most relevant text snippets."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural language search query.",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Number of results to return (default 5, max 10).",
                        "default": 5,
                    },
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_character_profile",
            "description": (
                "Look up a character by name (fuzzy match) and return their structured "
                "profile data (bounty, age, status, appearances, etc.) combined with "
                "their wiki introduction text."
            ),
            "parameters": {
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Character name to look up (e.g. 'Luffy', 'Zoro').",
                    }
                },
                "required": ["name"],
            },
        },
    },
]
