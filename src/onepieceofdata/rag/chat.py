"""RAG chat engine using Groq with tool-calling against One Piece data."""

import json

from groq import Groq

from .tool_handlers import ToolHandler
from .tools import TOOLS

SYSTEM_PROMPT = """\
You are a One Piece expert assistant with access to a comprehensive database and wiki.

You have 3 tools:
1. query_database — SQL queries for structured data (bounties, appearances, chapters, arcs, etc.)
2. search_wiki — semantic search over wiki articles for descriptions, abilities, backstory, etc.
3. get_character_profile — get a complete character profile (structured + wiki summary)

Guidelines:
- Use tools to find accurate information before answering
- For character questions, start with get_character_profile
- For "list" or "ranking" questions, use query_database with SQL
- For "explain" or "describe" questions, use search_wiki
- Cite your sources briefly
- Be concise but thorough
- Data sourced from One Piece Wiki (CC-BY-SA)"""


class OnePieceChat:
    """Interactive chat with tool-calling over One Piece data."""

    def __init__(self, db_path: str, model: str = "llama-3.3-70b-versatile"):
        self.client = Groq()
        self.tool_handler = ToolHandler(db_path)
        self.model = model
        self.history: list[dict] = [{"role": "system", "content": SYSTEM_PROMPT}]

    def chat(self, user_message: str, on_tool_call=None) -> str:
        """Process user message, handle tool calls, return final answer.

        Args:
            user_message: The user's input text.
            on_tool_call: Optional callback(tool_name, arguments) called when
                a tool is invoked, for UI feedback.
        """
        self.history.append({"role": "user", "content": user_message})

        response = self.client.chat.completions.create(
            model=self.model,
            messages=self.history,
            tools=TOOLS,
            tool_choice="auto",
            max_tokens=2048,
        )

        message = response.choices[0].message

        # Handle tool calls (may be multiple rounds)
        while message.tool_calls:
            self.history.append(message)

            for tool_call in message.tool_calls:
                arguments = json.loads(tool_call.function.arguments)
                if on_tool_call:
                    on_tool_call(tool_call.function.name, arguments)

                result = self.tool_handler.handle_tool_call(
                    tool_call.function.name, arguments
                )
                self.history.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_call.id,
                        "content": result,
                    }
                )

            # Get next response (may have more tool calls or final answer)
            response = self.client.chat.completions.create(
                model=self.model,
                messages=self.history,
                tools=TOOLS,
                tool_choice="auto",
                max_tokens=2048,
            )
            message = response.choices[0].message

        # Final text response
        self.history.append(message)
        return message.content or ""

    def reset(self):
        """Clear conversation history."""
        self.history = [{"role": "system", "content": SYSTEM_PROMPT}]
