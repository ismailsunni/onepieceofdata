"""Interactive One Piece chatbot CLI.

Usage:
    python -m onepieceofdata.cli.chat
    python -m onepieceofdata.cli.chat --db ./data/onepiece.duckdb
"""

import argparse
import sys

from dotenv import load_dotenv

from onepieceofdata.config.settings import get_settings


TOOL_LABELS = {
    "query_database": "Running SQL query",
    "search_wiki": "Searching wiki",
    "get_character_profile": "Looking up character",
}


def _on_tool_call(tool_name: str, arguments: dict):
    """Print tool call feedback to stderr."""
    label = TOOL_LABELS.get(tool_name, tool_name)
    detail = ""
    if tool_name == "query_database":
        sql = arguments.get("sql", "")
        detail = f": {sql[:80]}" if sql else ""
    elif tool_name == "search_wiki":
        detail = f": {arguments.get('query', '')}"
    elif tool_name == "get_character_profile":
        detail = f": {arguments.get('name', '')}"
    print(f"  > {label}{detail}", file=sys.stderr, flush=True)


def main():
    load_dotenv()

    default_db = str(get_settings().database_path)
    parser = argparse.ArgumentParser(description="One Piece chatbot")
    parser.add_argument(
        "--db",
        default=default_db,
        help=f"Path to DuckDB database (default: {default_db})",
    )
    parser.add_argument(
        "--model",
        default="llama-3.3-70b-versatile",
        help="Groq model name (default: llama-3.3-70b-versatile)",
    )
    args = parser.parse_args()

    from ..rag.chat import OnePieceChat

    bot = OnePieceChat(db_path=args.db, model=args.model)

    print("One Piece Chat (powered by Groq + RAG)")
    print("Type /help for commands, /quit to exit.\n")
    print("Data sourced from One Piece Wiki (CC-BY-SA)\n")

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not user_input:
            continue

        # Handle slash commands
        if user_input.startswith("/"):
            cmd = user_input.lower()
            if cmd in ("/quit", "/exit", "/q"):
                print("Goodbye!")
                break
            elif cmd == "/reset":
                bot.reset()
                print("Conversation reset.\n")
                continue
            elif cmd == "/help":
                print("Commands:")
                print("  /reset  — Clear conversation history")
                print("  /quit   — Exit the chatbot")
                print("  /help   — Show this help\n")
                continue
            # Fall through: treat as normal message if not a known command

        # Multi-line support: lines ending with \ continue
        while user_input.endswith("\\"):
            try:
                continuation = input("... ")
                user_input = user_input[:-1] + "\n" + continuation
            except (EOFError, KeyboardInterrupt):
                break

        print("Thinking...", file=sys.stderr, flush=True)

        try:
            answer = bot.chat(user_input, on_tool_call=_on_tool_call)
            print(f"\nAssistant: {answer}\n")
        except Exception as e:
            error_msg = str(e)
            if "rate_limit" in error_msg.lower() or "429" in error_msg:
                print(
                    "\nRate limited by Groq. Please wait a moment and try again.\n",
                    file=sys.stderr,
                )
            else:
                print(f"\nError: {e}\n", file=sys.stderr)


if __name__ == "__main__":
    main()
