from __future__ import annotations

import argparse
import os
import sys
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Serve the One Piece character network explorer web app."
    )
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind")
    parser.add_argument("--port", type=int, default=8765, help="Port to bind")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    os.chdir(repo_root)

    nodes_csv = repo_root / "exports/network_analysis/character_network_nodes_gt10.csv"
    edges_csv = (
        repo_root / "exports/network_analysis/character_coappearance_edges_gt10.csv"
    )

    if not nodes_csv.exists() or not edges_csv.exists():
        print(
            "Missing network CSV outputs. Generate them first in exports/network_analysis/",
            file=sys.stderr,
        )
        return 1

    app_path = "/web/network_explorer/index.html"
    url = f"http://{args.host}:{args.port}{app_path}"

    server = ThreadingHTTPServer((args.host, args.port), SimpleHTTPRequestHandler)
    print(f"Network explorer is running at: {url}")
    print("Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping network explorer...")
    finally:
        server.server_close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())