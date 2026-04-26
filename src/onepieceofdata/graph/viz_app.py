"""Interactive Streamlit + pyvis viewer for graph_edges.

Run with: streamlit run src/onepieceofdata/graph/viz_app.py

Controls in the sidebar let you focus on a node, expand by hops, filter
by relation/node type and confidence, and cap edge counts so the browser
doesn't choke on the 19K-edge full graph.
"""

from __future__ import annotations

import os
from collections import defaultdict
from pathlib import Path

import duckdb
import streamlit as st
from pyvis.network import Network


# --- data loading -----------------------------------------------------------


@st.cache_resource
def _connect(db_path: str) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(db_path, read_only=True)


@st.cache_data(show_spinner="Loading graph from DuckDB…")
def load_graph(db_path: str) -> tuple[dict, dict, list]:
    """Return (nodes_by_id, name_to_id, edges_list)."""
    conn = _connect(db_path)
    nodes_rows = conn.execute(
        "SELECT id, type, canonical_name FROM graph_nodes"
    ).fetchall()
    edges_rows = conn.execute(
        """
        SELECT subject_id, relation, object_id, confidence,
               evidence_chapter, evidence_text
        FROM graph_edges
        """
    ).fetchall()
    nodes = {
        nid: {"type": ntype, "name": name}
        for nid, ntype, name in nodes_rows
    }
    name_to_id = {n["name"]: nid for nid, n in nodes.items()}
    edges = [
        {
            "subj": s,
            "rel": r,
            "obj": o,
            "conf": float(c) if c is not None else 0.0,
            "chapter": ch,
            "evidence": ev or "",
        }
        for s, r, o, c, ch, ev in edges_rows
    ]
    return nodes, name_to_id, edges


# --- filtering --------------------------------------------------------------


def filter_edges(edges, *, relations, min_conf):
    return [
        e
        for e in edges
        if e["rel"] in relations and e["conf"] >= min_conf
    ]


def bfs_subgraph(focus_id, edges, hops, max_edges):
    """Bidirectional BFS up to `hops` hops, capped at max_edges."""
    if focus_id is None:
        return edges[:max_edges]

    adj = defaultdict(list)  # node_id → list of edge dicts
    for e in edges:
        adj[e["subj"]].append(e)
        adj[e["obj"]].append(e)

    visited_nodes = {focus_id}
    visited_edges = []
    seen_edge_keys = set()
    frontier = {focus_id}

    for _ in range(hops):
        if len(visited_edges) >= max_edges:
            break
        next_frontier = set()
        for n in frontier:
            for e in adj[n]:
                key = (e["subj"], e["rel"], e["obj"])
                if key in seen_edge_keys:
                    continue
                seen_edge_keys.add(key)
                visited_edges.append(e)
                visited_nodes.add(e["subj"])
                visited_nodes.add(e["obj"])
                if e["subj"] == n:
                    next_frontier.add(e["obj"])
                else:
                    next_frontier.add(e["subj"])
                if len(visited_edges) >= max_edges:
                    break
            if len(visited_edges) >= max_edges:
                break
        frontier = next_frontier - visited_nodes
        if not frontier:
            break
    return visited_edges


# --- rendering --------------------------------------------------------------

NODE_COLORS = {
    "character": "#4A90E2",
    "crew": "#E2725B",
    "organization": "#F5A623",
    "saga": "#9B59B6",
    "arc": "#7F8C8D",
    "devil_fruit": "#27AE60",
    "location": "#16A085",
}

REL_COLORS = {
    "fought": "#C0392B",
    "defeated_by": "#922B21",
    "enemy_of": "#E74C3C",
    "ally_of": "#27AE60",
    "member_of_crew": "#3498DB",
    "captain_of": "#2980B9",
    "affiliated_with": "#5DADE2",
    "family_of": "#8E44AD",
    "mentor_of": "#D4AC0D",
    "ate_devil_fruit": "#16A085",
    "originates_from": "#7F8C8D",
    "has_bounty_of": "#34495E",
}


def render_pyvis(nodes, edges, focus_id, height_px=720) -> str:
    net = Network(
        height=f"{height_px}px",
        width="100%",
        bgcolor="#0E1117",
        font_color="#FAFAFA",
        directed=True,
        notebook=False,
    )
    net.barnes_hut(
        gravity=-8000,
        central_gravity=0.3,
        spring_length=180,
        spring_strength=0.04,
        damping=0.25,
    )

    used_node_ids = set()
    for e in edges:
        used_node_ids.add(e["subj"])
        used_node_ids.add(e["obj"])

    for nid in used_node_ids:
        n = nodes.get(nid)
        if n is None:
            continue
        is_focus = nid == focus_id
        net.add_node(
            nid,
            label=n["name"],
            title=f"{n['name']} ({n['type']})",
            color=NODE_COLORS.get(n["type"], "#95A5A6"),
            size=30 if is_focus else 14,
            borderWidth=4 if is_focus else 1,
            font={"size": 18 if is_focus else 12, "color": "#FAFAFA"},
        )

    for e in edges:
        if e["subj"] not in used_node_ids or e["obj"] not in used_node_ids:
            continue
        title = f"{e['rel']} (conf {e['conf']:.2f})"
        if e["evidence"]:
            ev_short = e["evidence"][:160].replace("\n", " ")
            title += f"\n\n{ev_short}"
        net.add_edge(
            e["subj"],
            e["obj"],
            label=e["rel"],
            title=title,
            color=REL_COLORS.get(e["rel"], "#7F8C8D"),
            width=1 + e["conf"] * 2,
            arrows="to",
            font={"size": 9, "color": "#BDC3C7", "strokeWidth": 0},
        )

    net.set_options(
        """
        {
          "edges": {"smooth": {"type": "continuous"}, "selectionWidth": 3},
          "interaction": {"hover": true, "tooltipDelay": 100},
          "physics": {"stabilization": {"iterations": 150}}
        }
        """
    )
    return net.generate_html(notebook=False)


# --- app --------------------------------------------------------------------


def main():
    st.set_page_config(
        page_title="One Piece Story Graph",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.title("One Piece Story Graph")

    db_path = os.environ.get("OP_DATABASE_PATH", "./data/onepiece.duckdb")
    if not Path(db_path).exists():
        st.error(f"DB not found: {db_path}")
        st.stop()

    nodes, name_to_id, all_edges = load_graph(db_path)
    all_relations = sorted({e["rel"] for e in all_edges})
    all_node_types = sorted({n["type"] for n in nodes.values()})

    # --- sidebar -----------------------------------------------------------
    st.sidebar.header("Focus")
    sorted_names = sorted(name_to_id.keys())
    focus_default_idx = (
        sorted_names.index("Monkey D. Luffy")
        if "Monkey D. Luffy" in sorted_names
        else 0
    )
    focus_name = st.sidebar.selectbox(
        "Center on", sorted_names, index=focus_default_idx
    )
    focus_id = name_to_id.get(focus_name)
    hops = st.sidebar.slider("Hops from focus", 1, 4, 2)

    st.sidebar.header("Filters")
    min_conf = st.sidebar.slider("Min confidence", 0.6, 1.0, 0.7, 0.05)
    relations = st.sidebar.multiselect(
        "Relation types", all_relations, default=all_relations
    )
    node_types = st.sidebar.multiselect(
        "Node types", all_node_types, default=all_node_types
    )
    max_edges = st.sidebar.slider("Max edges to render", 50, 1500, 400, 50)

    st.sidebar.header("Display")
    height = st.sidebar.slider("Graph height (px)", 400, 1200, 720, 40)

    # --- filtering pipeline ------------------------------------------------
    edges = filter_edges(all_edges, relations=set(relations), min_conf=min_conf)
    edges = [
        e
        for e in edges
        if nodes.get(e["subj"], {}).get("type") in node_types
        and nodes.get(e["obj"], {}).get("type") in node_types
    ]
    visible = bfs_subgraph(focus_id, edges, hops, max_edges)

    # --- header stats ------------------------------------------------------
    total_edges = len(all_edges)
    visible_nodes = {nid for e in visible for nid in (e["subj"], e["obj"])}
    cols = st.columns(4)
    cols[0].metric("Total edges in DB", f"{total_edges:,}")
    cols[1].metric("After filters", f"{len(edges):,}")
    cols[2].metric("Rendered (BFS-capped)", f"{len(visible):,}")
    cols[3].metric("Visible nodes", f"{len(visible_nodes):,}")

    # --- main viz ----------------------------------------------------------
    if not visible:
        st.warning("No edges match the current filters. Loosen min confidence "
                   "or pick a more central node.")
        return

    html = render_pyvis(nodes, visible, focus_id, height_px=height)
    st.components.v1.html(html, height=height + 40, scrolling=False)

    # --- edge inspector ----------------------------------------------------
    with st.expander(f"Edge list ({len(visible)} rows) — sortable"):
        rows = [
            {
                "subject": nodes[e["subj"]]["name"],
                "relation": e["rel"],
                "object": nodes[e["obj"]]["name"],
                "conf": round(e["conf"], 2),
                "chapter": e["chapter"],
                "evidence": e["evidence"][:200],
            }
            for e in visible
            if e["subj"] in nodes and e["obj"] in nodes
        ]
        st.dataframe(rows, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
