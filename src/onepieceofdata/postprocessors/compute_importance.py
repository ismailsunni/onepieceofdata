"""Post-processor to compute a character importance score and tier.

Combines signals that already exist in the database (chapter/volume appearances,
bounty, saga breadth, bio completeness) with categorical signals read from
``data/characters_detail.json`` (devil fruit presence, affiliation tier).

No wiki-text or external lookups — purely structured attributes we have.

Output:
  * ``character_importance`` table with per-signal breakdown (for tuning).
  * ``importance_score`` (DOUBLE) and ``importance_tier`` (VARCHAR) columns on
    the ``character`` table for convenience.

Tier cutoffs are percentile-based on the final score:
  S: top ~1%    (≥ p99)
  A: next ~4%   (p95–p99)
  B: next ~15%  (p80–p95)
  C: next ~30%  (p50–p80)
  D: rest
"""

from __future__ import annotations

import csv
import json
import math
import re
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

import duckdb
import yaml
from loguru import logger


# ---------------------------------------------------------------------------
# Default weights — mirror ``config/importance_weights.yaml`` and act as a
# fallback when the YAML is absent. Tweak the YAML, not these. Must sum to
# 1.0 so the final score stays in [0, 1].
# ---------------------------------------------------------------------------
DEFAULT_WEIGHTS: Dict[str, float] = {
    "appearance": 0.25,   # log-normalized chapter appearance count
    "saga_breadth": 0.20, # distinct sagas the character shows up in
    "bounty": 0.15,       # log-normalized bounty
    "affiliation": 0.15,  # keyword-matched affiliation tier
    "cover": 0.10,        # log-normalized volume-cover appearances
    "arc_main": 0.05,     # arcs where the character appears in ≥50% of chapters
    "devil_fruit": 0.05,  # binary: has a devil fruit
    "bio_completeness": 0.05,  # proxy for editor attention
}
assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9, "DEFAULT_WEIGHTS must sum to 1.0"

DEFAULT_ARC_MAIN_THRESHOLD = 0.5
DEFAULT_WEIGHTS_PATH = Path("config/importance_weights.yaml")

# Kept for back-compat with callers that imported these module-level names.
# compute_importance() re-resolves them from the YAML on every call.
WEIGHTS: Dict[str, float] = dict(DEFAULT_WEIGHTS)
ARC_MAIN_THRESHOLD: float = DEFAULT_ARC_MAIN_THRESHOLD


def load_weights_config(
    path: Optional[Path] = None,
) -> Tuple[Dict[str, float], float, Optional[Path]]:
    """Load scoring weights + arc-main threshold from a YAML file.

    Returns ``(weights, arc_main_threshold, resolved_path_or_None)``. When
    ``path`` is ``None``, tries :data:`DEFAULT_WEIGHTS_PATH`; if that's
    missing too, falls back to :data:`DEFAULT_WEIGHTS` and logs a warning.
    When ``path`` is given explicitly, a missing file is a hard error.

    Validates that weight keys match :data:`DEFAULT_WEIGHTS` exactly, sum to
    1.0 within 1e-6, and that ``arc_main_threshold`` is in (0, 1].
    """
    target = path or DEFAULT_WEIGHTS_PATH
    if not target.exists():
        if path is not None:
            raise FileNotFoundError(f"Weights config not found: {target}")
        logger.warning(
            f"{target} not found — using built-in default weights. "
            "Create it to customise scoring."
        )
        return dict(DEFAULT_WEIGHTS), DEFAULT_ARC_MAIN_THRESHOLD, None

    with open(target, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    weights = data.get("weights") or {}
    if not isinstance(weights, dict):
        raise ValueError(f"{target}: 'weights' must be a mapping")

    expected = set(DEFAULT_WEIGHTS)
    got = set(weights)
    if got != expected:
        missing = expected - got
        extra = got - expected
        bits = []
        if missing:
            bits.append(f"missing={sorted(missing)}")
        if extra:
            bits.append(f"unknown={sorted(extra)}")
        raise ValueError(f"{target}: weight keys mismatch — {'; '.join(bits)}")

    try:
        weights_f = {k: float(v) for k, v in weights.items()}
    except (TypeError, ValueError) as e:
        raise ValueError(f"{target}: weight values must be numeric ({e})") from e

    total = sum(weights_f.values())
    if abs(total - 1.0) > 1e-6:
        raise ValueError(
            f"{target}: weights must sum to 1.0 (got {total:.6f}). "
            f"Values: {weights_f}"
        )

    threshold = float(data.get("arc_main_threshold", DEFAULT_ARC_MAIN_THRESHOLD))
    if not (0.0 < threshold <= 1.0):
        raise ValueError(
            f"{target}: arc_main_threshold must be in (0, 1], got {threshold}"
        )

    return weights_f, threshold, target


# Affiliation keyword tiers. Highest tier matched wins. Patterns are matched
# case-insensitively as substrings against the combined affiliation + occupation
# string (ranks like "Fleet Admiral" live in occupation, not affiliation).
AFFILIATION_TIERS: List[Tuple[float, List[str]]] = [
    (1.00, [
        # Emperor-class
        "Yonko", "Four Emperors", "Emperors of the Sea",
        # Top of the World Government
        "Gorosei", "Five Elders",
        "First Twenty", "Holy Knights",
        "King of the World",            # Imu
        "Celestial Dragon", "World Nobles",
        # Supreme command positions
        "Fleet Admiral", "Commander-in-Chief",
        "Supreme Commander",            # Dragon (head of Revolutionary Army)
        "Admiral",                      # current + former admirals
        # Legendary pirate crews
        "Pirate King", "Roger Pirates",
    ]),
    (0.75, [
        "Vice Admiral",
        "Shichibukai", "Seven Warlords", "Warlord of the Sea",
        "SWORD",
        "Revolutionary Army", "Revolutionary Commander",
        "Cipher Pol",                   # CP0 / CP9 / CP-AIGIS0
        "Straw Hat Pirates", "Straw Hat Grand Fleet",
    ]),
    (0.50, [
        "Rear Admiral",
        "Supernova", "Worst Generation",
        "Whitebeard Pirates", "Big Mom Pirates", "Beasts Pirates",
        "Red Hair Pirates", "Blackbeard Pirates",
        "Donquixote Pirates",
        "Baroque Works Officer Agents",
        "Tobiroppo", "Flying Six", "All-Stars",
    ]),
    (0.30, [
        "Pirates", "Marines", "Kingdom", "Samurai",
        "Fishman", "Giant", "Kuja",
    ]),
]


# Bio attributes counted toward completeness (on the character table).
BIO_COMPLETENESS_COLS = [
    "origin", "status", "birth", "blood_type",
    "bounty", "age", "birth_date",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


# Lower-rank phrases that must be masked out before testing higher-tier
# substrings, so that e.g. "Vice Admiral" does not trigger the plain "Admiral"
# keyword in tier 1.00. Order matters: longest phrases first.
_MASK_LOWER_RANKS = ["vice admiral", "rear admiral"]


def _affiliation_tier_score(search_text: Optional[str]) -> float:
    """Return highest affiliation tier score matched in the text, or 0.

    ``search_text`` is the concatenation of the character's affiliation and
    occupation strings — some rank keywords (Fleet Admiral, Vice Admiral,
    Supreme Commander) live in ``occupation`` rather than ``affiliation``.

    Lower-rank phrases like "Vice Admiral" are masked out before testing the
    tier-1.00 keywords so that "Admiral" does not falsely catch them.
    """
    if not search_text:
        return 0.0
    lowered = search_text.lower()

    for score, keywords in AFFILIATION_TIERS:
        probe = lowered
        if score == 1.00:
            # Blank out lower ranks so "Admiral" below doesn't catch them.
            for phrase in _MASK_LOWER_RANKS:
                probe = probe.replace(phrase, " ")
        for kw in keywords:
            if kw.lower() in probe:
                return score
    return 0.0


def _load_detail_lookups(detail_json_path: Path) -> Tuple[Dict[str, str], Dict[str, bool]]:
    """Load per-character affiliation string and devil-fruit presence.

    Only devil-fruit presence is strictly needed from the JSON now — occupation
    and affiliation are read from the ``character`` table in
    :func:`compute_importance`. Affiliation from the JSON is still returned as
    a fallback for when the table lacks the column (older pre-occupation DBs).
    """
    if not detail_json_path.exists():
        logger.warning(f"{detail_json_path} not found — devil-fruit signal will be 0")
        return {}, {}

    with open(detail_json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    aff_fallback: Dict[str, str] = {}
    devil_fruits: Dict[str, bool] = {}
    for entry in data:
        cid = entry.get("id")
        if not cid:
            continue
        parts = [p for p in (entry.get("affiliation"), entry.get("occupation")) if p]
        if parts:
            aff_fallback[cid] = " ; ".join(parts)
        df1 = entry.get("devil_fruit_name")
        df2 = entry.get("devil_fruit_name_2")
        if (df1 and df1.strip()) or (df2 and df2.strip()):
            devil_fruits[cid] = True
    return aff_fallback, devil_fruits


def _log_norm(value: float, max_value: float) -> float:
    """log1p-normalize ``value`` against ``max_value``. Returns 0..1."""
    if max_value <= 0 or value is None or value <= 0:
        return 0.0
    return math.log1p(value) / math.log1p(max_value)


def _percentile_tier(score: float, cuts: Dict[str, float]) -> str:
    if score >= cuts["S"]:
        return "S"
    if score >= cuts["A"]:
        return "A"
    if score >= cuts["B"]:
        return "B"
    if score >= cuts["C"]:
        return "C"
    return "D"


def _compute_cuts(scores: List[float]) -> Dict[str, float]:
    """Percentile cutoffs: S=99, A=95, B=80, C=50."""
    if not scores:
        return {"S": 1.0, "A": 1.0, "B": 1.0, "C": 1.0}
    sorted_scores = sorted(scores)
    n = len(sorted_scores)

    def pct(p: float) -> float:
        idx = min(n - 1, max(0, int(round(p * (n - 1)))))
        return sorted_scores[idx]

    return {
        "S": pct(0.99),
        "A": pct(0.95),
        "B": pct(0.80),
        "C": pct(0.50),
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


CSV_HEADER = [
    "rank", "tier", "score", "id", "name", "arc_main_count",
    "sig_appearance", "sig_cover", "sig_arc_main", "sig_saga_breadth",
    "sig_bio_completeness", "sig_bounty", "sig_devil_fruit", "sig_affiliation",
]


def _compute_arc_main_counts(
    conn: duckdb.DuckDBPyConnection, threshold: float
) -> Dict[str, int]:
    """Return ``{character_id: arc_main_count}``.

    An "arc main character" is one whose distinct-chapter appearances inside
    an arc's ``[start_chapter, end_chapter]`` range cover ≥ ``threshold`` of
    that arc's chapter count.
    """
    query = f"""
    WITH arc_len AS (
        SELECT arc_id, (end_chapter - start_chapter + 1) AS total FROM arc
    ),
    char_hits AS (
        SELECT a.arc_id, coc.character AS cid,
               COUNT(DISTINCT coc.chapter) AS hits
        FROM arc a
        JOIN coc ON coc.chapter BETWEEN a.start_chapter AND a.end_chapter
        GROUP BY a.arc_id, coc.character
    )
    SELECT ch.cid, COUNT(*) AS main_arcs
    FROM char_hits ch JOIN arc_len al USING (arc_id)
    WHERE ch.hits >= {threshold} * al.total
    GROUP BY ch.cid
    """
    return {cid: n for cid, n in conn.execute(query).fetchall()}


def _write_csv(path: Path, rows: List[tuple]) -> None:
    """Write scored rows to a CSV, ranked by descending score.

    Rows are tuples: (id, name, score, tier, sig_appearance, sig_cover,
    sig_arc_span, sig_saga_breadth, sig_bio_completeness, sig_bounty,
    sig_devil_fruit, sig_affiliation).
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows, key=lambda r: -r[2])
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(CSV_HEADER)
        for rank, r in enumerate(ordered, start=1):
            cid, name, score, tier, arc_main_count, *sigs = r
            w.writerow([
                rank, tier, f"{score:.4f}", cid, name, arc_main_count,
                *(f"{s:.4f}" for s in sigs),
            ])


def compute_importance(
    db_path: str,
    detail_json_path: Optional[str] = None,
    dry_run: bool = False,
    csv_path: Optional[str] = None,
    weights_config: Optional[str] = None,
    progress_callback: Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, object]:
    """Compute importance score + tier for every character.

    Args:
        db_path: Path to the DuckDB database.
        detail_json_path: Override for ``data/characters_detail.json``. If None,
            defaults to ``data/characters_detail.json`` relative to cwd.
        dry_run: If True, compute stats but do not write.
        csv_path: Optional path to write a ranked CSV for review.
        weights_config: Path to a YAML weights override. If None, falls back
            to ``config/importance_weights.yaml``; if that's also missing,
            uses :data:`DEFAULT_WEIGHTS`.
        progress_callback: Optional ``(current, total, msg)`` callback.

    Returns:
        Stats dict with tier counts, cutoffs, and the weight config used.
    """
    detail_path = Path(detail_json_path) if detail_json_path else Path("data/characters_detail.json")
    aff_fallback, devil_fruits = _load_detail_lookups(detail_path)

    weights, arc_main_threshold, weights_source = load_weights_config(
        Path(weights_config) if weights_config else None
    )
    if weights_source is not None:
        logger.info(f"Loaded importance weights from {weights_source}")

    conn = duckdb.connect(str(db_path))
    try:
        total_sagas = conn.execute(
            "SELECT COUNT(*) FROM saga"
        ).fetchone()[0] or 1

        arc_main_by_id = _compute_arc_main_counts(conn, arc_main_threshold)
        logger.info(
            f"Arc main-character counts computed (threshold={arc_main_threshold:.0%}): "
            f"{len(arc_main_by_id)} characters qualify for ≥1 arc"
        )

        existing_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(character)").fetchall()
        }
        has_occupation = "occupation" in existing_cols
        if not has_occupation:
            logger.warning(
                "character.occupation column not found — falling back to characters_detail.json. "
                "Run `sync-occupation` to add it."
            )
        occupation_select = "occupation" if has_occupation else "NULL"

        rows = conn.execute(
            f"""
            SELECT
                id,
                name,
                COALESCE(appearance_count, 0)          AS appearances,
                COALESCE(cover_appearance_count, 0)    AS covers,
                COALESCE(array_length(saga_list), 0)   AS saga_count,
                COALESCE(bounty, 0)                    AS bounty,
                origin, status, birth, blood_type, age, birth_date,
                {occupation_select}                    AS occupation
            FROM character
            """
        ).fetchall()
        total = len(rows)
        logger.info(f"Scoring {total} characters...")

        if progress_callback:
            progress_callback(0, total, "computing maxes")

        max_app = max((r[2] for r in rows), default=0)
        max_cov = max((r[3] for r in rows), default=0)
        max_bounty = max((r[5] for r in rows), default=0)
        max_arc_main = max(arc_main_by_id.values(), default=0)

        # ------------------------------------------------------------------
        # Per-row signal computation
        # ------------------------------------------------------------------
        enriched = []
        for r in rows:
            cid, name, appearances, covers, saga_count, bounty, \
                origin, status, birth, blood_type, age, birth_date, occupation = r

            # Bio completeness — fraction of BIO_COMPLETENESS_COLS that are filled.
            bio_values = [origin, status, birth, blood_type, bounty or None, age, birth_date]
            filled = sum(1 for v in bio_values if v not in (None, "", 0))
            bio_score = filled / len(bio_values)

            arc_main_count = arc_main_by_id.get(cid, 0)

            # Build the affiliation search text from DB columns when available,
            # else fall back to the raw JSON (older DBs without occupation).
            if has_occupation:
                # TODO: if an `affiliation` column is added later, include it
                # here alongside `occupation`. For now the JSON fallback still
                # provides affiliation text too.
                aff_parts = [p for p in (aff_fallback.get(cid), occupation) if p]
                aff_text = " ; ".join(aff_parts) if aff_parts else None
            else:
                aff_text = aff_fallback.get(cid)

            sig = {
                "appearance": _log_norm(appearances, max_app),
                "cover": _log_norm(covers, max_cov),
                "arc_main": _log_norm(arc_main_count, max_arc_main),
                "saga_breadth": min(1.0, saga_count / total_sagas),
                "bio_completeness": bio_score,
                "bounty": _log_norm(bounty, max_bounty),
                "devil_fruit": 1.0 if devil_fruits.get(cid) else 0.0,
                "affiliation": _affiliation_tier_score(aff_text),
            }
            score = sum(weights[k] * v for k, v in sig.items())
            enriched.append((cid, name, score, sig, arc_main_count))

        # ------------------------------------------------------------------
        # Tier assignment via percentile cutoffs
        # ------------------------------------------------------------------
        cuts = _compute_cuts([e[2] for e in enriched])
        logger.info(
            f"Tier cutoffs — S≥{cuts['S']:.3f}  A≥{cuts['A']:.3f}  "
            f"B≥{cuts['B']:.3f}  C≥{cuts['C']:.3f}"
        )

        tier_counts = {"S": 0, "A": 0, "B": 0, "C": 0, "D": 0}
        final: List[tuple] = []
        for cid, name, score, sig, arc_main_count in enriched:
            tier = _percentile_tier(score, cuts)
            tier_counts[tier] += 1
            final.append((
                cid, name, score, tier, arc_main_count,
                sig["appearance"], sig["cover"], sig["arc_main"],
                sig["saga_breadth"], sig["bio_completeness"],
                sig["bounty"], sig["devil_fruit"], sig["affiliation"],
            ))

        logger.info(
            "Tier counts — "
            + "  ".join(f"{t}:{tier_counts[t]}" for t in ("S", "A", "B", "C", "D"))
        )

        # ------------------------------------------------------------------
        # Persistence
        # ------------------------------------------------------------------
        if progress_callback:
            progress_callback(1, 2, "writing")

        if not dry_run:
            conn.execute("DROP TABLE IF EXISTS character_importance")
            conn.execute(
                """
                CREATE TABLE character_importance (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    score DOUBLE,
                    tier VARCHAR,
                    arc_main_count INTEGER,
                    sig_appearance DOUBLE,
                    sig_cover DOUBLE,
                    sig_arc_main DOUBLE,
                    sig_saga_breadth DOUBLE,
                    sig_bio_completeness DOUBLE,
                    sig_bounty DOUBLE,
                    sig_devil_fruit DOUBLE,
                    sig_affiliation DOUBLE
                )
                """
            )
            conn.executemany(
                """INSERT INTO character_importance
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                final,
            )

            # Ensure summary columns exist on character, then update.
            existing_cols = {
                r[1] for r in conn.execute("PRAGMA table_info(character)").fetchall()
            }
            if "importance_score" not in existing_cols:
                conn.execute("ALTER TABLE character ADD COLUMN importance_score DOUBLE")
            if "importance_tier" not in existing_cols:
                conn.execute("ALTER TABLE character ADD COLUMN importance_tier VARCHAR")

            conn.executemany(
                """UPDATE character
                   SET importance_score = ?, importance_tier = ?
                   WHERE id = ?""",
                [(r[2], r[3], r[0]) for r in final],
            )
            logger.success(
                f"Wrote character_importance ({total} rows) and updated character summary columns"
            )

        if csv_path:
            out = Path(csv_path)
            _write_csv(out, final)
            logger.success(f"Wrote review CSV to {out} ({len(final)} rows)")

        if progress_callback:
            progress_callback(2, 2, "done")

        return {
            "total": total,
            "tier_counts": tier_counts,
            "cutoffs": cuts,
            "weights": weights,
            "weights_source": str(weights_source) if weights_source else None,
            "arc_main_threshold": arc_main_threshold,
            "csv_path": str(Path(csv_path)) if csv_path else None,
        }
    finally:
        conn.close()
