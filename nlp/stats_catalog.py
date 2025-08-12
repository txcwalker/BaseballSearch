# nlp/stats_catalog.py

# Import Packages
import re
from rapidfuzz import process, fuzz

LOW_IS_BETTER = {"era","fip","whip","ra9","bb9"}  # small override list
IGNORE = {"idfg","name","team","season"}          # non-stat columns

def _fetch_columns(conn, table, domain):
    with conn.cursor() as cur:
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name=%s
        """, (table,))
        rows = cur.fetchall()
    catalog = {}
    for col, dtype in rows:
        cl = col.lower()
        if cl in IGNORE:
            continue
        numeric = any(x in dtype for x in ("int", "numeric", "double", "real", "decimal"))
        if not numeric:
            continue
        agg = "SUM" if "int" in dtype else "AVG"
        direction = "ASC" if cl in LOW_IS_BETTER else "DESC"
        catalog[cl] = {
            "domain": domain,
            "table": table,
            "agg_default": agg,
            "direction": direction
        }
    return catalog

def build_stat_catalog(conn):
    cat = {}
    cat.update(_fetch_columns(conn, "fangraphs_batting_lahman_like", "batting"))
    cat.update(_fetch_columns(conn, "fangraphs_pitching_lahman_like", "pitching"))
    return cat

def _variants(col):
    pretty = col.replace("_", " ").strip()
    sing = re.sub(r"s\b", "", pretty)  # <-- fix: r"s\b" (not r"s\\b")
    return {col, pretty, sing}


COMMON_SYNONYMS = {
    "home runs": "hr", "homers": "hr", "hr": "hr",
    "strikeouts": "so", "k": "so", "ks": "so",
    "walks": "bb"
}

def resolve_stat(text, catalog, domain_hint=None, score_cut=70):
    q = text.lower().strip()
    q = COMMON_SYNONYMS.get(q, q)

    candidates = [c for c, meta in catalog.items()
                  if (domain_hint is None or meta["domain"] == domain_hint)]

    search_space = []
    for col in candidates:
        for v in _variants(col):
            search_space.append((v, col))

    choices = [v for v, _ in search_space]
    if not choices:
        return None
    best, score, idx = process.extractOne(q, choices, scorer=fuzz.WRatio)
    if score < score_cut:
        return None
    return search_space[idx][1]
