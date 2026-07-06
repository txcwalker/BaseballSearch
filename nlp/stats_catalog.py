# nlp/stats_catalog.py
#
# Builds the fast-path stat catalog from the same canonical stat definitions
# nlp/template_router.py uses (STAT_MAP_BATTING/STAT_MAP_PITCHING), rather than
# introspecting raw Savant column names — "b_home_run"/"p_strikeout" don't
# fuzzy-match natural phrasing like "home runs" well, but the human-readable
# alternatives already embedded in those regex patterns do.

import re
from rapidfuzz import process, fuzz

from nlp.template_router import STAT_MAP_BATTING, STAT_MAP_PITCHING

LOW_IS_BETTER = {"era", "fip", "whip", "ra9", "bb9"}  # small override list


def _labels_from_pattern(pattern: str) -> list:
    """STAT_MAP_* keys look like r"(?i)^(hr|home\\s*runs?)$" — pull out the
    plain-text alternatives for fuzzy-match display labels."""
    inner = re.sub(r"^\(\?i\)\^\(|\)\$$", "", pattern)
    labels = []
    for alt in inner.split("|"):
        cleaned = alt.replace(r"\s*", " ").replace("?", "").strip()
        if cleaned:
            labels.append(cleaned)
    return labels


def _build_domain_catalog(stat_map: dict, table: str, domain: str) -> dict:
    # Batting and pitching share canonical short codes (so/bb/h mean different
    # things — strikeouts thrown vs. drawn, etc.) — key by domain to avoid one
    # domain's entry silently overwriting the other's when catalogs are merged.
    # `stat_label` (not the dict key) is what's SQL-safe to use in generated SQL.
    catalog = {}
    for pattern, cols in stat_map.items():
        canonical = cols["lahman"]  # short code, also the Lahman column name (hr, rbi, so, ...)
        key = f"{domain}_{canonical}"
        catalog[key] = {
            "domain": domain,
            "table": table,
            "savant_col": cols["savant"],
            "lahman_col": canonical,
            "stat_label": canonical,
            "agg_default": "SUM",
            "direction": "ASC" if canonical in LOW_IS_BETTER else "DESC",
            "nl_labels": _labels_from_pattern(pattern),
        }
    return catalog


def build_stat_catalog(conn=None) -> dict:
    """conn is accepted for interface compatibility with callers but unused —
    this catalog is derived from the static STAT_MAP_* definitions, not live
    DB introspection, so it never needs a live connection to build."""
    cat = {}
    cat.update(_build_domain_catalog(STAT_MAP_BATTING, "savant_batting_traditional", "batting"))
    cat.update(_build_domain_catalog(STAT_MAP_PITCHING, "savant_pitching_traditional", "pitching"))
    return cat


def _variants(col, nl_labels):
    variants = {col}
    for label in nl_labels:
        variants.add(label)
        variants.add(re.sub(r"s\b", "", label))  # crude singular
    return variants


COMMON_SYNONYMS = {
    "home runs": "hr", "homers": "hr", "hr": "hr",
    "strikeouts": "so", "k": "so", "ks": "so",
    "walks": "bb"
}


def resolve_stat(text, catalog, domain_hint=None, score_cut=85):
    q = text.lower().strip()
    q = COMMON_SYNONYMS.get(q, q)

    candidates = [c for c, meta in catalog.items()
                  if (domain_hint is None or meta["domain"] == domain_hint)]

    search_space = []
    for col in candidates:
        for v in _variants(col, catalog[col]["nl_labels"]):
            search_space.append((v, col))

    choices = [v for v, _ in search_space]
    if not choices:
        return None
    best, score, idx = process.extractOne(q, choices, scorer=fuzz.WRatio)
    if score < score_cut:
        return None
    return search_space[idx][1]
