# nlp/template_router.py
import re
from typing import Tuple, Dict, Optional, Any
from jinja2 import Environment, StrictUndefined

# ------- Stat label → SQL column mapping -------
STAT_MAP = {
    r"^(hr|home\s*runs?)$": "hr",
    r"^(rbi|runs?\s*batted\s*in)$": "rbi",
    r"^(sb|stolen\s*bases?)$": "sb",
    r"^(so|strikeouts?)$": "so",
    r"^(bb|walks?)$": "bb",
    r"^(h|hits?)$": "h",
    r"^(r|runs?\s*scored)$": "r",
    r"^(doubles?)$": "doubles",
    r"^(triples?)$": "triples",
}


# Add at the top of route_template()
def route_template(user_q: str) -> Tuple[Optional[str], Dict[str, str]]:
    # Range/multi-season queries must go to the model
    if re.search(
        r"(?i)\b("
        r"from\s+\d{4}\s+to\s+\d{4}"
        r"|\d{4}\s*(-|to)\s*\d{4}"
        r"|between\s+\d{4}\s+and\s+\d{4}"
        r"|combined|career|all.time|over\s+the\s+(last|past)\s+\d+"
        r")",
        user_q
    ):
        return None, {}

    for name, pat in TEMPLATE_PATTERNS:
        m = pat.search(user_q or "")
        if m:
            gd = {k: v for k, v in m.groupdict().items() if v is not None}
            return name, gd
    return None, {}

def nl_to_col(label: str) -> str:
    label = (label or "").strip().lower()
    for pat, col in STAT_MAP.items():
        if re.fullmatch(pat, label, re.I):
            return col
    raise ValueError(f"Unrecognized stat label: {label!r}")


# ------- Flexible pattern matching -------
# Each entry: (template_name, regex)
# Patterns are tried in order — first match wins.
# Named groups: stat_label (required), season (required), top_n (optional)
TEMPLATE_PATTERNS = [
    # "top N in [stat] in [year]" variants
    ("leaders_batting_counting", re.compile(
        r"(?i)\btop\s*(?P<top_n>\d+)\b[^?]*\b(?P<stat_label>hr|home\s*runs?|rbi|runs?\s*batted\s*in|sb|stolen\s*bases?|so|strikeouts?|bb|walks?|hits?|doubles?|triples?)\b[^?]*\b(?P<season>\d{4})\b"
    )),
    # "[year] ... top N ... [stat]"
    ("leaders_batting_counting", re.compile(
        r"(?i)\b(?P<season>\d{4})\b[^?]*\btop\s*(?P<top_n>\d+)\b[^?]*\b(?P<stat_label>hr|home\s*runs?|rbi|sb|so|bb)\b"
    )),
    # "who led/leads ... [stat] ... [year]" — no explicit N, defaults to 10
    ("leaders_batting_counting", re.compile(
        r"(?i)\b(?:who\s+)?(?:led|leads?|lead|are\s+the\s+leaders?|most)\b[^?]*\b(?P<stat_label>hr|home\s*runs?|rbi|runs?\s*batted\s*in|sb|stolen\s*bases?|so|strikeouts?|bb|walks?|hits?|doubles?|triples?)\b[^?]*\b(?P<season>\d{4})\b"
    )),
    # "leaders in [stat] in [year]"
    ("leaders_batting_counting", re.compile(
        r"(?i)\bleaders?\s+in\s+(?P<stat_label>hr|home\s*runs?|rbi|sb|so|bb|hits?|doubles?|triples?)\s+(?:in|for)\s+(?P<season>\d{4})\b"
    )),
]

# ------- Jinja renderer -------
_env = Environment(undefined=StrictUndefined, autoescape=False)

def render_ident_template(sql_template: str, ident_params: Dict[str, Any]) -> str:
    return _env.from_string(sql_template).render(**ident_params)


# ------- Public API -------
def build_sql_from_templates(
    user_q: str,
    templates_yaml: Dict[str, Any]
) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    """
    Match user query to a template and render SQL.
    Returns (sql, bound_params, template_name) or (None, None, None) if no match.
    """
    name, gd = route_template(user_q)
    if not name:
        return None, None, None

    tdef = templates_yaml.get(name)
    if not tdef:
        return None, None, None

    defaults = tdef.get("defaults", {})

    # Resolve season
    season_raw = gd.get("season")
    if not season_raw:
        return None, None, None  # season is required
    season = int(season_raw)

    # Resolve top_n
    top_n = int(gd.get("top_n") or defaults.get("top_n", 10))

    # Resolve stat
    stat_label_nl = (gd.get("stat_label") or defaults.get("stat_label", "hr")).strip().lower()
    try:
        stat_col = nl_to_col(stat_label_nl) if "stat_col" in tdef.get("params", []) else defaults.get("stat_col", stat_label_nl)
    except ValueError:
        return None, None, None  # unrecognized stat, fall through to model

    ident_params = {
        "stat_col": stat_col,
        "stat_label": stat_label_nl,
        "fragments": templates_yaml.get("fragments", {}),
    }
    bound_params = {"season": season, "top_n": top_n}

    try:
        sql = render_ident_template(tdef["sql"], ident_params)
    except Exception:
        return None, None, None

    return sql, bound_params, name