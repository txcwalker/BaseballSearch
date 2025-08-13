# nlp/template_router.py
import re
from typing import Tuple, Dict, Optional, Any
from jinja2 import Environment, StrictUndefined

# ------- Natural language → whitelisted stat columns -------
STAT_MAP = {
    r"(?i)^(hr|home\s*runs?)$": "hr",
    r"(?i)^(rbi|runs?\s*batted\s*in)$": "rbi",
    r"(?i)^(sb|stolen\s*bases?)$": "sb",
    r"(?i)^(so|strikeouts?)$": "so",
    r"(?i)^(bb|walks?)$": "bb",
    # add as needed...
}

def nl_to_col(label: str) -> str:
    for pat, col in STAT_MAP.items():
        if re.fullmatch(pat, (label or "").strip()):
            return col
    raise ValueError(f"Unrecognized stat label: {label!r}")

# ------- Template patterns (expand over time) -------
TEMPLATE_PATTERNS = [
    ("leaders_batting_counting",
     re.compile(r"(?i)\b(?:leaders?|top\s*(?P<top_n>\d+))\s+in\s+(?P<stat_label>hr|home\s*runs?)\s+(?:in|for)\s+(?P<season>\d{4})")),
    ("leaders_batting_counting",
     re.compile(r"(?i)\bwho\s+(?:leads?|are\s+the\s+leaders?)\b.*\b(?P<stat_label>hr|home\s*runs?)\b.*\b(?P<season>\d{4})")),
    ("leaders_batting_counting",
     re.compile(r"(?i)\bmost\s+(?P<stat_label>hr|home\s*runs?)\s+(?:in|for)\s+(?P<season>\d{4})")),
    ("leaders_batting_counting",
     re.compile(r"(?i)\btop\s*(?P<top_n>\d+)\b.*\b(?P<stat_label>hr|home\s*runs?)\b.*\b(?P<season>\d{4})")),
]


def route_template(user_q: str) -> Tuple[Optional[str], Dict[str, str]]:
    for name, pat in TEMPLATE_PATTERNS:
        m = pat.search(user_q or "")
        if m:
            gd = {k: v for k, v in m.groupdict().items() if v is not None}
            return name, gd
    return None, {}

# ------- Jinja renderer for identifiers/aliases only -------
_env = Environment(undefined=StrictUndefined, autoescape=False)
def render_ident_template(sql_template: str, ident_params: Dict[str, Any]) -> str:
    """Render template where ONLY identifiers/aliases are substituted (e.g., stat_col, stat_label)."""
    return _env.from_string(sql_template).render(**ident_params)

# ------- Public API: build_sql_from_templates / get_sql -------
def build_sql_from_templates(user_q: str, templates_yaml: Dict[str, Any]) -> Tuple[Optional[str], Optional[Dict[str, Any]], Optional[str]]:
    """
    If a template matches, return (sql, bound_params, template_name).
    Otherwise, return (None, None, None) and let caller fall back to model.
    """
    name, gd = route_template(user_q)
    if not name:
        return None, None, None

    tdef = templates_yaml["templates"][name]

    # Values to bind
    season = int(gd.get("season")) if gd.get("season") else None
    top_n = int(gd.get("top_n") or tdef.get("defaults", {}).get("top_n", 10))

    # NL → identifiers for rendering
    stat_label_nl = (gd.get("stat_label") or tdef.get("defaults", {}).get("stat_label", "stat")).lower()
    stat_col = nl_to_col(stat_label_nl) if "stat_col" in tdef.get("params", []) else tdef.get("defaults", {}).get("stat_col")

    ident_params = {
        "stat_col": stat_col,
        "stat_label": stat_label_nl,  # quoted in SQL where used
        "fragments": templates_yaml.get("fragments", {}),
    }
    bound_params = {"season": season, "top_n": top_n}

    sql = render_ident_template(tdef["sql"], ident_params)
    return sql, bound_params, name
