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
}

def nl_to_col(label: str) -> str:
    for pat, col in STAT_MAP.items():
        if re.fullmatch(pat, (label or "").strip()):
            return col
    raise ValueError(f"Unrecognized stat label: {label!r}")


# -----------------------------------------------------------------------
# Name extractor: strips common prefixes like "show me", "show", "display"
# -----------------------------------------------------------------------
_PREFIXES = re.compile(
    r"^(?:show\s+me|show|display|list|give\s+me|what\s+(?:were|are|is|was))\s+",
    re.I
)
_NAME_SUFFIXES = re.compile(
    r"(?:'s?\s+|\s+)(?:era|fip|war|wrc|batting|pitching|career|stats?|numbers?).*$",
    re.I
)

def _extract_player_name(raw: str) -> str:
    """Strip leading prefixes and trailing stat words to isolate the player name."""
    name = _PREFIXES.sub("", raw.strip())
    name = _NAME_SUFFIXES.sub("", name).strip().rstrip("'s").strip()
    return name


# ------- Direct SQL builders -------

def _team_era_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    season = int(m.group("season"))
    sql = """
SELECT tms.name AS team, tms.lgid AS league, tms.divid AS division,
       tms.era, tms.w AS wins, tms.l AS losses
FROM teams tms
WHERE tms.yearid = %(season)s
ORDER BY tms.era ASC
LIMIT 10;
""".strip()
    return sql, {"season": season}


def _team_batting_division_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    season = int(m.group("season"))
    raw_lg = (m.group("league") or "").upper().strip()
    raw_div = (m.group("division") or "").capitalize().strip()
    lg_map = {"AMERICAN LEAGUE": "AL", "NATIONAL LEAGUE": "NL", "AL": "AL", "NL": "NL"}
    league = lg_map.get(raw_lg, raw_lg)
    div_map = {"East": "E", "West": "W", "Central": "C"}
    division = div_map.get(raw_div, raw_div[0].upper() if raw_div else None)
    if not league or not division:
        return None, None
    sql = """
SELECT tms.name AS team,
       ROUND(tms.h::numeric / NULLIF(tms.ab, 0), 3) AS avg,
       tms.h AS hits, tms.ab AS at_bats,
       tms.w AS wins, tms.l AS losses
FROM teams tms
WHERE tms.yearid = %(season)s
  AND tms.lgid   = %(league)s
  AND tms.divid  = %(division)s
ORDER BY avg DESC;
""".strip()
    return sql, {"season": season, "league": league, "division": division}


def _player_pitching_career_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    raw = m.group("player_name")
    player_name = _extract_player_name(raw)
    if not player_name or len(player_name.split()) < 2:
        return None, None
    sql = """
WITH player AS (
    SELECT lfb.idfg
    FROM lahman_fangraphs_bridge lfb
    JOIN people peo ON peo.playerid = lfb.playerid
    WHERE LOWER(peo.namefirst || ' ' || peo.namelast) = LOWER(%(player_name)s)
    LIMIT 1
)
SELECT fpr.season, fpr.name, fpr.team,
       fpr.era, fpa.fip, fpa.xfip,
       fpl.w, fpl.l, fpl.ip, fpl.so
FROM fangraphs_pitching_standard_ratios fpr
LEFT JOIN fangraphs_pitching_advanced fpa
  ON fpa.idfg = fpr.idfg AND fpa.season = fpr.season AND fpa.team = fpr.team
LEFT JOIN fangraphs_pitching_lahman_like fpl
  ON fpl.idfg = fpr.idfg AND fpl.season = fpr.season AND fpl.team = fpr.team
JOIN player ON player.idfg = fpr.idfg
WHERE fpr.team NOT IN ('---')
  AND (
    fpr.team = 'TOT'
    OR NOT EXISTS (
      SELECT 1 FROM fangraphs_pitching_standard_ratios fpr2
      WHERE fpr2.idfg = fpr.idfg AND fpr2.season = fpr.season AND fpr2.team = 'TOT'
    )
  )
ORDER BY fpr.season ASC;
""".strip()
    return sql, {"player_name": player_name}


def _player_batting_career_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    raw = m.group("player_name")
    player_name = _extract_player_name(raw)
    if not player_name or len(player_name.split()) < 2:
        return None, None
    sql = """
WITH player AS (
    SELECT lfb.idfg
    FROM lahman_fangraphs_bridge lfb
    JOIN people peo ON peo.playerid = lfb.playerid
    WHERE LOWER(peo.namefirst || ' ' || peo.namelast) = LOWER(%(player_name)s)
    LIMIT 1
)
SELECT fbl.season, fbl.name, fbl.team,
       fbl.g, fbl.pa, fbl.hr, fbl.rbi, fbl.sb,
       fbr.avg, fbr.obp, fbr.slg, fbr.ops,
       fba.war, fba.wrc_plus, fba.woba
FROM fangraphs_batting_lahman_like fbl
LEFT JOIN fangraphs_batting_standard_ratios fbr
  ON fbr.idfg = fbl.idfg AND fbr.season = fbl.season AND fbr.team = fbl.team
LEFT JOIN fangraphs_batting_advanced fba
  ON fba.idfg = fbl.idfg AND fba.season = fbl.season AND fba.team = fbl.team
JOIN player ON player.idfg = fbl.idfg
WHERE fbl.team NOT IN ('---')
  AND (
    fbl.team = 'TOT'
    OR NOT EXISTS (
      SELECT 1 FROM fangraphs_batting_lahman_like fbl2
      WHERE fbl2.idfg = fbl.idfg AND fbl2.season = fbl.season AND fbl2.team = 'TOT'
    )
  )
ORDER BY fbl.season ASC;
""".strip()
    return sql, {"player_name": player_name}


# -----------------------------------------------------------------------
# Direct pattern registry
# Each: (compiled_regex, handler_fn)
# Handler receives match, returns (sql, params) or (None, None)
# -----------------------------------------------------------------------
DIRECT_PATTERNS = [

    # Team ERA
    (re.compile(
        r"(?i)(?:team|bullpen)\s+era"
        r"\s+(?:in|for|during|rankings?|leaders?)?\s*(?P<season>\d{4})"
    ), _team_era_sql),
    (re.compile(
        r"(?i)which\s+teams?\s+had\s+the\s+(?:best|lowest|worst|highest)"
        r"\s+(?:bullpen\s+)?era\s+(?:in|for)\s+(?P<season>\d{4})"
    ), _team_era_sql),

    # Division batting averages
    (re.compile(
        r"(?i)(?:compare|show|list)\s+(?:the\s+)?batting\s+averages?\s+of\s+(?:all\s+)?"
        r"(?:the\s+)?(?P<league>AL|NL|American\s+League|National\s+League)\s+"
        r"(?P<division>East|West|Central)\s+teams?\s+(?:in|for)\s+(?P<season>\d{4})"
    ), _team_batting_division_sql),
    (re.compile(
        r"(?i)(?P<league>AL|NL)\s+(?P<division>East|West|Central)"
        r"\s+(?:team\s+)?batting\s+averages?\s+(?:in|for)\s+(?P<season>\d{4})"
    ), _team_batting_division_sql),

    # Player pitching career by season
    # Captures "Clayton Kershaw" from queries like:
    # "Show me Clayton Kershaw ERA and FIP by season"
    # "Clayton Kershaw's ERA by season"
    (re.compile(
        r"(?i)(?:show(?:\s+me)?|display|list)?\s*"
        r"(?P<player_name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
        r"'?s?\s+(?:era|fip|era\s+and\s+fip|pitching\s+stats?)"
        r"\s+(?:by|each|per|every)\s+season"
    ), _player_pitching_career_sql),
    (re.compile(
        r"(?i)(?P<player_name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
        r"'s\s+(?:era|fip|era\s+and\s+fip|pitching)"
    ), _player_pitching_career_sql),

    # Player batting career by season
    (re.compile(
        r"(?i)(?:show(?:\s+me)?|display|list)?\s*"
        r"(?P<player_name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
        r"'?s?\s+(?:war|wrc\+?|batting\s+stats?|career\s+stats?|stats?)"
        r"\s+(?:by|each|per|every)\s+season"
    ), _player_batting_career_sql),
    (re.compile(
        r"(?i)(?P<player_name>[A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})"
        r"'s\s+(?:war|wrc\+?|batting|career\s+stats?)"
    ), _player_batting_career_sql),
]

# ------- Existing stat-based template patterns -------
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


def route_template(user_q: str) -> Tuple[Optional[str], Dict[str, Any]]:
    """
    Check direct patterns first, then stat-based templates.
    Returns (name, groupdict). For direct matches name = '__direct__'.
    """
    q = user_q or ""

    for pattern, handler in DIRECT_PATTERNS:
        m = pattern.search(q)
        if m:
            sql, params = handler(m)
            if sql:
                return "__direct__", {"__sql__": sql, "__params_dict__": params}

    for name, pat in TEMPLATE_PATTERNS:
        m = pat.search(q)
        if m:
            gd = {k: v for k, v in m.groupdict().items() if v is not None}
            return name, gd

    return None, {}


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
    Returns (sql, bound_params, template_name) if matched, else (None, None, None).
    """
    name, gd = route_template(user_q)
    if not name:
        return None, None, None

    # Direct pattern — SQL already built
    if name == "__direct__":
        sql = gd["__sql__"]
        params = gd.get("__params_dict__", {})
        return sql, params, "direct"

    # Stat-based YAML template
    tdef = templates_yaml["templates"][name]
    season = int(gd.get("season")) if gd.get("season") else None
    top_n = int(gd.get("top_n") or tdef.get("defaults", {}).get("top_n", 10))
    stat_label_nl = (gd.get("stat_label") or tdef.get("defaults", {}).get("stat_label", "stat")).lower()
    stat_col = (
        nl_to_col(stat_label_nl)
        if "stat_col" in tdef.get("params", [])
        else tdef.get("defaults", {}).get("stat_col")
    )
    ident_params = {
        "stat_col": stat_col,
        "stat_label": stat_label_nl,
        "fragments": templates_yaml.get("fragments", {}),
    }
    bound_params = {"season": season, "top_n": top_n}
    sql = render_ident_template(tdef["sql"], ident_params)
    return sql, bound_params, name