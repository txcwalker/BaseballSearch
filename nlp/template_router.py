# nlp/template_router.py
import re
from typing import Tuple, Dict, Optional, Any
from jinja2 import Environment, StrictUndefined

from nlp.linter import CAREER_WORDS

# ------- Natural language → whitelisted stat columns -------
# Stats that mean something different (or don't exist) for pitchers vs batters —
# e.g. "hits" is hits recorded (batting) vs hits allowed (pitching), and HR/RBI/SB
# leaderboards only make sense for batters. Domain is resolved in route_template()
# from question wording ("pitcher"/"pitching") before this map is consulted.
STAT_MAP_BATTING = {
    r"(?i)^(hr|home\s*runs?)$": {"lahman": "hr", "savant": "b_home_run"},
    r"(?i)^(rbi|runs?\s*batted\s*in)$": {"lahman": "rbi", "savant": "b_rbi"},
    r"(?i)^(sb|stolen\s*bases?)$": {"lahman": "sb", "savant": "b_stolen_base"},
    r"(?i)^(so|strikeouts?)$": {"lahman": "so", "savant": "b_strikeout"},
    r"(?i)^(bb|walks?)$": {"lahman": "bb", "savant": "b_walk"},
    r"(?i)^(h|hits?)$": {"lahman": "h", "savant": "b_total_hits"},
}
STAT_MAP_PITCHING = {
    r"(?i)^(so|strikeouts?)$": {"lahman": "so", "savant": "p_strikeout"},
    r"(?i)^(bb|walks?)$": {"lahman": "bb", "savant": "p_walk"},
    r"(?i)^(h|hits?)$": {"lahman": "h", "savant": "p_hit"},
}

# Stats that only make sense for batters — if the question also says "pitcher",
# it's an odd/ambiguous combination, so we skip the deterministic template and
# let the LLM sort it out rather than silently building a batting leaderboard.
_BATTING_ONLY_STAT_LABELS = {"hr", "home run", "home runs", "rbi", "runs batted in",
                             "sb", "stolen base", "stolen bases"}
_PITCHER_DOMAIN_RE = re.compile(r"(?i)\bpitch(?:er|ers|ing)\b")


def nl_to_cols(label: str, domain: str = "batting") -> Dict[str, str]:
    stat_map = STAT_MAP_PITCHING if domain == "pitching" else STAT_MAP_BATTING
    for pat, cols in stat_map.items():
        if re.fullmatch(pat, (label or "").strip()):
            return cols
    return {"lahman": label, "savant": label} # Fallback


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


# "Career/by-season" patterns without an explicit "by/each/per/every season" or
# "career" marker (see the second, looser regex in each DIRECT_PATTERNS pair
# below) can otherwise hijack a single-season question like "Aaron Judge's
# batting average this season?" — bail out of the career handler in that case
# and let the rest of the routing pipeline (which handles single seasons
# correctly) take it instead.
_SINGLE_SEASON_HINT_RE = re.compile(r"(?i)\bthis\s+(?:season|year)\b|\b(19|20)\d{2}\b")
_CAREER_HINT_RE = re.compile(r"(?i)\b(?:by|each|per|every)\s+season\b|\bcareer\b")


def _is_single_season_question(full_text: str) -> bool:
    return bool(_SINGLE_SEASON_HINT_RE.search(full_text)) and not _CAREER_HINT_RE.search(full_text)


def _player_pitching_career_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    raw = m.group("player_name")
    player_name = _extract_player_name(raw)
    if not player_name or len(player_name.split()) < 2:
        return None, None
    if _is_single_season_question(m.string):
        return None, None
    sql = """
WITH player AS (
    SELECT peo.playerid
    FROM people peo
    WHERE LOWER(peo.namefirst || ' ' || peo.namelast) = LOWER(%(player_name)s)
    ORDER BY peo.debut DESC
    LIMIT 1
),
fpa_by_season AS (
    -- Frozen FanGraphs archive -- only source for FIP/xFIP/WAR (see Rule 4).
    -- Pre-aggregate to one row per (idfg, season), preferring the TOT row.
    SELECT idfg, season,
           COALESCE(MAX(fip) FILTER (WHERE team = 'TOT'), MAX(fip)) AS fip,
           COALESCE(MAX(xfip) FILTER (WHERE team = 'TOT'), MAX(xfip)) AS xfip,
           COALESCE(MAX(war) FILTER (WHERE team = 'TOT'), MAX(war)) AS war
    FROM fangraphs_pitching_advanced
    WHERE team != '---'
    GROUP BY idfg, season
),
savant_seasons AS (
    SELECT s.year AS season, s.team, p.playerid,
           CASE WHEN position(', ' in s.playername) > 0
                THEN split_part(s.playername, ', ', 2) || ' ' || split_part(s.playername, ', ', 1)
                ELSE s.playername END AS name,
           s.p_game AS g, s.p_win AS w, s.p_loss AS l,
           r.p_era AS era, (s.p_earned_run * 27.0) / NULLIF(s.p_game, 0) / 9.0 AS ip_est
    FROM savant_pitching_traditional s
    JOIN lahman_savant_bridge lsb ON lsb.key_mlbam = s.player_id
    JOIN player p ON p.playerid = lsb.playerid
    LEFT JOIN savant_pitching_ratios r ON r.player_id = s.player_id AND r.year = s.year
),
lahman_seasons AS (
    SELECT pit.yearid AS season, pit.teamid AS team, p.playerid,
           peo.namefirst || ' ' || peo.namelast AS name,
           SUM(pit.g) AS g, SUM(pit.w) AS w, SUM(pit.l) AS l,
           (SUM(pit.er)::numeric * 9) / NULLIF(SUM(pit.ipouts) / 3.0, 0) AS era,
           SUM(pit.ipouts) / 3.0 AS ip_est
    FROM pitching pit
    JOIN player p ON p.playerid = pit.playerid
    JOIN people peo ON peo.playerid = pit.playerid
    WHERE NOT EXISTS (SELECT 1 FROM savant_pitching_traditional s2 WHERE s2.year = pit.yearid)
    GROUP BY pit.yearid, pit.teamid, p.playerid, peo.namefirst, peo.namelast
),
combined AS (
    SELECT season, team, playerid, name, g, w, l, era, ip_est FROM savant_seasons
    UNION ALL
    SELECT season, team, playerid, name, g, w, l, era, ip_est FROM lahman_seasons
)
SELECT c.season, c.name, c.team, c.g, c.w, c.l,
       ROUND(c.era::numeric, 2) AS era, ROUND(c.ip_est::numeric, 1) AS ip,
       fpa.fip, fpa.xfip, fpa.war
FROM combined c
LEFT JOIN lahman_fangraphs_bridge lfb ON lfb.playerid = c.playerid
LEFT JOIN fpa_by_season fpa ON fpa.idfg = lfb.idfg AND fpa.season = c.season
ORDER BY c.season ASC;
""".strip()
    return sql, {"player_name": player_name}


def _player_batting_career_sql(m: re.Match) -> Tuple[Optional[str], Optional[Dict]]:
    raw = m.group("player_name")
    player_name = _extract_player_name(raw)
    if not player_name or len(player_name.split()) < 2:
        return None, None
    if _is_single_season_question(m.string):
        return None, None
    sql = """
WITH player AS (
    SELECT peo.playerid
    FROM people peo
    WHERE LOWER(peo.namefirst || ' ' || peo.namelast) = LOWER(%(player_name)s)
    ORDER BY peo.debut DESC
    LIMIT 1
),
fba_by_season AS (
    -- Frozen FanGraphs archive -- only source for WAR/wOBA/wRC+ (see Rule 4).
    -- Pre-aggregate to one row per (idfg, season), preferring the TOT row.
    SELECT idfg, season,
           COALESCE(MAX(war) FILTER (WHERE team = 'TOT'), MAX(war)) AS war,
           COALESCE(MAX(wrc_plus) FILTER (WHERE team = 'TOT'), MAX(wrc_plus)) AS wrc_plus,
           COALESCE(MAX(woba) FILTER (WHERE team = 'TOT'), MAX(woba)) AS woba
    FROM fangraphs_batting_advanced
    WHERE team != '---'
    GROUP BY idfg, season
),
savant_seasons AS (
    SELECT s.year AS season, s.team, p.playerid,
           CASE WHEN position(', ' in s.playername) > 0
                THEN split_part(s.playername, ', ', 2) || ' ' || split_part(s.playername, ', ', 1)
                ELSE s.playername END AS name,
           s.b_game AS g, s.b_total_pa AS pa, s.b_home_run AS hr, s.b_rbi AS rbi, s.b_stolen_base AS sb,
           r.batting_avg AS avg, r.on_base_percent AS obp, r.slg_percent AS slg, r.on_base_plus_slg AS ops
    FROM savant_batting_traditional s
    JOIN lahman_savant_bridge lsb ON lsb.key_mlbam = s.player_id
    JOIN player p ON p.playerid = lsb.playerid
    LEFT JOIN savant_batting_ratios r ON r.player_id = s.player_id AND r.year = s.year
),
lahman_seasons AS (
    SELECT bat.yearid AS season, bat.teamid AS team, p.playerid,
           peo.namefirst || ' ' || peo.namelast AS name,
           SUM(bat.g) AS g, SUM(bat.ab) AS pa, SUM(bat.hr) AS hr, SUM(bat.rbi) AS rbi, SUM(bat.sb) AS sb,
           SUM(bat.h)::numeric / NULLIF(SUM(bat.ab), 0) AS avg,
           (SUM(bat.h) + SUM(bat.bb) + COALESCE(SUM(bat.hbp), 0))::numeric
             / NULLIF(SUM(bat.ab) + SUM(bat.bb) + COALESCE(SUM(bat.hbp), 0) + COALESCE(SUM(bat.sf), 0), 0) AS obp,
           (SUM(bat.h) + SUM(bat."2b") + 2 * SUM(bat."3b") + 3 * SUM(bat.hr))::numeric
             / NULLIF(SUM(bat.ab), 0) AS slg
    FROM batting bat
    JOIN player p ON p.playerid = bat.playerid
    JOIN people peo ON peo.playerid = bat.playerid
    WHERE NOT EXISTS (SELECT 1 FROM savant_batting_traditional s2 WHERE s2.year = bat.yearid)
    GROUP BY bat.yearid, bat.teamid, p.playerid, peo.namefirst, peo.namelast
),
combined AS (
    SELECT season, team, playerid, name, g, pa, hr, rbi, sb, avg, obp, slg, ops FROM savant_seasons
    UNION ALL
    SELECT season, team, playerid, name, g, pa, hr, rbi, sb, avg, obp, slg, (obp + slg) AS ops FROM lahman_seasons
)
SELECT c.season, c.name, c.team, c.g, c.pa, c.hr, c.rbi, c.sb,
       ROUND(c.avg::numeric, 3) AS avg, ROUND(c.obp::numeric, 3) AS obp,
       ROUND(c.slg::numeric, 3) AS slg, ROUND(c.ops::numeric, 3) AS ops,
       fba.war, fba.wrc_plus, fba.woba
FROM combined c
LEFT JOIN lahman_fangraphs_bridge lfb ON lfb.playerid = c.playerid
LEFT JOIN fba_by_season fba ON fba.idfg = lfb.idfg AND fba.season = c.season
ORDER BY c.season ASC;
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
# name is "leaders_counting" (domain-less) here — route_template() resolves it to
# leaders_batting_counting or leaders_pitching_counting based on question wording.
TEMPLATE_PATTERNS = [
    ("leaders_counting",
     re.compile(r"(?i)\b(?:leads?|leaders?|top\s*(?P<top_n>\d+)|most)\b.*\b(?P<stat_label>hr|home\s*runs?|rbi|runs\s*batted\s*in|sb|stolen\s*bases?|so|strikeouts?|bb|walks?|h|hits?)\b.*\b(?:(?P<season>\d{4})|this\s+year|this\s+season)")),
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

    # Open-ended / multi-season phrasing ("since 2015", "career", "all-time", ...)
    # needs real range logic the single-season leaderboard templates don't have —
    # defer to the LLM rather than silently collapsing it to one literal season.
    if CAREER_WORDS.search(q):
        return None, {}

    for name, pat in TEMPLATE_PATTERNS:
        m = pat.search(q)
        if m:
            gd = {k: v for k, v in m.groupdict().items() if v is not None}
            if name == "leaders_counting":
                stat_label = (gd.get("stat_label") or "").strip().lower()
                is_pitching = bool(_PITCHER_DOMAIN_RE.search(q))
                if is_pitching and stat_label in _BATTING_ONLY_STAT_LABELS:
                    # e.g. "most home runs by a pitcher" — ambiguous/unusual, let the LLM handle it
                    continue
                gd["__domain__"] = "pitching" if is_pitching else "batting"
                name = "leaders_pitching_counting" if is_pitching else "leaders_batting_counting"
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
    tdef = templates_yaml.get("templates", templates_yaml)[name]
    from datetime import date
    season = int(gd.get("season")) if gd.get("season") else date.today().year
    top_n = int(gd.get("top_n") or tdef.get("defaults", {}).get("top_n", 10))
    stat_label_nl = (gd.get("stat_label") or tdef.get("defaults", {}).get("stat_label", "stat")).lower()
    
    # Get mapped columns for both sources
    cols = nl_to_cols(stat_label_nl, domain=gd.get("__domain__", "batting"))
    
    stat_col_savant = cols["savant"]
    stat_col_lahman = cols["lahman"]

    ident_params = {
        "stat_col_savant": stat_col_savant,
        "stat_col_lahman": stat_col_lahman,
        "stat_label": stat_label_nl,
        "fragments": templates_yaml.get("fragments", {}),
    }
    bound_params = {"season": season, "top_n": top_n}
    sql = render_ident_template(tdef["sql"], ident_params)
    return sql, bound_params, name