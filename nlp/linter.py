# nlp/linter.py
import re
from dataclasses import dataclass
from datetime import date

# Simple counting stats that should not have PA/IP qualifiers on single-season leaderboards
COUNTING_STATS = {"hr", "rbi", "sb", "r", "h", "doubles", "triples", "bb", "so", "cs", "ibb", "hbp"}

LEADER_TRIG = re.compile(r"\b(league\s+leaders?|leaders?|most|top\s+\d+|who\s+has)\b", re.I)
YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b", re.I)
CAREER_WORDS = re.compile(r"\b(career|all[-\s]?time|since\s+\d{4}|over\s+\d+\s+seasons|rolling|span|multi[-\s]?year)\b", re.I)

# Triggers that indicate unavailable data for this database (Statcast, handedness splits, game logs, etc.)
UNAVAILABLE_TRIG = re.compile(
    r"\b(handedness|left[-\s]?handed|right[-\s]?handed|statcast|pitch[-\s]?by[-\s]?pitch|game\s*(log|by\s*game)|"
    r"exit\s*velocity|launch\s*angle|swing\s*speed|spray\s*chart|catch\s*probability)\b", re.I
)

# Any FanGraphs "advanced" table usage (restricted to 2002+)
ADVANCED_TABLE_RE = re.compile(
    r"\bfrom\s+fangraphs_(?:batting_advanced|pitching_advanced|plate_discipline|batted_ball|"
    r"pitching_batted_ball|batter_pitch_type_summary|pitching_pitch_type_summary)\b",
    re.I,
)

@dataclass
class LintResult:
    ok: bool
    reasons: list
    meta: dict

def is_single_season_leaderboard(q: str) -> bool:
    if not LEADER_TRIG.search(q or ""):
        return False
    years = YEAR_RE.findall(q or "")
    if len(years) != 1:
        return False
    if CAREER_WORDS.search(q or ""):
        return False
    return True

def is_counting_stat_leaderboard(q: str) -> bool:
    ql = (q or "").lower()
    return any(s in ql for s in COUNTING_STATS) and is_single_season_leaderboard(q)

def detect_year(q: str, fallback_year: int) -> int:
    m = YEAR_RE.search(q or "")
    return int(m.group(0)) if m else int(fallback_year)

def lint_sql(user_q: str, sql: str, current_year: int | None = None) -> LintResult:
    """Lint generated SQL against project rules. Returns ok/reasons/meta."""
    current_year_int = int(current_year or date.today().year)
    reasons, meta = [], {}
    q = user_q or ""
    s = (sql or "").strip().lower()

    # Must look like SQL
    if not re.search(r"^\s*(select|with)\b", s):
        return LintResult(ok=False, reasons=["Output is not SQL."],
                          meta={"uses_lahman": False, "uses_fangraphs": False})

    # Query-level refuses (question asks for unavailable data)
    if UNAVAILABLE_TRIG.search(q):
        reasons.append("Question requests unavailable data (handedness/Statcast/game-by-game/etc.).")

    # Single-season leaders: enforce constraints
    if is_single_season_leaderboard(q):
        if "filter(" in s:
            reasons.append("Single-season leaderboard must not use FILTER().")
        # any sign of computing TOT and non-TOT together in one step
        if ("team = 'tot'" in s or "team='tot'" in s) and (
            "team not in ('tot','---')" in s or "team not in('tot','---')" in s
        ):
            reasons.append("Do not compute TOT and non-TOT in the same SELECT/CTE for single-season leaders.")
        # counting stat: no PA/IP qualifiers
        if is_counting_stat_leaderboard(q) and re.search(r"\b(pa|ip)\s*>=\s*\d", s):
            reasons.append("Do not apply PA/IP thresholds to counting-stat leaderboards.")

    # Current-year rule: must not use Lahman tables
    year = detect_year(q, current_year_int)
    if year == current_year_int and re.search(r"\bfrom\s+(batting|pitching|teams|people)\b", s):
        reasons.append("Current-season query must use FanGraphs tables, not Lahman.")

    # Advanced FG tables: only allowed for 2002+
    if year < 2002 and ADVANCED_TABLE_RE.search(s):
        reasons.append("Advanced FanGraphs metrics are unavailable before 2002 for this database.")

    # quick table usage hints
    meta["uses_lahman"] = bool(re.search(r"\bfrom\s+(batting|pitching|teams|people)\b", s))
    meta["uses_fangraphs"] = "from fangraphs_" in s

    return LintResult(ok=(len(reasons) == 0), reasons=reasons, meta=meta)
