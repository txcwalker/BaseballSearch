# nlp/linter.py
import re
from dataclasses import dataclass
from datetime import date

# Simple counting stats that should not have PA/IP qualifiers on single-season leaderboards
COUNTING_STATS = {"hr", "rbi", "sb", "r", "h", "doubles", "triples", "bb", "so", "cs", "ibb", "hbp"}

LEADER_TRIG = re.compile(r"\b(league\s+leaders?|leaders?|most|top\s+\d+|who\s+has)\b", re.I)
YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b", re.I)
CAREER_WORDS = re.compile(r"\b(career|all[-\s]?time|since\s+\d{4}|over\s+\d+\s+seasons|rolling|span|multi[-\s]?year)\b", re.I)

# Triggers that indicate unavailable data for this database (game logs, etc.)
UNAVAILABLE_TRIG = re.compile(
    r"\b(handedness|left[-\s]?handed|right[-\s]?handed|pitch[-\s]?by[-\s]?pitch|game\s*(log|by\s*game))\b", re.I
)

# Any Savant "advanced" table usage (restricted to 2015+)
ADVANCED_TABLE_RE = re.compile(
    r"\bfrom\s+savant_(?:batting_expected|batting_physics|batting_discipline|"
    r"pitching_expected|pitching_physics|pitching_discipline)\b",
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
        # FILTER() is fine when it's the recognized traded-player (TOT) safeguard
        # idiom — e.g. MAX(stat) FILTER (WHERE team = 'TOT'). Only flag other uses.
        filter_clauses = re.findall(r"filter\s*\(\s*where\s+([^)]*)\)", s, re.I)
        bad_filters = [c for c in filter_clauses if "team" not in c or "tot" not in c]
        if bad_filters:
            reasons.append("Single-season leaderboard uses FILTER() for something other than TOT traded-player handling.")
        # Combining TOT and non-TOT via FILTER()-scoped aggregation (e.g.
        # COALESCE(MAX(stat) FILTER (WHERE team='TOT'), SUM(stat) FILTER (WHERE team NOT IN (...))))
        # is the correct traded-player-safe idiom used throughout this project — only flag
        # a mix that happens OUTSIDE any FILTER() clause (e.g. directly in a WHERE clause),
        # since that combination is never sensible there.
        s_no_filters = re.sub(r"filter\s*\([^)]*\)", "", s, flags=re.I)
        if ("team = 'tot'" in s_no_filters or "team='tot'" in s_no_filters) and (
            "team not in ('tot','---')" in s_no_filters or "team not in('tot','---')" in s_no_filters
        ):
            reasons.append("Do not compute TOT and non-TOT in the same SELECT/CTE for single-season leaders.")
        # counting stat: no PA/IP qualifiers
        if is_counting_stat_leaderboard(q) and re.search(r"\b(pa|ip)\s*>=\s*\d", s):
            reasons.append("Do not apply PA/IP thresholds to counting-stat leaderboards.")

    # Current-year rule: must not use Lahman tables
    year = detect_year(q, current_year_int)
    if year == current_year_int and re.search(r"\bfrom\s+(batting|pitching|teams|people)\b", s):
        reasons.append("Current-season query must use Savant tables, not Lahman.")

    # Advanced Savant tables: only allowed for 2015+
    if year < 2015 and ADVANCED_TABLE_RE.search(s):
        reasons.append("Advanced Statcast metrics are unavailable before 2015 for this database.")

    # quick table usage hints
    meta["uses_lahman"] = bool(re.search(r"\bfrom\s+(batting|pitching|teams|people)\b", s))
    meta["uses_savant"] = "from savant_" in s

    return LintResult(ok=(len(reasons) == 0), reasons=reasons, meta=meta)
