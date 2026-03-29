# nlp/sql_render.py
import re

ASCII_FIXES = {
    "≤": "<=",
    "≥": ">=",
}

# Matches either form of the non-TOT exclusion filter
_TRADE_EXCLUDE_RE = re.compile(
    r"FILTER\s*\(WHERE\s+team\s+NOT\s+IN\s*\(\s*'TOT'\s*(?:,\s*'---')?\s*\)\)",
    re.I
)

_DISTINCT_ON_RE = re.compile(
    r"DISTINCT\s+ON\s*\(\s*[a-z_]*\.?idfg\s*,\s*[a-z_]*\.?season\s*,\s*TRIM\(\s*[a-z_]*\.?team\s*\)\s*\)",
    re.I
)

_ILLEGAL_WHERE_TOT_RE = re.compile(
    r"(?<!FILTER\s)(?<!FILTER)\bWHERE\b\s+team\s*=\s*'TOT'",
    re.I
)


def lint_sql(sql: str) -> str:
    if "{{" in sql or "}}" in sql:
        raise ValueError("Unrendered template markers found in SQL.")
    for bad, good in ASCII_FIXES.items():
        sql = sql.replace(bad, good)
    return sql


def enforce_leaders_invariants(sql: str) -> str:
    is_fg = (
        "fangraphs_batting_lahman_like" in sql
        or "fangraphs_pitching_lahman_like" in sql
    )
    has_season_param = re.search(r"\bseason\s*=\s*%\(season\)s\b", sql, re.I) is not None

    if not (is_fg and has_season_param):
        return sql

    has_distinct_on = bool(_DISTINCT_ON_RE.search(sql))
    has_coalesce_tot = "FILTER (WHERE team = 'TOT')" in sql
    has_exclude_filter = bool(_TRADE_EXCLUDE_RE.search(sql))

    if not (has_distinct_on and has_coalesce_tot and has_exclude_filter):
        raise ValueError(
            "Missing traded-player safeguards "
            "(DISTINCT ON + COALESCE MAX 'TOT' + exclusion filter)."
        )

    # Strip FILTER(...) clauses before checking for illegal bare WHERE team='TOT'
    sql_stripped = re.sub(r"FILTER\s*\([^)]*\)", "", sql, flags=re.I)
    if re.search(r"\bWHERE\s+team\s*=\s*'TOT'", sql_stripped, re.I):
        raise ValueError("Illegal WHERE team='TOT' in leaders query.")

    return sql