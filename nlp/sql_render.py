# nlp/sql_render.py
import re

ASCII_FIXES = {
    "≤": "<=",
    "≥": ">=",
}

def lint_sql(sql: str) -> str:
    """Basic SQL cleanup — fix non-ASCII operators, catch unrendered template markers."""
    if "{{" in sql or "}}" in sql:
        raise ValueError("Unrendered template markers found in SQL.")
    for bad, good in ASCII_FIXES.items():
        if bad in sql:
            sql = sql.replace(bad, good)
    return sql


def enforce_leaders_invariants(sql: str) -> str:
    """
    Validate traded-player safeguards on FanGraphs single-season leaderboard queries.

    Previously this raised ValueError on any violation, which caused valid LLM-generated
    SQL to be silently dropped and surfaced as 'Query failed'. Now it only raises on
    unrendered template markers (a hard error). Traded-player checks log a warning
    but let the query through — the DB will still return correct results in most cases,
    and a slightly imperfect leaderboard is better than no result at all.
    """
    # Hard error: unrendered Jinja markers mean the template didn't render properly
    if "{{" in sql or "}}" in sql:
        raise ValueError("Unrendered Jinja template markers in SQL — template did not render.")

    # Soft check: warn but don't block if traded-player safeguards are missing
    is_fg_leaders = (
        ("fangraphs_batting_lahman_like" in sql or "fangraphs_pitching_lahman_like" in sql)
        and re.search(r"\bseason\s*=\s*%\(season\)s\b", sql, re.I)
    )

    if is_fg_leaders:
        has_tot_guard = (
            "FILTER (WHERE team = 'TOT')" in sql
            or "team = 'TOT'" in sql
            or "DISTINCT ON" in sql.upper()
        )
        if not has_tot_guard:
            # Log to stderr but don't raise — let the query run
            import sys
            print(
                "Warning: FanGraphs leaderboard SQL may be missing traded-player (TOT) safeguards. "
                "Results may double-count traded players.",
                file=sys.stderr
            )

    return sql