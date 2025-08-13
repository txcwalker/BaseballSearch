# nlp/sql_render.py
import re

ASCII_FIXES = {
    "≤": "<=",
    "≥": ">=",
}

def lint_sql(sql: str) -> str:
    if "{{" in sql or "}}" in sql:
        raise ValueError("Unrendered template markers found in SQL.")
    for bad, good in ASCII_FIXES.items():
        if bad in sql:
            sql = sql.replace(bad, good)
    return sql

def enforce_leaders_invariants(sql: str) -> str:
    import re
    is_fg = ("fangraphs_batting_lahman_like" in sql) or ("fangraphs_pitching_lahman_like" in sql)
    has_season = re.search(r"\bseason\s*=\s*%\(season\)s\b", sql, re.I) is not None
    if is_fg and has_season:
        has_distinct_on = re.search(
            r"DISTINCT\s+ON\s*\(\s*[a-z_]*\.?idfg\s*,\s*[a-z_]*\.?season\s*,\s*TRIM\(\s*[a-z_]*\.?team\s*\)\s*\)", sql, re.I
        )
        has_trade_safe = (
            "COALESCE(" in sql
            and "FILTER (WHERE team = 'TOT')" in sql
            and "FILTER (WHERE team NOT IN ('TOT','---'))" in sql
        )
        if not (has_distinct_on and has_trade_safe):
            raise ValueError("Missing traded-player safeguards (DISTINCT ON + COALESCE MAX 'TOT' + SUM non-'TOT').")
        if re.search(r"\bWHERE\b[^;]*\bteam\s*=\s*'TOT'", sql, re.I):
            raise ValueError("Illegal WHERE team='TOT' in leaders query.")
    return sql

