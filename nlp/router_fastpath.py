# nlp/router_fastpath.py

# Import Packages
import re
from nlp.templates import render_sql
from nlp.stats_catalog import build_stat_catalog, resolve_stat

COUNTING_BATTING_AGG = "SUM"
RATE_BATTING_AGG     = "AVG"

def init_fastpath(conn):
    # Build once at startup; keep in memory
    return build_stat_catalog(conn)

def _pick_template(table: str, agg: str, direction: str):
    if table == "fangraphs_batting_lahman_like":
        return "leaders_batting_counting" if agg == COUNTING_BATTING_AGG else "leaders_batting_rate"
    # pitching
    if direction == "ASC":
        return "leaders_pitching_rate_low_is_best"
    return "leaders_pitching_counting"

def try_fastpath(question: str, season: int, conn, stat_catalog, top_n: int = 10, qualified: bool = False):
    # Very light intent/slot extraction
    ql = question.lower()

    # extract stat guess (word after 'in' or common phrasing)
    # You can replace this with your existing NLP; for now we pass the whole question to resolver.
    stat_col = resolve_stat(ql, stat_catalog, domain_hint=None)
    if not stat_col:
        return None  # let LLM handle

    meta = stat_catalog[stat_col]
    table = meta["table"]
    agg = meta["agg_default"]
    direction = meta["direction"]
    stat_label = stat_col

    # Qualified path only for batting (PA based)
    if qualified and table == "fangraphs_batting_lahman_like":
        stat_agg = f"{agg}({stat_col})::numeric"
        return render_sql(
            "leaders_batting_qualified",
            season=season, top_n=top_n, stat_agg=stat_agg, stat_label=stat_label
        )

    tmpl = _pick_template(table, agg, direction)
    sql = render_sql(tmpl, season=season, top_n=top_n, stat_col=stat_col, stat_label=stat_label)

    # Adjust order if template is DESC but stat is ASC (rare for pitching rates)
    if direction == "ASC":
        sql = re.sub(r"ORDER BY .*?;", f"ORDER BY {stat_label} ASC, name;", sql, flags=re.I|re.S)
    return sql
