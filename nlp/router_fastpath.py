# nlp/router_fastpath.py
#
# Deterministic shortcut for simple counting-stat leaderboard questions
# ("most home runs in 2019", "top 5 in strikeouts by a pitcher"). Anything it
# doesn't recognize falls through to nlp/template_router.py or the LLM.

import re

from nlp.templates import render_sql
from nlp.stats_catalog import build_stat_catalog, resolve_stat
from nlp.linter import CAREER_WORDS

_PITCHER_DOMAIN_RE = re.compile(r"(?i)\bpitch(?:er|ers|ing)\b")

# This fast-path only ever builds a top-N leaderboard — it has no notion of a
# specific player at all. Without this guard, a question like "How many home
# runs did Bobby Witt Jr have in 2024?" or "Compare Trout and Betts' home runs
# in 2022" would silently resolve "home runs" as a stat and return an unrelated
# top-10 leaderboard instead of what was actually asked (confirmed live —
# neither query even mentions "most"/"leaders"/"top N"). Require real
# leaderboard-intent wording before considering the fast-path at all.
_LEADERBOARD_INTENT_RE = re.compile(r"(?i)\b(?:led|leads?|leaders?|top\s*\d+|most)\b")

# The catalog only ever contains 6 counting stats (HR/RBI/SB/SO/BB/H) built from
# nlp.template_router's STAT_MAP_*. Fuzzy-matching (rapidfuzz WRatio) is meant to
# tolerate typos/phrasing on those 6, not to be the sole gate against every other
# stat in English. On a long sentence, WRatio's partial-ratio behavior can find
# enough incidental word overlap to spuriously clear even an 85 score threshold
# for a completely unrelated stat — confirmed live: "What were the top 5 WAR
# seasons for position players in the 2010s?" scored 85.5 against "runs batted
# in" and silently returned an RBI leaderboard mislabeled as WAR (raising the
# threshold from 70 to 85 in an earlier session did not fully close this). Any
# question naming one of these real, different stats must never reach the fuzzy
# matcher at all, regardless of score.
_NON_CATALOG_STAT_RE = re.compile(
    r"(?i)\bwar\b|\bwoba\b|\bwrc\+?\b|\bfip\b|\bxfip\b|\bops\+?\b|\bobp\b|\bslg\b"
    r"|\bavg\b|\bera\b|\bwhip\b|\biso\b|\bxwoba\b|\bxba\b|\bxslg\b|\bbarrel"
    r"|exit velo|launch angle|sprint speed|hard.hit|whiff|chase.rate|batting average"
    r"|on.base|slugging|isolated power"
)


def _mentions_non_catalog_stat(question: str) -> bool:
    return bool(_NON_CATALOG_STAT_RE.search(question))


def init_fastpath(conn):
    # Build once at startup; keep in memory. The catalog is derived from static
    # stat definitions (see nlp/stats_catalog.py), not live DB introspection,
    # but conn is kept for interface compatibility with callers.
    return build_stat_catalog(conn)


def try_fastpath(question: str, season: int, conn, stat_catalog, top_n: int = 10, qualified: bool = False):
    if not _LEADERBOARD_INTENT_RE.search(question):
        return None  # not a leaderboard question -- let templates/LLM handle it

    # This fast-path only ever builds a single-season leaderboard for whatever
    # year normalize_query() extracted from the question. "Since 2010" / "career"
    # / "all-time" phrasing needs real multi-season range logic this fast-path
    # doesn't have — without this guard it silently collapsed "most strikeouts
    # since 2010" into an exact season=2010 leaderboard (confirmed live).
    # template_router.py already applies the same guard; mirror it here.
    if CAREER_WORDS.search(question):
        return None  # let templates/LLM handle open-ended ranges

    if _mentions_non_catalog_stat(question):
        return None  # a real stat this catalog can't serve -- don't risk a fuzzy false-positive

    is_pitching = bool(_PITCHER_DOMAIN_RE.search(question))
    domain_hint = "pitching" if is_pitching else "batting"

    stat_key = resolve_stat(question.lower(), stat_catalog, domain_hint=domain_hint)
    if not stat_key:
        return None  # let templates/LLM handle it

    meta = stat_catalog[stat_key]
    # The catalog only ever contains counting stats (see stats_catalog.py) —
    # qualification thresholds don't apply to counting-stat leaderboards (the
    # prompt explicitly forbids PA/IP qualifiers on HR/RBI/SO/etc.), so there's
    # no "qualified" branch here. A rate-stat "qualified" question (e.g. "best
    # OBP among qualified hitters") won't resolve against this catalog at all
    # and correctly falls through to templates/LLM instead.
    tmpl = "leaders_pitching_counting" if meta["domain"] == "pitching" else "leaders_batting_counting"

    sql = render_sql(
        tmpl, season=season, top_n=top_n,
        stat_col_savant=meta["savant_col"], stat_col_lahman=meta["lahman_col"],
        stat_label=meta["stat_label"],
    )

    if meta["direction"] == "ASC":
        sql = re.sub(r"ORDER BY .*?;", f'ORDER BY "{meta["stat_label"]}" ASC, name;', sql, flags=re.I | re.S)
    return sql
