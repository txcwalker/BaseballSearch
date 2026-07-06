# AGENTS.md — AI-to-AI Handoff Contract

Dense reference for agents picking up this repo. Full prose context lives in
[DEVELOPMENT.md](DEVELOPMENT.md) (architecture map) and [WORKLOG.md](WORKLOG.md)
(session-by-session change log, newest first) — read those before this file if
you have budget; this file is the compressed version for tight-context runs.

## 1. Core Purpose
NL → SQL baseball stats search. User asks a question in English → app routes it
through (fast-path shortcut → regex/YAML template → Gemini LLM fallback) → runs
the resulting SQL read-only against a Postgres DB (AWS RDS) → returns results via
Streamlit. Three-tier data model (see § Rules for details): Savant (current
season, daily), Lahman (all other seasons + awards/HOF/postseason/managers/
standings), FanGraphs (`fba`/`fpa` only — frozen historical archive, WAR/wOBA/
wRC+/FIP/xFIP exclusively, no longer receives new data).

## 2. Current Priorities
- Migrate `google-generativeai` (deprecated by Google) → `google-genai` in
  [nlp/generate_sql.py](nlp/generate_sql.py). Approved, not started.
- Consolidate local dev to Python 3.12 (`.venv/` is currently 3.8, too old for
  `requirements.txt`; `.venv_test/` at 3.12 works — not yet merged/renamed).
- `lahman_fangraphs_bridge` has real gaps (confirmed: no entry for Clayton
  Kershaw; only maps the 1980s-90s Bobby Witt, not the current shortstop) —
  WAR/wOBA/wRC+/FIP/xFIP will silently come back NULL for affected players via
  the LEFT JOINs in `nlp/template_router.py`'s career handlers. This is graceful
  degradation by design, not a bug, but worth knowing when debugging "missing"
  advanced stats for a specific player.
- The LLM doesn't always apply Rule 1's Savant-first/Lahman-fallback pattern on
  more complex query shapes — confirmed for a "qualified rate stat" leaderboard
  (e.g. "best batting average among qualified hitters in 2025"), where the model
  queried Savant only and returned zero rows for a season Savant doesn't have,
  instead of falling back to Lahman. The same question works fine against a
  season Savant *does* have, or a clearly historical season. This is LLM
  prompt-following variance on an underspecified case, not a deterministic bug —
  worth a targeted prompt example if it keeps recurring.

## 3. Active Files
See the table in [DEVELOPMENT.md § Active Code Areas](DEVELOPMENT.md#active-code-areas)
for the full active/legacy breakdown. Files touched most recently (2026-07-03/04,
FanGraphs-removal migration + Lahman 2025 data refresh): `nlp/prompts/base_prompt_gemini.txt`,
`nlp/schema/schema_description.txt`, `nlp/templates/sql_templates.yml`,
`nlp/template_router.py`, `nlp/router_fastpath.py`, `nlp/stats_catalog.py`,
`nlp/linter.py`, `nlp/generate_sql.py`, `streamlit/app.py`,
`streamlit/pages/how_to_use.py`, `etl/load_lahman.py` (rewritten — see § 6 and
§ 7, was previously non-functional).

## 4. Frozen/Legacy Zones — DO NOT TOUCH without explicit instruction
- [api/](api) — FastAPI wrapper, not deployed, not referenced by the live app.
  Uses a different env-var convention (`PGHOST` etc.) than everything else.
- [scratch/](scratch), [notebooks/](notebooks) — exploration only, not part of the app.
- [db/query_runner.py](db/query_runner.py) — self-documented "Currently not in Use."
- `data/` — gitignored raw/processed artifacts. The live app reads AWS RDS
  Postgres, **not** `data/processed/stats.db` — don't assume writes here are visible
  to the deployed app.
- `nlp/templates/sql_templates.yml`'s `team_era_season`, `team_batting_avg_division`,
  `player_pitching_career_by_season` — shadowed by hardcoded duplicates in
  `nlp/template_router.py`'s `DIRECT_PATTERNS`; only reachable via the CLI's
  `generate_sql.match_template_data_driven`, not the live app path. Don't assume
  editing these YAML entries changes live app behavior. (`player_pitching_career_by_season`
  is also still FanGraphs-only/unfixed — the live equivalent,
  `template_router._player_pitching_career_sql`, is the one that was migrated.)
- `nlp/templates/sql_templates.yml`'s `leaders_batting_rate` / `leaders_pitching_rate_low_is_best` —
  not reachable from any live path (no fast-path catalog entry ever has a rate
  stat; no `template_router.py` regex targets them either). Kept FanGraphs-free
  and syntactically valid, but not integration-tested against a real caller.

## 5. Rules & Coding Style
- Python, no type-checked strictness enforced (no mypy config found) but existing
  code uses type hints in newer files (`nlp/linter.py`, `nlp/generate_sql.py`) —
  match that style in new code.
- SQL is hand-written (Jinja2-templated for identifiers, `%(name)s` psycopg2
  params for values) — no ORM.
- **Never hardcode a data-source cutover year.** Savant only ever holds the
  current season; any fixed year (e.g. "use Savant for 2025+") goes stale every
  season. Use the `NOT EXISTS`-guarded UNION pattern in `sql_templates.yml`'s
  `leaders_batting_counting` template as the reference implementation.
- The LLM prompt ([nlp/prompts/base_prompt_gemini.txt](nlp/prompts/base_prompt_gemini.txt))
  and the schema doc ([nlp/schema/schema_description.txt](nlp/schema/schema_description.txt))
  are injected into the same prompt and MUST NOT contradict each other — this has
  been the single most common root cause of query failures found so far. When
  editing one, check the other.
- psycopg2 param dicts + literal `%` in SQL string literals don't mix — `LIKE
  '%foo%'` breaks `cur.execute(sql, {...})` (psycopg2 tries to parse the literal
  `%` as a placeholder). Use `position()`/`split_part()` instead of `LIKE '%...%'`
  when the query will be executed with a params dict.
- Savant IDs are integers (`player_id`), Lahman IDs are text (`playerid`, e.g.
  `'wittbo02'`) — casting one to match the other (`::text`) is required before
  `UNION`ing rows from both sources into the same identifier column.
- Lahman's `batting` table has `"2b"`/`"3b"` as the literal (quoted) column names
  for doubles/triples, not `doubles`/`triples` — this only got caught by actually
  executing the SQL against the live DB, not by reading `nlp/schema/schema_description.txt`,
  which describes them generically. Verify column names against `information_schema.columns`
  when in doubt, don't fully trust the schema doc's prose.
- Lahman `batting`/`pitching` have NO `'TOT'` row for traded players (unlike
  FanGraphs `fba`/`fpa`, which do) — a trade is just multiple rows with
  different `stint` values. `SUM` across all of a player's stint rows for a
  season; never filter for `team = 'TOT'` on a Lahman table.

## 6. Verification & Test Commands
```bash
# Full NL->SQL regression batch (fast-path -> template -> LLM, lint, live DB exec)
.venv/Scripts/python tests/run_regression.py
# -> tests/results/regression_<timestamp>.csv ; check exec_status column
#    (PASS / PASS_EMPTY / REFUSED / FAIL / ERROR)

# Single-question CLI check (fast iteration on prompt/template changes)
.venv/Scripts/python -m nlp.generate_sql "your question here" --print-prompt

# Local app for manual/by-hand testing
.venv/Scripts/streamlit run streamlit/app.py
# set DBBALL_DEBUG_UI=1 / DBBALL_ENABLE_TEST_UI=1 (.streamlit/secrets.toml, local
# only, gitignored) to see routing source + raw SQL + the hidden Test Mode page

# Incrementally load new-season rows from data/lahman_raw/*.csv into AWS RDS.
# Defaults to a dry run (reports what WOULD be inserted, writes nothing).
.venv/Scripts/python etl/load_lahman.py
.venv/Scripts/python etl/load_lahman.py --commit          # actually write
.venv/Scripts/python etl/load_lahman.py --only teams,batting --commit  # scoped
```
No unit test framework (pytest etc.) is configured — `tests/run_regression.py` is
the only automated correctness check in the repo. There is no CI gate running it.

## 7. Fragile Areas
- **`nlp/linter.py`'s real validation rules are not wired into the live app.**
  `streamlit/app.py` and `nlp/generate_sql.py` use `nlp/sql_render.py:lint_sql`
  instead, which only fixes non-ASCII operators and checks for unrendered `{{ }}`
  — it will not catch a logically wrong query. `nlp/linter.py:lint_sql` (question-
  aware) is only used in `tests/run_regression.py` and `streamlit/pages/test_mode.py`.
- **Two separate template-matching systems** exist: `nlp/template_router.py` (hand-
  coded `DIRECT_PATTERNS` + one YAML-backed pattern, used live) and
  `nlp/generate_sql.py:match_template_data_driven` (reads ALL YAML patterns, CLI
  only). Editing `sql_templates.yml` does not necessarily change live behavior —
  check which router actually consumes the template you're changing.
- `google-generativeai` (pinned in `requirements.txt`) is fully deprecated by
  Google (runtime `FutureWarning` on import). Works today; migrate before it stops.
- GitHub Actions daily Savant ETL ([.github/workflows/savant_autoload.yml](.github/workflows/savant_autoload.yml))
  has intermittent RDS security-group propagation timing failures — actively
  being iterated on by the project owner, not by AI agents so far.

## 8. Generated Artifacts (ignore/monitor, do not hand-edit)
- `tests/results/*.csv` — regenerated each `run_regression.py` run.
- `**/__pycache__/`, `*.pyc`, `.venv/`, `.venv_test/`.
- `data/processed/stats.db` — local SQLite snapshot, not what the live app queries.
