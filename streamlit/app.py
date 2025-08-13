# app.py
import os, sys
from pathlib import Path

import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# 0) Page config MUST be first Streamlit call
st.set_page_config(page_title="Welcome to Databaseball", layout="wide")

# Add project root to import path (so we can import nlp.* later, lazily)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- env helpers & flags ---
def env(key: str, default=None):
    """Read from Streamlit secrets first (Cloud), then environment."""
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

DEBUG_UI   = env("DBBALL_DEBUG_UI", "0") == "1"   # show extra debug controls
SAFE_START = env("DBBALL_SAFE_START", "0") == "1" # skip DB/LLM init on first load

# Load local .envs if present (harmless on Cloud)
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")
load_dotenv(Path(__file__).resolve().parents[1] / ".env.gemini")

DB_PARAMS = {
    "host": env("AWSHOST"),
    "port": env("AWSPORT"),
    "dbname": env("AWSDATABASE"),
    "user": env("AWSUSER"),
    "password": env("AWSPASSWORD"),
}

# --- cached resources & helpers ---
@st.cache_resource(show_spinner=False)
def get_stat_catalog(_init_fastpath_fn):
    # Short connection + statement timeouts so the app never hangs
    conn = psycopg2.connect(
        **DB_PARAMS,
        connect_timeout=5,
        options="-c statement_timeout=5000"
    )
    try:
        return _init_fastpath_fn(conn)
    finally:
        try:
            conn.close()
        except Exception:
            pass

@st.cache_data(show_spinner=False, ttl=300)
def run_sql(sql: str, params: dict | None = None):
    with psycopg2.connect(
        **DB_PARAMS,
        connect_timeout=5,
        options="-c statement_timeout=15000"
    ) as conn:
        return pd.read_sql_query(sql, conn, params=params or {})

def looks_like_sql(s: str) -> bool:
    lo = (s or "").lstrip().lower()
    return lo.startswith((
        "select", "with", "explain",
        "insert into", "update", "delete from",
        "create view", "create table"
    ))

def title_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.replace("_", " ").title() if isinstance(col, str) else col for col in df.columns]
    return df

def style_dataframe(df: pd.DataFrame):
    return (df.style
            .set_properties(**{
                'background-color': '#fdfdfd','color': '#111',
                'border-color': '#ccc','font-size': '14px','text-align': 'left'
            })
            .set_table_styles([{'selector': 'th','props': [
                ('background-color', '#003f5c'), ('color', 'white'), ('font-size', '15px')
            ]}]))

# IMPORTANT: Do NOT import nlp.* at module import time.
# We lazy-import below so any import error is visible in the UI instead of hanging the process.
_NLP_LOADED = False
gsql = None
init_fastpath = None
try_fastpath = None
route_template = None
lint_sql = None
enforce_leaders_invariants = None

def load_nlp_modules():
    """Lazy import all nlp modules. Raises on failure so we can show errors on screen."""
    global _NLP_LOADED, gsql, init_fastpath, try_fastpath, route_template, lint_sql, enforce_leaders_invariants
    if _NLP_LOADED:
        return
    import importlib
    gsql = importlib.import_module("nlp.generate_sql")
    rfp  = importlib.import_module("nlp.router_fastpath")
    tr   = importlib.import_module("nlp.template_router")
    sr   = importlib.import_module("nlp.sql_render")
    init_fastpath = getattr(rfp, "init_fastpath")
    try_fastpath  = getattr(rfp, "try_fastpath")
    route_template = getattr(tr, "route_template")
    lint_sql = getattr(sr, "lint_sql")
    enforce_leaders_invariants = getattr(sr, "enforce_leaders_invariants")
    _NLP_LOADED = True

# No DB / LLM calls at import time
STAT_CATALOG = None

# ------------------ PAGE: Home (callable) ------------------
def render_home():
    global STAT_CATALOG

    # Sidebar (local)
    try:
        from render_sidebar import render_sidebar
        render_sidebar()
    except Exception as e:
        st.error(f"Sidebar failed to load: {e}")

    # Header
    st.markdown("""
        <div style='text-align: center; padding: 3rem 0 2rem 0; background-color: #f0f8ff; border-radius: 12px;'>
            <h1 style='font-size: 3.5em; margin-bottom: 0.2em;'>
                Welcome to Databaseball!
                <span style='font-size: 0.4em; color: white; background-color: #f39c12; padding: 4px 8px; border-radius: 8px; margin-left: 10px; vertical-align: middle;'>BETA</span>
            </h1>
            <p style='font-size: 1.3em; color: #444;'>Ask questions. Explore stats. Discover the game.</p>
        </div>
    """, unsafe_allow_html=True)

    st.caption(f"Python {os.sys.version.split()[0]} • SAFE_START={int(SAFE_START)} • DEBUG_UI={int(DEBUG_UI)}")

    st.markdown("### What You Can Do")
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Ask Questions")
        st.markdown("- 'Show Shohei Ohtani\\'s stats in 2022'\n- 'Top 10 home run hitters since 2010'\n- 'Which pitchers have the biggest single-season gap between FIP and ERA since 2015?'")
    with col2:
        st.markdown("#### Updated Daily")
        st.markdown("- Powered by AWS + GitHub Actions\n- Data from FanGraphs & Lahman\n- Always current")

    st.markdown("---")
    st.markdown("### Read Me")
    st.page_link("pages/how_to_use.py", label="How to Use")

    # Lazy import NLP stack
    try:
        load_nlp_modules()
    except Exception as e:
        st.error(f"Failed to load NLP modules (nlp/*). {e}")
        st.stop()

    # Optional: lazy-init the fast-path catalog (DB) unless SAFE_START
    if not SAFE_START and STAT_CATALOG is None:
        try:
            with st.status("Initializing fast-path (short DB check)…", state="running"):
                STAT_CATALOG = get_stat_catalog(init_fastpath)
            st.success("Fast-path enabled.")
        except Exception as e:
            st.info(f"Fast-path init skipped: {e}")
            STAT_CATALOG = None

    # Load schema/prompt/templates AFTER UI draws (pure local)
    try:
        schema_str = gsql.load_schema()
        prompt_template = gsql.load_prompt_template()
        templates_yaml = gsql.load_templates_yaml()
        st.caption(f"Loaded templates: {len(templates_yaml.get('templates', {}))}")
    except Exception as e:
        st.error(f"Failed to load schema/prompts/templates: {e}")
        st.stop()

    # Controls
    nl_query = st.text_input("Ask a baseball question:")
    use_templates = st.checkbox("Use templates (faster & deterministic when available)", value=True)
    show_prompt = False
    if DEBUG_UI:
        show_prompt = st.checkbox("Show LLM prompt (debug)", value=False)

    if SAFE_START:
        st.warning("SAFE_START is enabled — DB/LLM calls are skipped until you turn this off.")

    submit = st.button("Generate SQL and Run")

    if submit and nl_query:
        norm_q, season = gsql.normalize_query(nl_query)
        sql_query, bound_params = None, {}

        # 0) fast-path (only if available)
        if STAT_CATALOG is not None:
            fast_sql = try_fastpath(
                question=norm_q, season=season, conn=None,
                stat_catalog=STAT_CATALOG, top_n=10,
                qualified=("qualified" in norm_q.lower())
            )
            if fast_sql:
                try:
                    fast_sql = lint_sql(fast_sql)
                    fast_sql = enforce_leaders_invariants(fast_sql)
                    sql_query = fast_sql
                    st.info("Using fast-path leaders (validated).")
                except Exception as e:
                    st.warning(f"Fast-path rejected ({e}); falling back to templates.")
                    sql_query = None

        # 1) templates
        if sql_query is None and use_templates:
            tname, tparams = route_template(norm_q)
            st.caption(f"Template route preview: {tname or '—'}  {tparams or ''}")
            try:
                sql_query, bound_params, source = gsql.get_sql_and_params(
                    user_question=norm_q,
                    schema_text=schema_str,
                    prompt_template=prompt_template,
                    templates_yaml=templates_yaml,
                    current_year=gsql.CURRENT_YEAR,
                    season=season,
                    preset_sql=""
                )
                st.info(f"Using {source}.")
            except Exception as e:
                sql_query, bound_params = None, {}
                st.warning(f"Template route failed: {e}")

        # 2) LLM (guarded)
        if sql_query is None:
            if SAFE_START:
                st.error("SAFE_START is enabled; LLM calls disabled. Turn off SAFE_START to allow LLM generation.")
                st.stop()

            full_prompt = gsql.build_prompt(
                norm_q, schema_str, prompt_template, season, current_year=gsql.CURRENT_YEAR
            )
            if DEBUG_UI and show_prompt:
                with st.expander("Show prompt sent to LLM", expanded=False):
                    st.code(full_prompt, language="markdown")

            sql_query = gsql.get_sql_from_gemini(full_prompt)
            action = gsql.handle_model_response(sql_query, season)
            if action == "__REPROMPT__":
                sql_query = gsql.get_sql_from_gemini(
                    full_prompt + "\n\n# REMINDER: REQUESTED_SEASON == CURRENT_YEAR; provide season-to-date SQL."
                )
                action = gsql.handle_model_response(sql_query, season)
            if action and action != "__REPROMPT__":
                st.error(action); st.stop()
            if not looks_like_sql(sql_query):
                st.error("I couldn’t generate executable SQL from that question."); st.stop()

        if DEBUG_UI and st.toggle("Show generated SQL", value=False):
            st.code(sql_query, language="sql")

        # Execute
        try:
            df_result = run_sql(sql_query, bound_params)
            df_result = title_case_columns(df_result)
            st.success(f"Query successful! Returned {len(df_result):,} rows.")
            st.dataframe(df_result)
            csv = df_result.to_csv(index=False).encode("utf-8")
            st.download_button("Download CSV", csv, file_name="results.csv", mime="text/csv")
        except Exception as e:
            if DEBUG_UI:
                st.error(f"Error running PostgreSQL query: {e}")
                if st.toggle("Show failed SQL?", value=False):
                    st.code(sql_query, language="sql")
            else:
                st.error("Something went wrong running your query.")


# ------------------ NAVIGATION ------------------
# Build Page objects (conditionally include file pages if they exist)
PAGES_DIR = Path(__file__).parent / "pages"

def _maybe_page(rel_path: str, title: str):
    fp = PAGES_DIR / Path(rel_path).name
    return st.Page(f"pages/{fp.name}", title=title) if fp.exists() else None

home_page    = st.Page(render_home, title="Home")
howto_page   = _maybe_page("how_to_use.py", "How to Use")
about_page   = _maybe_page("about.py", "About")
contact_page = _maybe_page("contact.py", "Contact")

pages = [home_page]
for p in (howto_page, about_page, contact_page):
    if p:
        pages.append(p)

# Only add Test Mode if env flag is set AND file exists
test_page = _maybe_page("test_mode.py", "Test Mode")
if env("DBBALL_ENABLE_TEST_UI", "0") == "1" and test_page:
    pages.append(test_page)

# Save Page objects for sidebar links
st.session_state["home_page"] = home_page
st.session_state["howto_page"] = howto_page
st.session_state["about_page"] = about_page
st.session_state["contact_page"] = contact_page

nav = st.navigation(pages, position="sidebar")
nav.run()
