# app.py (debug instrumented)
import os, sys, time
from pathlib import Path

import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv

T0 = time.time()

# -------- utilities --------
def mark(label: str):
    """Log a boot/progress marker to both logs and UI."""
    dt = time.time() - T0
    msg = f"[BOOT {dt:6.2f}s] {label}"
    print(msg, flush=True)
    st.caption(msg)

def env(key: str, default=None):
    """Read from Streamlit secrets first (Cloud), then environment."""
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

# Page config must be first Streamlit call
st.set_page_config(page_title="Welcome to Databaseball", layout="wide")
mark("after set_page_config")

# Add project root to import path so we can import nlp.* later (lazily)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
mark("sys.path appended")

# Flags (can be set in Streamlit Secrets or env)
DEBUG_UI  = env("DBBALL_DEBUG_UI", "0") == "1"
SAFE_START = env("DBBALL_SAFE_START", "0") == "1"  # if "1", skip external calls until user acts
PROBE_ONLY = env("DBBALL_PROBE_ONLY", "0") == "1"  # if "1", render probe UI and return early

# Load local .envs if present (harmless on Cloud)
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")
load_dotenv(Path(__file__).resolve().parents[1] / ".env.gemini")
mark("dotenv loaded")

DB_PARAMS = {
    "host": env("AWSHOST"),
    "port": env("AWSPORT"),
    "dbname": env("AWSDATABASE"),
    "user": env("AWSUSER"),
    "password": env("AWSPASSWORD"),
}

_required = ("AWSHOST", "AWSPORT", "AWSDATABASE", "AWSUSER", "AWSPASSWORD")
_missing = [k for k in _required if not env(k)]
if _missing:
    SAFE_START = True  # ensure first render works even with bad/missing secrets
mark(f"flags: SAFE_START={int(SAFE_START)} DEBUG_UI={int(DEBUG_UI)} PROBE_ONLY={int(PROBE_ONLY)}")
mark(f"python={os.sys.version.split()[0]} streamlit={st.__version__}")

# -------- cached resources --------
@st.cache_resource(show_spinner=False)
def get_stat_catalog(init_fastpath_fn):
    mark("get_stat_catalog: opening DB connection")
    conn = psycopg2.connect(
        **DB_PARAMS,
        connect_timeout=5,
        options="-c statement_timeout=5000"
    )
    try:
        out = init_fastpath_fn(conn)
        mark("get_stat_catalog: init_fastpath returned")
        return out
    finally:
        try:
            conn.close()
        except Exception:
            pass
        mark("get_stat_catalog: connection closed")

@st.cache_data(show_spinner=False, ttl=300)
def run_sql(sql: str, params: dict | None = None):
    mark("run_sql: opening DB connection")
    with psycopg2.connect(
        **DB_PARAMS,
        connect_timeout=5,
        options="-c statement_timeout=15000"
    ) as conn:
        mark("run_sql: executing query")
        df = pd.read_sql_query(sql, conn, params=params or {})
        mark("run_sql: query done")
        return df

def looks_like_sql(s: str) -> bool:
    lo = (s or "").lstrip().lower()
    return lo.startswith(("select", "with", "explain", "insert into", "update", "delete from", "create view", "create table"))

def title_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [col.replace("_", " ").title() if isinstance(col, str) else col for col in df.columns]
    return df

# IMPORTANT: no nlp.* imports at module import time
_NLP_LOADED = False
gsql = None
init_fastpath = None
try_fastpath = None
route_template = None
lint_sql = None
enforce_leaders_invariants = None

def load_nlp_modules():
    """Lazy import all nlp modules. Raises on failure so we can show errors."""
    global _NLP_LOADED, gsql, init_fastpath, try_fastpath, route_template, lint_sql, enforce_leaders_invariants
    if _NLP_LOADED:
        return
    import importlib
    mark("lazy importing nlp modules...")
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
    mark("nlp modules loaded")

STAT_CATALOG = None

# ------------------ view: Probe (for bisecting) ------------------
def render_probe():
    st.header("Probe Mode")
    st.write("This is a minimal page to confirm the app renders on the server.")
    st.write("If you can see this, the Streamlit runtime is fine and any hang is inside app logic.")
    st.write("Secrets present (true/false): " + ", ".join([f"{k}={bool(env(k))}" for k in _required]))
    with st.expander("Ping database"):
        if st.button("SELECT 1"):
            try:
                with psycopg2.connect(**DB_PARAMS, connect_timeout=5, options="-c statement_timeout=3000") as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        st.write("DB ping ok")
            except Exception as e:
                st.write(f"DB ping failed: {e}")

# ------------------ view: Home ------------------
def render_home():
    mark("render_home: start")

    # Sidebar (safe; local)
    try:
        from render_sidebar import render_sidebar
        render_sidebar()
    except Exception as e:
        st.error(f"Sidebar failed to load: {e}")

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
    if _missing:
        st.error("Missing secrets: " + ", ".join(_missing))
        st.info("The app will run without DB/LLM until secrets are added (SAFE_START active).")

    # Lazy import NLP stack
    try:
        load_nlp_modules()
    except Exception as e:
        st.error(f"Failed to load NLP modules (nlp/*). {e}")
        st.stop()

    # Optional: fast-path init (DB) unless SAFE_START
    global STAT_CATALOG
    if not SAFE_START and STAT_CATALOG is None:
        try:
            with st.status("Initializing fast-path (short DB check)…", state="running"):
                STAT_CATALOG = get_stat_catalog(init_fastpath)
            st.success("Fast-path enabled.")
        except Exception as e:
            st.info(f"Running without fast-path (DB unavailable): {e}")
            STAT_CATALOG = None
    mark("after fast-path init")

    # Load schema/prompt/templates (local only)
    try:
        schema_str = gsql.load_schema()
        prompt_template = gsql.load_prompt_template()
        templates_yaml = gsql.load_templates_yaml()
        st.caption(f"Loaded templates: {len(templates_yaml.get('templates', {}))}")
        mark("schema/prompts/templates loaded")
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

        # 0) fast-path
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
        mark("after fast-path attempt")

        # 1) templates
        if sql_query is None and use_templates:
            tname, tparams = gsql.route_template(norm_q) if hasattr(gsql, "route_template") else (None, None)
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
        mark("after templates attempt")

        # 2) LLM (guarded)
        if sql_query is None:
            if SAFE_START:
                st.error("SAFE_START is enabled; LLM calls disabled. Turn off SAFE_START to allow LLM generation.")
                st.stop()
            full_prompt = gsql.build_prompt(norm_q, schema_str, prompt_template, season, current_year=gsql.CURRENT_YEAR)
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
        mark("after LLM attempt")

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
        mark("after run_sql")

# ------------------ navigation ------------------
PAGES_DIR = Path(__file__).parent / "pages"

def _maybe_page(rel_path: str, title: str):
    fp = PAGES_DIR / Path(rel_path).name
    return st.Page(f"pages/{fp.name}", title=title) if fp.exists() else None

def run_navigation():
    if PROBE_ONLY:
        mark("PROBE_ONLY is set; rendering probe and returning")
        render_probe()
        return

    # Use new nav API if available
    if hasattr(st, "Page") and hasattr(st, "navigation"):
        home_page    = st.Page(render_home, title="Home")
        howto_page   = _maybe_page("how_to_use.py", "How to Use")
        about_page   = _maybe_page("about.py", "About")
        contact_page = _maybe_page("contact.py", "Contact")

        pages = [home_page]
        for p in (howto_page, about_page, contact_page):
            if p:
                pages.append(p)

        test_page = _maybe_page("test_mode.py", "Test Mode")
        if env("DBBALL_ENABLE_TEST_UI", "0") == "1" and test_page:
            pages.append(test_page)

        st.session_state["home_page"] = home_page
        st.session_state["howto_page"] = howto_page
        st.session_state["about_page"] = about_page
        st.session_state["contact_page"] = contact_page

        nav = st.navigation(pages, position="sidebar")
        mark("about to nav.run()")
        nav.run()
        mark("after nav.run()")
    else:
        # Fallback for older Streamlit
        render_home()

mark("before navigation")
run_navigation()
mark("end of app.py")
