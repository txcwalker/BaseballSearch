# app.py
import os, sys
from pathlib import Path

import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# 0) Page config MUST be first Streamlit call
st.set_page_config(
    page_title="Databaseball",
    page_icon="⚾",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Add project root to import path (so we can import nlp.* later, lazily)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# --- env helpers & flags ---
def env(key: str, default=None):
    try:
        return st.secrets.get(key, os.getenv(key, default))
    except Exception:
        return os.getenv(key, default)

DEBUG_UI   = env("DBBALL_DEBUG_UI", "0") == "1"
SAFE_START = env("DBBALL_SAFE_START", "0") == "1"

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

# --- Global CSS ---
st.markdown("""
<style>
    /* Clean font and base */
    html, body, [class*="css"] {
        font-family: 'Georgia', 'Times New Roman', serif;
    }

    /* Remove default top padding */
    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        max-width: 900px;
    }

    /* Hero section */
    .db-hero {
        text-align: center;
        padding: 2.5rem 1rem 1.5rem 1rem;
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        border-radius: 16px;
        color: white;
        margin-bottom: 1.5rem;
        box-shadow: 0 4px 24px rgba(0,0,0,0.18);
    }
    .db-hero h1 {
        font-size: 2.8em;
        font-weight: 700;
        margin: 0 0 0.3em 0;
        letter-spacing: -1px;
        color: white;
    }
    .db-hero .subtitle {
        font-size: 1.1em;
        color: #c5cfe0;
        margin-bottom: 0.5rem;
    }
    .db-beta {
        display: inline-block;
        background: #e74c3c;
        color: white;
        font-size: 0.55em;
        font-family: 'Arial', sans-serif;
        font-weight: 700;
        letter-spacing: 1px;
        text-transform: uppercase;
        padding: 3px 8px;
        border-radius: 5px;
        vertical-align: middle;
        margin-left: 8px;
    }

    /* Search box area */
    .search-area {
        background: #f8f9fa;
        border: 2px solid #e0e0e0;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1.2rem;
    }

    /* Example chip buttons */
    .stButton > button {
        border-radius: 20px !important;
        font-size: 0.82em !important;
        padding: 0.3rem 0.85rem !important;
        border: 1.5px solid #c0392b !important;
        color: #c0392b !important;
        background: white !important;
        font-family: 'Arial', sans-serif !important;
        transition: all 0.15s ease !important;
    }
    .stButton > button:hover {
        background: #c0392b !important;
        color: white !important;
    }

    /* Primary submit button override */
    .submit-btn > button {
        background: #1a1a2e !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-size: 1em !important;
        font-family: 'Arial', sans-serif !important;
        padding: 0.5rem 2rem !important;
        width: 100%;
    }
    .submit-btn > button:hover {
        background: #c0392b !important;
        color: white !important;
    }

    /* Info cards */
    .info-card {
        background: white;
        border: 1px solid #e8e8e8;
        border-radius: 10px;
        padding: 1rem 1.1rem;
        height: 100%;
        box-shadow: 0 1px 4px rgba(0,0,0,0.06);
    }
    .info-card h4 {
        margin: 0 0 0.5rem 0;
        color: #1a1a2e;
        font-size: 0.95em;
        text-transform: uppercase;
        letter-spacing: 0.5px;
        font-family: 'Arial', sans-serif;
    }
    .info-card p, .info-card li {
        font-size: 0.87em;
        color: #555;
        line-height: 1.6;
        font-family: 'Arial', sans-serif;
    }

    /* Results dataframe */
    .stDataFrame {
        border-radius: 10px;
        overflow: hidden;
    }

    /* Divider */
    hr {
        border-top: 1px solid #ececec !important;
        margin: 1.2rem 0 !important;
    }

    /* Sidebar */
    section[data-testid="stSidebar"] {
        background: #f7f7f9;
    }
</style>
""", unsafe_allow_html=True)

# --- cached resources & helpers ---
@st.cache_resource(show_spinner=False)
def get_stat_catalog(_init_fastpath_fn):
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

# Lazy imports for NLP stack
_NLP_LOADED = False
gsql = None
init_fastpath = None
try_fastpath = None
route_template = None
lint_sql = None
enforce_leaders_invariants = None

def load_nlp_modules():
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

STAT_CATALOG = None

# Example queries for chips
EXAMPLE_QUERIES = [
    "Top 10 home run hitters since 2015",
    "Shohei Ohtani's WAR by season",
    "Best ERA among qualified pitchers in 2023",
    "Compare Mike Trout and Mookie Betts in 2023",
    "Which pitchers had the biggest FIP vs ERA gap since 2018?",
    "Most strikeouts by a pitcher in a single season since 2010",
]

# ------------------ PAGE: Home ------------------
def render_home():
    global STAT_CATALOG

    try:
        from render_sidebar import render_sidebar
        render_sidebar()
    except Exception as e:
        if DEBUG_UI:
            st.error(f"Sidebar failed to load: {e}")

    # --- Hero ---
    st.markdown("""
        <div class='db-hero'>
            <h1>⚾ Databaseball <span class='db-beta'>Beta</span></h1>
            <div class='subtitle'>Ask baseball questions in plain English. Get real stats.</div>
            <div style='font-size:0.82em; color:#8a9bb5; margin-top:0.3rem;'>
                Powered by FanGraphs · Lahman DB · AWS RDS · Google Gemini
            </div>
        </div>
    """, unsafe_allow_html=True)

    # --- NLP stack load ---
    try:
        load_nlp_modules()
    except Exception as e:
        st.error("A required module failed to load.")
        if DEBUG_UI:
            st.exception(e)
        st.stop()

    # Fast-path catalog
    if not SAFE_START and STAT_CATALOG is None:
        try:
            STAT_CATALOG = get_stat_catalog(init_fastpath)
        except Exception as e:
            if DEBUG_UI:
                st.info(f"Fast-path init skipped: {e}")
            STAT_CATALOG = None

    # Load schema/prompt/templates
    try:
        schema_str = gsql.load_schema()
        prompt_template = gsql.load_prompt_template()
        templates_yaml = gsql.load_templates_yaml()
        if DEBUG_UI:
            st.caption(f"Loaded templates: {len(templates_yaml.get('templates', {}))}")
    except Exception as e:
        st.error("Failed to load configuration.")
        if DEBUG_UI:
            st.exception(e)
        st.stop()

    # --- Search Input Area ---
    st.markdown("##### Try asking a question:")

    # Initialise the text_input value in session_state if not present
    if "nl_query_value" not in st.session_state:
        st.session_state["nl_query_value"] = ""

    # Example query chips — write directly into the shared key, then rerun
    cols = st.columns(3)
    for i, example in enumerate(EXAMPLE_QUERIES):
        with cols[i % 3]:
            if st.button(f"📊 {example}", key=f"ex_{i}", use_container_width=True):
                st.session_state["nl_query_value"] = example
                st.session_state["pending_search"] = True
                st.rerun()

    st.markdown("")

    # Text input bound to session_state key so chip clicks persist correctly
    nl_query = st.text_input(
        label="Your question",
        key="nl_query_value",
        placeholder="e.g. Who led the NL in ERA in 2022 among qualified pitchers?",
        label_visibility="collapsed",
    )

    use_templates = st.checkbox("⚡ Use templates when available (faster)", value=True)

    show_prompt = False
    if DEBUG_UI:
        show_prompt = st.checkbox("Show LLM prompt (debug)", value=False)

    if SAFE_START and DEBUG_UI:
        st.warning("SAFE_START is enabled — DB/LLM calls are skipped.")

    with st.container():
        st.markdown('<div class="submit-btn">', unsafe_allow_html=True)
        search_clicked = st.button("🔍  Search", use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)

    # Trigger on explicit button click OR when a chip fired a pending search
    if search_clicked:
        st.session_state["pending_search"] = True
        st.session_state["last_query"] = nl_query

    # Only run once per submission — clear the flag immediately
    submit = st.session_state.pop("pending_search", False)
    # Use the frozen query value so reruns don't re-execute
    if submit and "last_query" not in st.session_state:
        st.session_state["last_query"] = nl_query
    query_to_run = st.session_state.pop("last_query", None) if submit else None

    # --- Info Cards (collapsed by default, below fold) ---
    with st.expander("💡 Tips & what you can ask", expanded=False):
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("""
            **What works well**
            - Player stats by season or career
            - Leaderboards (top 10, qualified players)
            - Team stats, comparisons
            - Advanced stats: WAR, wOBA, FIP, ISO, xFIP
            """)
        with c2:
            st.markdown("""
            **What doesn't work (yet)**
            - Game-by-game data
            - Monthly or streak stats
            - Batter vs pitcher handedness splits
            - Live / in-progress scores
            """)
        with c3:
            st.markdown("""
            **Tips for best results**
            - Use full player names
            - Specify a year when possible
            - Say "among qualified players" to filter out small samples
            - If it fails, rephrase and try again!
            """)

    st.markdown("---")

    # --- Query Execution ---
    # Show cached results from previous run (avoids re-running on every rerun)
    if "last_result" in st.session_state and not query_to_run:
        df_cached, cached_query_text = st.session_state["last_result"]
        st.markdown(f"*Results for: **{cached_query_text}***")
        st.markdown(f"**{len(df_cached)} result(s) found**")
        st.dataframe(df_cached, use_container_width=True, hide_index=True)
        csv = df_cached.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️  Download as CSV", csv, file_name="databaseball_results.csv", mime="text/csv")

    if query_to_run:
        norm_q, season = gsql.normalize_query(query_to_run)
        sql_query, bound_params = None, {}

        with st.spinner("🔍 Translating your question to SQL..."):
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
                        if DEBUG_UI:
                            st.info("Using fast-path leaders (validated).")
                    except Exception as e:
                        if DEBUG_UI:
                            st.warning(f"Fast-path rejected ({e}); falling back to templates.")

            # 1) template router
            if sql_query is None and use_templates:
                try:
                    tmpl_sql, bound_params = route_template(norm_q, season, templates_yaml)
                    if tmpl_sql:
                        tmpl_sql = lint_sql(tmpl_sql)
                        tmpl_sql = enforce_leaders_invariants(tmpl_sql)
                        sql_query = tmpl_sql
                        if DEBUG_UI:
                            st.info("Using template router (validated).")
                except Exception as e:
                    if DEBUG_UI:
                        st.warning(f"Template router failed ({e}); falling back to LLM.")

            # 2) LLM fallback
            if sql_query is None:
                try:
                    prompt = gsql.build_prompt(norm_q, schema_str, prompt_template, season)
                    if show_prompt:
                        st.text_area("LLM Prompt", prompt, height=200)
                    raw_sql = gsql.get_sql_from_gemini(prompt)
                    if DEBUG_UI:
                        st.text_area("Raw LLM SQL", raw_sql, height=120)
                    action = gsql.handle_model_response(raw_sql, season)
                    if DEBUG_UI:
                        st.info(f"handle_model_response returned: {repr(action)}")
                    if action and action != "__REPROMPT__":
                        st.error("The model response was rejected.")
                        if DEBUG_UI:
                            st.info(action)
                        st.stop()
                    sql_query = raw_sql
                except Exception as e:
                    st.error("Failed to generate SQL from your question.")
                    if DEBUG_UI:
                        st.exception(e)
                    st.stop()

            if not looks_like_sql(sql_query):
                st.error("Could not generate executable SQL for that question. Try rephrasing it.")
                st.stop()

            try:
                sql_query = lint_sql(sql_query)
                sql_query = enforce_leaders_invariants(sql_query)
            except Exception as e:
                if DEBUG_UI:
                    st.warning(f"Post-processing of model SQL failed: {e}")

        if DEBUG_UI and st.toggle("Show generated SQL", value=False):
            st.code(sql_query, language="sql")

        # Execute & display
        with st.spinner("⚡ Running query against the database..."):
            try:
                df_result = run_sql(sql_query, bound_params)
                df_result = title_case_columns(df_result)
            except Exception as e:
                st.error("Query failed. This question may be outside what the database supports.")
                if DEBUG_UI:
                    st.exception(e)
                    if st.toggle("Show failed SQL?", value=False):
                        st.code(sql_query, language="sql")
                st.stop()

        st.session_state["last_result"] = (df_result, query_to_run)
        st.markdown(f"*Results for: **{query_to_run}***")
        st.markdown(f"**{len(df_result)} result(s) found**")
        st.dataframe(df_result, use_container_width=True, hide_index=True)

        csv = df_result.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="⬇️  Download as CSV",
            data=csv,
            file_name="databaseball_results.csv",
            mime="text/csv",
        )


# ------------------ NAVIGATION ------------------
PAGES_DIR = Path(__file__).parent / "pages"

def _maybe_page(rel_path: str, title: str):
    fp = PAGES_DIR / Path(rel_path).name
    return st.Page(f"pages/{fp.name}", title=title) if fp.exists() else None

home_page    = st.Page(render_home, title="🏠 Home")
howto_page   = _maybe_page("how_to_use.py", "❓ How to Use")
about_page   = _maybe_page("about.py", "ℹ️ About")
contact_page = _maybe_page("contact.py", "✉️ Contact")

pages = [home_page]
for p in (howto_page, about_page, contact_page):
    if p:
        pages.append(p)

test_page = _maybe_page("test_mode.py", "🧪 Test Mode")
if env("DBBALL_ENABLE_TEST_UI", "0") == "1" and test_page:
    pages.append(test_page)

st.session_state["home_page"] = home_page
st.session_state["howto_page"] = howto_page
st.session_state["about_page"] = about_page
st.session_state["contact_page"] = contact_page

nav = st.navigation(pages, position="sidebar")
nav.run()