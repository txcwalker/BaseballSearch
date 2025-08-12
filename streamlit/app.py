# app.py

import os
import sys
import streamlit as st
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from pathlib import Path

# Add project root to import path (so we can import nlp.*)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Custom imports
import nlp.generate_sql as gsql
from render_sidebar import render_sidebar
from nlp.router_fastpath import init_fastpath, try_fastpath

def looks_like_sql(s: str) -> bool:
    lo = (s or "").lstrip().lower()
    return lo.startswith((
        "select", "with", "explain",
        "insert into", "update", "delete from",
        "create view", "create table"
    ))

def title_case_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [
        col.replace("_", " ").title() if isinstance(col, str) else col
        for col in df.columns
    ]
    return df

def style_dataframe(df: pd.DataFrame):
    return (df.style
            .set_properties(**{
                'background-color': '#fdfdfd',
                'color': '#111',
                'border-color': '#ccc',
                'font-size': '14px',
                'text-align': 'left'
            })
            .set_table_styles([{'selector': 'th', 'props': [
                ('background-color', '#003f5c'), ('color', 'white'), ('font-size', '15px')
            ]}]))

# Load AWS RDS environment
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

# PostgreSQL config
DB_PARAMS = {
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
}

with st.expander("🔎 Connection debug", expanded=False):
    st.write({
        "host": DB_PARAMS["host"],
        "port": DB_PARAMS["port"],
        "dbname": DB_PARAMS["dbname"],
        "user": DB_PARAMS["user"],
    })
    try:
        with psycopg2.connect(**DB_PARAMS) as conn, conn.cursor() as cur:
            cur.execute("SELECT current_database(), inet_server_addr(), inet_server_port(), now();")
            db, ip, port, now_ts = cur.fetchone()
            st.write({"current_database()": db, "server_ip": str(ip), "server_port": port, "server_now": now_ts})
            # quick freshness probe (customize if you have an etl log table)
            cur.execute("""
                SELECT max(season) AS max_season FROM fangraphs_batting_lahman_like;
            """)
            st.write({"fangraphs_batting_lahman_like.max_season": cur.fetchone()[0]})
    except Exception as e:
        st.warning(f"DB probe failed: {e}")


@st.cache_resource
def get_stat_catalog():
    # Build the (leaders/stat) catalog once per session
    with psycopg2.connect(**DB_PARAMS) as conn:
        return init_fastpath(conn)

try:
    STAT_CATALOG = get_stat_catalog()
except Exception as e:
    st.warning(f"Fast-path disabled (catalog init failed): {e}")
    STAT_CATALOG = None

@st.cache_data(show_spinner=False, ttl=300)
def run_sql(sql: str):
    """Cache ONLY successful results for 5 minutes. Exceptions are NOT cached."""
    with psycopg2.connect(**DB_PARAMS) as conn:
        return pd.read_sql_query(sql, conn)

# --------------- UI ----------------
st.set_page_config(page_title="Welcome to Databaseball", layout="wide")
render_sidebar()

st.markdown("""
    <div style='text-align: center; padding: 3rem 0 2rem 0; background-color: #f0f8ff; border-radius: 12px;'>
        <h1 style='font-size: 3.5em; margin-bottom: 0.2em;'>
            Welcome to Databaseball!
            <span style='font-size: 0.4em; color: white; background-color: #f39c12; padding: 4px 8px; border-radius: 8px; margin-left: 10px; vertical-align: middle;'>BETA</span>
        </h1>
        <p style='font-size: 1.3em; color: #444;'>Ask questions. Explore stats. Discover the game.</p>
    </div>
""", unsafe_allow_html=True)

st.markdown("### 🛠️ What You Can Do")
col1, col2 = st.columns(2)
with col1:
    st.markdown("#### 🔍 Ask Questions")
    st.markdown("- 'Show Shohei Ohtani\\'s stats in 2022'\n- 'Top 10 home run hitters since 2010'\n- 'Which pitchers have the biggest single-season gap between FIP and ERA since 2015?'")
with col2:
    st.markdown("#### 🔁 Updated Daily")
    st.markdown("- Powered by AWS + GitHub Actions\n- Data from FanGraphs & Lahman\n- Always current")

st.markdown("---")
st.markdown("### Read Me")
st.page_link("pages/how_to_use.py", label="❓ How to Use")

# Load schema and prompt template once
schema_str = gsql.load_schema()
prompt_template = gsql.load_prompt_template()

# Controls
nl_query = st.text_input("Ask a baseball question:")
use_templates = st.checkbox("Use YAML templates (faster & deterministic when available)", value=True)
show_prompt = st.checkbox("Show LLM prompt (debug)", value=False)
submit = st.button("Generate SQL and Run")

if submit and nl_query:
    norm_q, season = gsql.normalize_query(nl_query)

    sql_query = None
    # 0) Fast-path resolver (leaders/common stats) – skip LLM if it hits
    if STAT_CATALOG is not None:
        fast_sql = try_fastpath(
            question=norm_q,
            season=season,
            conn=None,  # not used
            stat_catalog=STAT_CATALOG,
            top_n=10,
            qualified=("qualified" in norm_q.lower())
        )
        if fast_sql:
            sql_query = fast_sql
            st.info("Using fast‑path leaders template.")

    used_template = False

    # 1) YAML templates (regex-driven), if fast-path didn't produce SQL
    if sql_query is None and use_templates:
        match = gsql.match_template_data_driven(norm_q, season_default=season)
        if match:
            name, params = match
            sql_query = gsql.render_template(name, **params)
            used_template = True
            st.info(f"Using template: **{name}**  •  Params: `{params}`")

    # 2) LLM fallback (Gemini), if neither fast-path nor YAML matched
    if sql_query is None:
        full_prompt = gsql.build_prompt(norm_q, schema_str, prompt_template, season, current_year=gsql.CURRENT_YEAR)
        if show_prompt:
            with st.expander("Show prompt sent to LLM", expanded=False):
                st.code(full_prompt, language="markdown")
        sql_query = gsql.get_sql_from_gemini(full_prompt)

        action = gsql.handle_model_response(sql_query, season)
        if action == "__REPROMPT__":  # handle current season incorrectly treated as future
            sql_query = gsql.get_sql_from_gemini(
                full_prompt + "\n\n# REMINDER: REQUESTED_SEASON == CURRENT_YEAR; provide season-to-date SQL."
            )
            action = gsql.handle_model_response(sql_query, season)

        if action and action != "__REPROMPT__":
            st.error(action)
            st.stop()
        if not looks_like_sql(sql_query):
            st.error("I couldn’t generate executable SQL from that question.")
            st.stop()

    # Show SQL
    st.code(sql_query, language="sql")

    # Execute (cached on success only)
    try:
        df_result = run_sql(sql_query)
        df_result = title_case_columns(df_result)
        st.success(f"✅ Query successful! Returned {len(df_result):,} rows.")
        st.dataframe(df_result)
        csv = df_result.to_csv(index=False).encode("utf-8")
        st.download_button("⬇️ Download CSV", csv, file_name="results.csv", mime="text/csv")
    except Exception as e:
        st.error(f"❌ Error running PostgreSQL query: {e}")
