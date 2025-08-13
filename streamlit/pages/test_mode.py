# streamlit/pages/test_mode.py
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

# Resolve project root from .../streamlit/pages/test_mode.py
ROOT = Path(__file__).resolve().parents[2]  # BaseballSearch/

# Load envs in a robust order (later calls can override earlier ones)
load_dotenv(find_dotenv(usecwd=True), override=False)      # CWD .env if present
load_dotenv(ROOT / ".env", override=False)                 # repo root .env
load_dotenv(ROOT / ".env.awsrds", override=False)          # DB creds if present
load_dotenv(ROOT / "test_mode/.env", override=True)        # test-only overrides

import os
import re
from datetime import date

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import RealDictCursor

# App imports
from nlp.generate_sql import (
    build_prompt,
    get_sql_from_gemini,
    load_schema,
    load_prompt_template,
)
from nlp.linter import lint_sql

# --- Gate the page (hidden unless enabled) ---
flag = os.getenv("DBBALL_ENABLE_TEST_UI", "")
enabled = str(flag).strip().lower() in {"1", "true", "yes", "on"}

st.sidebar.write("DBG DBBALL_ENABLE_TEST_UI =", os.environ.get("DBBALL_ENABLE_TEST_UI"))

if not enabled:
    st.error("404 – Page not found")
    st.stop()

st.set_page_config(page_title="Test Mode")
st.header("NL→SQL Test Harness")

# Preload shared resources once
schema_str = load_schema()
prompt_template = load_prompt_template()

YEAR_RE = re.compile(r"\b(18|19|20)\d{2}\b")
def extract_season(q: str, fallback: int) -> int:
    m = YEAR_RE.search(q or "")
    return int(m.group(0)) if m else fallback

def _synth_prompt(prompt_template: str, schema_str: str, question: str, season: str) -> str:
    # Flexible token replacement (covers several common names)
    repl = {
        "{schema}": schema_str, "{SCHEMA}": schema_str, "{{SCHEMA}}": schema_str,
        "{question}": question, "{QUESTION}": question, "{{QUESTION}}": question,
        "{season}": season, "{SEASON}": season, "{{SEASON}}": season,
        "{current_year}": season, "{CURRENT_YEAR}": season,
        "{requested_season}": season, "{REQUESTED_SEASON}": season,
        "{user_query}": question, "{USER_QUERY}": question,
    }
    p = prompt_template or ""
    for k, v in repl.items():
        p = p.replace(k, str(v))
    # If template is missing/short, craft a solid minimal prompt
    if len(p.strip()) < 200:
        p = f"""You are a PostgreSQL expert and baseball analyst.
Translate the user question into a single valid PostgreSQL query using the provided schema.
Only return SQL (no explanations).

Question: {question}
Requested season: {season}

Schema:
{schema_str}
"""
    return p

def call_build_prompt_adaptive(build_prompt_fn, *, schema_str, prompt_template, season, question):
    import inspect
    season_str = str(season)
    sig = inspect.signature(build_prompt_fn)

    # Try kwargs in several common variants
    kw_trials = [
        dict(schema_str=schema_str, prompt_template=prompt_template,
             requested_season=season_str, user_query=question),
        dict(schema_str=schema_str, prompt_template=prompt_template,
             season=season_str, question=question),
        dict(schema=schema_str, template=prompt_template,
             current_season=season_str, user_question=question),
        dict(schema_str=schema_str, prompt_template=prompt_template, question=question),
    ]
    for kw in kw_trials:
        try:
            use = {k: v for k, v in kw.items() if k in sig.parameters}
            p = build_prompt_fn(**use)
            if p and len(str(p)) > 300:
                return p
        except TypeError:
            pass

    # Try positional fallbacks
    for args in [
        (schema_str, prompt_template, season_str, question),
        (schema_str, prompt_template, question),
    ]:
        try:
            p = build_prompt_fn(*args[:len(sig.parameters)])
            if p and len(str(p)) > 300:
                return p
        except TypeError:
            pass

    # Final fallback: synthesize a correct prompt
    return _synth_prompt(prompt_template, schema_str, question, season_str)

# ---------- Read-only execution helpers ----------
SQL_WRITE_RE = re.compile(r"\b(insert|update|delete|create|alter|drop|truncate|grant|revoke)\b", re.I)
def is_read_only(sql: str) -> bool:
    s = (sql or "").strip().lower()
    return not SQL_WRITE_RE.search(s) and s.startswith(("select", "with", "explain"))

def run_query(sql: str, timeout_s: int = 45) -> pd.DataFrame:
    if not is_read_only(sql):
        raise RuntimeError("Blocked non-read SQL.")
    conn = psycopg2.connect(
        dbname=os.getenv("AWSDATABASE"),
        user=os.getenv("AWSUSER"),
        password=os.getenv("AWSPASSWORD"),
        host=os.getenv("AWSHOST"),
        port=os.getenv("AWSPORT"),
    )
    try:
        with conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"SET statement_timeout = {int(timeout_s*1000)};")
                cur.execute(sql)
                rows = cur.fetchall() if cur.description else []
                return pd.DataFrame(rows)
    finally:
        conn.close()

# ---------- UI ----------
up = st.file_uploader("Upload questions (CSV or Excel with a 'question' column)", type=["csv", "xlsx", "xls"])
exec_queries = st.checkbox("Execute SQL that passes lint (preview only)", value=False)

if up:
    # Read file, force 'question' as string; avoid NaN coercion
    name = (getattr(up, "name", "") or "").lower()
    if name.endswith((".xlsx", ".xls")):
        qdf = pd.read_excel(up, dtype={"question": str})
    else:
        qdf = pd.read_csv(up, dtype={"question": str}, keep_default_na=False)

    if "question" not in qdf.columns:
        st.error("File must have a 'question' column.")
        st.stop()

    results = []
    for raw_q in qdf["question"]:
        raw_q = str(raw_q)
        sql, status, reasons, exec_error = "", "ERROR", "", None
        rowcount, preview, printed_df, exec_ms = 0, "", "", None
        try:
            season = extract_season(raw_q, date.today().year)
            q = raw_q.replace("{season}", str(season))

            prompt = call_build_prompt_adaptive(
                build_prompt,
                schema_str=schema_str,
                prompt_template=prompt_template,
                season=str(season),
                question=q,
            )
            sql = get_sql_from_gemini(prompt)

            lint = lint_sql(q, sql, date.today().year)
            status = "PASS" if lint.ok else "FAIL"
            reasons = "; ".join(lint.reasons)

            # Optional execution test
            if exec_queries and lint.ok and sql and is_read_only(sql):
                import time
                t0 = time.time()
                try:
                    df = run_query(sql)  # local read-only exec
                    exec_ms = int((time.time() - t0) * 1000)
                    rowcount = len(df)
                    preview = df.head(5).to_string(index=False) if rowcount else "(0 rows)"
                    with pd.option_context("display.max_rows", 20, "display.max_columns", 40, "display.width", 160):
                        printed_df = df.head(20).to_string(index=False) if rowcount else "(0 rows)"
                except Exception as e:
                    status = "ERROR"
                    exec_error = str(e)
                    reasons = (reasons + f"; Execution error: {exec_error}").strip("; ")

        except Exception as e:
            status = "ERROR"
            reasons = str(e)

        results.append({
            "question": q,
            "status": status,
            "rowcount": rowcount,
            "execution_ms": exec_ms,
            "reasons": reasons.strip("; "),
            "sql": sql,
            "preview": preview,
            "printed_df": printed_df,
        })

    rdf = pd.DataFrame(results)
    st.subheader("Summary")
    st.dataframe(rdf[["status", "rowcount", "execution_ms", "question", "reasons"]], use_container_width=True)

    with st.expander("SQL & previews"):
        st.dataframe(rdf[["question", "sql", "preview"]], use_container_width=True)

    st.download_button(
        "Download results CSV",
        data=rdf.to_csv(index=False).encode("utf-8"),
        file_name="nl2sql_test_results.csv",
        mime="text/csv",
    )
