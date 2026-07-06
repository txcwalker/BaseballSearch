# tests/run_regression.py
# NL -> SQL regression harness. Mirrors streamlit/app.py's live routing order
# (fast-path -> template router -> LLM fallback) so results reflect what a
# real user hitting the app would actually get.

import argparse
import sys
import time
import traceback
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv
load_dotenv(ROOT / ".env.awsrds")
load_dotenv(ROOT / ".env.gemini")

import pandas as pd
import psycopg2

from nlp import generate_sql as gsql
from nlp import router_fastpath as rfp
from nlp import template_router as tr
from nlp.sql_render import lint_sql as basic_lint
from nlp.linter import lint_sql as rule_lint

import os

DB_PARAMS = dict(
    dbname=os.environ["AWSDATABASE"],
    user=os.environ["AWSUSER"],
    password=os.environ["AWSPASSWORD"],
    host=os.environ["AWSHOST"],
    port=os.environ["AWSPORT"],
)


def get_conn(timeout=10):
    return psycopg2.connect(**DB_PARAMS, connect_timeout=timeout)


def run_sql(sql, params=None, timeout_s=20):
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(timeout_s * 1000)};")
            cur.execute(sql, params or {})
            colnames = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchall() if cur.description else []
    return rows, colnames


def format_sample(rows, colnames, limit=5):
    """Compact human-readable preview of the first few result rows."""
    if not rows:
        return ""
    sample = [dict(zip(colnames, r)) for r in rows[:limit]]
    suffix = f" ... (+{len(rows) - limit} more)" if len(rows) > limit else ""
    return str(sample) + suffix


def route_question(q_raw, schema_str, prompt_template, templates_yaml, stat_catalog):
    """Mirror app.py's routing order. Returns (sql_or_none, source, bound_params, refusal_status, refusal_text)."""
    norm_q, season = gsql.normalize_query(q_raw)

    if stat_catalog is not None:
        try:
            fast_sql = rfp.try_fastpath(
                question=norm_q, season=season, conn=None,
                stat_catalog=stat_catalog, top_n=10, qualified=True,
            )
            if fast_sql:
                return basic_lint(fast_sql), "fastpath", {"season": season, "top_n": 10}, None, None
        except Exception as e:
            print(f"[warn] fastpath error for {q_raw!r}: {e}", file=sys.stderr)

    try:
        tmpl_sql, tmpl_params, tmpl_name = tr.build_sql_from_templates(norm_q, templates_yaml)
        if tmpl_sql:
            return basic_lint(tmpl_sql), f"template:{tmpl_name}", (tmpl_params or {}), None, None
    except Exception as e:
        print(f"[warn] template error for {q_raw!r}: {e}", file=sys.stderr)

    prompt = gsql.build_prompt(norm_q, schema_str, prompt_template, season)
    raw_sql = gsql.get_sql_from_gemini(prompt)
    verdict = gsql.handle_model_response(raw_sql, season)
    if verdict == "__REPROMPT__":
        return None, "model", {}, "REFUSED_REPROMPT", None
    if verdict is not None:
        return None, "model", {}, "REFUSED", verdict
    return basic_lint(raw_sql), "model", {}, None, None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--questions", default=str(ROOT / "tests" / "test_questions.csv"))
    parser.add_argument("--out", default=str(ROOT / "tests" / "results"))
    parser.add_argument("--no-fastpath", action="store_true")
    parser.add_argument("--no-exec", action="store_true", help="Generate + lint only, skip DB execution")
    parser.add_argument("--limit", type=int, default=None, help="Only run the first N questions (pilot/smoke runs)")
    args = parser.parse_args()

    schema_str = gsql.load_schema()
    prompt_template = gsql.load_prompt_template()
    templates_yaml = gsql.load_templates_yaml()

    stat_catalog = None
    if not args.no_fastpath:
        try:
            conn = get_conn(timeout=8)
            stat_catalog = rfp.init_fastpath(conn)
            conn.close()
            print(f"[info] fast-path stat catalog loaded ({len(stat_catalog)} stats)")
        except Exception as e:
            print(f"[warn] fastpath catalog init failed, disabling fastpath: {e}", file=sys.stderr)

    qdf = pd.read_csv(args.questions)
    if args.limit:
        qdf = qdf.head(args.limit)
    results = []

    for _, row in qdf.iterrows():
        q_raw, category = row["question"], row["category"]
        test_focus = row["test_focus"] if "test_focus" in qdf.columns else ""
        rec = {
            "question": q_raw, "category": category, "test_focus": test_focus,
            "source": None, "sql": "",
            "lint_ok": None, "lint_reasons": "", "exec_status": "SKIPPED",
            "exec_error": "", "rowcount": None, "sample_output": "", "latency_ms": None,
        }
        t0 = time.time()
        try:
            sql, source, bound_params, refusal_status, refusal_text = route_question(
                q_raw, schema_str, prompt_template, templates_yaml, stat_catalog
            )
            rec["source"] = source

            if refusal_status is not None:
                rec["exec_status"] = refusal_status
                rec["exec_error"] = refusal_text or ""
            else:
                rec["sql"] = sql
                try:
                    lint_res = rule_lint(q_raw, sql, current_year=date.today().year)
                    rec["lint_ok"] = lint_res.ok
                    rec["lint_reasons"] = "; ".join(lint_res.reasons)
                except Exception as e:
                    rec["lint_reasons"] = f"linter crashed: {e}"

                if args.no_exec:
                    rec["exec_status"] = "NOT_RUN"
                else:
                    try:
                        rows, colnames = run_sql(sql, bound_params)
                        rec["rowcount"] = len(rows)
                        rec["sample_output"] = format_sample(rows, colnames)
                        rec["exec_status"] = "PASS" if len(rows) else "PASS_EMPTY"
                    except Exception as e:
                        rec["exec_status"] = "FAIL"
                        rec["exec_error"] = f"{type(e).__name__}: {e}"
        except Exception as e:
            rec["exec_status"] = "ERROR"
            rec["exec_error"] = f"{type(e).__name__}: {e}"
            if os.environ.get("REGRESSION_VERBOSE"):
                traceback.print_exc()

        rec["latency_ms"] = int((time.time() - t0) * 1000)
        results.append(rec)
        print(f"[{rec['exec_status']:14s}] ({str(rec['source']):16s}) {q_raw}")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_df = pd.DataFrame(results)
    ts = time.strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"regression_{ts}.csv"
    out_df.to_csv(out_path, index=False)

    print("\n=== SUMMARY ===")
    print(out_df["exec_status"].value_counts().to_string())
    print("\n=== BY CATEGORY ===")
    print(out_df.groupby("category")["exec_status"].apply(lambda s: s.value_counts().to_dict()).to_string())
    print(f"\nFull results: {out_path}")


if __name__ == "__main__":
    main()
