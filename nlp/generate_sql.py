# nlp/generate_sql.py
# Uses Gemini to turn text → SQL (with optional YAML templates for deterministic queries)

from __future__ import annotations

import os
import re
from datetime import date
from pathlib import Path
from typing import Dict, Tuple, Optional

import google.generativeai as genai
from dotenv import load_dotenv

import yaml

from .template_router import build_sql_from_templates
from .sql_render import lint_sql, enforce_leaders_invariants


# ---------- Constants & basic helpers ----------

BASE_DIR = Path(__file__).parent
CURRENT_YEAR = date.today().year

_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b", re.I)
_THIS_YEAR_RE = re.compile(r"\b(this year|current year|ytd|so far)\b", re.I)


def extract_season(user_q: str) -> Optional[int]:
    m = _YEAR_RE.search(user_q)
    if m:
        return int(m.group(0))
    if _THIS_YEAR_RE.search(user_q):
        return CURRENT_YEAR
    return None


def normalize_query(user_q: str) -> Tuple[str, int]:
    season = extract_season(user_q) or CURRENT_YEAR
    q = user_q.strip().rstrip("?.! ")
    return q, season


def load_templates_yaml() -> dict:
    p = Path(__file__).resolve().parents[0] / "templates" / "sql_templates.yml"
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ---------- Prompt & schema loaders ----------

def load_schema() -> str:
    schema_path = BASE_DIR / "schema" / "schema_description.txt"
    if not schema_path.exists():
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")
    return schema_path.read_text(encoding="utf-8")


def load_prompt_template() -> str:
    prompt_path = BASE_DIR / "prompts" / "base_prompt_gemini.txt"
    if not prompt_path.exists():
        raise FileNotFoundError(f"Prompt template not found at: {prompt_path}")
    return prompt_path.read_text(encoding="utf-8", errors="replace")


def build_prompt(nl_query, schema_str, prompt_template, season, current_year=CURRENT_YEAR):
    data = {
        "schema": schema_str.strip(),
        "query": nl_query.strip(),
        "CURRENT_YEAR": current_year,
        "REQUESTED_SEASON": season,
        "preset_sql": "",
    }

    class _SafeDict(dict):
        def __missing__(self, key):
            return "{" + key + "}"

    try:
        prompt = prompt_template.format_map(_SafeDict(data))
    except Exception:
        prompt = prompt_template
        for k, v in data.items():
            prompt = prompt.replace("{" + k + "}", str(v))

        # Check for unresolved placeholders specifically — not just the word
    remaining = re.findall(r'\{[^}]*\}', prompt)
    if remaining:
        raise ValueError(
            f"Unresolved placeholders after substitution: {remaining}. "
            f"season={season}, current_year={current_year}"
        )

    return prompt


# ---------- Gemini client ----------

# Use gemini-2.5-flash
_GEMINI_MODEL = "gemini-2.5-flash"
_GEMINI_TIMEOUT = 45  # seconds — fail fast rather than hang for 45+s

def load_gemini_key() -> str:
    if "GEMINI_API_KEY" in os.environ:
        return os.environ["GEMINI_API_KEY"]
    env_path = BASE_DIR.parent / ".env.gemini"
    if env_path.exists():
        load_dotenv(env_path)
        if "GEMINI_API_KEY" in os.environ:
            return os.environ["GEMINI_API_KEY"]
    raise ValueError("Gemini API key not found in environment or .env.gemini")


def get_sql_from_gemini(prompt: str) -> str:
    import threading

    genai.configure(api_key=load_gemini_key())
    model = genai.GenerativeModel(_GEMINI_MODEL)

    result = [None]
    error = [None]

    def _call():
        try:
            resp = model.generate_content(
                contents=[{"role": "user", "parts": [prompt]}],
                generation_config={"temperature": 0.1, "max_output_tokens": 8192},
            )
            result[0] = (resp.text or "").strip()
        except Exception as e:
            error[0] = e

    t = threading.Thread(target=_call, daemon=True)
    t.start()
    t.join(timeout=_GEMINI_TIMEOUT)

    if error[0]:
        raise error[0]
    if result[0] is None:
        raise TimeoutError(f"Gemini did not respond within {_GEMINI_TIMEOUT}s")

    text = result[0]
    text = re.sub(r"^```(?:sql)?\s*", "", text, flags=re.I)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()

# ---------- Response validation ----------

_REFUSAL_MARKERS = (
    "i can only answer baseball questions",
    "unfortunately i currently do not have access",
    "i don't have future-season data",
    "cannot provide statistics for",
    "do not have access to future",
)


def handle_model_response(response_text: Optional[str], season: int) -> Optional[str]:
    """
    Returns:
      - None          → valid SQL, proceed
      - "__REPROMPT__"→ model thought season is future
      - str message   → show to user, do not execute
    """
    text = (response_text or "").strip()
    if not text:
        return "I wasn't able to generate a valid query for that question."

    lo = text.lower()

    if any(m in lo for m in _REFUSAL_MARKERS):
        if season == CURRENT_YEAR and ("future" in lo or "future-season" in lo):
            return "__REPROMPT__"
        return text

    looks_sql = (
        "select " in lo
        or lo.startswith("with ")
        or lo.startswith("explain ")
    )
    if not looks_sql:
        return "I wasn't able to generate a valid query for that question."

    return None


# ---------- Combined entry point ----------

def get_sql_and_params(
    user_question: str,
    schema_text: str,
    prompt_template: str,
    templates_yaml: dict,
    current_year: int,
    season: int,
    preset_sql: str = "",
) -> Tuple[str, dict, str]:
    """
    Returns (sql, params, source) where source is 'preset' | 'template:name' | 'model'.
    """
    if preset_sql:
        return lint_sql(preset_sql), {}, "preset"

    # Template fast-path
    try:
        sql, params, tname = build_sql_from_templates(user_question, templates_yaml)
    except Exception:
        sql, params, tname = None, None, None

    if sql:
        sql = lint_sql(sql)
        return sql, (params or {}), f"template:{tname}"

    # LLM fallback
    base_prompt = build_prompt(
        nl_query=user_question,
        schema_str=schema_text,
        prompt_template=prompt_template,
        season=season,
        current_year=current_year,
    )
    model_sql = get_sql_from_gemini(base_prompt)
    model_sql = lint_sql(model_sql)
    return model_sql, {}, "model"


# ---------- Template matching helpers (used by CLI / test mode) ----------

from typing import Optional as _Opt

_TEMPLATES: _Opt[Dict] = None

def _load_templates_file() -> Dict:
    for p in (
        BASE_DIR / "templates" / "sql_templates.yaml",
        BASE_DIR / "templates" / "sql_templates.yml",
    ):
        if p.exists():
            return yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    return {}

def get_templates() -> Dict:
    global _TEMPLATES
    if _TEMPLATES is None:
        _TEMPLATES = _load_templates_file()
    return _TEMPLATES

def match_template_data_driven(user_q: str, season_default: _Opt[int]) -> _Opt[Tuple[str, Dict]]:
    templates = get_templates()
    q = (user_q or "").strip()
    for name, meta in templates.get("templates", {}).items():
        for pat in meta.get("patterns", []):
            m = re.search(pat, q)
            if not m:
                continue
            params = dict(meta.get("defaults", {}))
            for k, v in list(params.items()):
                if v == "!season_from_query":
                    year_m = re.search(r"\b(19|20)\d{2}\b", q)
                    params[k] = int(year_m.group(0)) if year_m else season_default
                elif v == "!current_year":
                    params[k] = date.today().year
            for k, v in (m.groupdict() or {}).items():
                if v is not None:
                    params[k] = v
            types = meta.get("param_types", {})
            for k, t in types.items():
                if k in params and params[k] is not None:
                    try:
                        params[k] = int(params[k]) if t in ("int", int) else float(params[k])
                    except Exception:
                        pass
            required = meta.get("params", [])
            if any(p not in params or params[p] is None for p in required):
                continue
            return name, params
    return None

def render_template(name: str, **params) -> str:
    from nlp.templates import render_sql
    return render_sql(name, **params) or ""


# ---------- CLI ----------

def main() -> None:
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("query")
    parser.add_argument("--no-templates", action="store_true")
    parser.add_argument("--print-prompt", action="store_true")
    args = parser.parse_args()

    norm_q, season = normalize_query(args.query)

    if not args.no_templates:
        match = match_template_data_driven(norm_q, season_default=season)
        if match:
            name, params = match
            sql = render_template(name, **params)
            if sql:
                print(f"\n--- Template: {name} ---\n{sql}")
                return

    schema_str = load_schema()
    prompt_template = load_prompt_template()
    full_prompt = build_prompt(norm_q, schema_str, prompt_template, season)
    if args.print_prompt:
        print(f"\n--- Prompt ---\n{full_prompt}")

    sql = get_sql_from_gemini(full_prompt)
    verdict = handle_model_response(sql, season)
    if verdict is None:
        print(f"\n--- SQL ---\n{sql}")
    elif verdict == "__REPROMPT__":
        print("Couldn't generate query for that question.")
    else:
        print(verdict)


if __name__ == "__main__":
    main()