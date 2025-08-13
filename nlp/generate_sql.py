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

from pathlib import Path
import yaml

from .template_router import build_sql_from_templates
from .sql_render import lint_sql, enforce_leaders_invariants



# ---------- Constants & basic helpers ----------

BASE_DIR = Path(__file__).parent
CURRENT_YEAR = date.today().year

_YEAR_RE = re.compile(r"\b(19|20)\d{2}\b", re.I)
_THIS_YEAR_RE = re.compile(r"\b(this year|current year|ytd|so far)\b", re.I)


def extract_season(user_q: str) -> Optional[int]:
    """Return an explicit year if present, else CURRENT_YEAR for 'this year' phrases, else None."""
    m = _YEAR_RE.search(user_q)
    if m:
        return int(m.group(0))
    if _THIS_YEAR_RE.search(user_q):
        return CURRENT_YEAR
    return None


def normalize_query(user_q: str) -> Tuple[str, int]:
    """Normalize whitespace/punctuation and resolve season."""
    season = extract_season(user_q) or CURRENT_YEAR
    q = user_q.strip().rstrip("?.! ")
    return q, season


def load_templates_yaml() -> dict:
    p = Path(__file__).resolve().parents[0] / "templates" / "sql_templates.yml"
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def get_sql_and_params(
    user_question: str,
    schema_text: str,
    prompt_template: str,      # << NEW: pass the prompt template in
    templates_yaml: dict,
    current_year: int,
    season: int,               # << NEW: pass resolved season in
    preset_sql: str = "",
):
    """
    Returns: (sql_text, params_dict, source_str)
      - source_str is 'preset' | f'template:{name}' | 'model'
    Tries template first; if no match, falls back to your existing model path.
    """
    # 0) Preset passthrough
    if preset_sql:
        sql = lint_sql(preset_sql)
        return sql, {}, "preset"

    # 1) Template fast-path
    try:
        sql, params, tname = build_sql_from_templates(user_question, templates_yaml)
    except Exception:
        sql, params, tname = None, None, None

    if sql:
        sql = lint_sql(sql)
        sql = enforce_leaders_invariants(sql)
        return sql, (params or {}), f"template:{tname}"

    # 2) Model fallback — use your existing prompt + Gemini path
    base_prompt = build_prompt(
        nl_query=user_question,                 # << correct param name
        schema_str=schema_text,                 # << correct param name
        prompt_template=prompt_template,        # << supply the template
        season=season,                          # << supply season
        current_year=current_year,
    )
    model_sql = get_sql_from_gemini(base_prompt)

    # Safety pass
    model_sql = lint_sql(model_sql)
    try:
        model_sql = enforce_leaders_invariants(model_sql)
    except Exception:
        # Re-raise so the caller can show a clear error
        raise

    return model_sql, {}, "model"



# ---------- Templates (data‑driven router) ----------
from pathlib import Path
import yaml
import re
from typing import Optional, Tuple, Dict

BASE_DIR = Path(__file__).parent  # ensure this exists once in the file

_TEMPLATES: Optional[Dict] = None

def _load_templates_file() -> Dict:
    # Look for either .yaml or .yml inside nlp/templates/
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

def match_template_data_driven(user_q: str, season_default: Optional[int]) -> Optional[Tuple[str, Dict]]:
    """
    Match the user query against regex patterns defined in the YAML.
    YAML structure per template (optional):
      patterns: [ "(?i)leaders? in (?P<stat>\\w[\\w%/\\- ]+) in (?P<season>\\d{4})" ]
      defaults: { season: "!season_from_query", top_n: 10, stat_label: "hr" }
      param_types: { season: "int", top_n: "int" }
      params: [ "season", "top_n", "stat_col", "stat_label" ]  # required for render
      sql: "...jinja..."
    """
    templates = get_templates()
    q = (user_q or "").strip()
    for name, meta in templates.get("templates", {}).items():
        for pat in meta.get("patterns", []):
            m = re.search(pat, q)
            if not m:
                continue

            # 1) start with defaults
            params = dict(meta.get("defaults", {}))
            # resolve magic defaults
            for k, v in list(params.items()):
                if v == "!season_from_query":
                    # try to pull from text; fall back to season_default
                    year_m = re.search(r"\b(19|20)\d{2}\b", q)
                    if year_m:
                        params[k] = int(year_m.group(0))
                    else:
                        params[k] = season_default
                elif v == "!current_year":
                    from datetime import date
                    params[k] = date.today().year

            # 2) overlay named captures
            for k, v in (m.groupdict() or {}).items():
                if v is not None:
                    params[k] = v

            # 3) cast param types
            types = meta.get("param_types", {})
            for k, t in types.items():
                if k in params and params[k] is not None:
                    try:
                        if t in ("int", int):
                            params[k] = int(params[k])
                        elif t in ("float", float):
                            params[k] = float(params[k])
                    except Exception:
                        pass

            # 4) ensure required params present
            required = meta.get("params", [])
            if any(p not in params or params[p] is None for p in required):
                continue

            return name, params
    return None

def render_template(name: str, **params) -> str:
    # Use the shared Jinja renderer
    from nlp.templates import render_sql
    return render_sql(name, **params) or ""



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
    }

    class _SafeDict(dict):
        def __missing__(self, key):
            # leave unknown placeholders untouched, e.g. "{something}"
            return "{" + key + "}"

    try:
        return prompt_template.format_map(_SafeDict(data))
    except Exception:
        # ultra-safe fallback: do plain replaces for the known keys
        out = prompt_template
        for k, v in data.items():
            out = out.replace("{" + k + "}", str(v))
        return out



# ---------- Gemini client ----------

def load_gemini_key() -> str:
    if "GEMINI_API_KEY" in os.environ:
        return os.environ["GEMINI_API_KEY"]
    # Try loading from .env.gemini
    env_path = BASE_DIR.parent / ".env.gemini"
    if env_path.exists():
        load_dotenv(env_path)
        if "GEMINI_API_KEY" in os.environ:
            return os.environ["GEMINI_API_KEY"]
    raise ValueError("Gemini API key not found in environment or .env.gemini")


def get_sql_from_gemini(prompt: str) -> str:
    genai.configure(api_key=load_gemini_key())
    model = genai.GenerativeModel("models/gemini-2.5-flash")
    resp = model.generate_content(contents=[{"role": "user", "parts": [prompt]}])
    text = (resp.text or "").strip()
    # strip fences if present
    text = text.replace("```sql", "").replace("```", "").strip()
    return text


# ---------- Validation (optional but useful) ----------

_REFUSAL_MARKERS = (
    "i can only answer baseball questions",
    "unfortunately i currently do not have access",
    "i don’t have future-season data",
    "i don't have future-season data",
    "cannot provide statistics for",
    "do not have access to future",
)


def handle_model_response(response_text: Optional[str], season: int) -> Optional[str]:
    """
    Decide whether to proceed with execution or surface a message.
    Returns:
      - None → looks like SQL; proceed
      - "__REPROMPT__" → model thought 'future' for CURRENT_YEAR
      - str message → show to user (do NOT execute)
    """
    text = (response_text or "").strip()
    if not text:
        return "I wasn’t able to generate a valid query for that question."

    lo = text.lower()

    if any(m in lo for m in _REFUSAL_MARKERS):
        if season == CURRENT_YEAR and ("future" in lo or "future-season" in lo):
            return "__REPROMPT__"
        return text

    looks_sql = (
        "select " in lo
        or lo.startswith("with ")
        or lo.startswith("explain ")
        or lo.startswith("create view ")
        or lo.startswith("insert into ")
        or lo.startswith("update ")
        or lo.startswith("delete from ")
    )
    if not looks_sql:
        return "I wasn’t able to generate a valid query for that question."

    return None


# ---------- CLI entry ----------

def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description="Generate SQL from natural language (with optional templates).")
    parser.add_argument("query", help="Natural language query")
    parser.add_argument("--no-templates", action="store_true", help="Force LLM generation even if a template matches.")
    parser.add_argument("--print-prompt", action="store_true", help="Print the prompt sent to Gemini.")
    args = parser.parse_args()

    # Normalize input & determine season
    norm_q, season = normalize_query(args.query)

    # 1) Try YAML template first (fully data-driven; no Python edits for new templates)
    if not args.no_templates:
        match = match_template_data_driven(norm_q, season_default=season)
        if match:
            name, params = match
            sql = render_template(name, **params)
            if sql:
                print(f"\n--- Using template: {name} ---\n")
                print(sql)
                return

    # 2) Fall back to LLM using your base prompt + schema
    schema_str = load_schema()
    prompt_template = load_prompt_template()
    full_prompt = build_prompt(norm_q, schema_str, prompt_template, season)

    if args.print_prompt:
        print("\n--- Prompt Sent to Gemini ---\n")
        print(full_prompt)

    sql = get_sql_from_gemini(full_prompt)

    # Optional validation/guard
    verdict = handle_model_response(sql, season)
    if verdict is None:
        print("\n--- SQL Output ---\n")
        print(sql)
    elif verdict == "__REPROMPT__":
        # Minimal gentle nudge (you can customize)
        print("I wasn’t able to generate a valid query for that question.")
    else:
        # Surface the refusal/message
        print(verdict)


if __name__ == "__main__":
    main()
