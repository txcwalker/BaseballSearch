# Uses Gemini to turn text → SQL (OpenAI version commented out)

# import openai  #deprecated in favor of Gemini, first iteration used openai but usage limits were too low even during testing

import google.generativeai as genai
import os
from dotenv import load_dotenv

import re
from datetime import date

CURRENT_YEAR = date.today().year

def extract_season(user_q: str) -> int | None:
    # explicit year
    m = re.search(r'\b(19|20)\d{2}\b', user_q)
    if m:
        return int(m.group(0))
    # phrases → current year
    if re.search(r'\b(this year|current year|ytd|so far)\b', user_q, re.I):
        return CURRENT_YEAR
    return None

def normalize_query(user_q: str) -> tuple[str, int]:
    season = extract_season(user_q)
    if season is None:
        season = CURRENT_YEAR  # default to current year (your DB has YTD)
    # strip trailing punctuation to avoid brittle guards
    q = user_q.strip().rstrip("?.! ")
    return q, season


# Functions to load the schema and prompt to be provided to the model and the direction it will take to write its queries
# The schema_description tells it exactly what tables exist in the database along with their corresponding
# variables. Addtionally there a few small dictionaries to help the model sort through abbreviations and AL v NL
def load_schema():
    base_path = os.path.dirname(__file__)
    schema_path = os.path.join(base_path, "schema", "schema_description.txt")
    print(f"Loading schema from: {schema_path}")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")
    with open(schema_path, "r") as f:
        return f.read()

# The prompt includes the specific directions for the model to use when constructing the individual sql queries
def load_prompt_template():
    base_path = os.path.dirname(__file__)
    prompt_path = os.path.join(base_path, "prompts", "base_prompt_gemini.txt")
    print(f"Loading prompt from: {prompt_path}")
    if not os.path.exists(prompt_path):
        raise FileNotFoundError(f"Prompt template not found at: {prompt_path}")
    with open(prompt_path, "r", encoding = 'utf-8', errors = "replace") as f:
        return f.read()

# Function to build the prompt based on the input
def build_prompt(nl_query, schema_str, prompt_template, season, current_year=CURRENT_YEAR):
    return prompt_template.format(
        schema=schema_str.strip(),
        query=nl_query.strip(),
        CURRENT_YEAR=current_year,
        REQUESTED_SEASON=season
    )


# Fetching Gemini API Key
def load_gemini_key():
    if "GEMINI_API_KEY" in os.environ:
        return os.environ["GEMINI_API_KEY"]

    # Try loading from .env.gemini if available
    env_path = os.path.join(os.path.dirname(__file__), "..", ".env.gemini")
    if os.path.exists(env_path):
        load_dotenv(env_path)
        if "GEMINI_API_KEY" in os.environ:
            return os.environ["GEMINI_API_KEY"]

    raise ValueError("Gemini API key not found in environment or .env.gemini")

# # Calling model to get response
def get_sql_from_gemini(prompt):
    genai.configure(api_key=load_gemini_key())

    model = genai.GenerativeModel("models/gemini-2.5-flash")


    response = model.generate_content(
        contents=[{"role": "user", "parts": [prompt]}]
    )

    return response.text.strip().replace("```sql", "").replace("```", "").strip()


# Optional: legacy OpenAI fallback (commented out)
# def load_openai_key():
#     from openai import OpenAI
#     if "OPENAI_API_KEY" in os.environ:
#         return os.environ["OPENAI_API_KEY"]
#     env_path = os.path.join(os.path.dirname(__file__), "..", ".env.openai")
#     if os.path.exists(env_path):
#         load_dotenv(env_path)
#     return os.environ.get("OPENAI_API_KEY")

# def get_sql_from_gpt(prompt, model="gpt-4", temperature=0):
#     client = OpenAI(api_key=load_openai_key())
#     response = client.chat.completions.create(
#         model=model,
#         temperature=temperature,
#         messages=[{"role": "user", "content": prompt}]
#     )
#     return response.choices[0].message.content.strip()

# Function to handle erroneous requests
def handle_model_response(response_text: str | None, season: int):
    """
    Validates the model output and decides whether to proceed, reprompt, or surface a user-facing message.

    Returns:
      - None  → looks like valid SQL; go ahead and execute
      - "__REPROMPT__" → model wrongly claimed "future" for CURRENT_YEAR; gently reprompt
      - str message → a user-facing error/refusal string to display (do NOT execute)
    """
    text = (response_text or "").strip()
    if not text:
        return "I wasn’t able to generate a valid query for that question."

    lo = text.lower()

    # Known refusal/canned messages you want to surface AS-IS
    refusal_markers = (
        "i can only answer baseball questions",
        "unfortunately i currently do not have access",
        "i don’t have future-season data",
        "i don't have future-season data",
        "cannot provide statistics for",
        "do not have access to future",
    )
    if any(m in lo for m in refusal_markers):
        # If it's the current year but the model thinks it's "future", ask to reprompt
        if season == CURRENT_YEAR and ("future" in lo or "future-season" in lo):
            return "__REPROMPT__"
        return text  # surface the refusal message

    # Some models prepend prose/markdown. Bail if it doesn't look like SQL at all.
    # Accept common SQL starters beyond SELECT (CTEs, EXPLAIN, etc.).
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

    # Passed checks → treat as executable SQL
    return None



def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Natural language query")
    args = parser.parse_args()

    # NEW: resolve “this year”, explicit years, etc.
    norm_q, season = normalize_query(args.query)

    schema_str = load_schema()
    prompt_template = load_prompt_template()
    full_prompt = build_prompt(norm_q, schema_str, prompt_template, season)

    print("\n--- Prompt Sent to Gemini ---\n")
    print(full_prompt)
    print("\n--- SQL Output ---\n")
    sql = get_sql_from_gemini(full_prompt)
    print(sql)


if __name__ == "__main__":
    main()
