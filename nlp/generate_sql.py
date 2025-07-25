# Uses Gemini to turn text → SQL (OpenAI version commented out)

# import openai  # ← deprecated in favor of Gemini
import google.generativeai as genai
import os
from dotenv import load_dotenv

def load_schema():
    base_path = os.path.dirname(__file__)
    schema_path = os.path.join(base_path, "schema", "schema_description.txt")
    print(f"Loading schema from: {schema_path}")
    if not os.path.exists(schema_path):
        raise FileNotFoundError(f"Schema file not found at: {schema_path}")
    with open(schema_path, "r") as f:
        return f.read()

def load_prompt_template():
    base_path = os.path.dirname(__file__)
    prompt_path = os.path.join(base_path, "prompts", "base_prompt_gemini.txt")
    print(f"Loading prompt from: {prompt_path}")
    if not os.path.exists(prompt_path):
        raise FileNotFoundError(f"Prompt template not found at: {prompt_path}")
    with open(prompt_path, "r") as f:
        return f.read()

def build_prompt(nl_query, schema_str, prompt_template):
    return prompt_template.format(schema=schema_str.strip(), query=nl_query.strip())

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

def get_sql_from_gemini(prompt):
    genai.configure(api_key=load_gemini_key())

    model = genai.GenerativeModel("models/gemini-1.5-flash")


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

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("query", help="Natural language query")
    args = parser.parse_args()

    schema_str = load_schema()
    prompt_template = load_prompt_template()
    full_prompt = build_prompt(args.query, schema_str, prompt_template)

    print("\n--- Prompt Sent to Gemini ---\n")
    print(full_prompt)
    print("\n--- SQL Output ---\n")
    sql = get_sql_from_gemini(full_prompt)
    print(sql)

if __name__ == "__main__":
    main()
