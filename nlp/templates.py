# nlp/templates.py
from pathlib import Path
import yaml
from jinja2 import Environment, BaseLoader

BASE_DIR = Path(__file__).parent  # .../nlp

def load_templates(path: str | None = None):
    """
    Load the shared SQL templates YAML.
    Looks in nlp/templates/sql_templates.yaml or .yml
    """
    if path is None:
        candidates = [
            BASE_DIR / "templates" / "sql_templates.yaml",
            BASE_DIR / "templates" / "sql_templates.yml",
        ]
        for p in candidates:
            if p.exists():
                path = p
                break
    if path is None:
        raise FileNotFoundError(
            f"Could not find sql_templates.yaml/.yml under {BASE_DIR / 'templates'}"
        )

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}

TPL = load_templates()

_env = Environment(loader=BaseLoader(), trim_blocks=True, lstrip_blocks=True)

def render_sql(template_name: str, **vars) -> str:
    # expose fragments if present
    ctx = {**vars, "fragments": (TPL.get("fragments") or {})}
    tmpl = (TPL.get("templates") or {}).get(template_name)
    if not tmpl:
        raise KeyError(f"SQL template '{template_name}' not found in YAML.")
    sql = tmpl["sql"]
    return _env.from_string(sql).render(**ctx).strip()
