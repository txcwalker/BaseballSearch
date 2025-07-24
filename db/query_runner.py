import psycopg2
import os
from dotenv import load_dotenv
from pathlib import Path

# Load .env or a specific file like .env.awsrds
BASE_DIR = Path(__file__).resolve().parents[1]
load_dotenv(BASE_DIR / ".env.awsrds")

def run_query(sql: str):
    try:
        conn = psycopg2.connect(
            dbname=os.environ["PGDATABASE"],
            user=os.environ["PGUSER"],
            password=os.environ["PGPASSWORD"],
            host=os.environ["PGHOST"],
            port=os.environ["PGPORT"]
        )
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        colnames = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return [dict(zip(colnames, row)) for row in rows]

    except Exception as e:
        return {"error": str(e)}
