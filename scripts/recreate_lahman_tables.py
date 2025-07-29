#scripts/recreate_lahman_tables

# Script to recreate lahman tables so they can be input into aws rds

# Import Statements
import os # Package to provide accss to file system paths and environment variables
import psycopg2 # postgres adaptor for python
from pathlib import Path # Using this as better version of os.path function
from dotenv import load_dotenv # Package which load enviroment variables from the .env files
import re

# Load environment variables
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_PARAMS = {
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
}

# Fucntion that adds a DROP TABLE statement to make sure we are starting clean before tables are put into aws rds
def inject_drop_statements(sql_script):
    # Find all CREATE TABLE table_name occurrences
    pattern = re.compile(r"CREATE TABLE (\w+)", re.IGNORECASE)
    table_names = pattern.findall(sql_script)

    # Generate DROP TABLE IF EXISTS lines
    drops = "\n".join([f'DROP TABLE IF EXISTS {table} CASCADE;' for table in table_names])

    return f"{drops}\n\n{sql_script}"

# Function that laods the SQL schema file and, executes the ijnect drop function then loads the tables in to aws rds
def execute_sql_file(path):
    with open(path, "r", encoding="utf-8") as f:
        raw_sql = f.read()

    full_sql = inject_drop_statements(raw_sql)

    with psycopg2.connect(**DB_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(full_sql)
            conn.commit()

    print("✅ Lahman schema successfully dropped and recreated in AWS RDS.")

if __name__ == "__main__":
    sql_path = Path(__file__).resolve().parents[1] / "db" / "schema_lahman.sql"
    execute_sql_file(sql_path)
