# etl/update_fangraphs_playwright.py
# (Reusing this filename so GitHub Actions doesn't break)
import os
import time
import json
from datetime import date
from pathlib import Path
import pandas as pd
import pg8000.native
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from dotenv import load_dotenv

# ---------------- Env & DB params ----------------
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_CONFIG = {
    "database": os.getenv("AWSDATABASE") or os.getenv("PGDATABASE"),
    "user": os.getenv("AWSUSER") or os.getenv("PGUSER"),
    "password": os.getenv("AWSPASSWORD") or os.getenv("PGPASSWORD"),
    "host": os.getenv("AWSHOST") or os.getenv("PGHOST"),
    "port": int(os.getenv("AWSPORT") or os.getenv("PGPORT") or 5432),
}

YEAR = date.today().year

import subprocess

def get_chrome_major_version():
    try:
        process = subprocess.Popen(['google-chrome', '--version'], stdout=subprocess.PIPE)
        output, _ = process.communicate()
        version_string = output.decode('utf-8').strip()
        return int(version_string.split()[2].split('.')[0])
    except:
        return None

# ---------------- Fetcher Logic (Undetected Chromedriver) ----------------
def fetch_fangraphs_uc(stats_type: str, year: int) -> pd.DataFrame:
    api_url = f"https://www.fangraphs.com/api/leaders/major-league/data?age=&pos=all&stats={stats_type}&lg=all&qual=0&type=8&season={year}&month=0&season1={year}&ind=0&team=0&rost=0&filter=&players=0&pageitems=10000"
    
    print(f"🕵️  Booting undetected Chrome for {stats_type} {year}...")
    
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    
    # Initialize the undetectable browser
    chrome_version = get_chrome_major_version()
    print(f"⚙️  Detected Chrome Version: {chrome_version}")
    
    if chrome_version:
        driver = uc.Chrome(options=options, version_main=chrome_version)
    else:
        driver = uc.Chrome(options=options)
    
    try:
        # Step 1: Visit home page to solve Cloudflare challenge and get cookies
        print(f"📡 Handshaking with Cloudflare...")
        driver.get("https://www.fangraphs.com/leaders/major-league")
        time.sleep(5) # Wait for challenge to process
        
        # Step 2: Navigate to the API JSON endpoint
        print(f"📡 Requesting JSON API...")
        driver.get(api_url)
        time.sleep(3)
        
        # Extract the JSON from the pre/body tag
        body_text = driver.find_element(By.TAG_NAME, "body").text
        
        try:
            data = json.loads(body_text)
            df = pd.DataFrame(data['data'] if 'data' in data else data)
            
            if not df.empty:
                print(f"✅ Success! Fetched {len(df)} rows.")
                rename_map = {'PlayerName': 'name', 'playerid': 'idfg', 'TeamNameAbbreviation': 'team', 'TeamName': 'team'}
                df.columns = [c.lower() for c in df.columns]
                df.rename(columns=rename_map, inplace=True)
                return df
            else:
                print("⚠️ Loaded JSON, but it was empty.")
        except json.JSONDecodeError:
            print("❌ Failed to parse JSON. Cloudflare might still be showing a Captcha page.")
            print(f"Page output snippet: {body_text[:200]}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        driver.quit()
        
    return pd.DataFrame()

# ---------------- Database Logic ----------------
def clean_and_normalize(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty: return df
    out = df.copy()
    for col in out.columns:
        if out[col].dtype == object:
            col_str = out[col].astype(str)
            if col_str.str.contains('%').any():
                out[col] = col_str.str.replace('%', '', regex=False).str.strip()
            try: out[col] = pd.to_numeric(out[col], errors='ignore')
            except: pass
    return out.fillna(0.0)

def upsert_table_pg8000(db: pg8000.native.Connection, df: pd.DataFrame, table_name: str):
    if df.empty: return
    all_cols = list(df.columns)
    key_cols = ["idfg", "season", "team"] if "team" in all_cols else ["idfg", "season"]
    non_key_cols = [c for c in all_cols if c not in key_cols]
    
    col_list = ", ".join([f'"{c}"' for c in all_cols])
    placeholders = ", ".join([f":{c}" for c in all_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
    
    sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) ON CONFLICT ({", ".join(key_cols)}) DO UPDATE SET {set_clause}'
    
    for row in df.to_dict('records'):
        db.run(sql, **row)

# Table mappings
batting_splits = {
    "fangraphs_batting_lahman_like": ["idfg","season","name","team","g","ab","pa","h","singles","doubles","triples","hr","r","rbi","bb","ibb","so","hbp","sf","sh","sb","cs"],
    "fangraphs_batting_standard_ratios": ["idfg","season","name","team","avg","obp","slg","ops","iso","babip","bb_pc","k_pc","bb_k","gdp"],
    "fangraphs_batting_advanced": ["idfg","season","name","team","woba","wraa","wrc","wrc_plus","war","rar","bat","fld","rep","pos","off","def","dol"],
}
pitching_splits = {
    "fangraphs_pitching_lahman_like": ["idfg","season","name","team","w","l","g","gs","cg","sho","sv","ip","h","r","er","hr","bb","so","hbp","wp","bk","tbf"],
    "fangraphs_pitching_standard_ratios": ["idfg","season","name","team","era","k_9","bb_9","k_bb","h_9","hr_9","avg","whip","babip","lob_pc"],
}

def main():
    print("🔌 Connecting to AWS RDS...")
    db = pg8000.native.Connection(**DB_CONFIG)
    try:
        # Batting
        df_bat = fetch_fangraphs_uc('bat', YEAR)
        if not df_bat.empty:
            df_bat['season'] = YEAR
            df_bat = clean_and_normalize(df_bat)
            for t, c in batting_splits.items():
                valid = [col for col in c if col in df_bat.columns]
                if len(valid) >= 4:
                    print(f"📁 Updating {t}...")
                    upsert_table_pg8000(db, df_bat[valid], t)
        
        # Pitching
        df_pit = fetch_fangraphs_uc('pit', YEAR)
        if not df_pit.empty:
            df_pit['season'] = YEAR
            df_pit = clean_and_normalize(df_pit)
            for t, c in pitching_splits.items():
                valid = [col for col in c if col in df_pit.columns]
                if len(valid) >= 4:
                    print(f"📁 Updating {t}...")
                    upsert_table_pg8000(db, df_pit[valid], t)
        print("✅ Finished.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
