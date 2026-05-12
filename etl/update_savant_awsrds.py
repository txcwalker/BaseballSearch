# etl/update_savant_awsrds.py
import os
import io
import time
from datetime import date
from pathlib import Path
import pandas as pd
import pg8000.native
import pybaseball
from pybaseball import chadwick_register
from curl_cffi import requests
from dotenv import load_dotenv
import re
import sys

# Enable caching to speed up pybaseball
pybaseball.cache.enable()

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

# ---------------- Fetcher Logic ----------------
def get_mlb_rosters(year: int) -> dict:
    import requests
    print(f"  Fetching active team rosters from MLB API ({year})...")
    mapping = {}
    try:
        # First get teams
        teams_url = f"http://statsapi.mlb.com/api/v1/teams?sportId=1&season={year}"
        teams_data = requests.get(teams_url, timeout=10).json()
        teams = {t['id']: t['abbreviation'] for t in teams_data.get('teams', [])}
        
        # Then get players
        players_url = f"http://statsapi.mlb.com/api/v1/sports/1/players?season={year}"
        players_data = requests.get(players_url, timeout=10).json()
        for p in players_data.get('people', []):
            if 'currentTeam' in p and 'id' in p['currentTeam']:
                team_id = p['currentTeam']['id']
                if team_id in teams:
                    mapping[p['id']] = teams[team_id]
        print(f"  Mapped {len(mapping)} players to teams.")
    except Exception as e:
        print(f"  Warning: failed to fetch rosters: {e}")
    return mapping
def get_chadwick_map() -> pd.DataFrame:
    print("  Loading Chadwick ID Map...")
    cw = chadwick_register()
    # We only need the map between MLBAM (Savant) and BBREF
    mapping = cw[['key_mlbam', 'key_bbref']].dropna()
    mapping['key_mlbam'] = mapping['key_mlbam'].astype(int)
    return mapping

def fetch_savant_traditional(year: int, id_map: pd.DataFrame) -> pd.DataFrame:
    print(f" Fetching Traditional Stats from Baseball Reference ({year})...")
    # Fetch B-Ref traditional stats
    df_bref = pybaseball.batting_stats_bref(year)
    
    # Rename for merging
    df_bref.rename(columns={'mlb_ID': 'key_bbref', 'Tm': 'team'}, inplace=True)
    
    # Not all Bref responses have mlb_ID natively depending on version, so we join on Name if needed
    # But best way is to use chadwick map if they provide bref_id. 
    # B-Ref usually doesn't provide bref_id in this specific function, so let's rely on name matching for B-Ref, 
    # OR better yet: Just use MLB's official JSON API for traditional stats to avoid name mapping entirely!
    pass

# We will use Savant's custom CSV generator with curl_cffi because it contains literally everything!
# It perfectly matches our schema and guarantees MLBAM ID for every row without messy name joining.
def fetch_savant_master_csv(year: int, player_type: str) -> pd.DataFrame:
    """
    player_type: 'batter' or 'pitcher'
    """
    print(f"  Downloading {player_type.title()} Master CSV from Savant ({year})...")
    # Add a timestamp to bypass any caching on Savant's side
    ts = int(time.time())
    # Force game_type=R (Regular Season) to avoid Spring Training data
    if player_type == 'batter':
        selections = "hit,single,double,triple,home_run,strikeout,walk,b_k_percent,b_bb_percent,batting_avg,slg_percent,on_base_percent,on_base_plus_slg,isolated_power,b_rbi,b_total_bases,b_game,ab,pa,xba,xslg,xwoba,xobp,xiso,wobacon_diff,sweet_spot_percent,barrel_batted_rate,hard_hit_percent,exit_velocity_avg,launch_angle_avg,sprint_speed,hp_to_first,chase_percent,whiff_percent,zone_swing_percent,zone_contact_percent,meatball_swing_percent,meatball_percent,team,b_stolen_base"
    else:
        selections = "p_game,p_started,p_save,p_win,p_loss,p_shutout,p_complete_game,p_strikeout,p_walk,p_era,p_earned_run,p_run,hit,p_home_run,batting_avg,on_base_percent,slg_percent,on_base_plus_slg,xba,xslg,xwoba,xobp,xiso,barrel_batted_rate,hard_hit_percent,exit_velocity_avg,launch_angle_avg,chase_percent,whiff_percent,zone_percent,putaway_percent,fastball_avg_speed,fastball_avg_spin,breaking_avg_spin,release_extension,team,b_stolen_base"
    
    url = f"https://baseballsavant.mlb.com/leaderboard/custom?year={year}&type={player_type}&filter=&sort=4&sortDir=desc&min=0&selections={selections}&chart=false&x=hit&y=hit&r=no&chartType=scatter&game_type=R&csv=true&_={ts}"

    for attempt in range(3):
        try:
            # Impersonate to bypass any basic scraping protections
            resp = requests.get(url, impersonate="chrome120", timeout=30)
            df = pd.read_csv(io.StringIO(resp.text))
            
            # Robust column cleaning: lowercase, spaces to underscores, remove all non-alphanumeric/underscore
            df.columns = [re.sub(r'[^a-z0-9_]', '', c.lower().replace(' ', '_')) for c in df.columns]
            
            # Standardize the name column for search tool compatibility
            if 'last_name_first_name' in df.columns:
                df.rename(columns={'last_name_first_name': 'playername'}, inplace=True)
            elif 'player_name' in df.columns:
                df.rename(columns={'player_name': 'playername'}, inplace=True)
            
            # Ensure BOTH 'playername' and 'player_name' exist to avoid UndefinedColumn errors
            if 'playername' in df.columns:
                df['player_name'] = df['playername']
                
            # Fuzzy match for common columns if the 'b_' or 'p_' versions are missing or NaN
            fuzzy_map = {
                'b_total_hits': ['hit', 'h', 'hits'],
                'b_single': ['single', '1b'],
                'b_double': ['double', '2b'],
                'b_triple': ['triple', '3b'],
                'b_home_run': ['home_run', 'hr', 'homeruns'],
                'b_strikeout': ['strikeout', 'so', 'k'],
                'b_walk': ['walk', 'bb'],
                'b_ab': ['ab', 'at_bats'],
                'b_total_pa': ['b_total_pa', 'pa'],
                'b_stolen_base': ['b_stolen_base', 'sb'],
                'p_hit': ['hit', 'h', 'hits'],
                'team': ['team_name', 'team_abbreviation', 'tm']
            }
            for target, alternatives in fuzzy_map.items():
                if target not in df.columns or df[target].isnull().all():
                    for alt in alternatives:
                        if alt in df.columns and not df[alt].isnull().all():
                            df[target] = df[alt]
                            break

            if 'player_id' not in df.columns and 'id' in df.columns:
                df.rename(columns={'id': 'player_id'}, inplace=True)
                
            print(f" Success! Fetched {len(df)} rows.")
            print(f" Debug: CSV Columns: {df.columns.tolist()}")
            if not df.empty:
                print(f" Debug: First row team: {df.iloc[0].get('team')}")
            return df
        except Exception as e:
            print(f" Attempt {attempt+1} failed to download CSV: {e}")
            time.sleep(2)
            
    print(" All attempts to download CSV failed.")
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
            
    # Replace NaN with None for database compatibility
    return out.where(pd.notnull(out), None)

def create_table_if_not_exists(db: pg8000.native.Connection, df: pd.DataFrame, table_name: str, key_cols: list):
    cols = []
    for col in df.columns:
        if col == 'year' or col == 'player_id': 
            cols.append(f'"{col}" INT')
        elif df[col].dtype == 'int64': 
            cols.append(f'"{col}" INT')
        elif df[col].dtype == 'float64': 
            cols.append(f'"{col}" FLOAT')
        elif df[col].dtype == 'bool':
            cols.append(f'"{col}" BOOLEAN')
        else: 
            cols.append(f'"{col}" TEXT')
    
    col_def = ", ".join(cols)
    pk_def = ", ".join([f'"{k}"' for k in key_cols])
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_def}, PRIMARY KEY ({pk_def}));'
    db.run(sql)
    
    # Schema Evolution: Add missing columns if table already exists
    existing_cols = [row[0] for row in db.run(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")]
    for col in df.columns:
        if col not in existing_cols:
            print(f" Adding missing column '{col}' to table '{table_name}'...")
            # Determine type
            if df[col].dtype == 'float64':
                col_type = "FLOAT"
            elif df[col].dtype == 'int64':
                col_type = "INT"
            elif df[col].dtype == 'bool':
                col_type = "BOOLEAN"
            else:
                col_type = "TEXT"
            db.run(f'ALTER TABLE "{table_name}" ADD COLUMN "{col}" {col_type};')

def upsert_table_pg8000(db: pg8000.native.Connection, df: pd.DataFrame, table_name: str, batch_size: int = 500):
    if df.empty: return
    all_cols = list(df.columns)
    
    # Define primary keys based on table
    if table_name == "lahman_savant_bridge":
        key_cols = ["playerid", "key_mlbam"]
    else:
        key_cols = ["player_id", "year"]
        
    non_key_cols = [c for c in all_cols if c not in key_cols]
    
    # Auto-create the table
    create_table_if_not_exists(db, df, table_name, key_cols)
    
    col_list = ", ".join([f'"{c}"' for c in all_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
    
    # pg8000.native doesn't have a direct executemany for UPSERTS with dictionaries easily,
    # so we'll use a transaction block with individual runs, which is still MUCH faster than
    # individual round-trips if we don't commit between every row. 
    # But for real speed, we'll use a single statement with many VALUES if possible, 
    # OR just wrap the loop in a single transaction.
    
    # Optimization: Use a single transaction for the whole dataframe
    try:
        db.run("BEGIN;")
        key_cols_escaped = ", ".join([f'"{k}"' for k in key_cols])
        # Use named placeholders (:col) which is most stable for pg8000.native 
        placeholders = ", ".join([f":{c}" for c in all_cols])
        
        if not non_key_cols:
            # If all columns are keys, we just DO NOTHING on conflict
            sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) ON CONFLICT ({key_cols_escaped}) DO NOTHING'
        else:
            set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
            sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) ON CONFLICT ({key_cols_escaped}) DO UPDATE SET {set_clause}'
        
        # Batching logic
        records = df.to_dict('records')
        for row in records:
            db.run(sql, **row)
        db.run("COMMIT;")
    except Exception as e:
        db.run("ROLLBACK;")
        print(f" Batch update failed for {table_name}: {e}")
        raise e

def update_id_bridge(db: pg8000.native.Connection):
    print("  Updating Lahman-Savant ID Bridge (this may take a moment on first run)...")
    cache_path = "chadwick_register.csv"
    try:
        df_bridge = None
        # Use local cache if it exists and is less than 7 days old
        if os.path.exists(cache_path):
            file_age = time.time() - os.path.getmtime(cache_path)
            if file_age < 86400 * 7:
                print(" Using cached player lookup table.")
                df_bridge = pd.read_csv(cache_path)

        if df_bridge is None:
            print("Gathering player lookup table via pybaseball...")
            # Use pybaseball's native register which handles split files automatically
            df_bridge = chadwick_register()
            df_bridge.to_csv(cache_path, index=False)
            print(" Downloaded and cached new player lookup table.")

        # Process and clean
        bridge = df_bridge[['key_mlbam', 'key_bbref', 'name_first', 'name_last']].dropna(subset=['key_mlbam', 'key_bbref'])
        bridge.rename(columns={'key_bbref': 'playerid'}, inplace=True)
        bridge['key_mlbam'] = bridge['key_mlbam'].astype(int)
        bridge['playername'] = bridge['name_first'] + ' ' + bridge['name_last']
        
        # Sync to DB
        upsert_table_pg8000(db, bridge[['key_mlbam', 'playerid', 'playername']], "lahman_savant_bridge")
        print(f" Bridge updated with {len(bridge)} mappings.")
    except Exception as e:
        print(f" Could not update ID bridge: {e}")

# ---------------- MAIN ----------------
def main():
    print(f" Connecting to AWS RDS at {DB_CONFIG['host'][:4]}***:{DB_CONFIG['port']}...")
    import socket
    try:
        # Force IPv4 resolution. GitHub Actions sometimes prefers IPv6, which we didn't whitelist.
        ipv4_host = socket.gethostbyname(DB_CONFIG['host'])
        DB_CONFIG['host'] = ipv4_host
    except Exception as e:
        print(f" Warning: Could not resolve IPv4 for host: {e}")

    db = None
    # Retry connection in case SG hasn't propagated yet
    for i in range(10):
        try:
            db = pg8000.native.Connection(**DB_CONFIG, timeout=30)
            print(" Connected to database.")
            break
        except Exception as e:
            if i == 9: raise e
            print(f" Waiting for connection (attempt {i+1}/10)...")
            time.sleep(10)
    
    try:
        # Update the ID bridge once per run to keep joins working
        # update_id_bridge(db)
        
        # Fetch Live Rosters for accurate teams
        player_team_map = get_mlb_rosters(YEAR)
        
        # ---- BATTING ----
        df_bat = fetch_savant_master_csv(YEAR, 'batter')
        if not df_bat.empty:
            df_bat = clean_and_normalize(df_bat)
            
            # Apply accurate team map
            if player_team_map:
                if 'team' not in df_bat.columns:
                    df_bat['team'] = None
                df_bat['team'] = df_bat['player_id'].map(player_team_map).fillna(df_bat['team']).fillna('FA')

            # Map the exact Savant columns to our schema
            if 'b_home_run' not in df_bat.columns or df_bat['b_home_run'].isnull().all():
                print(f"  Debug: Raw CSV Headers: {list(df_bat.columns)[:20]}")
                if not df_bat.empty:
                    print(f"  Debug: First row values: {df_bat.iloc[0].to_dict()}")

            # DEBUG: Print Top 5 HR leaders to verify data freshness
            if 'b_home_run' in df_bat.columns:
                # Fill NaNs with 0 for sorting
                df_bat['b_home_run'] = pd.to_numeric(df_bat['b_home_run'], errors='coerce').fillna(0)
                top_hr = df_bat.sort_values('b_home_run', ascending=False).head(5)
                print(f" Verification: Top 5 HR Leaders in fetched {YEAR} Regular Season data:")
                for _, row in top_hr.iterrows():
                    print(f"   - {row.get('playername', 'Unknown')} ({row.get('team', '???')}): {row['b_home_run']} HR (PA: {row.get('b_total_pa', 0)})")
            else:
                print(" Warning: 'b_home_run' column not found in fetched data!")
                print(f"   Available columns: {list(df_bat.columns)[:10]}...")
            
            # Map the exact Savant columns to our schema
            schema_map = {
                "savant_batting_traditional": ['player_id','year','playername','player_name','team','b_game','b_ab','b_total_pa','b_total_hits','b_single','b_double','b_triple','b_home_run','b_rbi','b_walk','b_strikeout','b_stolen_base'],
                "savant_batting_ratios": ['player_id','year','playername','player_name','batting_avg','on_base_percent','slg_percent','on_base_plus_slg','isolated_power','b_bb_percent','b_k_percent'],
                "savant_batting_expected": ['player_id','year','playername','player_name','xwoba','xba','xslg','xobp','xiso','wobacon_diff','sweet_spot_percent','barrel_batted_rate','hard_hit_percent'],
                "savant_batting_physics": ['player_id','year','playername','player_name','exit_velocity_avg','launch_angle_avg','sprint_speed','hp_to_first'],
                "savant_batting_discipline": ['player_id','year','playername','player_name','zone_swing_percent','zone_contact_percent','chase_percent','whiff_percent','meatball_swing_percent','meatball_percent']
            }
            
            # Calculate BB/K
            if 'b_walk' in df_bat.columns and 'b_strikeout' in df_bat.columns:
                df_bat['bb_k'] = df_bat['b_walk'] / df_bat['b_strikeout'].replace(0, 1) # prevent div by 0
                schema_map["savant_batting_ratios"].append('bb_k')

            for table, cols in schema_map.items():
                valid = [col for col in cols if col in df_bat.columns]
                if len(valid) >= 3:
                    print(f" Updating {table}...")
                    upsert_table_pg8000(db, df_bat[valid], table)
                    
        # ---- PITCHING ----
        df_pit = fetch_savant_master_csv(YEAR, 'pitcher')
        if not df_pit.empty:
            df_pit = clean_and_normalize(df_pit)
            
            # Apply accurate team map
            if player_team_map:
                if 'team' not in df_pit.columns:
                    df_pit['team'] = None
                df_pit['team'] = df_pit['player_id'].map(player_team_map).fillna(df_pit['team']).fillna('FA')
            
            schema_map = {
                "savant_pitching_traditional": ['player_id','year','playername','player_name','team','p_game','p_started','p_win','p_loss','p_save','p_shutout','p_complete_game','p_strikeout','p_walk','p_earned_run','p_run','p_hit','p_home_run'],
                "savant_pitching_ratios": ['player_id','year','playername','player_name','p_era','batting_avg','on_base_percent','slg_percent'],
                "savant_pitching_expected": ['player_id','year','playername','player_name','xwoba','xba','xslg','xobp','xiso','barrel_batted_rate','hard_hit_percent'],
                "savant_pitching_physics": ['player_id','year','playername','player_name','exit_velocity_avg','launch_angle_avg','fastball_avg_speed','fastball_avg_spin','breaking_avg_spin','release_extension'],
                "savant_pitching_discipline": ['player_id','year','playername','player_name','chase_percent','whiff_percent','zone_percent','putaway_percent']
            }
            
            if 'p_walk' in df_pit.columns and 'p_strikeout' in df_pit.columns:
                df_pit['bb_k'] = df_pit['p_walk'] / df_pit['p_strikeout'].replace(0, 1)
                schema_map["savant_pitching_ratios"].append('bb_k')
                
            for table, cols in schema_map.items():
                valid = [col for col in cols if col in df_pit.columns]
                if len(valid) >= 3:
                    print(f" Updating {table}...")
                    upsert_table_pg8000(db, df_pit[valid], table)
                    
        print(" Finished.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
