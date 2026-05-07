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
def get_chadwick_map() -> pd.DataFrame:
    print("🗺️  Loading Chadwick ID Map...")
    cw = chadwick_register()
    # We only need the map between MLBAM (Savant) and BBREF
    mapping = cw[['key_mlbam', 'key_bbref']].dropna()
    mapping['key_mlbam'] = mapping['key_mlbam'].astype(int)
    return mapping

def fetch_savant_traditional(year: int, id_map: pd.DataFrame) -> pd.DataFrame:
    print(f"⚾ Fetching Traditional Stats from Baseball Reference ({year})...")
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
    print(f"🕵️  Downloading {player_type.title()} Master CSV from Savant ({year})...")
    # This URL requests the custom leaderboard with ALL standard and expected stats selected
    url = f"https://baseballsavant.mlb.com/leaderboard/custom?year={year}&type={player_type}&filter=&sort=4&sortDir=desc&min=0&selections=b_total_hits,b_single,b_double,b_triple,b_home_run,b_strikeout,b_walk,b_k_percent,b_bb_percent,batting_avg,slg_percent,on_base_percent,on_base_plus_slg,isolated_power,b_rbi,b_total_bases,b_game,b_ab,b_total_pa,xba,xslg,xwoba,xobp,xiso,wobacon_diff,sweet_spot_percent,barrel_batted_rate,hard_hit_percent,exit_velocity_avg,launch_angle_avg,sprint_speed,hp_to_first,chase_percent,whiff_percent,zone_swing_percent,zone_contact_percent,meatball_swing_percent,meatball_percent&chart=false&x=b_total_hits&y=b_total_hits&r=no&chartType=scatter&csv=true"
    
    # Pitcher specific URL selection (if requested)
    if player_type == 'pitcher':
        url = f"https://baseballsavant.mlb.com/leaderboard/custom?year={year}&type={player_type}&filter=&sort=4&sortDir=desc&min=0&selections=p_game,p_started,p_save,p_win,p_loss,p_shutout,p_complete_game,p_strikeout,p_walk,p_era,p_earned_run,p_run,p_hit,p_home_run,batting_avg,on_base_percent,slg_percent,on_base_plus_slg,xba,xslg,xwoba,xobp,xiso,barrel_batted_rate,hard_hit_percent,exit_velocity_avg,launch_angle_avg,chase_percent,whiff_percent,zone_percent,putaway_percent,fastball_avg_speed,fastball_avg_spin,breaking_avg_spin,release_extension&chart=false&x=p_game&y=p_game&r=no&chartType=scatter&csv=true"

    try:
        # Impersonate to bypass any basic scraping protections
        resp = requests.get(url, impersonate="chrome120", timeout=30)
        df = pd.read_csv(io.StringIO(resp.text))
        
        # Clean up column names
        df.columns = [c.lower().replace('.', '') for c in df.columns]
        if 'player_id' not in df.columns and 'id' in df.columns:
            df.rename(columns={'id': 'player_id'}, inplace=True)
            
        print(f"✅ Success! Fetched {len(df)} rows.")
        return df
    except Exception as e:
        print(f"❌ Failed to download CSV: {e}")
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

def create_table_if_not_exists(db: pg8000.native.Connection, df: pd.DataFrame, table_name: str, key_cols: list):
    cols = []
    for col in df.columns:
        if col == 'year' or col == 'player_id': 
            cols.append(f'"{col}" INT')
        elif df[col].dtype == 'int64': 
            cols.append(f'"{col}" INT')
        elif df[col].dtype == 'float64': 
            cols.append(f'"{col}" FLOAT')
        else: 
            cols.append(f'"{col}" VARCHAR(255)')
    
    col_def = ", ".join(cols)
    pk_def = ", ".join([f'"{k}"' for k in key_cols])
    sql = f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_def}, PRIMARY KEY ({pk_def}));'
    db.run(sql)

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
        placeholders = ", ".join([f":{c}" for c in all_cols])
        sql = f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) ON CONFLICT ({", ".join([f"\\"{k}\\"" for k in key_cols])}) DO UPDATE SET {set_clause}'
        
        # Batching logic
        records = df.to_dict('records')
        for row in records:
            db.run(sql, **row)
        db.run("COMMIT;")
    except Exception as e:
        db.run("ROLLBACK;")
        print(f"❌ Batch update failed for {table_name}: {e}")
        raise e

def update_id_bridge(db: pg8000.native.Connection):
    print("🗺️  Updating Lahman-Savant ID Bridge (this may take a moment on first run)...")
    try:
        cw = pybaseball.chadwick_register()
        bridge = cw[['key_mlbam', 'key_bbref']].dropna().drop_duplicates()
        bridge.rename(columns={'key_bbref': 'playerid', 'key_mlbam': 'key_mlbam'}, inplace=True)
        bridge['key_mlbam'] = bridge['key_mlbam'].astype(int)
        
        # Use our optimized upsert
        upsert_table_pg8000(db, bridge, "lahman_savant_bridge")
        print(f"✅ Bridge updated with {len(bridge)} mappings.")
    except Exception as e:
        print(f"⚠️ Could not update ID bridge: {e}")

# ---------------- MAIN ----------------
def main():
    print("🔌 Connecting to AWS RDS...")
    db = pg8000.native.Connection(**DB_CONFIG)
    
    try:
        # Update the ID bridge once per run to keep joins working
        update_id_bridge(db)
        
        # ---- BATTING ----
        df_bat = fetch_savant_master_csv(YEAR, 'batter')
        if not df_bat.empty:
            df_bat = clean_and_normalize(df_bat)
            
            # Map the exact Savant columns to our schema
            schema_map = {
                "savant_batting_traditional": ['player_id','year','last_name, first_name','b_game','b_ab','b_total_pa','b_total_hits','b_single','b_double','b_triple','b_home_run','b_rbi','b_walk','b_strikeout'],
                "savant_batting_ratios": ['player_id','year','last_name, first_name','batting_avg','on_base_percent','slg_percent','on_base_plus_slg','isolated_power','b_bb_percent','b_k_percent'],
                "savant_batting_expected": ['player_id','year','xwoba','xba','xslg','xobp','xiso','wobacon_diff','sweet_spot_percent','barrel_batted_rate','hard_hit_percent'],
                "savant_batting_physics": ['player_id','year','exit_velocity_avg','launch_angle_avg','sprint_speed','hp_to_first'],
                "savant_batting_discipline": ['player_id','year','zone_swing_percent','zone_contact_percent','chase_percent','whiff_percent','meatball_swing_percent','meatball_percent']
            }
            
            # Calculate BB/K
            if 'b_walk' in df_bat.columns and 'b_strikeout' in df_bat.columns:
                df_bat['bb_k'] = df_bat['b_walk'] / df_bat['b_strikeout'].replace(0, 1) # prevent div by 0
                schema_map["savant_batting_ratios"].append('bb_k')

            for table, cols in schema_map.items():
                valid = [col for col in cols if col in df_bat.columns]
                if len(valid) >= 3:
                    print(f"📁 Updating {table}...")
                    upsert_table_pg8000(db, df_bat[valid], table)
                    
        # ---- PITCHING ----
        df_pit = fetch_savant_master_csv(YEAR, 'pitcher')
        if not df_pit.empty:
            df_pit = clean_and_normalize(df_pit)
            
            schema_map = {
                "savant_pitching_traditional": ['player_id','year','last_name, first_name','p_game','p_started','p_win','p_loss','p_save','p_shutout','p_complete_game','p_strikeout','p_walk','p_earned_run','p_run','p_hit','p_home_run'],
                "savant_pitching_ratios": ['player_id','year','last_name, first_name','p_era','batting_avg','on_base_percent','slg_percent'],
                "savant_pitching_expected": ['player_id','year','xwoba','xba','xslg','xobp','xiso','barrel_batted_rate','hard_hit_percent'],
                "savant_pitching_physics": ['player_id','year','exit_velocity_avg','launch_angle_avg','fastball_avg_speed','fastball_avg_spin','breaking_avg_spin','release_extension'],
                "savant_pitching_discipline": ['player_id','year','chase_percent','whiff_percent','zone_percent','putaway_percent']
            }
            
            if 'p_walk' in df_pit.columns and 'p_strikeout' in df_pit.columns:
                df_pit['bb_k'] = df_pit['p_walk'] / df_pit['p_strikeout'].replace(0, 1)
                schema_map["savant_pitching_ratios"].append('bb_k')
                
            for table, cols in schema_map.items():
                valid = [col for col in cols if col in df_pit.columns]
                if len(valid) >= 3:
                    print(f"📁 Updating {table}...")
                    upsert_table_pg8000(db, df_pit[valid], table)
                    
        print("✅ Finished.")
    finally:
        db.close()

if __name__ == "__main__":
    main()
