# etl/update_fangraphs_awsrds.py

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pybaseball import batting_stats, pitching_stats
from datetime import date
from dotenv import load_dotenv
import os
from pathlib import Path
import re


# Load environment
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_PARAMS = {
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
}

CURRENT_YEAR = date.today().year

# Renaming maps
batting_rename_map = {
    'BB%': 'bb_pc', 'K%': 'k_pc', 'BB/K': 'bb_k', 'wRC+': 'wrc_plus',
    'O-Swing%': 'o_swing_pc', 'Z-Swing%': 'z_swing_pc', 'Swing%': 'swing_pc',
    'O-Contact%': 'o_contact_pc', 'Z-Contact%': 'z_contact_pc', 'Contact%': 'contact_pc',
    'Zone%': 'zone_pc', 'F-Strike%': 'f_strike_pc', 'SwStr%': 'swstr_pc',
    'CStr%': 'cstr_pc', 'CSW%': 'csw_pc', 'WPA+': 'wpa_plus', 'wRC': 'wrc',
    'IFH%': 'ifh_pc', 'BUH%': 'buh_pc', 'Pull%': 'pull_pc', 'Cent%': 'cent_pc',
    'Oppo%': 'oppo_pc', 'Soft%': 'soft_pc', 'Med%': 'med_pc', 'Hard%': 'hard_pc',
    'HardHit%': 'hardhit_pc', 'Barrel%': 'barrel_pc', 'TTO%': 'tto_pc','+WPA': 'wpa_plus',
    '-WPA': 'wpa_minus',
}

pitching_rename_map = {
    'LOB%': 'lob_pc', 'K/9': 'k_9', 'BB/9': 'bb_9', 'K/BB': 'k_bb',
    'H/9': 'h_9', 'HR/9': 'hr_9', 'O-Swing%': 'o_swing_pc', 'Z-Swing%': 'z_swing_pc',
    'Swing%': 'swing_pc', 'O-Contact%': 'o_contact_pc', 'Z-Contact%': 'z_contact_pc',
    'Contact%': 'contact_pc', 'Zone%': 'zone_pc', 'F-Strike%': 'f_strike_pc',
    'SwStr%': 'swstr_pc', 'CStr%': 'cstr_pc', 'CSW%': 'csw_pc', 'Barrel%': 'barrel_pc',
    'HardHit%': 'hardhit_pc', 'TTO%': 'tto_pc', 'ERA-': 'era_minus',
    'FIP-': 'fip_minus', 'xFIP-': 'xfip_minus', 'RA9_WAR': 'ra9_war', '+WPA': 'wpa_plus',
    '-WPA': 'wpa_minus',
}

YEAR = date.today().year

try:
    df_bat = batting_stats(YEAR)
    df_bat["Season"] = YEAR
    df_bat.rename(columns=batting_rename_map, inplace=True)
    df_bat.replace({'\$': ''}, regex=True, inplace=True)
except Exception as e:
    print(f"⚠️ Skipped batting {YEAR}: {e}")

try:
    df_pitch = pitching_stats(YEAR)
    df_pitch["Season"] = YEAR
    df_pitch.rename(columns=pitching_rename_map, inplace=True)
    df_pitch.replace({'\$': ''}, regex=True, inplace=True)
except Exception as e:
    print(f"⚠️ Skipped pitching {YEAR}: {e}")

df_bat = df_bat[df_bat['PA'] > 0]

def normalize(df):
    df.replace({'\\$': '', '%': ''}, regex=True, inplace=True)
    df = df.map(lambda x: str(x).strip() if isinstance(x, str) else x)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

# Standardize column names (strip and lowercase)
def clean_columns(df):
    original_columns = df.columns
    renamed_columns = [
        col.strip().lower().replace('%', '_pc') for col in original_columns
    ]
    df.columns = renamed_columns

    for orig_col, new_col in zip(original_columns, renamed_columns):
        if '%' in orig_col or new_col.endswith('_pc'):
            df[new_col] = (
                df[new_col]
                .astype(str)
                .str.replace('%', '', regex=False)
                .str.strip()
            )
            df[new_col] = pd.to_numeric(df[new_col], errors='coerce')

    return df

# Attempt to convert all data to numeric where possible
def convert_numeric(df):
    for col in df.columns:
        df[col] = pd.to_numeric(df[col], errors='ignore')
    return df

# Normalize parenthesis-style negative numbers like (0.2) -> -0.2
def normalize_negatives(df):
    return df.applymap(
        lambda x: float(re.sub(r'^\((.*)\)$', r'-\1', x)) if isinstance(x, str) and re.match(r'^\(\d+(\.\d+)?\)$', x) else x
    )

# Fix duplicate 'fb%' column ambiguity in pitching data
def resolve_fb_conflict(df):
    rename_map = {
        'fb%': 'fyb_pc',
        'fb% 2': 'fb_pc'
    }

    affected_cols = [col for col in df.columns if col in rename_map]

    if affected_cols:
        print(f"🔧 Renaming columns due to FB% conflict:")
        for col in affected_cols:
            print(f"   ➤ '{col}' → '{rename_map[col]}'")
    else:
        print("✅ No FB% column conflicts found.")

    return df.rename(columns=rename_map)

# Clean and fix dataframes
df_bat = normalize_negatives(convert_numeric(clean_columns(df_bat)))
df_pitch = clean_columns(df_pitch)
df_pitch = resolve_fb_conflict(df_pitch)
df_pitch = normalize_negatives(convert_numeric(df_pitch))

# Fill missing values
df_bat.fillna(0.00, inplace=True)
df_pitch.fillna(0.00, inplace=True)

# Define the schema mappings
batting_splits = {
        "fangraphs_batting_lahman_like": [
            "idfg", "season", "name", "team", "g", "ab", "pa", "h", "singles", "doubles", "triples", "hr",
            "r", "rbi", "bb", "ibb", "so", "hbp", "sf", "sh", "sb", "cs"
        ],
        "fangraphs_batting_standard_ratios": [
            "idfg", "season", "name", "team", "avg", "obp", "slg", "ops", "iso", "babip",
            "bb_pc", "kpc", "bb_k", "gdp"
        ],
        "fangraphs_batting_advanced": [
            "idfg", "season", "name", "team", "woba", "wraa", "wrc", "wrc_plus", "war", "rar",
            "bat", "fld", "rep", "pos", "off", "def", "dol"
        ],
        "fangraphs_plate_discipline": [
            "idfg", "season", "name", "team", "o_swing_pc", "z_swing_pc", "swing_pc", "o_contact_pc",
            "z_contact_pc", "contact_pc", "zone_pc", "f_strike_pc", "swstr_pc", "cstr_pc", "csw_pc", "wpa",
            "clutch", "re24", "rew", "pli", "phli", "ph"
        ],
        "fangraphs_batted_ball": [
            "idfg", "season", "name", "team", "gb", "fb", "ld", "iffb", "gb_fb", "ld_pc", "gb_pc", "fb_pc",
            "iffb_pc", "hr_fb", "ifh", "ifh_pc", "bu", "buh", "buh_pc", "pull_pc", "cent_pc", "oppo_pc", "soft_pc",
            "med_pc", "hard_pc", "hardhit", "hardhit_pc", "ev", "la", "barrels", "barrel_pc", "maxev", "tto_pc"
        ],
        "fangraphs_baserunning_fielding": [
            "idfg", "season", "name", "team", "bsr", "spd", "wsb", "ubr", "wgdp"
        ],
        "fangraphs_batter_pitch_type_summary": [
            "idfg", "season", "name", "team", "fb_pc", "fbv", "sl_pc", "slv", "ch_pc", "chv", "cb_pc", "cbv",
            "sf_pc", "sfv", "ct_pc", "ctv", "kn_pc", "knv", "xx_pc", "po_pc", "wfb", "wsl", "wch", "wcb", "wsf",
            "wct", "wkn", "wfb_c", "wsl_c", "wch_c", "wcb_c", "wsf_c", "wct_c", "wkn_c"
        ]
    }

pitching_splits = {
        "fangraphs_pitching_lahman_like": [
            "idfg", "season", "name", "team", "w", "l", "g", "gs", "cg", "sho", "sv", "ip", "h",
            "r", "er", "hr", "bb", "so", "hbp", "wp", "bk", "tbf"
        ],
        "fangraphs_pitching_standard_ratios": [
            "idfg", "season", "name", "team", "era", "k_9", "bb_9", "k_bb", "h_9", "hr_9", "avg",
            "whip", "babip", "lob_pc"
        ],
        "fangraphs_pitching_advanced": [
            "idfg", "season", "name", "team", "war", "fip", "xfip", "siera", "era_minus", "fip_minus", "xfip_minus",
            "rar", "dollars", "ra9_war"
        ],
        "fangraphs_pitching_plate_discipline": [
            "idfg", "season", "name", "team", "o_swing_pc", "z_swing_pc", "swing_pc", "o_contact_pc",
            "z_contact_pc", "contact_pc", "zone_pc", "f_strike_pc", "swstr_pc", "cstr_pc", "csw_pc"
        ],
        "fangraphs_pitching_batted_ball": [
            "idfg", "season", "name", "team", "gb_fb", "ld_pc", "gb_pc", "fyb_pc", "iffb_pc", "hr_fb",
            "pull_pc", "cent_pc", "oppo_pc", "soft_pc", "med_pc", "hard_pc", "ev", "la", "barrels", "barrel_pc",
            "maxev", "hardhit", "hardhit_pc", "tto_pc"
        ],
        "fangraphs_pitching_pitch_type_summary": [
            "idfg", "season", "name", "team", "fb_pc", "fbv", "sl_pc", "slv", "ct_pc", "ctv", "cb_pc", "cbv",
            "ch_pc", "chv", "sf_pc", "sfv", "kn_pc", "knv", "xx_pc", "po_pc", "wfb", "wsl", "wct", "wcb", "wch", "wsf",
            "wkn",
            "wfb_c", "wsl_c", "wct_c", "wcb_c", "wch_c", "wsf_c", "wkn_c"
        ]
    }

# Helper to split DataFrames by schema mapping (no file I/O)
def split_dataframe(df, mapping):
    result = {}
    for table_name, columns in mapping.items():
        valid_cols = [col for col in columns if col in df.columns]
        if len(valid_cols) >= 4:
            result[table_name] = df[valid_cols].copy()
    return result

# Create split DataFrames
batting_dfs = split_dataframe(df_bat, batting_splits)
pitching_dfs = split_dataframe(df_pitch, pitching_splits)


def get_changed_rows(df, table_name, conn):
    key_cols = ["idfg", "season"]
    all_cols = df.columns.tolist()
    non_key_cols = [col for col in all_cols if col not in key_cols]

    cursor = conn.cursor()

    # Fetch matching rows from DB
    keys = df[key_cols].drop_duplicates()
    keys_tuple = [tuple(map(lambda x: x.item() if hasattr(x, "item") else x, row)) for row in keys.to_numpy()]
    if not keys_tuple:
        return pd.DataFrame()

    select_sql = f"""
        SELECT {", ".join(all_cols)}
        FROM "{table_name}"
        WHERE ({", ".join(key_cols)}) IN %s
    """
    try:
        cursor.execute(select_sql, (tuple(keys_tuple),))
        db_rows = cursor.fetchall()
    except Exception as e:
        print(f"⚠️ Could not check changes for `{table_name}`: {e}")
        return pd.DataFrame()

    # Turn DB result into DataFrame
    db_df = pd.DataFrame(db_rows, columns=all_cols)
    if db_df.empty:
        return df  # everything is new

    db_df = db_df.sort_values(by=key_cols).reset_index(drop=True)
    df_sorted = df.sort_values(by=key_cols).reset_index(drop=True)

    # Compare non-key columns
    changed_rows = df_sorted[
        ~df_sorted[non_key_cols].eq(db_df[non_key_cols]).all(axis=1)
    ]
    return changed_rows


def upsert_table(df, table_name, conn):
    if df.empty:
        print(f"⚠️ Skipping empty table: {table_name}")
        return

    # Detect changed rows only
    changed = get_changed_rows(df, table_name, conn)
    if changed.empty:
        print(f"✅ No updates needed for `{table_name}`")
        return

    print(f"🔍 {len(changed)} rows in `{table_name}` will be updated.")

    # Show preview of changed rows
    preview_cols = ["idfg", "season"]
    if "name" in changed.columns:
        preview_cols += ["name"]
    if "team" in changed.columns:
        preview_cols += ["team"]
    print(changed[preview_cols].head(5))  # Just show top 5 for readability

    key_cols = ["idfg", "season"]
    all_cols = list(df.columns)
    non_key_cols = [col for col in all_cols if col not in key_cols]

    col_list = ", ".join([f'"{col}"' for col in all_cols])
    set_clause = ", ".join([
        f'"{col}" = EXCLUDED."{col}"' for col in non_key_cols
    ])
    where_clause = " OR ".join([
        f'"{table_name}"."{col}" IS DISTINCT FROM EXCLUDED."{col}"' for col in non_key_cols
    ])

    sql = f"""
        INSERT INTO "{table_name}" ({col_list})
        VALUES %s
        ON CONFLICT (idfg, season) DO UPDATE
        SET {set_clause}
        WHERE {where_clause}
    """

    values = [tuple(row) for row in df.to_numpy()]
    cursor = conn.cursor()
    try:
        print(f"🚀 Running UPSERT on `{table_name}` with {len(values)} total rows...")
        execute_values(cursor, sql, values)
        conn.commit()
        print(f"✅ Successfully updated {len(changed)} row(s) in `{table_name}`")
    except Exception as e:
        print(f"❌ Failed to update `{table_name}`: {e}")
        conn.rollback()
    finally:
        cursor.close()

if __name__ == "__main__":
    print("🔌 Connecting to AWS RDS...")
    conn = psycopg2.connect(**DB_PARAMS)

    # Merge all split tables
    processed_tables = {**batting_dfs, **pitching_dfs}
    print(f"🧩 Found {len(processed_tables)} FanGraphs tables to process")

    for table_name, df in processed_tables.items():
        print(f"📁 Processing `{table_name}` with {len(df)} rows")
        upsert_table(df, table_name, conn)

    conn.close()
    print("✅ All tables processed and connection closed.")



