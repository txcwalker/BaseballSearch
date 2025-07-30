# scripts/fangraphs_split

# Script to split the fangrapsh data into smaller easier to handle and eventually search data tan;es

# Import statements
import pandas as pd
import os
import re

# Load the uploaded batting and pitching files
batting_path = "../data/fangraphs/fangraphs_batting_merged.csv"
pitching_path = "../data/fangraphs/fangraphs_pitching_merged.csv"

batting_df = pd.read_csv(batting_path)
pitching_df = pd.read_csv(pitching_path)

# Filter out rows with no PA (plate appearances)
batting_df = batting_df[batting_df['PA'] > 0]

# Fix duplicate 'fb%' column ambiguity in pitching data
def resolve_fb_conflict(df):
    rename_map = {
        'FB%': 'fyb_pc',
        'FB% 2': 'fb_pc'
    }

    affected_cols = [col for col in df.columns if col in rename_map]

    if affected_cols:
        print(f"🔧 Renaming columns due to FB% conflict:")
        for col in affected_cols:
            print(f"   ➤ '{col}' → '{rename_map[col]}'")
    else:
        print("✅ No FB% column conflicts found.")

    return df.rename(columns=rename_map)

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

# Clean and fix dataframes
pitching_df = resolve_fb_conflict(pitching_df)
batting_df = normalize_negatives(convert_numeric(clean_columns(batting_df)))
pitching_df = clean_columns(pitching_df)
pitching_df = normalize_negatives(convert_numeric(pitching_df))

# Fill missing values
batting_df.fillna(0.00, inplace=True)
pitching_df.fillna(0.00, inplace=True)


# Define the schema mappings
batting_splits = {
    "fangraphs_batting_lahman_like": [
        "idfg", "season", "name", "team", "g", "ab", "pa", "h", "singles", "doubles", "triples", "hr",
        "r", "rbi", "bb", "ibb", "so", "hbp", "sf", "sh", "sb", "cs"
    ],
    "fangraphs_batting_standard": [
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
    "fangraphs_pitching_standard": [
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
        "ch_pc", "chv", "sf_pc", "sfv", "kn_pc", "knv", "xx_pc", "po_pc", "wfb", "wsl", "wct", "wcb", "wch", "wsf", "wkn",
        "wfb_c", "wsl_c", "wct_c", "wcb_c", "wch_c", "wsf_c", "wkn_c"
    ]
}

# Output directory
output_dir = "../data/processed/fangraphs"
os.makedirs(output_dir, exist_ok=True)

# Helper to write selected columns to CSV if all columns exist
def write_split(df, mapping, df_name):
    for table_name, columns in mapping.items():
        valid_cols = [col for col in columns if col in df.columns]
        if len(valid_cols) >= 4:
            subset = df[valid_cols].copy()
            filename = os.path.join(output_dir, f"{table_name}.csv")
            subset.to_csv(filename, index=False)


# Write batting and pitching splits
write_split(batting_df, batting_splits, "batting")
write_split(pitching_df, pitching_splits, "pitching")
