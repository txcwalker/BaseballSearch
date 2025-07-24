# scripts/download_fangraphs_all_years.py

from pybaseball import batting_stats, pitching_stats
import pandas as pd
from datetime import date
from tqdm import tqdm
import os

today = date.today().isoformat()
START_YEAR = 1901
END_YEAR = 2025
QUALIFIED = 0  # Set to 1+ to reduce row count (e.g., 300 PA)

batting_dir = f"../data/fangraphs/batting_by_year/"
pitching_dir = f"../data/fangraphs/pitching_by_year/"
os.makedirs(batting_dir, exist_ok=True)
os.makedirs(pitching_dir, exist_ok=True)

print(f"📊 Downloading FanGraphs stats ({START_YEAR}–{END_YEAR})")

# Column renaming maps
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

for year in tqdm(range(START_YEAR, END_YEAR + 1), desc="Year Progress"):
    try:
        df_bat = batting_stats(year, qual=QUALIFIED)
        df_bat["Season"] = year
        df_bat.rename(columns=batting_rename_map, inplace=True)
        df_bat.replace({'\$': ''}, regex=True, inplace=True)
        df_bat.to_csv(f"{batting_dir}/batting_{year}.csv", index=False)
    except Exception as e:
        print(f"⚠️ Skipped batting {year}: {e}")

    try:
        df_pitch = pitching_stats(year, qual=QUALIFIED)
        df_pitch["Season"] = year
        df_pitch.rename(columns=pitching_rename_map, inplace=True)
        df_pitch.replace({'\$': ''}, regex=True, inplace=True)
        df_pitch.to_csv(f"{pitching_dir}/pitching_{year}.csv", index=False)
    except Exception as e:
        print(f"⚠️ Skipped pitching {year}: {e}")
