# scripts/backfill_savant_statcast_history.py
#
# One-time backfill: populate 2015-2025 into the Statcast-exclusive tables
# (savant_*_expected / physics / discipline). These six tables are the ONLY
# source in this DB for exit velocity, launch angle, barrel%, xwOBA, whiff%,
# chase%, and sprint speed -- Lahman never had them and the frozen FanGraphs
# archive doesn't either. The daily incremental job (etl/update_savant_awsrds.py)
# only ever pulls the current season by design, so every season before 2026
# was empty for these metrics until this script runs.
#
# savant_*_traditional / ratios are intentionally left alone here -- Lahman
# already covers their counting/rate stats for historical seasons, per the
# three-tier data model documented in AGENTS.md.
#
# Usage:
#   .venv/Scripts/python scripts/backfill_savant_statcast_history.py
#
# Safe to re-run: upserts on (player_id, year), so a partial/interrupted run
# can just be re-run and will only overwrite rows it already touched.

import socket
import sys
import time
from pathlib import Path

import pg8000.native

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from etl.update_savant_awsrds import (
    DB_CONFIG,
    clean_and_normalize,
    fetch_savant_master_csv,
    upsert_table_pg8000,
)

START_YEAR = 2015  # first full Statcast tracking season
END_YEAR = 2025  # 2026+ stays owned by the daily incremental job

BATTING_SCHEMA_MAP = {
    "savant_batting_expected": ['player_id', 'year', 'playername', 'player_name', 'xwoba', 'xba', 'xslg', 'xobp', 'xiso', 'wobacon_diff', 'sweet_spot_percent', 'barrel_batted_rate', 'hard_hit_percent'],
    "savant_batting_physics": ['player_id', 'year', 'playername', 'player_name', 'exit_velocity_avg', 'launch_angle_avg', 'sprint_speed', 'hp_to_first'],
    "savant_batting_discipline": ['player_id', 'year', 'playername', 'player_name', 'zone_swing_percent', 'zone_contact_percent', 'chase_percent', 'whiff_percent', 'meatball_swing_percent', 'meatball_percent'],
}

PITCHING_SCHEMA_MAP = {
    "savant_pitching_expected": ['player_id', 'year', 'playername', 'player_name', 'xwoba', 'xba', 'xslg', 'xobp', 'xiso', 'barrel_batted_rate', 'hard_hit_percent'],
    "savant_pitching_physics": ['player_id', 'year', 'playername', 'player_name', 'exit_velocity_avg', 'launch_angle_avg', 'fastball_avg_speed', 'fastball_avg_spin', 'breaking_avg_spin', 'release_extension'],
    "savant_pitching_discipline": ['player_id', 'year', 'playername', 'player_name', 'chase_percent', 'whiff_percent', 'zone_percent', 'putaway_percent'],
}


def backfill_year(db, year):
    print(f"\n=== {year} ===")

    df_bat = fetch_savant_master_csv(year, 'batter')
    if not df_bat.empty:
        df_bat = clean_and_normalize(df_bat)
        for table, cols in BATTING_SCHEMA_MAP.items():
            valid = [c for c in cols if c in df_bat.columns]
            if len(valid) >= 3:
                print(f"  Updating {table} ({year})...")
                upsert_table_pg8000(db, df_bat[valid], table)
    else:
        print(f"  No batting data returned for {year}, skipping.")

    time.sleep(1)  # be polite to Savant between the two CSV pulls

    df_pit = fetch_savant_master_csv(year, 'pitcher')
    if not df_pit.empty:
        df_pit = clean_and_normalize(df_pit)
        for table, cols in PITCHING_SCHEMA_MAP.items():
            valid = [c for c in cols if c in df_pit.columns]
            if len(valid) >= 3:
                print(f"  Updating {table} ({year})...")
                upsert_table_pg8000(db, df_pit[valid], table)
    else:
        print(f"  No pitching data returned for {year}, skipping.")

    time.sleep(1)  # be polite to Savant between years


def main():
    cfg = dict(DB_CONFIG)
    try:
        # Force IPv4 resolution, same reasoning as the daily job.
        cfg['host'] = socket.gethostbyname(cfg['host'])
    except Exception as e:
        print(f"Warning: could not resolve IPv4 for host: {e}")

    db = pg8000.native.Connection(**cfg, timeout=30)
    try:
        for year in range(START_YEAR, END_YEAR + 1):
            backfill_year(db, year)
    finally:
        db.close()
    print("\nBackfill complete.")


if __name__ == "__main__":
    main()
