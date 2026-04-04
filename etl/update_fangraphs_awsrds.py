# etl/update_fangraphs_awsrds.py
# Script that updates the AWS RDS database, runs mornings at 7 AM CST via GitHub runner

from __future__ import annotations

# Imports
import os
import re
import time
import requests
from datetime import date
from pathlib import Path
from typing import Iterable, List, Tuple, Dict

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pybaseball import batting_stats, pitching_stats
from dotenv import load_dotenv

# ---------------------------------------------------------------
# FanGraphs 403 fix: patch requests with a real browser User-Agent
# GitHub Actions runners get blocked without this.
# ---------------------------------------------------------------
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.fangraphs.com/",
}

_original_get = requests.Session.get

def _patched_get(self, url, **kwargs):
    kwargs.setdefault("headers", {})
    kwargs["headers"] = {**_HEADERS, **kwargs["headers"]}
    return _original_get(self, url, **kwargs)

requests.Session.get = _patched_get
# ---------------------------------------------------------------

# ---------------- Env & DB params ----------------
load_dotenv(Path(__file__).resolve().parents[1] / ".env.awsrds")

DB_PARAMS = {
    "dbname": os.getenv("AWSDATABASE"),
    "user": os.getenv("AWSUSER"),
    "password": os.getenv("AWSPASSWORD"),
    "host": os.getenv("AWSHOST"),
    "port": os.getenv("AWSPORT"),
}

# Optional behavior
AUTO_DEDUPE = (os.getenv("AUTO_DEDUPE", "true").lower() in {"1", "true", "yes", "y"})
YEAR = date.today().year

# ---------------- Rename maps ----------------
batting_rename_map = {
    'BB%': 'bb_pc', 'K%': 'k_pc', 'BB/K': 'bb_k', 'wRC+': 'wrc_plus',
    'O-Swing%': 'o_swing_pc', 'Z-Swing%': 'z_swing_pc', 'Swing%': 'swing_pc',
    'O-Contact%': 'o_contact_pc', 'Z-Contact%': 'z_contact_pc', 'Contact%': 'contact_pc',
    'Zone%': 'zone_pc', 'F-Strike%': 'f_strike_pc', 'SwStr%': 'swstr_pc',
    'CStr%': 'cstr_pc', 'CSW%': 'csw_pc', 'WPA+': 'wpa_plus', 'wRC': 'wrc',
    'IFH%': 'ifh_pc', 'BUH%': 'buh_pc', 'Pull%': 'pull_pc', 'Cent%': 'cent_pc',
    'Oppo%': 'oppo_pc', 'Soft%': 'soft_pc', 'Med%': 'med_pc', 'Hard%': 'hard_pc',
    'HardHit%': 'hardhit_pc', 'Barrel%': 'barrel_pc', 'TTO%': 'tto_pc', '+WPA': 'wpa_plus',
    '-WPA': 'wpa_minus', '1b':'singles','2b':'doubles','3b':'triples', "IDfg":"idfg","Season":"season","Name":"name",
    "Team":'team'
}

pitching_rename_map = {
    'LOB%': 'lob_pc', 'K/9': 'k_9', 'BB/9': 'bb_9', 'K/BB': 'k_bb',
    'H/9': 'h_9', 'HR/9': 'hr_9', 'O-Swing%': 'o_swing_pc', 'Z-Swing%': 'z_swing_pc',
    'Swing%': 'swing_pc', 'O-Contact%': 'o_contact_pc', 'Z-Contact%': 'z_contact_pc',
    'Contact%': 'contact_pc', 'Zone%': 'zone_pc', 'F-Strike%': 'f_strike_pc',
    'SwStr%': 'swstr_pc', 'CStr%': 'cstr_pc', 'CSW%': 'csw_pc', 'Barrel%': 'barrel_pc',
    'HardHit%': 'hardhit_pc', 'TTO%': 'tto_pc', 'ERA-': 'era_minus',
    'FIP-': 'fip_minus', 'xFIP-': 'xfip_minus', 'RA9_WAR': 'ra9_war', '+WPA': 'wpa_plus',
    '-WPA': 'wpa_minus', "IDfg":"idfg","Season":"season","Name":"name","Team":'team'
}

# ---------------- FB% conflict helper ----------------
def resolve_fb_conflict(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {'fb%': 'fyb_pc', 'fb% 2': 'fb_pc'}
    affected = [c for c in df.columns if c in rename_map]
    if affected:
        print("🔧 Renaming columns due to FB% conflict:")
        for c in affected:
            print(f"   ➤ '{c}' → '{rename_map[c]}'")
    else:
        print("✅ No FB% column conflicts found.")
    return df.rename(columns=rename_map)

# ---------------- Load data ----------------
print(f"📡 Fetching FanGraphs batting stats for {YEAR}...")
try:
    df_bat = batting_stats(YEAR)
    df_bat["Season"] = YEAR
    df_bat.rename(columns=batting_rename_map, inplace=True)
    df_bat.replace({'\$': ''}, regex=True, inplace=True)
    print(f"✅ Batting fetch succeeded: {len(df_bat)} rows")
except Exception as e:
    print(f"⚠️ Skipped batting {YEAR}: {e}")
    df_bat = pd.DataFrame()

# Small delay between requests to be polite
time.sleep(3)

print(f"📡 Fetching FanGraphs pitching stats for {YEAR}...")
try:
    df_pitch = pitching_stats(YEAR)
    df_pitch["Season"] = YEAR
    df_pitch.rename(columns=pitching_rename_map, inplace=True)
    df_pitch.replace({'\$': ''}, regex=True, inplace=True)
    print(f"✅ Pitching fetch succeeded: {len(df_pitch)} rows")
except Exception as e:
    print(f"⚠️ Skipped pitching {YEAR}: {e}")
    df_pitch = pd.DataFrame()

# Remove batters with 0 PA
if not df_bat.empty and "PA" in df_bat.columns:
    df_bat = df_bat[df_bat['PA'] > 0]

# ---------------- Cleaning helpers ----------------
def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    original = df.columns
    renamed = [c.strip().lower().replace('%', '_pc') for c in original]
    df = df.copy()
    df.columns = renamed
    for orig, new in zip(original, renamed):
        if '%' in orig or new.endswith('_pc'):
            df[new] = (
                df[new].astype(str).str.replace('%', '', regex=False).str.strip()
            )
            df[new] = pd.to_numeric(df[new], errors='coerce')
    return df

def convert_numeric(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    for col in out.columns:
        try:
            out[col] = pd.to_numeric(out[col])
        except Exception:
            pass
    return out

def normalize_negatives(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    def conv(x):
        if isinstance(x, str) and re.match(r'^\(\d+(\.\d+)?\)$', x):
            return float(re.sub(r'^\((.+)\)$', r'-\1', x))
        return x
    return df.apply(lambda col: col.map(conv))

# Apply cleaning
df_pitch = resolve_fb_conflict(df_pitch)
df_bat = normalize_negatives(convert_numeric(clean_columns(df_bat)))

def finalize_batting(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()

    if "team" in out.columns:
        out["team"] = out["team"].replace({"- - -": "TOT"})

    for c in ("h", "hr", "2b", "3b"):
        if c in out.columns:
            out[c] = pd.to_numeric(out[c], errors="coerce")

    if "2b" in out.columns:
        out["doubles"] = pd.to_numeric(out["2b"], errors="coerce")
    else:
        if "doubles" not in out.columns:
            out["doubles"] = pd.NA

    if "3b" in out.columns:
        out["triples"] = pd.to_numeric(out["3b"], errors="coerce")
    else:
        if "triples" not in out.columns:
            out["triples"] = pd.NA

    for c in ("h", "hr", "doubles", "triples"):
        if c not in out.columns:
            out[c] = pd.NA

    out["singles"] = (
        pd.to_numeric(out["h"], errors="coerce")
        - pd.to_numeric(out["doubles"], errors="coerce")
        - pd.to_numeric(out["triples"], errors="coerce")
        - pd.to_numeric(out["hr"], errors="coerce")
    )

    out["singles"] = out["singles"].where(out["singles"].ge(0) | out["singles"].isna(), 0)

    return out

df_bat = finalize_batting(df_bat)

# ---------------- Table splits ----------------
batting_splits = {
    "fangraphs_batting_lahman_like": ["idfg", "season", "name", "team", "g", "ab", "pa", "h", "singles", "doubles", "triples", "hr", "r", "rbi", "bb", "ibb", "so", "hbp", "sf", "sh", "gidp", "sb", "cs"],
    "fangraphs_batting_standard_ratios": ["idfg", "season", "name", "team", "avg", "obp", "slg", "ops", "iso", "babip", "bb_pc", "k_pc", "bb_k", "gidp"],
    "fangraphs_batting_advanced": ["idfg", "season", "name", "team", "woba", "wraa", "wrc", "wrc_plus", "war", "rar", "bat", "fld", "rep", "pos", "off", "def", "dol"],
    "fangraphs_plate_discipline": ["idfg", "season", "name", "team", "o_swing_pc", "z_swing_pc", "swing_pc", "o_contact_pc", "z_contact_pc", "contact_pc", "zone_pc", "f_strike_pc", "swstr_pc", "cstr_pc", "csw_pc", "wpa", "wpa_li", "clutch", "re24", "rew", "pli", "phli", "ph"],
    "fangraphs_batted_ball": ["idfg", "season", "name", "team", "gb", "fyb", "ld", "iffb", "gb_fb", "ld_pc", "gb_pc", "fyb_pc", "iffb_pc", "hr_fb", "ifh", "ifh_pc", "bu", "buh", "buh_pc", "pull_pc", "cent_pc", "oppo_pc", "soft_pc", "med_pc", "hard_pc", "hardhit", "hardhit_pc", "ev", "la", "barrels", "barrel_pc", "maxev", "tto_pc"],
    "fangraphs_baserunning_fielding": ["idfg", "season", "name", "team", "bsr", "spd", "wsb", "ubr", "wgdp"],
    "fangraphs_batter_pitch_type_summary": ["idfg", "season", "name", "team", "fb_pc", "fbv", "sl_pc", "slv", "ch_pc", "chv", "cb_pc", "cbv", "sf_pc", "sfv", "ct_pc", "ctv", "kn_pc", "knv", "xx_pc"],
}

pitching_splits = {
    "fangraphs_pitching_lahman_like": ["idfg", "season", "name", "team", "w", "l", "g", "gs", "cg", "sho", "sv", "ip", "h", "r", "er", "hr", "bb", "so", "hbp", "wp", "bk", "tbf"],
    "fangraphs_pitching_standard_ratios": ["idfg", "season", "name", "team", "era", "k_9", "bb_9", "k_bb", "h_9", "hr_9", "avg", "whip", "babip", "lob_pc"],
    "fangraphs_pitching_advanced": ["idfg", "season", "name", "team", "war", "fip", "xfip", "siera", "era_minus", "fip_minus", "xfip_minus", "rar", "dollars", "ra9_war"],
    "fangraphs_pitching_plate_discipline": ["idfg", "season", "name", "team", "o_swing_pc", "z_swing_pc", "swing_pc", "o_contact_pc", "z_contact_pc", "contact_pc", "zone_pc", "f_strike_pc", "swstr_pc", "cstr_pc", "csw_pc"],
    "fangraphs_pitching_batted_ball": ["idfg", "season", "name", "team", "gb_fb", "ld_pc", "gb_pc", "fyb_pc", "iffb_pc", "hr_fb", "hardhit_pc", "barrel_pc", "ev", "la"],
    "fangraphs_pitching_pitch_type_summary": ["idfg", "season", "name", "team", "fb_pc", "fbv", "sl_pc", "slv", "ct_pc", "ctv", "cb_pc", "cbv", "ch_pc", "chv", "sf_pc", "sfv", "kn_pc", "knv", "xx_pc", "po_pc", "wfb", "wsl", "wct", "wcb", "wch", "wsf", "wkn"],
}

def split_dataframe(df: pd.DataFrame, mapping: dict) -> dict:
    out = {}
    for table, cols in mapping.items():
        valid = [c for c in cols if c in df.columns]
        if len(valid) >= 4:
            out[table] = df[valid].copy()
    return out

batting_dfs = split_dataframe(df_bat, batting_splits)
pitching_dfs = split_dataframe(df_pitch, pitching_splits)

# ---------------- Key selection & index checks ----------------
SEASON_ONLY: set[str] = set()

def conflict_cols_for(table_name: str, df_cols: List[str]) -> List[str]:
    if table_name in SEASON_ONLY:
        return ["idfg", "season"]
    return ["idfg", "season", "team"] if "team" in df_cols else ["idfg", "season"]

def existing_unique_indexes(conn, table_name: str) -> List[List[str]]:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT array_agg(a.attname ORDER BY k.ord) AS cols
            FROM pg_index ix
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_class t ON t.oid = ix.indrelid
            JOIN unnest(ix.indkey) WITH ORDINALITY AS k(attnum, ord) ON TRUE
            JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = k.attnum
            WHERE t.relname = %s
              AND ix.indisunique
            GROUP BY i.relname
        """, (table_name,))
        rows = cur.fetchall()
    return [list(cols) for (cols,) in rows] if rows else []

def has_matching_unique_index(conn, table_name: str, conflict_cols: List[str]) -> bool:
    return conflict_cols in existing_unique_indexes(conn, table_name)

def ensure_unique_index(conn, table_name: str, conflict_cols: list[str]) -> None:
    if has_matching_unique_index(conn, table_name, conflict_cols):
        return
    index_name = f"uix_{'_'.join(conflict_cols)}_{table_name}"[:63]
    dsn = conn.dsn
    import psycopg2 as _pg2
    ac_conn = _pg2.connect(dsn)
    try:
        ac_conn.autocommit = True
        with ac_conn.cursor() as cur:
            cur.execute(
                f'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "{index_name}" '
                f'ON "{table_name}" ({", ".join(conflict_cols)})'
            )
        print(f"  ✅ Created unique index {index_name}")
    finally:
        ac_conn.close()

def count_duplicates(conn, table_name: str, key_cols: List[str]) -> int:
    key_expr = ", ".join(key_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(SUM(cnt - 1), 0)
            FROM (
                SELECT COUNT(*) AS cnt
                FROM "{table_name}"
                GROUP BY {key_expr}
                HAVING COUNT(*) > 1
            ) sub
        """)
        return cur.fetchone()[0]

def dedupe_table(conn, table_name: str, key_cols: List[str]) -> int:
    key_expr = ", ".join(key_cols)
    with conn.cursor() as cur:
        cur.execute(f"""
            DELETE FROM "{table_name}"
            WHERE ctid NOT IN (
                SELECT MIN(ctid)
                FROM "{table_name}"
                GROUP BY {key_expr}
            )
        """)
        deleted = cur.rowcount
    conn.commit()
    return deleted

def chunk(it: Iterable, size: int):
    buf = []
    for item in it:
        buf.append(item)
        if len(buf) == size:
            yield buf
            buf = []
    if buf:
        yield buf

def diff_dataframe(df: pd.DataFrame, table_name: str, conn, key_cols: List[str], batch_size: int = 500) -> pd.DataFrame:
    all_cols = list(df.columns)
    key_expr = ", ".join([f'"{c}"' for c in key_cols])
    key_values = [tuple(row) for row in df[key_cols].itertuples(index=False, name=None)]

    existing_rows = []
    for batch in chunk(key_values, batch_size):
        placeholders = ", ".join([f"({', '.join(['%s'] * len(key_cols))})" for _ in batch])
        flat = [v for row in batch for v in row]
        with conn.cursor() as cur:
            cur.execute(f'SELECT {", ".join([f"{chr(34)}{c}{chr(34)}" for c in all_cols])} FROM "{table_name}" WHERE ({key_expr}) IN ({placeholders})', flat)
            existing_rows.extend(cur.fetchall())

    if not existing_rows:
        return df

    existing_df = pd.DataFrame(existing_rows, columns=all_cols)
    merged = df.merge(existing_df, on=key_cols, suffixes=("_new", "_old"), how="left")
    mask = pd.Series([False] * len(df), index=df.index)
    for col in all_cols:
        if col in key_cols:
            continue
        new_col, old_col = f"{col}_new", f"{col}_old"
        if new_col in merged.columns and old_col in merged.columns:
            mask |= merged[new_col].fillna("__NA__").astype(str) != merged[old_col].fillna("__NA__").astype(str)
        else:
            mask |= True
    new_keys = ~df[key_cols].apply(tuple, axis=1).isin(existing_df[key_cols].apply(tuple, axis=1))
    return df[mask | new_keys]

def upsert_table(df: pd.DataFrame, table_name: str, conn, batch_size: int = 500) -> None:
    if df.empty:
        print(f"⏭️  Skipping `{table_name}` — no data.")
        return

    changed = diff_dataframe(df, table_name, conn, conflict_cols_for(table_name, list(df.columns)))
    if changed.empty:
        print(f"✅ `{table_name}` is already up-to-date — nothing to upsert.")
        return

    all_cols = list(df.columns)
    key_cols = conflict_cols_for(table_name, all_cols)
    non_key_cols = [c for c in all_cols if c not in key_cols]

    missing = [c for c in key_cols if c not in all_cols]
    if missing:
        raise ValueError(f"`{table_name}` upsert missing key columns in payload: {missing}")

    dupes = count_duplicates(conn, table_name, key_cols)
    if dupes:
        msg = f"♻️ Found {dupes} duplicate row(s) by key {tuple(key_cols)} in `{table_name}`."
        if AUTO_DEDUPE:
            print(msg + " Auto-deduping…")
            deleted = dedupe_table(conn, table_name, key_cols)
            print(f"🗑️  Deleted {deleted} duplicate row(s) from `{table_name}`.")
        else:
            raise RuntimeError(msg + " Set AUTO_DEDUPE=true to allow automatic cleanup.")

    ensure_unique_index(conn, table_name, key_cols)

    col_list = ", ".join([f'"{c}"' for c in all_cols])
    set_clause = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_key_cols])
    where_clause = " OR ".join([f'"{table_name}"."{c}" IS DISTINCT FROM EXCLUDED."{c}"' for c in non_key_cols])

    sql = f"""
        INSERT INTO "{table_name}" ({col_list})
        VALUES %s
        ON CONFLICT ({", ".join(key_cols)}) DO UPDATE
        SET {set_clause}
        WHERE {where_clause}
    """

    values_iter = (tuple(row) for row in changed.itertuples(index=False, name=None))
    cur = conn.cursor()
    total = 0

    try:
        print(f"🚀 Running UPSERT on `{table_name}` with {len(changed)} changed row(s) (batch={batch_size})...")
        for batch in chunk(values_iter, batch_size):
            execute_values(cur, sql, batch)
            total += len(batch)
        conn.commit()
        print(f"✅ Successfully upserted {total} row(s) into `{table_name}`")
    except Exception as e:
        conn.rollback()
        print(f"❌ Failed to update `{table_name}`: {e}")
        raise
    finally:
        cur.close()


# ---------------- Main ----------------
if __name__ == "__main__":
    print("🔌 Connecting to AWS RDS...")
    with psycopg2.connect(**DB_PARAMS) as conn:
        processed_tables: Dict[str, pd.DataFrame] = {**batting_dfs, **pitching_dfs}
        print(f"🧩 Found {len(processed_tables)} FanGraphs tables to process")

        for table_name, df in processed_tables.items():
            print(f"📁 Processing `{table_name}` with {len(df)} rows")
            upsert_table(df, table_name, conn)

    print("✅ All tables processed and connection closed.")