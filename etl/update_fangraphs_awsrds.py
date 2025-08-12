# etl/update_fangraphs_awsrds.py
# Script that updates the AWS RDS database, runs mornings at 7 AM CST via GitHub runner

from __future__ import annotations

# Imports
import os
import re
from datetime import date
from pathlib import Path
from typing import Iterable, List, Tuple, Dict

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pybaseball import batting_stats, pitching_stats
from dotenv import load_dotenv

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
try:
    df_bat = batting_stats(YEAR)
    df_bat["Season"] = YEAR
    df_bat.rename(columns=batting_rename_map, inplace=True)
    df_bat.replace({'\$': ''}, regex=True, inplace=True)
except Exception as e:
    print(f"⚠️ Skipped batting {YEAR}: {e}")
    df_bat = pd.DataFrame()

try:
    df_pitch = pitching_stats(YEAR)
    df_pitch["Season"] = YEAR
    df_pitch.rename(columns=pitching_rename_map, inplace=True)
    df_pitch.replace({'\$': ''}, regex=True, inplace=True)
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
            return float(re.sub(r'^\((.*)\)$', r'-\1', x))
        return x
    return df.apply(lambda col: col.map(conv))

# Apply cleaning
df_pitch = resolve_fb_conflict(df_pitch)
df_bat = normalize_negatives(convert_numeric(clean_columns(df_bat)))
df_pitch = normalize_negatives(convert_numeric(clean_columns(df_pitch)))

# Fill NA
if not df_bat.empty:
    df_bat = df_bat.fillna(0.0)
if not df_pitch.empty:
    df_pitch = df_pitch.fillna(0.0)

# ---------------- Schema mappings ----------------
batting_splits: Dict[str, List[str]] = {
    "fangraphs_batting_lahman_like": [
        "idfg","season","name","team","g","ab","pa","h","singles","doubles","triples","hr",
        "r","rbi","bb","ibb","so","hbp","sf","sh","sb","cs"
    ],
    "fangraphs_batting_standard_ratios": [
        "idfg","season","name","team","avg","obp","slg","ops","iso","babip","bb_pc","k_pc","bb_k","gdp"
    ],
    "fangraphs_batting_advanced": [
        "idfg","season","name","team","woba","wraa","wrc","wrc_plus","war","rar","bat","fld","rep","pos","off","def","dol"
    ],
    "fangraphs_plate_discipline": [
        "idfg","season","name","team","o_swing_pc","z_swing_pc","swing_pc","o_contact_pc","z_contact_pc","contact_pc",
        "zone_pc","f_strike_pc","swstr_pc","cstr_pc","csw_pc","wpa","clutch","re24","rew","pli","phli","ph"
    ],
    "fangraphs_batted_ball": [
        "idfg","season","name","team","gb","fb","ld","iffb","gb_fb","ld_pc","gb_pc","fb_pc","iffb_pc","hr_fb",
        "ifh","ifh_pc","bu","buh","buh_pc","pull_pc","cent_pc","oppo_pc","soft_pc","med_pc","hard_pc","hardhit",
        "hardhit_pc","ev","la","barrels","barrel_pc","maxev","tto_pc"
    ],
    "fangraphs_baserunning_fielding": [
        "idfg","season","name","team","bsr","spd","wsb","ubr","wgdp"
    ],
    "fangraphs_batter_pitch_type_summary": [
        "idfg","season","name","team","fb_pc","fbv","sl_pc","slv","ch_pc","chv","cb_pc","cbv","sf_pc","sfv","ct_pc","ctv",
        "kn_pc","knv","xx_pc","po_pc","wfb","wsl","wch","wcb","wsf","wct","wkn","wfb_c","wsl_c","wch_c","wcb_c",
        "wsf_c","wct_c","wkn_c"
    ],
}

pitching_splits: Dict[str, List[str]] = {
    "fangraphs_pitching_lahman_like": [
        "idfg","season","name","team","w","l","g","gs","cg","sho","sv","ip","h","r","er","hr","bb","so","hbp","wp","bk","tbf"
    ],
    "fangraphs_pitching_standard_ratios": [
        "idfg","season","name","team","era","k_9","bb_9","k_bb","h_9","hr_9","avg","whip","babip","lob_pc"
    ],
    "fangraphs_pitching_advanced": [
        "idfg","season","name","team","war","fip","xfip","siera","era_minus","fip_minus","xfip_minus","rar","dollars","ra9_war"
    ],
    "fangraphs_pitching_plate_discipline": [
        "idfg","season","name","team","o_swing_pc","z_swing_pc","swing_pc","o_contact_pc","z_contact_pc",
        "contact_pc","zone_pc","f_strike_pc","swstr_pc","cstr_pc","csw_pc"
    ],
    "fangraphs_pitching_batted_ball": [
        "idfg","season","name","team","gb_fb","ld_pc","gb_pc","fyb_pc","iffb_pc","hr_fb","pull_pc","cent_pc","oppo_pc",
        "soft_pc","med_pc","hard_pc","ev","la","barrels","barrel_pc","maxev","hardhit","hardhit_pc","tto_pc"
    ],
    "fangraphs_pitching_pitch_type_summary": [
        "idfg","season","name","team","fb_pc","fbv","sl_pc","slv","ct_pc","ctv","cb_pc","cbv","ch_pc","chv","sf_pc","sfv",
        "kn_pc","knv","xx_pc","po_pc","wfb","wsl","wct","wcb","wch","wsf","wkn","wfb_c","wsl_c","wct_c","wcb_c","wch_c",
        "wsf_c","wkn_c"
    ],
}

# ---------------- Split helper ----------------
def split_dataframe(df: pd.DataFrame, mapping: Dict[str, List[str]]) -> Dict[str, pd.DataFrame]:
    out: Dict[str, pd.DataFrame] = {}
    if df.empty:
        return out
    for table, cols in mapping.items():
        valid = [c for c in cols if c in df.columns]
        if len(valid) >= 4:  # basic sanity
            out[table] = df[valid].copy()
    return out

batting_dfs = split_dataframe(df_bat, batting_splits)
pitching_dfs = split_dataframe(df_pitch, pitching_splits)

# ---------------- Key selection & index checks ----------------
SEASON_ONLY: set[str] = set()  # Add tables here if they have no per-team rows

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
    # Require exact column list match (order matters for arbiter)
    return conflict_cols in existing_unique_indexes(conn, table_name)

def ensure_unique_index(conn, table_name: str, conflict_cols: list[str]) -> None:
    """
    Ensure a UNIQUE index exists for the given conflict columns.
    Use a separate autocommit connection for CREATE INDEX CONCURRENTLY
    to avoid 'set_session cannot be used inside a transaction'.
    """
    if has_matching_unique_index(conn, table_name, conflict_cols):
        return

    idx_name = f"ux_{table_name}_{'_'.join(conflict_cols)}"
    cols_sql = ", ".join(conflict_cols)
    stmt = f'CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON "{table_name}" ({cols_sql});'

    # Use a brand-new connection in autocommit mode
    admin_conn = None
    try:
        admin_conn = psycopg2.connect(**DB_PARAMS)
        admin_conn.autocommit = True
        with admin_conn.cursor() as cur:
            print(f"🧱 Creating UNIQUE index on `{table_name}` for ({cols_sql}) ...")
            cur.execute(stmt)
        print(f"✅ Ensured UNIQUE index `{idx_name}` on `{table_name}`")
    finally:
        if admin_conn:
            admin_conn.close()

def count_duplicates(conn, table_name: str, key_cols: List[str]) -> int:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT COALESCE(SUM(cnt) ,0) FROM (
              SELECT COUNT(*) - 1 AS cnt
              FROM "{table_name}"
              GROUP BY {", ".join(key_cols)}
              HAVING COUNT(*) > 1
            ) s;
        """)
        (dupes,) = cur.fetchone()
    return int(dupes or 0)

def dedupe_table(conn, table_name: str, key_cols: List[str]) -> int:
    """
    Delete perfect duplicates keeping the first physical row (arbitrary) per key.
    Returns number of rows deleted.
    """
    with conn.cursor() as cur:
        cur.execute(f"""
            WITH ranked AS (
              SELECT ctid, ROW_NUMBER() OVER (
                  PARTITION BY {", ".join(key_cols)} ORDER BY ctid
              ) AS rn
              FROM "{table_name}"
            )
            DELETE FROM "{table_name}" t
            USING ranked r
            WHERE t.ctid = r.ctid
              AND r.rn > 1;
        """)
        deleted = cur.rowcount or 0
    if deleted:
        conn.commit()
    return deleted

# ---------------- Change detection ----------------
def get_changed_rows(df: pd.DataFrame, table_name: str, conn) -> pd.DataFrame:
    if df.empty:
        return df
    key_cols = conflict_cols_for(table_name, df.columns.tolist())
    non_key_cols = [c for c in df.columns if c not in key_cols]

    with conn.cursor() as cur:
        keys = df[key_cols].drop_duplicates()
        keys_tuple = [tuple((x.item() if hasattr(x, "item") else x) for x in row) for row in keys.to_numpy()]
        if not keys_tuple:
            return pd.DataFrame()

        placeholders = ", ".join(["%s"] * len(key_cols))
        select_sql = f"""
            SELECT {", ".join(f'"{c}"' for c in df.columns)}
            FROM "{table_name}"
            WHERE ({", ".join(key_cols)}) IN %s
        """
        try:
            cur.execute(select_sql, (tuple(keys_tuple),))
            db_rows = cur.fetchall()
        except Exception as e:
            print(f"⚠️ Could not check changes for `{table_name}`: {e}")
            return df  # be conservative: treat all as changed

    db_df = pd.DataFrame(db_rows, columns=list(df.columns))
    if db_df.empty:
        return df  # everything is new

    db_df = db_df.sort_values(by=key_cols).reset_index(drop=True)
    df_sorted = df.sort_values(by=key_cols).reset_index(drop=True)
    changed_mask = ~df_sorted[non_key_cols].eq(db_df[non_key_cols]).all(axis=1)
    return df_sorted.loc[changed_mask]

# ---------------- UPSERT ----------------
def chunk(iterable: Iterable[Tuple], size: int) -> Iterable[List[Tuple]]:
    batch: List[Tuple] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch

def upsert_table(df: pd.DataFrame, table_name: str, conn, batch_size: int = 5000) -> None:
    if df.empty:
        print(f"⚠️ Skipping empty table: {table_name}")
        return

    # Detect changed rows only
    changed = get_changed_rows(df, table_name, conn)
    if changed.empty:
        print(f"✅ No updates needed for `{table_name}`")
        return

    print(f"🔍 {len(changed)} rows in `{table_name}` will be updated.")
    preview_cols = [c for c in ["idfg","season","name","team"] if c in changed.columns]
    if preview_cols:
        print(changed[preview_cols].head(5))

    all_cols = list(df.columns)
    key_cols = conflict_cols_for(table_name, all_cols)
    non_key_cols = [c for c in all_cols if c not in key_cols]

    # Sanity: ensure key columns exist
    missing = [c for c in key_cols if c not in all_cols]
    if missing:
        raise ValueError(f"`{table_name}` upsert missing key columns in payload: {missing}")

    # Preflight: dedupe + ensure unique index
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
