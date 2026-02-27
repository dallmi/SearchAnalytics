#!/usr/bin/env python3
"""
HR History Processing Script

Reads GEDULD Excel files from mappings/ and consolidates them into a single
Parquet file for historical headcount analysis.

Usage:
    python scripts/process_hr_history.py              # Append only new GEDULD files
    python scripts/process_hr_history.py --force      # Full rebuild from all GEDULD files

Input folder: mappings/
    Place your monthly GEDULD files here with date suffix _YYYY_MM_DD, e.g.:
    - GEDULD_2024_01_15.xlsx
    - GEDULD_2024_02_15.xlsx

Output:
    - output/hr_history.parquet   (all monthly snapshots consolidated)

Default mode detects which snapshots are already in the parquet and only
processes new GEDULD files. Use --force to rebuild everything from scratch.
"""

import sys
import os
import re
import duckdb
import pandas as pd
from pathlib import Path
from datetime import datetime


def log(message):
    """Print timestamped log message"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    """
    Extract date from filename with format _YYYY_MM_DD.
    Returns a date object or None if not found.
    """
    match = re.search(r'_(\d{4})_(\d{2})_(\d{2})', str(filepath))
    if match:
        try:
            return datetime(int(match.group(1)), int(match.group(2)), int(match.group(3))).date()
        except ValueError:
            return None
    return None


def find_geduld_files(mappings_dir):
    """
    Find all GEDULD files in the mappings directory.
    Returns list of (filepath, date) tuples sorted by date (oldest first).
    """
    if not mappings_dir.exists():
        return []

    all_files = list(mappings_dir.glob('GEDULD*.xlsx')) + list(mappings_dir.glob('GEDULD*.xls'))

    if not all_files:
        return []

    files_with_dates = []
    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((f, file_date))
        else:
            log(f"  WARNING: Could not extract date from {f.name} - skipping")

    files_with_dates.sort(key=lambda x: x[1])
    return files_with_dates


def normalize_columns(df):
    """Normalize column names to snake_case for clean SQL."""
    df.columns = [
        col.strip().lower().replace(' ', '_').replace('-', '_').replace('(', '').replace(')', '')
        for col in df.columns
    ]
    return df


def read_geduld_file(filepath, file_date):
    """
    Read a single GEDULD Excel file and add snapshot columns.
    Forces GPN to string to preserve leading zeros.
    """
    # Try to read GPN as string
    try:
        df = pd.read_excel(filepath, dtype={'GPN': str})
    except Exception:
        # If GPN column doesn't exist or other issue, read without dtype constraint
        df = pd.read_excel(filepath)

    row_count = len(df)

    # Normalize column names
    df = normalize_columns(df)

    # Ensure gpn is string and drop non-data rows (e.g. filter info rows at end of file)
    if 'gpn' in df.columns:
        df['gpn'] = df['gpn'].astype(str).str.strip()
        before = len(df)
        df = df[~df['gpn'].isin(['', 'nan', 'None', 'NaN', 'null'])]
        dropped = before - len(df)
        if dropped > 0:
            log(f"    Dropped {dropped} non-data row(s) (empty/invalid GPN)")

    # Robustly convert date columns to proper datetime for parquet compatibility.
    # Excel dates may arrive as: datetime objects, serial numbers, or locale-
    # formatted strings (e.g. "28.02.2026", "28 Februar 2026", "2026-02-28").
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            continue  # already datetime
        sample = df[col].dropna().head(10)
        if len(sample) == 0:
            continue
        # Strategy 1: column is numeric and values look like Excel serial dates (30000-60000 range)
        if pd.api.types.is_numeric_dtype(df[col]):
            median = sample.median()
            if 25000 < median < 65000:
                df[col] = pd.to_timedelta(df[col], unit='D') + pd.Timestamp('1899-12-30')
                log(f"    Converted {col} from Excel serial number to datetime")
                continue
        # Strategy 2: string column — try pd.to_datetime with dayfirst=True
        if df[col].dtype == 'object':
            try:
                parsed = pd.to_datetime(sample.astype(str), dayfirst=True, format='mixed', errors='coerce')
                if parsed.notna().sum() >= len(sample) * 0.8:
                    df[col] = pd.to_datetime(df[col].astype(str), dayfirst=True, format='mixed', errors='coerce')
                    log(f"    Converted {col} to datetime (parsed from string)")
            except Exception:
                pass

    # Extract snapshot date from headcount_date column if available
    snapshot_year = None
    snapshot_month = None

    if 'headcount_date' in df.columns:
        valid_dates = df['headcount_date'].dropna()
        if len(valid_dates) > 0:
            first_date = valid_dates.iloc[0]
            try:
                if isinstance(first_date, (datetime, pd.Timestamp)):
                    snapshot_year = first_date.year
                    snapshot_month = first_date.month
                elif isinstance(first_date, str):
                    parsed = pd.to_datetime(first_date, dayfirst=True)
                    snapshot_year = parsed.year
                    snapshot_month = parsed.month
            except Exception:
                pass

    # Fallback to filename date
    if snapshot_year is None:
        snapshot_year = file_date.year
        snapshot_month = file_date.month
        log(f"    Using filename date as snapshot: {snapshot_year}-{snapshot_month:02d}")

    # Add snapshot columns
    df['snapshot_year'] = snapshot_year
    df['snapshot_month'] = snapshot_month

    return df, row_count, snapshot_year, snapshot_month


def get_existing_snapshots(output_file):
    """Read existing parquet and return set of (year, month) tuples already processed."""
    con = duckdb.connect(':memory:')
    try:
        result = con.execute(f"""
            SELECT DISTINCT snapshot_year, snapshot_month
            FROM '{output_file}'
        """).fetchall()
        return {(int(r[0]), int(r[1])) for r in result}
    finally:
        con.close()


def export_to_parquet(con, output_file):
    """Export hr_history table to parquet and log GPN type."""
    if 'gpn' in [r[0] for r in con.execute("DESCRIBE hr_history").fetchall()]:
        gpn_type = con.execute("SELECT typeof(gpn) FROM hr_history LIMIT 1").fetchone()[0]
        log(f"  GPN column type: {gpn_type}")
    con.execute(f"COPY hr_history TO '{output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY)")


def log_summary(output_file, files_processed, mode='full'):
    """Log summary statistics from the output parquet."""
    con = duckdb.connect(':memory:')
    try:
        stats = con.execute(f"""
            SELECT COUNT(*) as rows,
                   COUNT(DISTINCT gpn) as unique_gpns,
                   COUNT(DISTINCT (snapshot_year, snapshot_month)) as snapshots
            FROM '{output_file}'
        """).fetchone()
        cols = con.execute(f"SELECT * FROM '{output_file}' LIMIT 0").description

        file_size = output_file.stat().st_size / (1024 * 1024)

        log("")
        log("=" * 60)
        log(f"SUMMARY ({mode})")
        log("=" * 60)
        log(f"  Files processed:  {files_processed}")
        log(f"  Snapshots:        {stats[2]}")
        log(f"  Total rows:       {stats[0]:,}")
        log(f"  Unique GPNs:      {stats[1]:,}")
        log(f"  Columns:          {len(cols)}")
        log(f"  Output file:      {output_file}")
        log(f"  File size:        {file_size:.1f} MB")
        log("")
        log("Done!")
    finally:
        con.close()


def process_hr_history(force=False):
    """Main processing function."""
    project_dir = Path(os.path.dirname(os.path.abspath(__file__))).parent
    mappings_dir = project_dir / 'mappings'
    output_dir = project_dir / 'output'
    output_file = output_dir / 'hr_history.parquet'

    log("=" * 60)
    log("HR HISTORY PROCESSING")
    log("=" * 60)

    # Find GEDULD files
    geduld_files = find_geduld_files(mappings_dir)

    if not geduld_files:
        log("  No GEDULD files found in mappings/")
        log("  Place files like GEDULD_2024_01_15.xlsx in the mappings/ folder")
        return

    log(f"  Found {len(geduld_files)} GEDULD file(s)")
    log(f"  Date range: {geduld_files[0][1]} → {geduld_files[-1][1]}")

    # Determine mode: incremental append vs full rebuild
    incremental = output_file.exists() and not force

    if incremental:
        size_mb = output_file.stat().st_size / (1024 * 1024)
        existing_snapshots = get_existing_snapshots(output_file)
        log(f"  Existing parquet: {size_mb:.1f} MB with {len(existing_snapshots)} snapshot(s)")

        # Filter to only new files
        new_files = [
            (fp, fd) for fp, fd in geduld_files
            if (fd.year, fd.month) not in existing_snapshots
        ]

        if not new_files:
            log("")
            log("  All snapshots up to date — nothing to do.")
            log("  Use --force to rebuild from scratch.")
            return

        log(f"  New file(s) to process: {len(new_files)}")
        files_to_process = new_files
    else:
        if force and output_file.exists():
            log("  --force: rebuilding from all files")
        files_to_process = geduld_files

    log("")

    # Read files
    all_dfs = []
    new_snapshots = set()

    for filepath, file_date in files_to_process:
        log(f"  Reading {filepath.name}...")
        df, row_count, snap_year, snap_month = read_geduld_file(filepath, file_date)
        all_dfs.append(df)
        new_snapshots.add((snap_year, snap_month))
        log(f"    {row_count:,} rows → snapshot {snap_year}-{snap_month:02d}")

    log("")
    log(f"  Concatenating {len(all_dfs)} file(s)...")

    # Concatenate new DataFrames
    combined = pd.concat(all_dfs, ignore_index=True)
    log(f"  Combined: {len(combined):,} rows × {len(combined.columns)} columns")

    # Deduplicate new data: same GPN in same snapshot month → keep last
    if 'gpn' in combined.columns:
        before = len(combined)
        combined = combined.drop_duplicates(
            subset=['gpn', 'snapshot_year', 'snapshot_month'],
            keep='last'
        )
        dupes = before - len(combined)
        if dupes > 0:
            log(f"  Removed {dupes:,} duplicate rows (same GPN in same snapshot)")

    # Export to parquet
    log("")
    output_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(':memory:')

    if incremental:
        log("  Appending to existing parquet...")
        # Load existing data
        con.execute(f"CREATE TABLE hr_history AS SELECT * FROM '{output_file}'")
        existing_count = con.execute("SELECT COUNT(*) FROM hr_history").fetchone()[0]
        log(f"  Existing rows: {existing_count:,}")

        # Delete rows for snapshots being refreshed (handles re-uploaded files)
        for year, month in new_snapshots:
            con.execute(f"DELETE FROM hr_history WHERE snapshot_year = {year} AND snapshot_month = {month}")

        # Insert new data
        con.register('_new_df', combined)
        con.execute("INSERT INTO hr_history SELECT * FROM _new_df")
        con.unregister('_new_df')

        total = con.execute("SELECT COUNT(*) FROM hr_history").fetchone()[0]
        log(f"  Total rows after append: {total:,}")
    else:
        log("  Exporting to parquet...")
        con.register('_hr_df', combined)
        con.execute("CREATE TABLE hr_history AS SELECT * FROM _hr_df")
        con.unregister('_hr_df')

    export_to_parquet(con, output_file)
    con.close()

    # Summary
    mode = 'incremental' if incremental else 'full rebuild'
    log_summary(output_file, len(files_to_process), mode)


if __name__ == '__main__':
    force = '--force' in sys.argv
    process_hr_history(force=force)
