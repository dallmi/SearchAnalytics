#!/usr/bin/env python3
"""
Transform raw App Insights xlsx exports into the flattened format expected by
process_search_analytics.py.

Input format (new):
    timestamp | name | customDimensions | client_OS | client_Browser | session_id | user_id

    customDimensions is a JSON string with up to 4 levels of nesting:
      Level 1: {"CustomProps": {...}, ...}
      Level 2: CustomProps contains flat values (deviceType) and nested objects
               (searchQuery, userDetails, searchResultsInteraction, searchPerformance)
      Level 3: Sub-object fields (queryText, department, clickedTAB, searchLatency, ...)
      Level 4: Deep nesting (totalResultCount.totalResultCount, clickedResult.resultTitle, ...)

Output format:
    timestamp | name | user_id | session_id | client_OS | client_Browser | CP_* columns
    (same structure as the old KQL-flattened exports)

Usage:
    python scripts/transform_input.py                           # Auto-detect latest file in input/
    python scripts/transform_input.py input/export_2025_03_10.xlsx  # Process specific file
    python scripts/transform_input.py --all                     # Process all files in input/
"""

import sys
import os
import re
import json
import glob
from pathlib import Path
from datetime import datetime

import pandas as pd


SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
INPUT_DIR = PROJECT_DIR / "input" / "raw"
OUTPUT_DIR = PROJECT_DIR / "input"


def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")


def extract_date_from_filename(filepath):
    filename = Path(filepath).stem
    # Match _YYYY-MM-DD (new format) or _YYYY_MM_DD (old format)
    match = re.search(r'_(\d{4})[-_](\d{2})[-_](\d{2})$', filename)
    if match:
        try:
            year, month, day = int(match.group(1)), int(match.group(2)), int(match.group(3))
            return datetime(year, month, day).date()
        except ValueError:
            return None
    return None


def find_latest_input_file(input_dir):
    patterns = ['*.xlsx', '*.xls']
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))

    if not all_files:
        return None

    files_with_dates = []
    for f in all_files:
        file_date = extract_date_from_filename(f)
        if file_date:
            files_with_dates.append((Path(f), file_date))

    if not files_with_dates:
        log("  Warning: No files with _YYYY_MM_DD suffix found, using modification time")
        all_files.sort(key=os.path.getmtime, reverse=True)
        return Path(all_files[0])

    files_with_dates.sort(key=lambda x: x[1], reverse=True)
    return files_with_dates[0][0]


def find_all_input_files(input_dir):
    patterns = ['*.xlsx', '*.xls']
    all_files = []
    for pattern in patterns:
        all_files.extend(glob.glob(str(input_dir / pattern)))
    return sorted([Path(f) for f in all_files])


def flatten_custom_dimensions(cd_string):
    """
    Flatten a customDimensions JSON string into a dict with CP_ prefixed keys.

    Replicates the 4-level flattening logic from search_analytics_query.kql:
      - Level 2 flat values:  CP_key
      - Level 3 nested:       CP_key_subKey
      - Level 4 nested:       CP_key_subKey_subSubKey
    """
    if pd.isna(cd_string) or not cd_string:
        return {}

    try:
        cd = json.loads(cd_string)
    except (json.JSONDecodeError, TypeError):
        return {}

    result = {}

    # Extract CustomProps; also keep any top-level keys outside CustomProps
    custom_props = cd.get("CustomProps", {})
    if isinstance(custom_props, str):
        try:
            custom_props = json.loads(custom_props)
        except (json.JSONDecodeError, TypeError):
            custom_props = {}

    # Level 2: iterate CustomProps keys
    for key, value in custom_props.items():
        if isinstance(value, dict):
            # Level 3: nested object
            for sub_key, sub_value in value.items():
                if isinstance(sub_value, dict):
                    # Level 4: deeply nested object
                    for sub_sub_key, sub_sub_value in sub_value.items():
                        col_name = f"CP_{key}_{sub_key}_{sub_sub_key}"
                        result[col_name] = _to_string(sub_sub_value)
                else:
                    col_name = f"CP_{key}_{sub_key}"
                    result[col_name] = _to_string(sub_value)
        else:
            # Level 2 flat value
            col_name = f"CP_{key}"
            result[col_name] = _to_string(value)

    # Top-level keys outside CustomProps (rare, but handle them)
    for key, value in cd.items():
        if key == "CustomProps":
            continue
        result[key] = _to_string(value)

    return result


def _to_string(value):
    """Convert a value to string representation, keeping None as None."""
    if value is None:
        return None
    if isinstance(value, bool):
        return str(value).lower()
    return str(value)


def transform_file(input_path):
    """Transform a single input file and write the flattened output."""
    log(f"Reading {input_path.name}...")

    # Read with timestamp as string to preserve precision
    df_cols = pd.read_excel(input_path, nrows=0)
    all_cols = df_cols.columns.tolist()
    timestamp_cols = [col for col in all_cols if 'timestamp' in col.lower()]
    dtype_dict = {col: str for col in timestamp_cols} if timestamp_cols else {}
    df = pd.read_excel(input_path, dtype=dtype_dict if dtype_dict else None)

    log(f"  {len(df):,} rows, columns: {list(df.columns)}")

    # Check that customDimensions column exists
    cd_col = None
    for candidate in ['customDimensions', 'CustomDimensions', 'custom_dimensions']:
        if candidate in df.columns:
            cd_col = candidate
            break

    if cd_col is None:
        log(f"  ERROR: No customDimensions column found. Available: {list(df.columns)}")
        return None

    # Flatten customDimensions for each row
    log("  Flattening customDimensions (4-level JSON)...")
    flattened_rows = df[cd_col].apply(flatten_custom_dimensions)
    df_flat = pd.DataFrame(flattened_rows.tolist())

    log(f"  Extracted {len(df_flat.columns)} CP_ columns")

    # Build output dataframe: base columns + flattened CP_ columns
    # Normalize column names to match what process_search_analytics.py expects
    rename_map = {
        'user_Id': 'user_id',
        'session_Id': 'session_id',
        'timestamp [UTC]': 'timestamp',
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Select base columns (drop customDimensions)
    base_cols = [c for c in df.columns if c != cd_col]
    df_out = pd.concat([df[base_cols].reset_index(drop=True), df_flat.reset_index(drop=True)], axis=1)

    # Write output
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / input_path.name
    df_out.to_excel(output_path, index=False)

    log(f"  Written {len(df_out):,} rows x {len(df_out.columns)} columns to {output_path}")
    return output_path


def main():
    args = sys.argv[1:]

    if '--all' in args:
        files = find_all_input_files(INPUT_DIR)
        if not files:
            log(f"No xlsx files found in {INPUT_DIR}")
            sys.exit(1)
        log(f"Processing all {len(files)} file(s) in {INPUT_DIR}")
        for f in files:
            transform_file(f)
    elif args and not args[0].startswith('--'):
        input_path = Path(args[0])
        if not input_path.is_absolute():
            input_path = PROJECT_DIR / input_path
        if not input_path.exists():
            log(f"File not found: {input_path}")
            sys.exit(1)
        transform_file(input_path)
    else:
        input_path = find_latest_input_file(INPUT_DIR)
        if not input_path:
            log(f"No input files found in {INPUT_DIR}")
            sys.exit(1)
        log(f"Auto-detected latest file: {input_path.name}")
        transform_file(input_path)

    log("Done.")


if __name__ == "__main__":
    main()
