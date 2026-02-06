#!/usr/bin/env python3
"""
DuckDB WAL Recovery Script

Recovers data from a DuckDB write-ahead log (WAL) file by opening the database
and forcing a checkpoint. This flushes all WAL transactions to the main database file.

Usage:
    python recover_duckdb_wal.py <path_to_database.duckdb>

Example:
    python recover_duckdb_wal.py P:/IMPORTANT/Projects/brightcove_ori/UnifiedPipeline/analytics.duckdb
"""

import sys
import shutil
from pathlib import Path
from datetime import datetime


def recover_wal(db_path: str) -> bool:
    """
    Recover WAL file by opening database and checkpointing.

    Args:
        db_path: Path to the .duckdb file (WAL file must be in same directory)

    Returns:
        True if recovery succeeded, False otherwise
    """
    # Import here to give better error message if not installed
    try:
        import duckdb
    except ImportError:
        print("ERROR: duckdb not installed. Run: pip install duckdb")
        return False

    db_path = Path(db_path)
    wal_path = Path(str(db_path) + ".wal")

    # Validate paths
    if not db_path.exists():
        print(f"ERROR: Database file not found: {db_path}")
        return False

    if not wal_path.exists():
        print(f"WARNING: No WAL file found at: {wal_path}")
        print("Nothing to recover - database may already be consistent.")
        return True

    # Show file info
    db_size = db_path.stat().st_size / (1024 * 1024)
    wal_size = wal_path.stat().st_size / (1024 * 1024)
    db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
    wal_mtime = datetime.fromtimestamp(wal_path.stat().st_mtime)

    print("=" * 60)
    print("  DuckDB WAL Recovery")
    print("=" * 60)
    print(f"  Database: {db_path}")
    print(f"    Size: {db_size:.2f} MB")
    print(f"    Modified: {db_mtime}")
    print(f"  WAL File: {wal_path}")
    print(f"    Size: {wal_size:.2f} MB")
    print(f"    Modified: {wal_mtime}")
    print("=" * 60)

    # Create backup
    backup_dir = db_path.parent / "backup_before_recovery"
    backup_dir.mkdir(exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    db_backup = backup_dir / f"{db_path.name}.{timestamp}.bak"
    wal_backup = backup_dir / f"{wal_path.name}.{timestamp}.bak"

    print(f"\nCreating backups in: {backup_dir}")
    shutil.copy2(db_path, db_backup)
    print(f"  Backed up: {db_path.name}")
    shutil.copy2(wal_path, wal_backup)
    print(f"  Backed up: {wal_path.name}")

    # Attempt recovery
    print("\nOpening database (this triggers WAL replay)...")
    try:
        con = duckdb.connect(str(db_path), read_only=False)
        print("  Database opened successfully.")
    except Exception as e:
        print(f"ERROR opening database: {e}")
        print("\nTrying recovery mode...")
        try:
            con = duckdb.connect(str(db_path), config={'recovery_mode': 'recover'})
            print("  Database opened in recovery mode.")
        except Exception as e2:
            print(f"ERROR in recovery mode: {e2}")
            return False

    # Show tables and row counts
    print("\nTables in database:")
    try:
        tables = con.execute("SHOW TABLES").fetchall()
        for (table_name,) in tables:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name}: {count:,} rows")
    except Exception as e:
        print(f"  (Could not list tables: {e})")

    # Force checkpoint
    print("\nRunning CHECKPOINT to flush WAL to main database...")
    try:
        con.execute("CHECKPOINT")
        print("  CHECKPOINT completed successfully.")
    except Exception as e:
        print(f"ERROR during CHECKPOINT: {e}")
        con.close()
        return False

    # Close connection
    con.close()
    print("  Connection closed.")

    # Verify WAL is gone or reduced
    if wal_path.exists():
        new_wal_size = wal_path.stat().st_size / (1024 * 1024)
        if new_wal_size < wal_size:
            print(f"\nWAL file reduced: {wal_size:.2f} MB -> {new_wal_size:.2f} MB")
        else:
            print(f"\nWAL file still exists ({new_wal_size:.2f} MB) - this is normal")
    else:
        print("\nWAL file removed after checkpoint (fully flushed).")

    # Show new database size
    new_db_size = db_path.stat().st_size / (1024 * 1024)
    new_db_mtime = datetime.fromtimestamp(db_path.stat().st_mtime)
    print(f"\nDatabase after recovery:")
    print(f"  Size: {new_db_size:.2f} MB (was {db_size:.2f} MB)")
    print(f"  Modified: {new_db_mtime}")

    print("\n" + "=" * 60)
    print("  RECOVERY COMPLETE")
    print("=" * 60)
    print(f"\nBackups saved in: {backup_dir}")
    print("You can delete the backups once you verify everything is correct.")

    return True


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        print("\nERROR: Please provide the path to the .duckdb file")
        print("\nExample:")
        print("  python recover_duckdb_wal.py /path/to/analytics.duckdb")
        sys.exit(1)

    db_path = sys.argv[1]
    success = recover_wal(db_path)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
