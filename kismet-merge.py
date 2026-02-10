#!/usr/bin/env python3
"""
kismet-merge - Merge multiple Kismet .kismet database files into one

Usage:
    kismet-merge file1.kismet file2.kismet -o merged.kismet
    kismet-merge *.kismet -o combined.kismet

Merges all tables (devices, packets, data, alerts, etc.) and deduplicates.
"""

import argparse
import sqlite3
import json
import glob
import sys
from pathlib import Path
from datetime import datetime


def get_table_schema(cursor, table_name):
    """Get CREATE TABLE statement for a table."""
    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
    result = cursor.fetchone()
    return result[0] if result else None


def get_table_columns(cursor, table_name):
    """Get column names for a table."""
    cursor.execute(f"PRAGMA table_info({table_name})")
    return [col[1] for col in cursor.fetchall()]


def merge_kismet_files(input_files, output_file, verbose=False):
    """Merge multiple .kismet files into one, including all tables."""
    
    # Track all data by table
    all_tables = {}
    table_schemas = {}
    
    # Track unique packets by hash to avoid duplicates
    seen_packets = set()
    seen_devices = set()
    
    files_processed = 0
    stats = {}
    
    for input_file in input_files:
        if verbose:
            print(f"Reading: {input_file}")
        
        try:
            conn = sqlite3.connect(input_file)
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                # Skip internal sqlite tables
                if table.startswith('sqlite_'):
                    continue
                
                # Get schema if we don't have it
                if table not in table_schemas:
                    schema = get_table_schema(cursor, table)
                    if schema:
                        table_schemas[table] = schema
                        all_tables[table] = []
                
                # Get columns
                columns = get_table_columns(cursor, table)
                if not columns:
                    continue
                
                # Read all rows
                cursor.execute(f"SELECT * FROM {table}")
                rows = cursor.fetchall()
                
                table_count = 0
                for row in rows:
                    row_dict = dict(zip(columns, row))
                    
                    # Deduplication logic based on table type
                    if table == 'packets':
                        # Create unique key from timestamp + macs + location
                        key = (
                            row_dict.get('ts_sec', 0),
                            row_dict.get('ts_usec', 0),
                            row_dict.get('sourcemac', ''),
                            row_dict.get('destmac', ''),
                            row_dict.get('lat', 0),
                            row_dict.get('lon', 0)
                        )
                        if key in seen_packets:
                            continue
                        seen_packets.add(key)
                    
                    elif table == 'devices':
                        # Dedupe by device MAC
                        devmac = row_dict.get('devmac', '')
                        if devmac in seen_devices:
                            # Merge with existing - update if newer
                            for i, existing in enumerate(all_tables[table]):
                                if existing.get('devmac') == devmac:
                                    # Keep the one with more data or newer timestamp
                                    if row_dict.get('last_time', 0) > existing.get('last_time', 0):
                                        all_tables[table][i] = row_dict
                                    # Merge packet counts if we have the device blob
                                    break
                            continue
                        seen_devices.add(devmac)
                    
                    elif table == 'KISMET':
                        # Only keep one metadata row
                        if all_tables.get(table):
                            continue
                    
                    all_tables[table].append(row_dict)
                    table_count += 1
                
                if table not in stats:
                    stats[table] = 0
                stats[table] += table_count
                
                if verbose and table_count > 0:
                    print(f"  {table}: {table_count} rows")
            
            conn.close()
            files_processed += 1
            
        except sqlite3.Error as e:
            print(f"Error reading {input_file}: {e}", file=sys.stderr)
            continue
    
    if not all_tables:
        print("Error: No data found in input files", file=sys.stderr)
        return False
    
    if verbose:
        print(f"\nMerging data from {files_processed} files...")
    
    # Create output database
    if Path(output_file).exists():
        Path(output_file).unlink()
    
    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()
    
    # Create tables and insert data
    for table, rows in all_tables.items():
        if not rows:
            continue
        
        # Create table using captured schema or build one
        if table in table_schemas and table_schemas[table]:
            try:
                cursor.execute(table_schemas[table])
            except sqlite3.Error:
                # Table might already exist or schema issue
                pass
        else:
            # Build CREATE TABLE from columns
            columns = list(rows[0].keys())
            col_defs = ', '.join([f'"{col}" BLOB' for col in columns])
            cursor.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
        
        # Insert rows
        if rows:
            columns = list(rows[0].keys())
            placeholders = ', '.join(['?' for _ in columns])
            col_names = ', '.join([f'"{col}"' for col in columns])
            
            for row in rows:
                values = [row.get(col) for col in columns]
                try:
                    cursor.execute(f'INSERT INTO "{table}" ({col_names}) VALUES ({placeholders})', values)
                except sqlite3.Error as e:
                    if verbose:
                        print(f"  Warning: Could not insert row into {table}: {e}")
    
    # Create indexes for common queries
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS packets_ts ON packets (ts_sec)')
        cursor.execute('CREATE INDEX IF NOT EXISTS packets_location ON packets (lat, lon)')
        cursor.execute('CREATE INDEX IF NOT EXISTS devices_devmac ON devices (devmac)')
        cursor.execute('CREATE INDEX IF NOT EXISTS devices_last_time ON devices (last_time)')
    except sqlite3.Error:
        pass  # Indexes might already exist or table doesn't exist
    
    conn.commit()
    conn.close()
    
    # Print final stats
    print(f"\nCreated: {output_file}")
    print(f"  Merged from {files_processed} files:")
    for table, count in sorted(stats.items()):
        final_count = len(all_tables.get(table, []))
        if count != final_count:
            print(f"    {table}: {count} -> {final_count} (deduped)")
        else:
            print(f"    {table}: {final_count}")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Merge multiple Kismet .kismet database files into one',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Merges ALL tables from Kismet databases:
  - packets (for heatmaps/location data)
  - devices (device info)
  - data, alerts, snapshots, etc.

Duplicate packets (same timestamp/MAC/location) are removed.
Duplicate devices (same MAC) are merged, keeping newest data.

Examples:
  kismet-merge day1.kismet day2.kismet -o combined.kismet
  kismet-merge *.kismet -o all_captures.kismet
  kismet-merge site_*.kismet -o site_survey.kismet -v
        """
    )
    
    parser.add_argument('input', nargs='+', help='Input .kismet files to merge')
    parser.add_argument('-o', '--output', required=True, help='Output .kismet file')
    parser.add_argument('-v', '--verbose', action='store_true', help='Show detailed output')
    
    args = parser.parse_args()
    
    # Expand glob patterns
    input_files = []
    for pattern in args.input:
        expanded = glob.glob(pattern)
        if expanded:
            input_files.extend(expanded)
        elif Path(pattern).exists():
            input_files.append(pattern)
        else:
            print(f"Warning: '{pattern}' not found, skipping", file=sys.stderr)
    
    # Remove duplicates and exclude output file
    input_files = list(dict.fromkeys(input_files))
    input_files = [f for f in input_files if Path(f).resolve() != Path(args.output).resolve()]
    
    if len(input_files) < 1:
        print("Error: Need at least 1 input file", file=sys.stderr)
        sys.exit(1)
    
    if args.verbose:
        print(f"Merging {len(input_files)} files -> {args.output}\n")
    
    success = merge_kismet_files(input_files, args.output, verbose=args.verbose)
    
    if not success:
        sys.exit(1)


if __name__ == '__main__':
    main()
