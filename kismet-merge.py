#!/usr/bin/env python3
"""
kismet-merge - Merge multiple Kismet .kismet database files into one

Usage:
    kismet-merge file1.kismet file2.kismet -o merged.kismet
    kismet-merge *.kismet -o combined.kismet

Duplicates (by MAC) are merged:
  - Packet counts summed
  - Timestamps use earliest/latest
  - Best signal values kept
"""

import argparse
import sqlite3
import json
import glob
import sys
import shutil
from pathlib import Path
from datetime import datetime


def merge_device_json(existing, new):
    """Merge two device JSON objects, combining their data."""
    
    # Sum packet counts
    existing['kismet.device.base.packets.total'] = (
        existing.get('kismet.device.base.packets.total', 0) +
        new.get('kismet.device.base.packets.total', 0)
    )
    existing['kismet.device.base.packets.data'] = (
        existing.get('kismet.device.base.packets.data', 0) +
        new.get('kismet.device.base.packets.data', 0)
    )
    existing['kismet.device.base.datasize'] = (
        existing.get('kismet.device.base.datasize', 0) +
        new.get('kismet.device.base.datasize', 0)
    )
    
    # Earliest first_time
    if new.get('kismet.device.base.first_time', 0) > 0:
        if existing.get('kismet.device.base.first_time', 0) == 0:
            existing['kismet.device.base.first_time'] = new['kismet.device.base.first_time']
        else:
            existing['kismet.device.base.first_time'] = min(
                existing['kismet.device.base.first_time'],
                new['kismet.device.base.first_time']
            )
    
    # Latest last_time
    existing['kismet.device.base.last_time'] = max(
        existing.get('kismet.device.base.last_time', 0),
        new.get('kismet.device.base.last_time', 0)
    )
    
    # Merge signal data - keep best values
    existing_signal = existing.get('kismet.device.base.signal', {})
    new_signal = new.get('kismet.device.base.signal', {})
    
    if new_signal:
        if not existing_signal:
            existing['kismet.device.base.signal'] = new_signal
        else:
            # Best (least negative) max signal
            new_max = new_signal.get('kismet.common.signal.max_signal')
            existing_max = existing_signal.get('kismet.common.signal.max_signal')
            if new_max is not None:
                if existing_max is None or new_max > existing_max:
                    existing_signal['kismet.common.signal.max_signal'] = new_max
            
            # Worst (most negative) min signal
            new_min = new_signal.get('kismet.common.signal.min_signal')
            existing_min = existing_signal.get('kismet.common.signal.min_signal')
            if new_min is not None:
                if existing_min is None or new_min < existing_min:
                    existing_signal['kismet.common.signal.min_signal'] = new_min
            
            # Use most recent last_signal
            if new.get('kismet.device.base.last_time', 0) >= existing.get('kismet.device.base.last_time', 0):
                if new_signal.get('kismet.common.signal.last_signal') is not None:
                    existing_signal['kismet.common.signal.last_signal'] = new_signal['kismet.common.signal.last_signal']
            
            existing['kismet.device.base.signal'] = existing_signal
    
    # Merge location data - use location with better fix or more recent
    existing_loc = existing.get('kismet.device.base.location', {})
    new_loc = new.get('kismet.device.base.location', {})
    
    if new_loc and new_loc.get('kismet.common.location.avg_loc'):
        if not existing_loc or not existing_loc.get('kismet.common.location.avg_loc'):
            existing['kismet.device.base.location'] = new_loc
    
    return existing


def get_device_key(device_json):
    """Get unique key for a device (MAC address)."""
    return device_json.get('kismet.device.base.macaddr', '')


def merge_kismet_files(input_files, output_file, verbose=False):
    """Merge multiple .kismet files into one."""
    
    # Dictionary to hold merged devices by MAC
    merged_devices = {}
    
    # Track statistics
    total_raw = 0
    files_processed = 0
    
    # Other tables we might want to preserve
    other_data = {
        'snapshots': [],
        'packets': [],
        'data': [],
        'alerts': [],
    }
    
    for input_file in input_files:
        if verbose:
            print(f"Reading: {input_file}")
        
        try:
            conn = sqlite3.connect(input_file)
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Process devices table
            if 'devices' in tables:
                cursor.execute("SELECT device FROM devices")
                rows = cursor.fetchall()
                
                file_count = 0
                for row in rows:
                    try:
                        device_json = json.loads(row[0])
                        mac = get_device_key(device_json)
                        
                        if not mac:
                            continue
                        
                        if mac in merged_devices:
                            merged_devices[mac] = merge_device_json(merged_devices[mac], device_json)
                        else:
                            merged_devices[mac] = device_json
                        
                        file_count += 1
                        total_raw += 1
                        
                    except (json.JSONDecodeError, KeyError) as e:
                        if verbose:
                            print(f"  Warning: Could not parse device: {e}")
                        continue
                
                if verbose:
                    print(f"  -> {file_count} devices")
            
            # Collect data from other tables
            for table in ['snapshots', 'packets', 'data', 'alerts']:
                if table in tables:
                    try:
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        # Get column names
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        other_data[table].extend([dict(zip(columns, row)) for row in rows])
                    except sqlite3.Error:
                        pass
            
            conn.close()
            files_processed += 1
            
        except sqlite3.Error as e:
            print(f"Error reading {input_file}: {e}", file=sys.stderr)
            continue
    
    if not merged_devices:
        print("Error: No devices found in input files", file=sys.stderr)
        return False
    
    if verbose:
        print(f"\nMerging {total_raw} total entries -> {len(merged_devices)} unique devices")
    
    # Create output database
    if Path(output_file).exists():
        Path(output_file).unlink()
    
    conn = sqlite3.connect(output_file)
    cursor = conn.cursor()
    
    # Create devices table (matching Kismet schema)
    cursor.execute('''
        CREATE TABLE devices (
            first_time INT,
            last_time INT,
            devkey TEXT,
            phyname TEXT,
            devmac TEXT,
            strongest_signal INT,
            min_lat REAL,
            min_lon REAL,
            max_lat REAL,
            max_lon REAL,
            avg_lat REAL,
            avg_lon REAL,
            bytes_data INT,
            type TEXT,
            device BLOB
        )
    ''')
    
    # Create indexes
    cursor.execute('CREATE INDEX devices_devkey ON devices (devkey)')
    cursor.execute('CREATE INDEX devices_devmac ON devices (devmac)')
    cursor.execute('CREATE INDEX devices_first_time ON devices (first_time)')
    cursor.execute('CREATE INDEX devices_last_time ON devices (last_time)')
    cursor.execute('CREATE INDEX devices_phyname ON devices (phyname)')
    cursor.execute('CREATE INDEX devices_type ON devices (type)')
    
    # Insert merged devices
    for mac, device_json in merged_devices.items():
        # Extract fields for table columns
        first_time = device_json.get('kismet.device.base.first_time', 0)
        last_time = device_json.get('kismet.device.base.last_time', 0)
        devkey = device_json.get('kismet.device.base.key', '')
        phyname = device_json.get('kismet.device.base.phyname', '')
        devmac = device_json.get('kismet.device.base.macaddr', '')
        
        signal = device_json.get('kismet.device.base.signal', {})
        strongest_signal = signal.get('kismet.common.signal.max_signal', 0)
        
        location = device_json.get('kismet.device.base.location', {})
        avg_loc = location.get('kismet.common.location.avg_loc', {})
        geopoint = avg_loc.get('kismet.common.location.geopoint', [0, 0])
        
        min_lat = location.get('kismet.common.location.min_loc', {}).get('kismet.common.location.geopoint', [0, 0])[1] if location.get('kismet.common.location.min_loc') else 0
        min_lon = location.get('kismet.common.location.min_loc', {}).get('kismet.common.location.geopoint', [0, 0])[0] if location.get('kismet.common.location.min_loc') else 0
        max_lat = location.get('kismet.common.location.max_loc', {}).get('kismet.common.location.geopoint', [0, 0])[1] if location.get('kismet.common.location.max_loc') else 0
        max_lon = location.get('kismet.common.location.max_loc', {}).get('kismet.common.location.geopoint', [0, 0])[0] if location.get('kismet.common.location.max_loc') else 0
        avg_lat = geopoint[1] if len(geopoint) > 1 else 0
        avg_lon = geopoint[0] if len(geopoint) > 0 else 0
        
        bytes_data = device_json.get('kismet.device.base.datasize', 0)
        dev_type = device_json.get('kismet.device.base.type', '')
        
        device_blob = json.dumps(device_json)
        
        cursor.execute('''
            INSERT INTO devices 
            (first_time, last_time, devkey, phyname, devmac, strongest_signal,
             min_lat, min_lon, max_lat, max_lon, avg_lat, avg_lon, bytes_data, type, device)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (first_time, last_time, devkey, phyname, devmac, strongest_signal,
              min_lat, min_lon, max_lat, max_lon, avg_lat, avg_lon, bytes_data, dev_type, device_blob))
    
    # Create KISMET table for metadata
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS KISMET (
            kismet_version TEXT,
            build_uuid TEXT,
            build_compile TEXT,
            db_version INT
        )
    ''')
    cursor.execute('''
        INSERT INTO KISMET (kismet_version, build_uuid, build_compile, db_version)
        VALUES (?, ?, ?, ?)
    ''', ('merged', 'wifi-sort-merge', datetime.now().isoformat(), 6))
    
    conn.commit()
    conn.close()
    
    if verbose:
        print(f"\nCreated: {output_file}")
        print(f"  {len(merged_devices)} devices from {files_processed} files")
    
    return True


def main():
    parser = argparse.ArgumentParser(
        description='Merge multiple Kismet .kismet database files into one',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
When merging duplicates (same MAC address):
  - Packet counts are summed
  - First_Seen uses earliest timestamp
  - Last_Seen uses latest timestamp  
  - Signal strength keeps best min/max values
  - Location data is preserved from best source

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
    
    # Remove duplicates
    input_files = list(dict.fromkeys(input_files))
    
    # Don't include output file in input
    input_files = [f for f in input_files if Path(f).resolve() != Path(args.output).resolve()]
    
    if len(input_files) < 1:
        print("Error: Need at least 1 input file", file=sys.stderr)
        sys.exit(1)
    
    if args.verbose:
        print(f"Merging {len(input_files)} files -> {args.output}\n")
    
    success = merge_kismet_files(input_files, args.output, verbose=args.verbose)
    
    if success:
        print(f"\nSuccessfully created: {args.output}")
    else:
        sys.exit(1)


if __name__ == '__main__':
    main()
