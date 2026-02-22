"""
Lap Data Organizer
Reads raw parquet files and organizes them by car/lap with lap-relative timestamps.
Runs continuously, processing new data as it arrives.

Usage:
    python organize_laps.py --input D:\26DAY1_Race --output D:\26DAY1_Race\laps
    python organize_laps.py --input D:\26DAY1_Race --output D:\26DAY1_Race\laps --vehicles 7 71 77
    python organize_laps.py --input D:\26DAY1_Race --output D:\26DAY1_Race\laps --names Speed "Engine RPM"

Output format: {output}/by_lap/{name}_{vehicle}_lap{N}.parquet
    Columns: timestamp, lap_time, value_numeric, value_location
    
Also creates: {output}/combined/{name}_{vehicle}.parquet  
    Columns: timestamp, lap, lap_time, value_numeric, value_location
"""

import argparse
import time
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional, Set
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd


# Configuration
LAP_SOURCE = 'linecrossing'  # Name of parquet with lap data
PROCESS_INTERVAL = 2.0       # Seconds between processing runs
STATS_INTERVAL = 30.0        # Seconds between stats


def print_info(msg):
    print(f'\033[94m[ORGANIZER]\033[0m {msg}')


def print_stats(msg):
    print(f'\033[92m[STATS]\033[0m {msg}')


class LapOrganizer:
    """Organizes raw telemetry data by lap with lap-relative timestamps"""
    
    def __init__(self, input_dir: str, output_dir: str, lap_source: str = LAP_SOURCE):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.lap_source = lap_source
        
        # Create output directories
        self.by_lap_dir = self.output_dir / 'by_lap'
        self.combined_dir = self.output_dir / 'combined'
        self.by_lap_dir.mkdir(parents=True, exist_ok=True)
        self.combined_dir.mkdir(parents=True, exist_ok=True)
        
        # Track processing state
        self.processed_rows: Dict[str, int] = {}  # {filename: last_row_processed}
        self.lap_data: Dict[str, List] = {}       # {vehicle: [(timestamp, lap_num), ...]}
        self.lap_data_rows: Dict[str, int] = {}   # {vehicle: last_row_processed}
        
        # Stats
        self.files_processed = 0
        self.records_organized = 0
        self.start_time = time.time()
    
    def _get_lap_source_file(self, vehicle: str) -> Path:
        """Get lap source parquet file path"""
        return self.input_dir / f"{self.lap_source}_{vehicle}.parquet"
    
    def _update_lap_data(self, vehicle: str):
        """Load/update lap timestamps from lap source"""
        filepath = self._get_lap_source_file(vehicle)
        if not filepath.exists():
            return
        
        try:
            table = pq.read_table(filepath)
            current_rows = len(table)
            
            last_row = self.lap_data_rows.get(vehicle, 0)
            if current_rows <= last_row:
                return
            
            df = table.to_pandas()
            new_data = df.iloc[last_row:]
            
            if vehicle not in self.lap_data:
                self.lap_data[vehicle] = []
            
            for _, row in new_data.iterrows():
                ts = row['timestamp']
                val_loc = row.get('value_location')
                
                if val_loc and pd.notna(val_loc):
                    try:
                        loc_data = json.loads(str(val_loc))
                        
                        # Only use SF (start/finish) line crossings for lap starts
                        loop = loc_data.get('loop', {})
                        is_sf = loop.get('sf') == True or loop.get('name') == 'SF'
                        
                        if is_sf:
                            lap_num = loc_data.get('lap')
                            if lap_num is not None:
                                self.lap_data[vehicle].append((ts, lap_num))
                    except:
                        pass
            
            # Sort by timestamp
            self.lap_data[vehicle].sort(key=lambda x: x[0])
            self.lap_data_rows[vehicle] = current_rows
            
        except Exception as e:
            print_info(f"Error reading lap source for {vehicle}: {e}")
    
    def _lookup_lap(self, vehicle: str, timestamp: int) -> Optional[int]:
        """Find lap number for a given timestamp"""
        lap_list = self.lap_data.get(vehicle, [])
        if not lap_list:
            return None
        
        # Binary search for closest timestamp <= target
        left, right = 0, len(lap_list) - 1
        result_lap = None
        
        while left <= right:
            mid = (left + right) // 2
            ts, lap = lap_list[mid]
            
            if ts == timestamp:
                return lap
            elif ts < timestamp:
                result_lap = lap
                left = mid + 1
            else:
                right = mid - 1
        
        return result_lap
    
    def _get_lap_start_ts(self, vehicle: str, lap_num: int) -> Optional[int]:
        """Get the start timestamp for a lap"""
        lap_list = self.lap_data.get(vehicle, [])
        for ts, lap in lap_list:
            if lap == lap_num:
                return ts
        return None
    
    def process_file(self, name: str, vehicle: str):
        """Process a single telemetry file and organize by lap"""
        filename = f"{name}_{vehicle}.parquet"
        filepath = self.input_dir / filename
        
        if not filepath.exists():
            return
        
        # Update lap data first (for lap number lookup)
        self._update_lap_data(vehicle)
        
        if not self.lap_data.get(vehicle):
            return  # No lap data yet
        
        try:
            # Try to read with error handling for files being written
            try:
                table = pq.read_table(filepath)
            except Exception as read_err:
                # File might be corrupted or still being written - skip for now
                return
            
            current_rows = len(table)
            
            last_row = self.processed_rows.get(filename, 0)
            if current_rows <= last_row:
                return
            
            df = table.to_pandas()
            new_data = df.iloc[last_row:]
            
            # Group records by lap
            lap_records: Dict[int, List] = defaultdict(list)
            lap_first_ts: Dict[int, int] = {}  # Track first timestamp per lap
            
            for _, row in new_data.iterrows():
                ts = row['timestamp']
                lap_num = self._lookup_lap(vehicle, ts)
                
                if lap_num is None or lap_num == 0:
                    continue  # Skip if no lap or lap 0
                
                # Track first timestamp for this lap
                if lap_num not in lap_first_ts:
                    lap_first_ts[lap_num] = ts
                
                lap_records[lap_num].append({
                    'timestamp': ts,
                    'value_numeric': row.get('value_numeric'),
                    'value_location': row.get('value_location'),
                })
            
            # Calculate lap_time using first timestamp of each lap as offset
            for lap_num, records in lap_records.items():
                lap_start = lap_first_ts[lap_num]
                for record in records:
                    record['lap_time'] = (record['timestamp'] - lap_start) / 1000.0
            
            # Write organized data
            for lap_num, records in lap_records.items():
                if not records:
                    continue
                
                self._write_lap_file(name, vehicle, lap_num, records)
                self.records_organized += len(records)
            
            self.processed_rows[filename] = current_rows
            self.files_processed += 1
            
        except Exception as e:
            print_info(f"Error processing {filename}: {e}")
    
    def _write_lap_file(self, name: str, vehicle: str, lap_num: int, records: List[dict]):
        """Write or append lap-organized data"""
        # By-lap file
        lap_filename = f"{name}_{vehicle}_lap{lap_num}.parquet"
        lap_filepath = self.by_lap_dir / lap_filename
        
        # Combined file (all laps in one file)
        combined_filename = f"{name}_{vehicle}.parquet"
        combined_filepath = self.combined_dir / combined_filename
        
        # Create new table (reset index to avoid schema issues)
        df = pd.DataFrame(records)
        df['lap'] = lap_num
        df = df.sort_values('lap_time').reset_index(drop=True)
        
        new_table = pa.Table.from_pandas(df[['timestamp', 'lap', 'lap_time', 'value_numeric', 'value_location']], preserve_index=False)
        
        # Write by-lap file (replace - contains only this lap)
        try:
            if lap_filepath.exists():
                try:
                    existing = pq.read_table(lap_filepath)
                    combined = pa.concat_tables([existing, new_table])
                    combined_df = combined.to_pandas().drop_duplicates('timestamp').sort_values('lap_time').reset_index(drop=True)
                    pq.write_table(pa.Table.from_pandas(combined_df, preserve_index=False), lap_filepath, compression='snappy')
                except Exception:
                    # Existing file corrupted, overwrite with new data
                    pq.write_table(new_table, lap_filepath, compression='snappy')
            else:
                pq.write_table(new_table, lap_filepath, compression='snappy')
        except Exception as e:
            pass  # Silently skip write errors
        
        # Write combined file (all laps)
        try:
            if combined_filepath.exists():
                try:
                    existing = pq.read_table(combined_filepath)
                    combined = pa.concat_tables([existing, new_table])
                    combined_df = combined.to_pandas().drop_duplicates('timestamp').sort_values(['lap', 'lap_time']).reset_index(drop=True)
                    pq.write_table(pa.Table.from_pandas(combined_df, preserve_index=False), combined_filepath, compression='snappy')
                except Exception as inner_e:
                    # Existing file corrupted, overwrite with new data
                    try:
                        pq.write_table(new_table, combined_filepath, compression='snappy')
                    except Exception as write_e:
                        print_info(f"Combined write failed {combined_filename}: {write_e}")
            else:
                pq.write_table(new_table, combined_filepath, compression='snappy')
        except Exception as e:
            print_info(f"Combined outer error {combined_filename}: {e}")
    
    def discover_files(self, names: Optional[List[str]] = None, vehicles: Optional[List[str]] = None) -> List[tuple]:
        """Discover available parquet files to process"""
        files = list(self.input_dir.glob("*.parquet"))
        result = []
        
        # Exclusion patterns - skip files containing these strings
        exclude_patterns = ['Low Oil', 'Meta', 'NLED', 'Low Water', 'Stuck Throttle', 
                            'Rollover', 'bwN', 'NTAG', 'NLoad', 'bw']
        
        for f in files:
            # Parse filename: name_vehicle.parquet
            stem = f.stem
            if '_' not in stem:
                continue
            
            # Find last underscore to split name and vehicle
            last_underscore = stem.rfind('_')
            name = stem[:last_underscore]
            vehicle = stem[last_underscore + 1:]
            
            # Skip lap source files
            if name == self.lap_source:
                continue
            
            # Skip excluded patterns
            if any(pattern in name for pattern in exclude_patterns):
                continue
            
            # Filter if specified
            if names and name not in names:
                continue
            if vehicles and vehicle not in vehicles:
                continue
            
            result.append((name, vehicle))
        
        return result
    
    def get_stats(self) -> str:
        elapsed = time.time() - self.start_time
        rate = self.records_organized / elapsed if elapsed > 0 else 0
        return (
            f"Records: {self.records_organized:,} | "
            f"Rate: {rate:.1f}/sec | "
            f"Vehicles: {len(self.lap_data)} | "
            f"Files: {self.files_processed}"
        )


def main():
    parser = argparse.ArgumentParser(description='Organize telemetry data by lap')
    parser.add_argument('--input', type=str, default='D:\\26DAY1_Race',
                        help='Input directory with raw parquet files')
    parser.add_argument('--output', type=str, default=None,
                        help='Output directory for organized files (default: input/laps)')
    parser.add_argument('--lap-source', type=str, default='linecrossing',
                        help='Name of parquet with lap data')
    parser.add_argument('--names', type=str, nargs='+',
                        help='Only process specific data names (e.g., Speed "Engine RPM")')
    parser.add_argument('--vehicles', type=str, nargs='+',
                        help='Only process specific vehicles (e.g., 7 71 77)')
    parser.add_argument('--interval', type=float, default=PROCESS_INTERVAL,
                        help='Seconds between processing runs')
    parser.add_argument('--once', action='store_true',
                        help='Process once and exit (no continuous mode)')
    parser.add_argument('--clean', action='store_true',
                        help='Delete existing output files before processing')
    args = parser.parse_args()
    
    # Default output to input/laps
    output_dir = args.output or str(Path(args.input) / 'laps')
    
    # Clean output if requested
    if args.clean:
        import shutil
        output_path = Path(output_dir)
        if output_path.exists():
            shutil.rmtree(output_path)
            print_info(f'Cleaned output directory: {output_dir}')
    
    print_info('Starting Lap Organizer...')
    print_info(f'Input: {args.input}')
    print_info(f'Output: {output_dir}')
    print_info(f'Lap source: {args.lap_source}')
    if args.names:
        print_info(f'Names filter: {args.names}')
    if args.vehicles:
        print_info(f'Vehicles filter: {args.vehicles}')
    print_info('-' * 60)
    
    organizer = LapOrganizer(args.input, output_dir, args.lap_source)
    
    last_stats = time.time()
    
    try:
        while True:
            # Discover and process files
            files = organizer.discover_files(args.names, args.vehicles)
            
            for name, vehicle in files:
                organizer.process_file(name, vehicle)
            
            # Print stats
            if (time.time() - last_stats) >= STATS_INTERVAL:
                print_stats(organizer.get_stats())
                last_stats = time.time()
            
            if args.once:
                print_stats(f"FINAL: {organizer.get_stats()}")
                break
            
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        print_info('Shutting down...')
        print_stats(f"FINAL: {organizer.get_stats()}")


if __name__ == '__main__':
    main()
