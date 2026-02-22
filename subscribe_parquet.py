"""
Integrated NATS Subscriber with Direct Parquet Processing
Receives live telemetry data and writes directly to organized parquet files.
No intermediate JSON files - maximum throughput.

Usage:
python subscribe_parquet.py --server nats://streaming.us-east-1.nextgen.nascarracedata.com:4222 --creds Spire_Creds_020426/spire_prod/spire.creds --tlsca Spire_Creds_020426/spire_prod/ca_file --subscription "spire.>"
"""

import argparse
import os
import json
import asyncio
import ssl
import datetime
import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional
import orjson
import pyarrow as pa
import pyarrow.parquet as pq
from nats.aio.client import Client as NATS
from concurrent.futures import ThreadPoolExecutor

# Configuration
OUTPUT_DIR = 'D:\\26DAY1_Race'
BATCH_SIZE = 500            # Records per (name, vehicle) before flushing
FLUSH_INTERVAL = 2.0        # Seconds between forced flushes (faster for plotting)
STATS_INTERVAL = 10.0       # Seconds between stats printing

# Thread pool for non-blocking parquet writes
write_executor = ThreadPoolExecutor(max_workers=4)


def print_info(msg):
    print(f'\033[94mINFO:\033[0m {datetime.datetime.now().timestamp()} - {msg}')


def print_error(msg):
    print(f'\033[91mERROR:\033[0m {datetime.datetime.now()} - {msg}')


def print_stats(msg):
    print(f'\033[92mSTATS:\033[0m {datetime.datetime.now().timestamp()} - {msg}')


class ParquetBuffer:
    """
    Async-compatible buffer that accumulates records by (name, vehicle_id) and flushes to parquet.
    Output format: {name}_{vehicle_id}.parquet for easy plotting access.
    """
    
    def __init__(self, output_dir: str):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Buffers: (name, vehicle_id) -> list of record tuples
        self.buffers: Dict[tuple, List] = defaultdict(list)
        self.last_flush: Dict[tuple, float] = defaultdict(lambda: time.time())
        
        # Stats
        self.records_received = 0
        self.records_written = 0
        self.files_written = 0
        self.start_time = time.time()
        
        # Lock for thread safety
        self._lock = asyncio.Lock()
    
    def _make_key(self, name: str, vehicle_id: str) -> tuple:
        """Create buffer key from name and vehicle_id"""
        return (name, str(vehicle_id))
    
    def _make_filename(self, name: str, vehicle_id: str) -> str:
        """Create filename like aSteering_71.parquet"""
        safe_name = "".join(c if c.isalnum() or c in (' ', '-', '_') else '_' for c in name)[:80]
        return f"{safe_name}_{vehicle_id}.parquet"
    
    async def add_record(self, record: dict):
        """Add a single record to the appropriate buffer"""
        name = record.get('name')
        vehicle_id = record.get('vehicle_id')
        if not name or not vehicle_id:
            return
        
        session = record.get('sessionInfo', {})
        value = record.get('value')
        
        # Create tuple for efficient storage
        record_tuple = (
            record.get('timestamp', 0),
            str(vehicle_id),
            session.get('race_id', 0),
            session.get('run_id', 0),
            float(value) if isinstance(value, (int, float)) else None,
            orjson.dumps(value).decode() if not isinstance(value, (int, float, type(None))) else None
        )
        
        key = self._make_key(name, vehicle_id)
        
        async with self._lock:
            self.buffers[key].append(record_tuple)
            self.records_received += 1
            
            # Check if we should flush this buffer
            if len(self.buffers[key]) >= BATCH_SIZE:
                await self._flush_buffer(key)
    
    async def add_batch(self, records: List[dict]):
        """Add multiple records efficiently"""
        async with self._lock:
            for record in records:
                name = record.get('name')
                vehicle_id = record.get('vehicle_id')
                if not name or not vehicle_id:
                    continue
                
                session = record.get('sessionInfo', {})
                value = record.get('value')
                
                record_tuple = (
                    record.get('timestamp', 0),
                    str(vehicle_id),
                    session.get('race_id', 0),
                    session.get('run_id', 0),
                    float(value) if isinstance(value, (int, float)) else None,
                    orjson.dumps(value).decode() if not isinstance(value, (int, float, type(None))) else None
                )
                
                key = self._make_key(name, vehicle_id)
                self.buffers[key].append(record_tuple)
                self.records_received += 1
            
            # Check for buffers that need flushing
            for key in list(self.buffers.keys()):
                if len(self.buffers[key]) >= BATCH_SIZE:
                    await self._flush_buffer(key)
    
    async def _flush_buffer(self, key: tuple):
        """Flush a single buffer to parquet (must hold lock)"""
        records = self.buffers[key]
        if not records:
            return
        
        # Clear buffer immediately
        self.buffers[key] = []
        self.last_flush[key] = time.time()
        
        name, vehicle_id = key
        
        # Write in thread pool to not block async loop
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(
            write_executor,
            self._write_parquet,
            name,
            vehicle_id,
            records
        )
    
    def _write_parquet(self, name: str, vehicle_id: str, records: List):
        """Write records to parquet file (runs in thread pool)"""
        if not records:
            return
        
        # Sort by timestamp
        records.sort(key=lambda x: x[0])
        
        # Create filename like aSteering_71.parquet
        filename = self._make_filename(name, vehicle_id)
        output_path = self.output_dir / filename
        
        # Unzip records
        timestamps, vehicle_ids, race_ids, run_ids, values_num, values_loc = zip(*records)
        
        new_table = pa.table({
            'timestamp': pa.array(timestamps, type=pa.int64()),
            'vehicle_id': pa.array(vehicle_ids, type=pa.string()),
            'race_id': pa.array(race_ids, type=pa.int32()),
            'run_id': pa.array(run_ids, type=pa.int32()),
            'value_numeric': pa.array(values_num, type=pa.float64()),
            'value_location': pa.array(values_loc, type=pa.string()),
        })
        
        # Append to existing file or create new
        if output_path.exists():
            try:
                existing = pq.read_table(output_path)
                combined = pa.concat_tables([existing, new_table])
                combined = combined.sort_by('timestamp')
                pq.write_table(combined, output_path, compression='snappy')
            except Exception as e:
                # If error reading existing, just write new
                pq.write_table(new_table, output_path, compression='snappy')
        else:
            pq.write_table(new_table, output_path, compression='snappy')
        
        self.records_written += len(records)
        self.files_written += 1
    
    async def periodic_flush(self):
        """Flush buffers that haven't been written recently"""
        async with self._lock:
            current_time = time.time()
            for key in list(self.buffers.keys()):
                if self.buffers[key] and (current_time - self.last_flush[key]) >= FLUSH_INTERVAL:
                    await self._flush_buffer(key)
    
    async def flush_all(self):
        """Flush all remaining buffers"""
        async with self._lock:
            for key in list(self.buffers.keys()):
                if self.buffers[key]:
                    await self._flush_buffer(key)
    
    def get_stats(self) -> str:
        elapsed = time.time() - self.start_time
        rate = self.records_received / elapsed if elapsed > 0 else 0
        pending = sum(len(v) for v in self.buffers.values())
        unique_streams = len(self.buffers)
        return (
            f"Received: {self.records_received:,} | "
            f"Written: {self.records_written:,} | "
            f"Pending: {pending:,} | "
            f"Streams: {unique_streams} | "
            f"Files: {self.files_written} | "
            f"Rate: {rate:.0f}/sec"
        )


# Global buffer instance
parquet_buffer: Optional[ParquetBuffer] = None
message_queue: asyncio.Queue = None  # Queue for decoupling receive from process
dropped_messages = 0  # Counter for dropped messages


async def message_handler(msg):
    """Handle incoming NATS messages - just queue them for processing"""
    global message_queue, dropped_messages
    try:
        # Non-blocking put - drop if queue is full rather than block
        message_queue.put_nowait(msg.data)
    except asyncio.QueueFull:
        dropped_messages += 1
    except:
        pass  # Ignore all errors in hot path


async def process_queue():
    """Process messages from queue in batches"""
    global parquet_buffer, message_queue
    
    batch = []
    batch_size = 1000  # Process in larger batches
    
    while True:
        try:
            # Get messages with short timeout to allow batch processing
            try:
                data = await asyncio.wait_for(message_queue.get(), timeout=0.05)
                batch.append(data)
                
                # Drain more messages if available (up to batch_size)
                while len(batch) < batch_size:
                    try:
                        data = message_queue.get_nowait()
                        batch.append(data)
                    except asyncio.QueueEmpty:
                        break
                        
            except asyncio.TimeoutError:
                pass
            
            # Process batch
            if batch:
                records_to_add = []
                for raw_data in batch:
                    try:
                        json_data = orjson.loads(raw_data)
                        
                        if isinstance(json_data, list):
                            records_to_add.extend(json_data)
                        else:
                            records_to_add.append(json_data)
                    except:
                        pass  # Skip bad messages silently
                
                # Add all records in one batch call
                if records_to_add:
                    await parquet_buffer.add_batch(records_to_add)
                batch = []
                
        except Exception as e:
            print_error(f'Error in queue processor: {e}')
            await asyncio.sleep(0.1)


async def periodic_tasks():
    """Background task for periodic flushing and stats"""
    global parquet_buffer
    
    last_stats = time.time()
    
    while True:
        await asyncio.sleep(1.0)
        
        # Periodic flush
        await parquet_buffer.periodic_flush()
        
        # Print stats
        if (time.time() - last_stats) >= STATS_INTERVAL:
            global dropped_messages
            stats = parquet_buffer.get_stats()
            if dropped_messages > 0:
                stats += f" | Dropped: {dropped_messages:,}"
            print_stats(stats)
            last_stats = time.time()


async def run(loop, args):
    global parquet_buffer, message_queue
    
    # Initialize parquet buffer and message queue
    parquet_buffer = ParquetBuffer(OUTPUT_DIR)
    message_queue = asyncio.Queue(maxsize=500000)  # Very large queue buffer
    
    # Create TLS context
    tls_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
    tls_ctx.load_verify_locations(args.tlsca)
    
    # Connection callbacks
    async def disconnected_cb():
        print_info('Disconnected from NATS server')
    
    async def reconnected_cb():
        print_info(f'Reconnected to NATS server')
    
    async def error_cb(e):
        print_error(f'NATS error: {e}')
    
    async def closed_cb():
        print_info('NATS connection closed')
    
    print_info(f'Attempting to connect to: {args.server}')
    nc = NATS()
    
    await nc.connect(
        servers=args.server, 
        tls=tls_ctx, 
        user_credentials=args.creds,
        reconnect_time_wait=2,          # Wait 2 seconds between reconnect attempts
        max_reconnect_attempts=-1,       # Unlimited reconnect attempts
        ping_interval=20,                # Ping every 20 seconds
        max_outstanding_pings=3,         # Allow 3 outstanding pings before disconnect
        disconnected_cb=disconnected_cb,
        reconnected_cb=reconnected_cb,
        error_cb=error_cb,
        closed_cb=closed_cb,
    )
    print_info(f'Successfully connected to: {args.server}')
    print_info(f'Writing parquet files to: {OUTPUT_DIR}/')
    
    # Subscribe to topic with large buffer limits
    sub = await nc.subscribe(
        args.subscription, 
        cb=message_handler,
        pending_msgs_limit=1000000,      # 1M pending messages
        pending_bytes_limit=512*1024*1024  # 512MB pending bytes
    )
    print_info(f'Subscribed to: {args.subscription}')
    
    # Start queue processor and periodic tasks
    queue_task = asyncio.create_task(process_queue())
    periodic_task = asyncio.create_task(periodic_tasks())
    
    try:
        while True:
            # Periodically log queue size
            qsize = message_queue.qsize()
            if qsize > 1000:
                print_info(f'Queue size: {qsize:,}')
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        pass
    finally:
        # Final flush
        print_info('Flushing remaining data...')
        queue_task.cancel()
        await parquet_buffer.flush_all()
        periodic_task.cancel()
        print_stats(f'FINAL: {parquet_buffer.get_stats()}')


def parse_args():
    parser = argparse.ArgumentParser(description='NATS Subscriber with Direct Parquet Output')
    parser.add_argument('--server', type=str, 
                        default='nats://streaming.us-east-1.nextgen.nascarracedata.com:4222',
                        help='NATS server URL')
    parser.add_argument('--creds', type=str, 
                        default='nats.user.credentials',
                        help='Path to credentials file')
    parser.add_argument('--tlsca', type=str, 
                        default='ca.crt',
                        help='Path to TLS CA certificate')
    parser.add_argument('--subscription', type=str, 
                        default='>',
                        help='NATS subscription pattern')
    parser.add_argument('--output', type=str,
                        default='D:\\26DAY1_Heat1',
                        help='Output directory for parquet files')
    parser.add_argument('--batch-size', type=int,
                        default=500,
                        help='Records per (name, vehicle) before flushing to parquet')
    parser.add_argument('--flush-interval', type=float,
                        default=2.0,
                        help='Seconds between forced flushes')
    return parser.parse_args()


if __name__ == '__main__':
    print_info('Starting NATS Subscriber with Parquet Output...')
    args = parse_args()
    
    # Update globals from args
    OUTPUT_DIR = args.output
    BATCH_SIZE = args.batch_size
    FLUSH_INTERVAL = args.flush_interval
    
    print_info(f'Server: {args.server}')
    print_info(f'Credentials: {args.creds}')
    print_info(f'TLS CA: {args.tlsca}')
    print_info(f'Subscription: {args.subscription}')
    print_info(f'Output Directory: {OUTPUT_DIR}')
    print_info(f'Batch Size: {BATCH_SIZE}')
    print_info(f'Flush Interval: {FLUSH_INTERVAL}s')
    print_info('Use Ctrl+C to quit')
    print_info('-' * 60)
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(run(loop, args))
    except KeyboardInterrupt:
        print_info('Shutting down...')
    finally:
        loop.close()
