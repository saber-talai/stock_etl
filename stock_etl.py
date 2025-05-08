#!/usr/bin/env python3
"""
ETL Process for Stock Data (100GB across 10 CSV files)

Usage:
    python stock_etl.py --config config.yaml
"""

import pandas as pd
import numpy as np
import os
import glob
import time
import logging
import gc
import tempfile
import shutil
import json
import yaml
import argparse
from datetime import datetime
import psycopg2
import concurrent.futures
import sys
import signal
from contextlib import contextmanager

def setup_logging(config):
    """Set up logging based on configuration"""
    log_level = getattr(logging, config['logging']['level'])
    log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    handlers = [logging.StreamHandler()]
    
    # Add file handler if log_file is specified
    if 'log_file' in config['logging']:
        handlers.append(logging.FileHandler(config['logging']['log_file']))
    
    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=handlers
    )
    return logging.getLogger('stock_etl')

def load_config(config_file):
    """Load configuration from YAML file"""
    with open(config_file, 'r') as f:
        return yaml.safe_load(f)

# Global variable to track processed files for resuming
PROCESSED_FILES = set()

def signal_handler(sig, frame, checkpoint_file):
    """Handle Ctrl+C and other termination signals"""
    logger.info("Interrupt received, saving checkpoint and exiting...")
    save_checkpoint(checkpoint_file)
    sys.exit(0)

def save_checkpoint(checkpoint_file):
    """Save the list of processed files to a checkpoint file"""
    checkpoint_data = {
        "processed_files": list(PROCESSED_FILES),
        "timestamp": datetime.now().isoformat()
    }
    with open(checkpoint_file, 'w') as f:
        json.dump(checkpoint_data, f)
    logger.info(f"Checkpoint saved: {len(PROCESSED_FILES)} files processed")

def load_checkpoint(checkpoint_file):
    """Load the list of processed files from a checkpoint file"""
    global PROCESSED_FILES
    if os.path.exists(checkpoint_file):
        with open(checkpoint_file, 'r') as f:
            checkpoint_data = json.load(f)
            PROCESSED_FILES = set(checkpoint_data["processed_files"])
        logger.info(f"Checkpoint loaded: {len(PROCESSED_FILES)} files already processed")
    else:
        logger.info("No checkpoint file found, starting fresh")

@contextmanager
def postgres_connection(db_url):
    """Context manager for database connections"""
    conn = None
    try:
        conn = psycopg2.connect(db_url)
        yield conn
    except Exception as e:
        logger.error(f"Database connection error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()

def create_tables(db_url, schema="public"):
    """Create the necessary database tables if they don't exist"""
    with postgres_connection(db_url) as conn:
        cursor = conn.cursor()
        
        # Set schema if provided
        if schema != "public":
            cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
            cursor.execute(f"SET search_path TO {schema}")
        
        # Create price table
        create_price_sql = f"""
        CREATE TABLE IF NOT EXISTS price_table (
            date DATE PRIMARY KEY,
        """
        for i in range(1, 201):
            create_price_sql += f"    stk_{i:03d} FLOAT,"
        create_price_sql = create_price_sql.rstrip(',') + "\n);"
        cursor.execute(create_price_sql)
        
        # Create volume table
        create_volume_sql = f"""
        CREATE TABLE IF NOT EXISTS volume_table (
            date DATE PRIMARY KEY,
        """
        for i in range(1, 201):
            create_volume_sql += f"    stk_{i:03d} INTEGER,"
        create_volume_sql = create_volume_sql.rstrip(',') + "\n);"
        cursor.execute(create_volume_sql)
        
        # Create returns table
        create_returns_sql = f"""
        CREATE TABLE IF NOT EXISTS returns_table (
            date DATE PRIMARY KEY,
        """
        for i in range(1, 201):
            create_returns_sql += f"    stk_{i:03d} FLOAT,"
        create_returns_sql = create_returns_sql.rstrip(',') + "\n);"
        cursor.execute(create_returns_sql)
        
        # Add indexes for better query performance
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_price_date ON price_table (date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_volume_date ON volume_table (date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_returns_date ON returns_table (date)")
        
        conn.commit()
        logger.info("Database tables created or verified")

def validate_data_chunk(chunk):
    """Validate and clean a chunk of data"""
    # Drop rows with missing values
    chunk = chunk.dropna()
    
    # Ensure IDs are within valid range (1-200)
    chunk = chunk[(chunk['id'] >= 1) & (chunk['id'] <= 200)]
    
    # Ensure ID is integer
    chunk['id'] = chunk['id'].astype(int)
    
    # Ensure price is float
    chunk['price'] = chunk['price'].astype(float)
    
    # Ensure trade_volume is integer
    chunk['trade_volume'] = chunk['trade_volume'].astype(int)
    
    return chunk

def process_file(args):
    """
    Process a single CSV file in chunks and load to database to minimizes memory usage
    """
    file_path, db_url, chunk_size, schema, checkpoint_file, checkpoint_interval = args
    
    # Skip if this file has already been processed (for resuming)
    if file_path in PROCESSED_FILES:
        logger.info(f"Skipping already processed file: {file_path}")
        return file_path
    
    start_time = time.time()
    logger.info(f"Processing {file_path}...")
    
    # Create a temporary directory for this file's processing
    temp_dir = tempfile.mkdtemp()
    date_records = {}
    
    try:
        # Process file in chunks to avoid memory issues
        reader = pd.read_csv(
            file_path, 
            chunksize=chunk_size,
            names=['date', 'id', 'price', 'trade_volume'],
            parse_dates=['date']
        )
        
        # Process each chunk
        for i, chunk in enumerate(reader):
            chunk_start = time.time()
            
            # Clean and validate data
            chunk = validate_data_chunk(chunk)
            
            # Group by date and accumulate records
            for date, group in chunk.groupby('date'):
                date_str = date.strftime('%Y-%m-%d') if hasattr(date, 'strftime') else str(date)
                
                if date_str not in date_records:
                    date_records[date_str] = []
                
                # Append this chunk's records for this date
                date_records[date_str].extend(group[['id', 'price', 'trade_volume']].values.tolist())
            
            # Free memory
            del chunk
            gc.collect()
            
            chunk_end = time.time()
            logger.info(f"Processed chunk {i+1} from {file_path} in {chunk_end - chunk_start:.2f} seconds")
        
        # Connect to the database and insert the data
        with postgres_connection(db_url) as conn:
            cursor = conn.cursor()
            
            # Set schema if necessary
            if schema != "public":
                cursor.execute(f"SET search_path TO {schema}")
            
            # Prepare column names for SQL queries
            price_columns = "date," + ','.join([f"stk_{i:03d}" for i in range(1, 201)])
            volume_columns = "date," + ','.join([f"stk_{i:03d}" for i in range(1, 201)])
            
            # Process each date's data
            for date_str, records in date_records.items():
                # Create price row and volume row for this date
                price_row = [date_str] + [None] * 200
                volume_row = [date_str] + [None] * 200
                
                # Populate data for this date
                for stock_id, price, volume in records:
                    idx = int(stock_id)
                    if 1 <= idx <= 200:
                        price_row[idx] = price
                        volume_row[idx] = volume
                
                # Insert into price table with UPSERT
                price_placeholders = ','.join(['%s'] * len(price_row))
                price_update_set = ', '.join([f"stk_{i:03d} = EXCLUDED.stk_{i:03d}" for i in range(1, 201)])
                price_sql = f"""
                INSERT INTO price_table ({price_columns}) 
                VALUES ({price_placeholders})
                ON CONFLICT (date) DO UPDATE SET {price_update_set}
                """
                cursor.execute(price_sql, price_row)
                
                # Insert into volume table with UPSERT
                volume_placeholders = ','.join(['%s'] * len(volume_row))
                volume_update_set = ', '.join([f"stk_{i:03d} = EXCLUDED.stk_{i:03d}" for i in range(1, 201)])
                volume_sql = f"""
                INSERT INTO volume_table ({volume_columns}) 
                VALUES ({volume_placeholders})
                ON CONFLICT (date) DO UPDATE SET {volume_update_set}
                """
                cursor.execute(volume_sql, volume_row)
            
            conn.commit()
        
        # Add to processed files list
        PROCESSED_FILES.add(file_path)
        
        # Save checkpoint based on configured interval
        if len(PROCESSED_FILES) % checkpoint_interval == 0:
            save_checkpoint(checkpoint_file)
        
        end_time = time.time()
        logger.info(f"Completed processing {file_path} in {end_time - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        raise
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
    
    return file_path

def calculate_returns(db_url, schema="public"):
    """Calculate daily returns for all stocks based on price data"""
    logger.info("Calculating stock returns...")
    start_time = time.time()
    
    with postgres_connection(db_url) as conn:
        cursor = conn.cursor()
        
        # Set schema if necessary
        if schema != "public":
            cursor.execute(f"SET search_path TO {schema}")
        
        # Get all dates in ascending order
        cursor.execute("SELECT date FROM price_table ORDER BY date")
        dates = [row[0] for row in cursor.fetchall()]
        
        if len(dates) < 2:
            logger.warning("Not enough data to calculate returns (need at least 2 dates)")
            return
        
        # Clear existing returns data
        cursor.execute("DELETE FROM returns_table")
        
        # For each pair of consecutive dates, calculate returns
        for i in range(1, len(dates)):
            current_date = dates[i]
            previous_date = dates[i-1]
            
            # Create a temporary table for this calculation
            cursor.execute("""
            CREATE TEMP TABLE temp_returns AS
            SELECT 
                curr.date,
            """ + ',\n'.join([
                f"(curr.stk_{j:03d} - prev.stk_{j:03d}) / NULLIF(prev.stk_{j:03d}, 0) AS stk_{j:03d}"
                for j in range(1, 201)
            ]) + """
            FROM price_table curr
            JOIN price_table prev ON prev.date = %s
            WHERE curr.date = %s
            """, (previous_date, current_date))
            
            # Insert into returns table
            cursor.execute("""
            INSERT INTO returns_table
            SELECT * FROM temp_returns
            """)
            
            # Drop the temporary table
            cursor.execute("DROP TABLE temp_returns")
            
            # Commit after each date to avoid long transactions
            conn.commit()
        
        # Final commit
        conn.commit()
    
    end_time = time.time()
    logger.info(f"Returns calculation completed in {end_time - start_time:.2f} seconds")

def main():
    """Main function to orchestrate the ETL process"""
    parser = argparse.ArgumentParser(description='ETL process for large stock pricing data')
    parser.add_argument('--config', required=True, help='Path to config YAML file')
    parser.add_argument('--input-dir', help='Override input directory from config')
    parser.add_argument('--db-url', help='Override database URL from config')
    parser.add_argument('--resume', action='store_true', help='Resume from checkpoint if available')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config(args.config)
    
    # Set up logging
    global logger
    logger = setup_logging(config)
    
    # Override config with command line arguments if provided
    if args.input_dir:
        config['input']['directory'] = args.input_dir
    if args.db_url:
        config['database']['url'] = args.db_url
    
    # Extract config values
    db_url = config['database']['url']
    schema = config['database'].get('schema', 'public')
    input_dir = config['input']['directory']
    file_pattern = config['input'].get('file_pattern', '*.csv')
    chunk_size = config['processing'].get('chunk_size', 100000)
    max_workers = config['processing'].get('max_workers', 3)
    checkpoint_enabled = config['checkpoint'].get('enabled', True)
    checkpoint_interval = config['checkpoint'].get('interval', 2)
    checkpoint_file = config['checkpoint'].get('file', 'etl_checkpoint.json')
    
    # Register signal handler for graceful termination
    signal.signal(signal.SIGINT, lambda sig, frame: signal_handler(sig, frame, checkpoint_file))
    signal.signal(signal.SIGTERM, lambda sig, frame: signal_handler(sig, frame, checkpoint_file))
    
    # Create database tables
    create_tables(db_url, schema)
    
    # Load checkpoint if resuming
    if args.resume and checkpoint_enabled:
        load_checkpoint(checkpoint_file)
    
    # Get list of CSV files
    csv_files = glob.glob(os.path.join(input_dir, file_pattern))
    if not csv_files:
        logger.error(f"No CSV files found in {input_dir} matching pattern {file_pattern}")
        return
    
    logger.info(f"Found {len(csv_files)} CSV files to process")
    
    # Create a list of arguments for each file
    file_args = [(file_path, db_url, chunk_size, schema, checkpoint_file, checkpoint_interval) 
                for file_path in csv_files]
    
    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        for file_path in executor.map(process_file, file_args):
            logger.info(f"Completed ETL for {file_path}")
    
    # Calculate returns after all data is loaded
    calculate_returns(db_url, schema)
    
    # Clean up checkpoint file after successful completion if checkpointing was enabled
    if checkpoint_enabled and os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
    
    logger.info("ETL process completed successfully!")

if __name__ == "__main__":
    main()