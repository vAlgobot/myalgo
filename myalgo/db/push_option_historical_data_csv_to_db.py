
"""
Options Data Ingestion Pipeline
------------------------------
Objective:
    - Ingest historical options CSV data (NIFTY) into DuckDB.
    - Transform raw data into a standardized master schema.
    - Handle expiry mapping, symbol construction, and efficient batch insertion.

Usage:
    Run this script directly to process all CSV files in the configured folder.
    > python -m myalgo.db.ingest_historical_data
"""

import os
import glob
import logging
import duckdb
import pandas as pd
import shutil
from datetime import datetime, timedelta

# CONFIGURATION
# =============================================================================

# Hardcoded Paths
CSV_FOLDER_PATH = r"C:\Users\Srivi\myalgo_openalgo\myalgo\option_expired_data"
DB_PATH = r"C:\Users\Srivi\myalgo_openalgo\myalgo\data\market_data.db"
ARCHIVE_FOLDER_PATH = r"C:\Users\Srivi\myalgo_openalgo\myalgo\option_expired_data\Archive"
# 🔹 SYMBOL CONFIGURATION (CHANGE HERE)
UNDERLYING_SYMBOL = "SENSEX"   # Options: NIFTY, BANKNIFTY, SENSEX
EXCHANGE = "NFO"              # Options: NFO (For Nifty/BankNifty), BFO (For Sensex)

# Expiry List (Source of Truth NSE)
EXPIRY_LIST_RAW = [
    "06-JAN-26",
    "13-JAN-26","20-JAN-26","27-JAN-26","03-FEB-26","10-FEB-26","17-FEB-26","24-FEB-26", "03-MAR-2026","10-MAR-2026","17-MAR-2026","24-MAR-2026","31-MAR-2026"]

# Logging Setup
def setup_logger():
    logger = logging.getLogger("options_ingestion")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logger()


class OptionsIngestionPipeline:
    def __init__(self, db_path, csv_folder, expiry_list_raw, underlying, exchange):
        self.db_path = db_path
        self.csv_folder = csv_folder
        self.raw_expiries = [datetime.strptime(e.strip(), "%d-%b-%y").date() for e in expiry_list_raw]
        self.raw_expiries.sort()
        self.underlying = underlying.upper()
        self.exchange = exchange.upper()
        self.conn = None

    def connect(self):
        """Establish DuckDB connection."""
        self.conn = duckdb.connect(self.db_path)
        logger.info(f"Connected to DuckDB at {self.db_path}")

    def close(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed.")

    def setup_schema(self):
        """Create table and indexes if they don't exist."""
        try:
            # 1. Create Table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS options_data_master (
                symbol VARCHAR NOT NULL,
                exchange VARCHAR NOT NULL,
                underlying VARCHAR NOT NULL,
                expiry VARCHAR NOT NULL,
                strike BIGINT NOT NULL,
                timeframe VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                volume BIGINT NOT NULL,
                oi BIGINT NOT NULL,
                iv BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, exchange, timeframe, timestamp)
            );
            """
            self.conn.execute(create_table_sql)

            # 2. Create Indexes
            indexes = [
                "CREATE INDEX IF NOT EXISTS idx_options_symbol ON options_data_master(symbol);",
                "CREATE INDEX IF NOT EXISTS idx_options_expiry ON options_data_master(expiry);",
                "CREATE INDEX IF NOT EXISTS idx_options_timestamp ON options_data_master(timestamp);",
                "CREATE INDEX IF NOT EXISTS idx_options_underlying_time ON options_data_master(underlying, timeframe, timestamp);",
                "CREATE INDEX IF NOT EXISTS idx_options_underlying_expiry ON options_data_master(underlying, expiry);"
            ]
            for idx_sql in indexes:
                self.conn.execute(idx_sql)
            
            logger.info("Schema and indexes verified.")
            
        except Exception as e:
            logger.error(f"Schema setup failed: {e}")
            raise

    def get_mapped_expiry(self, trade_date: datetime.date) -> datetime.date:
        """
        Map a trade date to the nearest valid expiry (Next Approaching Expiry).
        Logic: Find the first expiry in the list that is >= trade_date.
        """
        for expiry in self.raw_expiries:
            if expiry >= trade_date:
                return expiry
        
        # If no future expiry found, return the last one (edge case) or raise error
        logger.warning(f"No future expiry found for {trade_date}, using last available.")
        return self.raw_expiries[-1]

    def process_csv_file(self, file_path):
        """Process a single CSV file."""
        file_name = os.path.basename(file_path)
        
        # 1. Extract timeframe from filename
        # Expected format: NIFTY_YYYY-MM_1min.csv or similar containing '1min' or '5min'
        if "1min" in file_name.lower() or "1m" in file_name.lower():
            timeframe = "1m"
        elif "5min" in file_name.lower() or "5m" in file_name.lower():
            timeframe = "5m"
        else:
            logger.warning(f"Skipping file {file_name}: Unknown timeframe")
            return

        logger.info(f"Processing {file_name} | Timeframe: {timeframe}")

        processed = False
        try:
            # 2. Read CSV
            # Format: timestamp,open,high,low,close,volume,iv,oi,spot,strike_price,side,strike_expr,expiry_flag,expiry_code,date
            # Example timestamp: 01-12-2025 09:15 OR 2024-01-01 09:15:00
            df = pd.read_csv(file_path, on_bad_lines='skip')
            
            rows_read = len(df)
            if rows_read == 0:
                logger.warning(f"Empty file: {file_name}")
                return

            # 3. Standardization & Transformation
            
            # Timestamp Parsing
            # Try ISO format detection first (default), then fallback if needed.
            # Sample seen: 2024-01-01 09:22:00
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

            # Drop invalid timestamps
            valid_ts_mask = df['timestamp'].notna()
            dropped_count = len(df) - valid_ts_mask.sum()
            if dropped_count > 0:
                logger.warning(f"Dropped {dropped_count} rows with invalid timestamps")
                df = df[valid_ts_mask]

            if df.empty:
                return

            # Extract basic columns
            df['exchange'] = self.exchange
            df['underlying'] = self.underlying
            df['timeframe'] = timeframe
            
            # Ensure number types with SAFE truncation for floats -> int
            # .astype(float).astype('int64') truncates 22.9 -> 22. This is robust for "safe to int64" errors.
            df['strike'] = pd.to_numeric(df['strike_price'], errors='coerce').fillna(0).astype(float).astype('int64')
            df['volume'] = pd.to_numeric(df['volume'], errors='coerce').fillna(0).astype(float).astype('int64')
            df['oi'] = pd.to_numeric(df['oi'], errors='coerce').fillna(0).astype(float).astype('int64')
            df['iv'] = pd.to_numeric(df['iv'], errors='coerce').fillna(0).astype(float).astype('int64') 
            
            # Expiry Mapping
            # We need a date column to lookup expiry. 'date' column exists in CSV: 01-12-2025 or 2024-01-01
            # Try parsing 'date' column robustly
            df['temp_date'] = pd.to_datetime(df['date'], errors='coerce').dt.date
            
            # Drop rows where date parsing failed (critical for expiry mapping)
            if df['temp_date'].isna().any():
                 logger.warning(f"Dropped {df['temp_date'].isna().sum()} rows due to invalid 'date' column")
                 df = df.dropna(subset=['temp_date'])
            
            # Vectorized mapping is hard with a custom lookup list and inequality.
            # We will use apply for robustness, though slightly slower.
            # Optimization: Map unique dates.
            unique_dates = df['temp_date'].unique()
            date_map = {d: self.get_mapped_expiry(d) for d in unique_dates}
            
            # Apply map
            df['mapped_expiry_dt'] = df['temp_date'].map(date_map)
            
            # Format expiry as DDMMMYY (e.g., 30DEC25)
            # Upper case is required
            df['expiry'] = df['mapped_expiry_dt'].apply(lambda x: x.strftime("%d%b%y").upper())
            
            # Symbol Construction
            # Format: UNDERLYING + EXPIRY + STRIKE + SIDE (e.g., CE)
            # Example data: side = CE
            df['symbol'] = (
                self.underlying + 
                df['expiry'] + 
                df['strike'].astype(str) + 
                df['side'].str.upper()
            )

            # Final Column Selection & Renaming
            # Target columns: 
            # symbol, exchange, underlying, expiry, strike, timeframe, timestamp, 
            # open, high, low, close, volume, oi, iv
            
            final_df = df[[
                'symbol', 'exchange', 'underlying', 'expiry', 'strike', 'timeframe', 'timestamp',
                'open', 'high', 'low', 'close', 'volume', 'oi', 'iv'
            ]].copy()
            
            # IV Casting to Big Int as requested
            final_df['iv'] = final_df['iv'].fillna(0).astype('int64') # Truncates float

            # 4. Ingest into DuckDB
            try:
                # Use Appender or SQL Insert
                # Since we need ON CONFLICT DO NOTHING (ignore duplicates), we use SQL.
                self.conn.register('temp_ingest_df', final_df)
                
                insert_query = """
                INSERT INTO options_data_master 
                (symbol, exchange, underlying, expiry, strike, timeframe, timestamp, 
                 open, high, low, close, volume, oi, iv)
                SELECT 
                    symbol, exchange, underlying, expiry, strike, timeframe, timestamp, 
                    open, high, low, close, volume, oi, iv
                FROM temp_ingest_df
                ON CONFLICT (symbol, exchange, timeframe, timestamp) DO NOTHING;
                """
                
                
                # Get count of actually inserted (rough approximation, or we can trust it works)
                # DuckDB execute returns undefined for insert usually, but we can assume success if no error.
                rows_skipped = 0 # Difficult to track exact skip count in batch insert without RETURNING
                # But we can calculate total rows
                
                self.conn.execute(insert_query)

                logger.info(f"-> Processed {len(final_df)} rows for {file_name}")
                processed = True

            except Exception as e:
                logger.error(f"Error inserting batch for {file_name}: {e}")
            finally:
                try:
                    self.conn.unregister('temp_ingest_df')
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"Failed to process {file_name}: {e}")
            return False

        # Move successfully processed files to archive folder
        if processed:
            try:
                os.makedirs(ARCHIVE_FOLDER_PATH, exist_ok=True)
                dest = os.path.join(ARCHIVE_FOLDER_PATH, file_name)
                shutil.move(file_path, dest)
                logger.info(f"Moved {file_name} to archive: {ARCHIVE_FOLDER_PATH}")
            except Exception as e:
                logger.warning(f"Failed to move {file_name} to archive: {e}")
            return True

    def run(self, file_filter=None):
        self.connect()
        self.setup_schema()
        
        # Get list of files
        all_files = glob.glob(os.path.join(self.csv_folder, "*.csv"))
        all_files.sort()
        
        # Filter files if requested
        if file_filter:
            files = [f for f in all_files if file_filter in os.path.basename(f)]
            if not files:
                logger.warning(f"No files matched filter: '{file_filter}'")
                return
        else:
            files = all_files
        
        logger.info(f"Found {len(files)} CSV files (Total: {len(all_files)})")
        logger.info(f"Configuration: Underlying={self.underlying}, Exchange={self.exchange}")
        
        for i, file_path in enumerate(files, 1):
            logger.info(f"[{i}/{len(files)}] Processing File...")
            self.process_csv_file(file_path)
            
        self.close()
        logger.info("Ingestion Pipeline Complete.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest Options Data")
    parser.add_argument("--file", type=str, help="Specific file name/pattern to process (e.g., '2025-12')")
    args = parser.parse_args()

    pipeline = OptionsIngestionPipeline(
        db_path=DB_PATH,
        csv_folder=CSV_FOLDER_PATH,
        expiry_list_raw=EXPIRY_LIST_RAW,
        underlying=UNDERLYING_SYMBOL,
        exchange=EXCHANGE
    )
    pipeline.run(file_filter=args.file)
