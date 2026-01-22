"""
Database Manager for vAlgo Trading System

This module provides comprehensive database management for OHLCV market data
using DuckDB for high-performance analytics and backtesting operations.

Features:
- OHLCV data storage with proper schema and indexing
- Data loading methods for DataLoad mode
- Data retrieval methods for Backtesting mode
- Data validation and quality assurance
- Performance optimization for large datasets

Author: vAlgo Development Team
Created: June 27, 2025
"""

import logging
import re
from typing import Dict, List, Optional, Any, Tuple, Union
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path
import time
import threading
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
try:
    import duckdb
except ImportError:
    raise ImportError("DuckDB library not installed. Run: pip install duckdb")

from core.logger import get_logger


class DatabaseManager:
    """
    Professional database manager for OHLCV market data storage and retrieval.
    
    Uses DuckDB for high-performance analytical queries suitable for backtesting
    and market data analysis.
    
    Singleton pattern implementation to prevent multiple database connections.
    """
    
    _instance = None
    _lock = threading.Lock()
    _initialized = False
    
    def __new__(cls, db_path: Optional[str] = None, config_path: Optional[str] = None):
        """Ensure singleton pattern with thread safety."""
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(DatabaseManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self, db_path: Optional[str] = None, config_path: Optional[str] = None):
        """
        Initialize Database Manager (singleton pattern).
        
        Args:
            db_path: Path to DuckDB database file
            config_path: Path to Excel configuration file
        """
        # Only initialize once due to singleton pattern
        if self._initialized:
            return
            
        self.logger = get_logger(__name__)
        self.config_path = config_path or "config/config.xlsx"
        
        # Set up database path
        if db_path is None:
            project_root = Path(__file__).parent.parent
            data_dir = project_root / "data"
            data_dir.mkdir(exist_ok=True)
            db_path = str(data_dir / "market_data.db")
        
        self.db_path = db_path
        self.connection = None

        # Add thread lock for DB operations (for v2 method)
        import threading
        self._db_lock = threading.Lock()
        
        # Database configuration
        self.spot_table = "spot_data"
        self.options_table = "options_data"
        self.metadata_table = "data_metadata"
        
        # Initialize database
        self._initialize_database()
        
        self.logger.info(f"DatabaseManager singleton initialized with database: {self.db_path}")
        DatabaseManager._initialized = True
        
    def get_ohlcv_data_v2(
            self,
            symbol: str,
            exchange: str,
            timeframe: str,
            start_date: str,
            end_date: str,
            instrument_type: str = "SPOT",
            expiry: Optional[str] = None,
            underlying: Optional[str] = None
        ) -> 'pd.DataFrame':
            """
            Alternative OHLCV data retrieval method using explicit SQL and thread lock.
            Args:
                symbol: Trading symbol
                exchange: Exchange name
                timeframe: Time interval
                start_date: Start date (YYYY-MM-DD)
                end_date: End date (YYYY-MM-DD)
                instrument_type: "SPOT" or "OPTIONS"
                expiry: Expiry date (dd-mm-YYYY) for options
                strike: Strike price for options
                underlying: Underlying symbol for options
            Returns:
                pd.DataFrame: OHLCV data indexed by timestamp
            """
            instrument_type = instrument_type.upper()
            timeframe = timeframe.lower()

            start_ts = f"{start_date} 00:00:00"
            end_ts = f"{end_date} 23:59:59"

            if instrument_type == "SPOT":
                query = f"""
                    SELECT timestamp, open, high, low, close, volume
                    FROM {self.spot_table}
                    WHERE symbol = ?
                      AND exchange = ?
                      AND timeframe = ?
                      AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp
                """
                params = [
                    symbol.upper(),
                    exchange.upper(),
                    timeframe,
                    start_ts,
                    end_ts,
                ]

            else:
                strike = extract_strike_from_symbol(symbol)
                if not all([expiry, strike, underlying]):
                    raise ValueError("OPTIONS require expiry, strike, underlying")
                query = f"""
                    SELECT timestamp, open, high, low, close, volume, oi
                    FROM {self.options_table}
                    WHERE symbol = ?
                      AND expiry = ?
                      AND strike = ?
                      AND timeframe = ?
                      AND exchange = ?
                      AND underlying = ?
                      AND timestamp BETWEEN ? AND ?
                    ORDER BY timestamp
                """

                params = [
                    symbol.upper(),
                    datetime.strptime(expiry, "%d-%m-%Y").date(),
                    strike,
                    timeframe,
                    exchange.upper(),
                    underlying.upper(),
                    start_ts,
                    end_ts,
                ]

            with self._db_lock:
                df = self.connection.execute(query, params).fetchdf()

            if df.empty:
                return pd.DataFrame()

            df["timestamp"] = pd.to_datetime(df["timestamp"])
            return df.set_index("timestamp")
    
    def _initialize_database(self) -> None:
        """Initialize database connection and create tables if they don't exist."""
        try:
            # Establish DuckDB connection

            self.connection = duckdb.connect(self.db_path)  

            # Create OHLCV data table
            self._create_ohlcv_table()
            
            # Create metadata table
            self._create_metadata_table()
            
            # Create indexes for better performance
            self._create_indexes()
            
            self.logger.info("Database initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize database: {e}")
            raise
    
    def _create_ohlcv_table(self) -> None:
        """Create the main OHLCV data table with optimized schema."""
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.spot_table} (
            symbol VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL,
            timeframe VARCHAR NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            open DOUBLE NOT NULL,
            high DOUBLE NOT NULL,
            low DOUBLE NOT NULL,
            close DOUBLE NOT NULL,
            volume BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, exchange, timeframe, timestamp)
        )
        """
        
        self.connection.execute(create_table_sql)
        self.logger.debug("OHLCV table created/verified")
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.options_table} (
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (symbol, exchange, timeframe, timestamp)
        )
        """
        
        self.connection.execute(create_table_sql)
        self.logger.debug("OHLCV table created/verified")
    
    def _create_metadata_table(self) -> None:
        """Create metadata table for tracking data completeness and quality."""
        # Drop existing table if it has old schema
        try:
            self.connection.execute(f"DROP TABLE IF EXISTS {self.metadata_table}")
        except Exception:
            pass  # Table might not exist
        
        create_metadata_sql = f"""
        CREATE TABLE {self.metadata_table} (
            symbol VARCHAR NOT NULL,
            exchange VARCHAR NOT NULL,
            timeframe VARCHAR NOT NULL,
            start_date DATE NOT NULL,
            end_date DATE NOT NULL,
            total_records BIGINT NOT NULL,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            missing_days TEXT,
            PRIMARY KEY (symbol, exchange, timeframe)
        )
        """
        
        self.connection.execute(create_metadata_sql)
        self.logger.debug("Metadata table created with new schema")
    
    def _create_indexes(self) -> None:
        """Create database indexes for optimized query performance."""
        try:
            # Spot data indexes
            # Index on symbol for fast symbol-based queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_symbol 
                ON {self.spot_table} (symbol)
            """)
            
            # Index on timestamp for time-based queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON {self.spot_table} (timestamp)
            """)
            
            # Composite index for backtesting queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_symbol_timeframe_timestamp 
                ON {self.spot_table} (symbol, timeframe, timestamp)
            """)
            # repeat for options table
            # Index on symbol for fast symbol-based queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_symbol 
                ON {self.options_table} (symbol)
            """)
            
            # Index on timestamp for time-based queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_timestamp 
                ON {self.options_table} (timestamp)
            """)
            
            # Composite index for backtesting queries
            self.connection.execute(f"""
                CREATE INDEX IF NOT EXISTS idx_symbol_timeframe_timestamp 
                ON {self.options_table} (symbol, timeframe, timestamp)
            """)

            
            self.logger.debug("Database indexes created/verified")
            
        except Exception as e:
            self.logger.warning(f"Some indexes may already exist: {e}")
    
    def store_ohlcv_data(self, symbol: str, exchange: str, timeframe: str, 
                        data: pd.DataFrame, replace: bool = False, instrument_type: str = "SPOT") -> bool:
        """
        Store OHLCV data in the database.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Time interval
            data: OHLCV DataFrame with timestamp index
            replace: Whether to replace existing data
            
        Returns:
            bool: True if successful
        """
        try:
            if data.empty:
                self.logger.warning(f"No data to store for {symbol}")
                return False
            if instrument_type.upper() == "OPTIONS":
                self.spot_table = self.options_table    
            else:
                self.spot_table = self.spot_table
            
            # Prepare data for insertion
            df_to_store = self._prepare_data_for_storage(symbol, exchange, timeframe, data)
            
            if df_to_store.empty:
                self.logger.warning(f"No valid data after preparation for {symbol}")
                return False
            
            # Use safe upsert/insert logic directly
            insert_count = self._insert_ohlcv_data(df_to_store, instrument_type, replace=replace)
            
            # Update metadata
            # self._update_metadata(symbol, exchange, timeframe, df_to_store)
            
            self.logger.info(f"Stored {insert_count} records for {symbol} ({exchange}, {timeframe})")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to store data for {symbol}: {e}")
            return False
        
    def create_table_new(self, instrument: str = 'OPTIONS'):
        conn = duckdb.connect(self.db_path)
        
        conn.execute("DROP TABLE IF EXISTS options_data")
        self.logger.debug("OHLCV table created/verified")
        conn.close()
        print("Table created")
    
    def _prepare_data_for_storage(self, symbol: str, exchange: str, timeframe: str, 
                                 data: pd.DataFrame) -> pd.DataFrame:
        """
        Prepare DataFrame for database storage with minimal timestamp modification.
        
        PRESERVE RAW API DATA: This method now prioritizes keeping exact timestamps
        from API responses without artificial generation or excessive conversion.
        """
        try:
            df = data.copy()
            
            # Ensure required columns exist
            if exchange == "NFO":             
                required_columns = ['open', 'high', 'low', 'close', 'volume', 'oi']
                # Select final columns in correct order
                final_columns = [
                'symbol', 'exchange', 'underlying', 'strike', 'expiry', 'timeframe', 'timestamp',
                'open', 'high', 'low', 'close', 'volume', 'oi']
                missing_columns = [col for col in required_columns if col not in df.columns]
            else:
                required_columns = ['open', 'high', 'low', 'close', 'volume']
                missing_columns = [col for col in required_columns if col not in df.columns]
                # Select final columns in correct order
                final_columns = [
                'symbol', 'exchange', 'timeframe', 'timestamp',
                'open', 'high', 'low', 'close', 'volume']
                
            if missing_columns:
                raise ValueError(f"Missing required columns: {missing_columns}")
            
            # Add metadata columns
            df['symbol'] = symbol.upper()
            df['exchange'] = exchange.upper()
            df['timeframe'] = timeframe.lower()
            
            # SIMPLIFIED TIMESTAMP HANDLING - Preserve raw API data
            self.logger.info(f"Raw data received: {len(df)} records")
            
            # Handle timestamp column/index  
            if isinstance(df.index, pd.DatetimeIndex):
                # We have DatetimeIndex - preserve it as timestamp column
                df = df.reset_index()
                # If index doesn't have a name, call it 'timestamp'
                if df.columns[0] == 0 or df.columns[0] == 'index':
                    df.columns = ['timestamp'] + list(df.columns[1:])
                self.logger.info("Preserved DatetimeIndex as timestamp column")
            elif df.index.name == 'timestamp':
                # Index is named timestamp - move to column
                df = df.reset_index()
                self.logger.info("Moved named timestamp index to column")
            elif 'timestamp' not in df.columns:
                # No DatetimeIndex and no timestamp column - use index values
                df['timestamp'] = df.index
                df = df.reset_index(drop=True)
                self.logger.info("Used index as timestamp column")
            
            # Log what we received from API
            if 'timestamp' in df.columns and not df.empty:
                sample = df['timestamp'].head(3).tolist()
                self.logger.info(f"API timestamp sample: {sample}")
                self.logger.info(f"API timestamp dtype: {df['timestamp'].dtype}")
            
            # MINIMAL conversion - only if absolutely necessary
            if 'timestamp' in df.columns:
                # Only convert if not already datetime
                if df['timestamp'].dtype != 'datetime64[ns]' and not isinstance(df['timestamp'].iloc[0] if not df.empty else None, pd.Timestamp):
                    self.logger.info("Converting timestamps to datetime format")
                    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
                else:
                    self.logger.info("Timestamps already in correct format - preserving as-is")
                
                # Only remove truly invalid timestamps (NaT, null) - preserve everything else including holidays
                before_filter = len(df)
                invalid_mask = df['timestamp'].isna()
                df = df[~invalid_mask]
                removed_count = before_filter - len(df)
                
                if removed_count > 0:
                    self.logger.info(f"Removed {removed_count} records with invalid/null timestamps")
                
                # Log final timestamp range
                if not df.empty:
                    self.logger.info(f"Final timestamp range: {df['timestamp'].min()} to {df['timestamp'].max()}")
            
            # Basic OHLCV validation (simplified)
            df = self._validate_spot_data(df)
                      
            df = df[final_columns]
            
            # Sort by timestamp
            df = df.sort_values('timestamp')
            
            self.logger.info(f"Prepared {len(df)} records for storage (preserving API timestamps)")
            return df
            
        except Exception as e:
            self.logger.error(f"Error preparing data: {e}")
            return pd.DataFrame()
    
    def _parse_timeframe_to_minutes(self, timeframe: str) -> int:
        """Parse timeframe string to minutes."""
        try:
            timeframe = timeframe.lower().strip()
            if timeframe.endswith('m'):
                return int(timeframe[:-1])
            elif timeframe.endswith('h'):
                return int(timeframe[:-1]) * 60
            elif timeframe.endswith('d'):
                return int(timeframe[:-1]) * 60 * 24
            else:
                # Default to 5 minutes for 5m timeframe
                return 5
        except Exception as e:
            self.logger.error(f"Error parsing timeframe {timeframe}: {e}")
            return 5

    def _validate_spot_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Simplified OHLCV validation - minimal processing to preserve API data.
        """
        try:
            original_count = len(df)
            
            if df.empty:
                return df
            
            
            # Basic OHLC relationship validation (preserve price data as much as possible)
            valid_mask = (
                (df['high'] >= df['low']) &
                (df['volume'] >= 0) &  # Allow zero volume (API may provide it)
                (df['open'] > 0) &
                (df['high'] > 0) &
                (df['low'] > 0) &
                (df['close'] > 0)
            )
            
            invalid_ohlc = len(df) - valid_mask.sum()
            df = df[valid_mask]
            
            if invalid_ohlc > 0:
                self.logger.warning(f"Removed {invalid_ohlc} records with invalid OHLC relationships")
            
            # Remove duplicate timestamps (keep first occurrence)
            before_dedup = len(df)
            if 'timestamp' in df.columns:
                df = df.drop_duplicates(subset=['timestamp'], keep='first')
                duplicates_removed = before_dedup - len(df)
                
                if duplicates_removed > 0:
                    self.logger.info(f"Removed {duplicates_removed} duplicate timestamps")
            
            total_removed = original_count - len(df)
            if total_removed > 0:
                self.logger.info(f"Validation summary: {len(df)}/{original_count} records retained ({total_removed} removed)")
            else:
                self.logger.debug(f"All {len(df)} records passed validation")
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error validating OHLCV data: {e}")
            return df

    def _insert_ohlcv_data(self, df: pd.DataFrame, instrument_type, replace: bool = False) -> int:
        """
        Insert OHLCV data into database using efficient Upsert (ON CONFLICT) logic.
        
        Args:
            df: DataFrame to insert
            instrument_type: Type of instrument (SPOT/OPTIONS)
            replace: If True, update existing records. If False, ignore duplicates.
        """
        try:
            if df.empty:
                self.logger.warning("No data to insert")
                return 0
                
            if instrument_type == 'OPTIONS':
                self.spot_table = self.options_table
                target_table = self.options_table
                columns = ['symbol', 'exchange', 'underlying', 'strike', 'expiry', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi']
                conflict_targets = "symbol, exchange, timeframe, timestamp" 
                # Note: Options PK in schema is (symbol, exchange, timeframe, timestamp)
            else:
                self.spot_table = self.spot_table
                target_table = self.spot_table
                columns = ['symbol', 'exchange', 'timeframe', 'timestamp', 'open', 'high', 'low', 'close', 'volume']
                conflict_targets = "symbol, exchange, timeframe, timestamp"

            # Log what we're inserting
            self.logger.info(f"Inserting {len(df)} records into {target_table} (replace={replace})")
            
            # Minimal validation - only remove null timestamps
            valid_mask = df['timestamp'].notna()
            df_clean = df[valid_mask].copy()
            
            if df_clean.empty:
                return 0
            
            # Use DuckDB's efficient DataFrame insertion
            self.connection.register('temp_df', df_clean)

            # Construct SQL
            col_str = ", ".join(columns)
            
            if replace:
                # Upsert: Update conflicting records
                # Exclude primary key columns from UPDATE SET
                update_cols = [c for c in columns if c not in ['symbol', 'exchange', 'timeframe', 'timestamp']]
                update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in update_cols])
                
                insert_sql = f"""
                    INSERT INTO {target_table} ({col_str})
                    SELECT {col_str} FROM temp_df
                    ON CONFLICT ({conflict_targets}) DO UPDATE SET
                    {update_set}
                """
            else:
                # Insert safe: Update nothing on conflict
                insert_sql = f"""
                    INSERT INTO {target_table} ({col_str})
                    SELECT {col_str} FROM temp_df
                    ON CONFLICT ({conflict_targets}) DO NOTHING
                """
                
            self.connection.execute(insert_sql)
            
            # Clean up temporary registration
            self.connection.unregister('temp_df')
            
            return len(df_clean)
            
        except Exception as e:
            self.logger.error(f"Error inserting data: {e}")
            raise
    

    
    def _update_metadata(self, symbol: str, exchange: str, timeframe: str, df: pd.DataFrame) -> None:
        """Update metadata table with data information (incremental-friendly)."""
        try:
            if df.empty:
                return
            
            # Get current metadata if exists
            current_metadata_query = f"""
            SELECT start_date, end_date, total_records 
            FROM {self.metadata_table} 
            WHERE symbol = ? AND exchange = ? AND timeframe = ?
            """
            
            current_result = self.connection.execute(current_metadata_query, [
                symbol.upper(), exchange.upper(), timeframe.lower()
            ]).fetchone()
            
            # Get actual data range from database (not just this batch)
            actual_data_query = f"""
            SELECT MIN(DATE(timestamp)) as min_date, MAX(DATE(timestamp)) as max_date, COUNT(*) as total_records
            FROM {self.spot_table} 
            WHERE symbol = ? AND exchange = ? AND timeframe = ?
            """
            
            actual_result = self.connection.execute(actual_data_query, [
                symbol.upper(), exchange.upper(), timeframe.lower()
            ]).fetchone()
            
            if actual_result and actual_result[0]:
                start_date = actual_result[0]
                end_date = actual_result[1] 
                total_records = actual_result[2]
                
                # Check for missing days based on actual data range
                expected_days = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days + 1
                
                actual_days_query = f"""
                SELECT COUNT(DISTINCT DATE(timestamp)) 
                FROM {self.spot_table} 
                WHERE symbol = ? AND exchange = ? AND timeframe = ?
                """
                actual_days_result = self.connection.execute(actual_days_query, [
                    symbol.upper(), exchange.upper(), timeframe.lower()
                ]).fetchone()
                
                actual_days = actual_days_result[0] if actual_days_result else 0
                missing_days_count = expected_days - actual_days
                missing_days = f"{missing_days_count} potential missing days" if missing_days_count > 0 else "None"
                
                # Upsert metadata with actual database coverage
                upsert_sql = f"""
                INSERT OR REPLACE INTO {self.metadata_table} 
                (symbol, exchange, timeframe, start_date, end_date, total_records, 
                 missing_days, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                
                self.connection.execute(upsert_sql, [
                    symbol.upper(), exchange.upper(), timeframe.lower(),
                    start_date, end_date, total_records, missing_days
                ])
                
                self.logger.debug(f"Updated metadata for {symbol}: {start_date} to {end_date}, {total_records} records")
            
        except Exception as e:
            self.logger.error(f"Error updating metadata: {e}")
    
    def load_data_from_market_data_manager(self, market_data_dict: Dict[str, pd.DataFrame],
                                         instruments_config: List[Dict[str, Any]]) -> Dict[str, bool]:
        """
        Load data from MarketDataManager results into database.
        
        Args:
            market_data_dict: Dictionary of symbol -> DataFrame from MarketDataManager
            instruments_config: List of instrument configurations
            
        Returns:
            Dict mapping symbols to success status
        """
        results = {}
        
        self.logger.info(f"Loading data for {len(market_data_dict)} instruments into database")
        
        # Create mapping of symbols to their config
        symbol_config_map = {
            config['symbol'].upper(): config 
            for config in instruments_config
        }
        
        for symbol, df in market_data_dict.items():
            try:
                # Get instrument configuration
                config = symbol_config_map.get(symbol.upper())
                if not config:
                    self.logger.warning(f"No configuration found for {symbol}")
                    results[symbol] = False
                    continue
                
                exchange = config.get('exchange', 'NSE')
                timeframe = config.get('timeframe', '5m')
                
                # Store in database
                success = self.store_spot_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=timeframe,
                    data=df,
                    replace=True  # Replace existing data during DataLoad
                )
                
                results[symbol] = success
                
            except Exception as e:
                self.logger.error(f"Failed to load data for {symbol}: {e}")
                results[symbol] = False
        
        successful_loads = sum(results.values())
        self.logger.info(f"Successfully loaded {successful_loads}/{len(results)} instruments")
        
        return results
    
    def get_ohlcv_data(self, symbol: str, exchange: str, timeframe: str,
                      start_date: Optional[str] = None, end_date: Optional[str] = None, instrument_type: str = "SPOT", underlying_symbol: str = "NIFTY", expiry_date: Optional[str] = None) -> pd.DataFrame:
        """
        Retrieve OHLCV data for backtesting.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Time interval
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            pd.DataFrame: OHLCV data with timestamp index
        """
        try:
            # TABLE ASSIGNMENT BASED ON INSTRUMENT TYPE

            if instrument_type.upper() == "SPOT":
                self.spot_table = self.spot_table  # Main OHLCV table
                # Build query
                where_conditions = [
                "symbol = ?",
                "exchange = ?", 
                "LOWER(timeframe) = ?"
                ]
                params = [symbol.upper(), exchange.upper(), timeframe.lower()]
                
            elif instrument_type.upper() == "OPTIONS":
                self.spot_table = self.options_table  # Options-specific table    
                # Build query
                where_conditions = [
                "underlying = ?",
                "exchange = ?",
                "expiry = ?",
                "strike = ?",
                "symbol = ?",
                "LOWER(timeframe) = ?"
                ]
                strike_price = extract_strike_from_symbol(symbol)
                params = [underlying_symbol.upper(), exchange.upper(), expiry_date, strike_price, symbol.upper(), timeframe.lower()]
            else:
                self.logger.error(f"Invalid instrument type: {instrument_type}")
                return pd.DataFrame()
            # Build query conditions
            if start_date:
                where_conditions.append("timestamp >= ?")
                params.append(start_date)
            if end_date:
                where_conditions.append("timestamp <= ?")
                # Fix: For same-day queries or single day backtesting, extend end_date to include full day
                if end_date and len(end_date) == 10:  # YYYY-MM-DD format
                    end_date_extended = end_date + ' 23:59:59'
                    params.append(end_date_extended)
                    self.logger.debug(f"Extended end_date for full day coverage: {end_date_extended}")
                else:
                    params.append(end_date)
            # Execute query

            if instrument_type.upper() == "OPTIONS":
                query = f"""
                SELECT timestamp, open, high, low, close, volume, oi, strike, expiry, underlying
                FROM {self.spot_table}
                WHERE {' AND '.join(where_conditions)}
                ORDER BY timestamp ASC
                """
            else:
                query = f"""
                SELECT timestamp, open, high, low, close, volume
                FROM {self.spot_table}
                WHERE {' AND '.join(where_conditions)}
                ORDER BY timestamp ASC
                """

            df = self.connection.execute(query, params).df()
            
            if df.empty:
                self.logger.warning(f"No data found for {symbol} ({exchange}, {timeframe})")
                return pd.DataFrame()
            
            # Set timestamp as index
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df = df.set_index('timestamp')
            if timeframe.lower() == "d":
                df.index = df.index.normalize()
            
            self.logger.info(f"Retrieved {len(df)} records for {symbol} ({exchange}, {timeframe})")
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to retrieve data for {symbol}: {e}")
            return pd.DataFrame()
    
    def get_available_dates(self, symbol: Optional[str] = None, exchange: Optional[str] = None, 
                           timeframe: Optional[str] = None) -> List[str]:
        """
        Get distinct trading dates from OHLC data.
        
        Args:
            symbol: Filter by specific symbol (optional)
            exchange: Filter by specific exchange (optional)
            timeframe: Filter by specific timeframe (optional)
            
        Returns:
            List of distinct dates in YYYY-MM-DD format
        """
        try:
            # Build query conditions
            where_conditions = []
            params = []
            
            if symbol:
                where_conditions.append("symbol = ?")
                params.append(symbol.upper())
            
            if exchange:
                where_conditions.append("exchange = ?")
                params.append(exchange.upper())
            
            if timeframe:
                where_conditions.append("timeframe = ?")
                params.append(timeframe.lower())
            
            # Build the query
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            query = f"""
            SELECT DISTINCT DATE(timestamp) as date_only
            FROM {self.spot_table}
            WHERE {where_clause}
            ORDER BY date_only
            """
            
            result = self.connection.execute(query, params).fetchall()
            dates = [str(row[0]) for row in result]
            
            filter_desc = f" (filtered by {', '.join(f'{k}={v}' for k, v in zip(['symbol', 'exchange', 'timeframe'], [symbol, exchange, timeframe]) if v)})" if any([symbol, exchange, timeframe]) else ""
            self.logger.info(f"Retrieved {len(dates)} distinct trading dates from OHLC data{filter_desc}")
            
            return dates
            
        except Exception as e:
            self.logger.error(f"Error getting available dates: {e}")
            return []
    
    def get_ohlc_date_coverage_stats(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        """
        Get comprehensive date coverage statistics for OHLC data.
        
        Args:
            symbol: Filter by specific symbol (optional)
            
        Returns:
            Dictionary with date coverage analysis
        """
        try:
            # Get all available dates
            all_dates = self.get_available_dates(symbol=symbol)
            
            # Analyze date gaps
            date_gaps = self._analyze_ohlc_date_gaps(all_dates)
            
            # Get symbols and their coverage
            if symbol:
                symbol_filter = "WHERE symbol = ?"
                params = [symbol.upper()]
            else:
                symbol_filter = ""
                params = []
                
            # Records per date analysis
            records_per_date = self.connection.execute(f"""
                SELECT DATE(timestamp) as date_only, COUNT(*) as record_count
                FROM {self.spot_table}
                {symbol_filter}
                GROUP BY DATE(timestamp)
                ORDER BY record_count DESC
            """, params).fetchall()
            
            # Symbol coverage analysis
            symbols_per_date = self.connection.execute(f"""
                SELECT DATE(timestamp) as date_only, COUNT(DISTINCT symbol) as symbol_count
                FROM {self.spot_table}
                {symbol_filter}
                GROUP BY DATE(timestamp)
                ORDER BY symbol_count DESC
            """, params).fetchall()
            
            return {
                "trading_dates": {
                    "total_dates": len(all_dates),
                    "start_date": all_dates[0] if all_dates else None,
                    "end_date": all_dates[-1] if all_dates else None,
                    "date_gaps": date_gaps
                },
                "data_density": {
                    "max_records_per_day": records_per_date[0] if records_per_date else None,
                    "min_records_per_day": records_per_date[-1] if records_per_date else None,
                    "avg_records_per_day": sum(r[1] for r in records_per_date) / len(records_per_date) if records_per_date else 0
                },
                "symbol_coverage": {
                    "max_symbols_per_day": symbols_per_date[0] if symbols_per_date else None,
                    "min_symbols_per_day": symbols_per_date[-1] if symbols_per_date else None,
                    "avg_symbols_per_day": sum(r[1] for r in symbols_per_date) / len(symbols_per_date) if symbols_per_date else 0
                },
                "filter_applied": {"symbol": symbol} if symbol else None
            }
            
        except Exception as e:
            self.logger.error(f"Error getting OHLC date coverage stats: {e}")
            return {"error": str(e)}
    
    def _analyze_ohlc_date_gaps(self, dates: List[str]) -> List[Dict[str, Any]]:
        """Analyze gaps in OHLC date sequence."""
        try:
            from datetime import datetime, timedelta
            
            if len(dates) < 2:
                return []
            
            gaps = []
            for i in range(1, len(dates)):
                prev_date = datetime.strptime(dates[i-1], '%Y-%m-%d')
                curr_date = datetime.strptime(dates[i], '%Y-%m-%d')
                
                # Check for gaps (more than 1 day for weekdays, more than 3 days for weekends)
                diff = (curr_date - prev_date).days
                if diff > 3:  # Likely a gap (considering weekends)
                    gaps.append({
                        "gap_start": dates[i-1],
                        "gap_end": dates[i],
                        "days_missing": diff - 1
                    })
            
            return gaps
            
        except Exception as e:
            self.logger.error(f"Error analyzing OHLC date gaps: {e}")
            return []

    def get_multiple_instruments_data(self, instruments: List[Dict[str, str]],
                                    start_date: Optional[str] = None, 
                                    end_date: Optional[str] = None) -> Dict[str, pd.DataFrame]:
        """
        Retrieve data for multiple instruments (for backtesting).
        
        Args:
            instruments: List of dicts with symbol, exchange, timeframe
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            Dict mapping symbols to DataFrames
        """
        results = {}
        
        self.logger.info(f"Retrieving data for {len(instruments)} instruments")
        
        for instrument in instruments:
            symbol = instrument['symbol']
            exchange = instrument['exchange']
            timeframe = instrument['timeframe']
            
            df = self.get_spot_data(symbol, exchange, timeframe, start_date, end_date)
            
            if not df.empty:
                results[symbol] = df
        
        self.logger.info(f"Successfully retrieved data for {len(results)} instruments")
        return results
    
    # def get_backtest_data_from_config(self, config_path: Optional[str] = None) -> Dict[str, pd.DataFrame]:
    #     """
    #     Get data for all active instruments using config dates (for backtesting).
    #     
    #     Args:
    #         config_path: Path to Excel configuration file
    #         
    #     Returns:
    #         Dict mapping symbols to DataFrames
    #     """
    #     try:
    #         config_path = config_path or self.config_path
    #         config_loader = ConfigLoader(config_path)
    #         
    #         if not config_loader.load_config():
    #             self.logger.error("Failed to load configuration")
    #             return {}
    #         
    #         # Get date range from config
    #         init_config = config_loader.get_initialize_config()
    #         start_date = init_config.get('start date')
    #         end_date = init_config.get('end date')
    #         
    #         # Get active instruments
    #         instruments = config_loader.get_active_instruments()
    #         
    #         self.logger.info(f"Loading backtest data from {start_date} to {end_date}")
    #         
    #         return self.get_multiple_instruments_data(instruments, start_date, end_date)
    #         
    #     except Exception as e:
    #         self.logger.error(f"Error getting backtest data from config: {e}")
    #         return {}
    
    def get_data_summary(self) -> pd.DataFrame:
        """Get summary of available data in the database."""
        try:
            query = f"""
            SELECT 
                symbol,
                exchange,
                timeframe,
                COUNT(*) as total_records,
                MIN(timestamp) as start_date,
                MAX(timestamp) as end_date
            FROM {self.spot_table}
            GROUP BY symbol, exchange, timeframe
            ORDER BY symbol, timeframe
            """
            
            df = self.connection.execute(query).df()
            
            # Format dates
            if not df.empty:
                df['start_date'] = pd.to_datetime(df['start_date']).dt.date
                df['end_date'] = pd.to_datetime(df['end_date']).dt.date
            
            return df
            
        except Exception as e:
            self.logger.error(f"Error getting data summary: {e}")
            return pd.DataFrame()
        
    def get_option_premium_at_time(self, symbol: str, strike: int, option_type: str, 
                                   timestamp: datetime) -> Optional[Dict[str, Any]]:
        """
        Get option premium at specific time from database.
        
        Args:
            symbol: Underlying symbol (e.g., 'NIFTY')
            strike: Strike price
            option_type: 'CALL' or 'PUT'
            timestamp: Timestamp to query
            
        Returns:
            Dict with premium and option data or None if not found
        """
        try:
            # Determine the correct LTP column based on option type
            ltp_column = option_type
            iv_column = f"{option_type.lower()}_iv"
            delta_column = f"{option_type.lower()}_delta"
            
            # Query for exact time - filter by strike and timestamp only
            query = f"""
                SELECT 
                    {ltp_column} as premium,
                    timestamp,
                    dte,
                    expiry_date
                FROM {self.options_table}
                WHERE strike = ? AND timestamp = ?
                AND {ltp_column} IS NOT NULL AND {ltp_column} > 0
                ORDER BY expiry_date DESC
                LIMIT 1
            """
            
            result = self.connection.execute(
                query, [strike, timestamp]
            ).fetchone()
            
            if result:
                return {
                    'premium': result[0],
                    'iv': result[1],
                    'delta': result[2],
                    'timestamp': result[3],
                    'dte': result[4],
                    'expiry_date': result[5],
                    'strike': strike,
                    'option_type': option_type
                } 
            else:
                self.logger.debug(f"No premium data found for strike {strike} {option_type} at {timestamp} from database)")
                return None
            
        except Exception as e:
            self.logger.error(f"Error getting option premium at time: {e}")
            return None
    
    def get_option_candles(self, symbol: str, exchange: str,
                           timeframe: str, start_ts: str, end_ts: str) -> pd.DataFrame:
        """
        Database-agnostic candle loader.
        Fetch option candles from the DUCKDB database within a timestamp range.
        """
        # conn =  

        try:
            # 1️⃣ Validate inputs
            if not all([symbol, exchange, timeframe, start_ts, end_ts]):
                raise ValueError("Missing required query parameters")

            # 2️⃣ Get DB connection
            conn = self.connection

            query = f"""
                SELECT
                    timestamp,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    oi
                FROM nifty_expired_option
                WHERE symbol = ?
                AND exchange = ?
                AND timeframe = ?
                AND DATE(timestamp + INTERVAL '5 hours 30 minutes')
                    BETWEEN DATE '{start_ts}' AND DATE '{end_ts}'
                ORDER BY timestamp
            """

            # Works for DuckDB, SQLite, Postgres (psycopg2 uses %s — adapt in factory)
            # df = pd.read_sql_query(
            #     query,
            #     conn,
            #     params=[symbol, exchange, timeframe, start_ts, end_ts]
            # )
            # Works for DuckDB, SQLite, Postgres (psycopg2 uses %s — adapt in factory)
            # df = pd.read_sql_query(
            # query,
            # conn,
            # params=[symbol, exchange, timeframe, start_ts, end_ts]
            # )

            # ✅ DuckDB native parameter binding
            df = conn.execute(query,[symbol, exchange, timeframe]).fetchdf()

            if df.empty:
                self.logger.warning(
                    f"No candles: {symbol} {timeframe} {start_ts} → {end_ts}"
                )
            return df

        except ValueError as e:
            self.logger.error(f"❌ Input error: {e}")

        except Exception as e:
            # Catches DuckDB, SQLite, Postgres, file IO, locks, corruption
            self.logger.exception("❌ Database error while loading option candles")
        finally:
            pass

        # Engine-safe fallback
        return pd.DataFrame(
            columns=["timestamp", "open", "high", "low", "close", "volume", "oi"]
        )    
        
    def get_option_intraday_for_strike(
        self,
        strike: int,
        option_type: str,
        trade_date
    ) -> pd.DataFrame:
        """
        Fetch FULL intraday option data for a strike & date (09:15–15:30).
        - Forces SQL plan recompile (prevents silent zero-row bugs)
        - Uses timestamp RANGE filtering (index-friendly)
        - Selects correct LTP column based on CE / PE
        - Safe for LIVE + SIMULATION + REPLAY

        Args:
            strike: option strike price (int)
            option_type: "CE" or "PE"
            trade_date: date object or YYYY-MM-DD string

        Returns:
            DataFrame with columns: timestamp, strike, expiry_date, ltp
        """

        # -------------------------------
        # 1️⃣ Validate option type
        # -------------------------------
        option_type = option_type.upper()
        if option_type not in ("CE", "PE"):
            raise ValueError(f"Invalid option_type: {option_type}")

        ltp_column = "call_ltp" if option_type == "CE" else "put_ltp"

        # -------------------------------
        # 2️⃣ Normalize trade date
        # -------------------------------
        if hasattr(trade_date, "strftime"):
            date_str = trade_date.strftime("%Y-%m-%d")
        else:
            date_str = str(trade_date)

        start_ts = f"{date_str} 09:15:00"
        end_ts   = f"{date_str} 15:30:00"

        # -------------------------------
        # 3️⃣ SQL (force plan recompile)
        # -------------------------------
        # NOTE:
        #  - The SQL COMMENT with timestamp forces DuckDB to recompile
        #  - Prevents prepared-statement poisoning
        #  - Zero performance downside
        query = f"""
            SELECT
                timestamp,
                strike,
                expiry_date,
                {ltp_column} AS ltp
            FROM {self.options_table}
            WHERE strike = ?
              AND timestamp >= ?
              AND timestamp <= ?
              AND {ltp_column} IS NOT NULL
            ORDER BY timestamp
            -- force_recompile_{time.time()}
        """

        params = [strike, start_ts, end_ts]

        # -------------------------------
        # 4️⃣ Execute safely
        # -------------------------------
        try:
            df = self.connection.execute(query, params).fetchdf()

            self.logger.info(
                f"[OPTION_INTRADAY] rows={len(df)} | "
                f"strike={strike} | type={option_type} | "
                f"date={date_str}"
            )

            return df

        except Exception as e:
            self.logger.error(
                f"[OPTION_INTRADAY][ERROR] "
                f"strike={strike} type={option_type} date={date_str} | {e}",
                exc_info=True
            )
            return pd.DataFrame()
    
    def get_database_stats(self) -> Dict[str, Any]:
        """Get database statistics."""
        try:
            stats = {}
            
            # Total records
            total_records = self.connection.execute(
                f"SELECT COUNT(*) FROM {self.spot_table}"
            ).fetchone()[0]
            
            # Unique symbols
            unique_symbols = self.connection.execute(
                f"SELECT COUNT(DISTINCT symbol) FROM {self.spot_table}"
            ).fetchone()[0]
            
            # Date range
            date_range = self.connection.execute(f"""
                SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date 
                FROM {self.spot_table}
            """).fetchone()
            
            # Database file size
            db_size_mb = os.path.getsize(self.db_path) / (1024 * 1024) if os.path.exists(self.db_path) else 0
            
            stats = {
                'total_records': total_records,
                'unique_symbols': unique_symbols,
                'start_date': date_range[0] if date_range[0] else None,
                'end_date': date_range[1] if date_range[1] else None,
                'database_size_mb': round(db_size_mb, 2),
                'database_path': self.db_path
            }
            
            return stats
            
        except Exception as e:
            self.logger.error(f"Error getting database stats: {e}")
            return {}
    
    def cleanup_invalid_timestamps(self) -> int:
        """
        Remove epoch and other obviously invalid timestamps from database.
        
        Returns:
            int: Number of records deleted
        """
        try:
            # Query to identify invalid timestamps before deletion
            count_query = f"""
            SELECT COUNT(*) FROM {self.spot_table}
            WHERE timestamp <= '1970-01-01 01:00:00'
               OR timestamp IS NULL
               OR timestamp > '2040-01-01'
            """
            
            invalid_count = self.connection.execute(count_query).fetchone()[0]
            
            if invalid_count > 0:
                # Delete invalid timestamps
                cleanup_query = f"""
                DELETE FROM {self.spot_table}
                WHERE timestamp <= '1970-01-01 01:00:00'
                   OR timestamp IS NULL
                   OR timestamp > '2040-01-01'
                """
                
                self.connection.execute(cleanup_query)
                
                # Also clean up metadata for invalid timestamp ranges
                metadata_cleanup = f"""
                DELETE FROM {self.metadata_table}
                WHERE start_date <= '1970-01-01'
                   OR end_date <= '1970-01-01'
                   OR start_date > '2040-01-01'
                """
                
                self.connection.execute(metadata_cleanup)
                
                self.logger.info(f"Cleaned up {invalid_count} records with invalid timestamps (epoch, null, far future)")
            else:
                self.logger.info("No invalid timestamps found - database is clean")
            
            return invalid_count
            
        except Exception as e:
            self.logger.error(f"Error cleaning up invalid timestamps: {e}")
            return 0
    
    def cleanup_old_data(self, days_to_keep: int = 365) -> int:
        """
        Clean up old data beyond specified days.
        
        Args:
            days_to_keep: Number of days to retain
            
        Returns:
            int: Number of records deleted
        """
        try:
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            delete_query = f"""
            DELETE FROM {self.spot_table}
            WHERE timestamp < ?
            """
            
            # Get count before deletion
            count_before = self.connection.execute(
                f"SELECT COUNT(*) FROM {self.spot_table} WHERE timestamp < ?",
                [cutoff_date]
            ).fetchone()[0]
            
            # Execute deletion
            self.connection.execute(delete_query, [cutoff_date])
            
            self.logger.info(f"Cleaned up {count_before} old records (older than {days_to_keep} days)")
            return count_before
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old data: {e}")
            return 0
    
    def export_data(self, symbol: str, exchange: str, timeframe: str, 
                   output_path: str, format: str = 'csv',
                   start_date: Optional[str] = None, end_date: Optional[str] = None) -> bool:
        """
        Export data for a specific instrument.
        
        Args:
            symbol: Trading symbol
            exchange: Exchange name
            timeframe: Time interval
            output_path: Output file path
            format: Export format ('csv' or 'parquet')
            start_date: Start date in YYYY-MM-DD format (optional)
            end_date: End date in YYYY-MM-DD format (optional)
            
        Returns:
            bool: True if successful
        """
        try:
            df = self.get_spot_data(symbol, exchange, timeframe, start_date, end_date)
            
            if df.empty:
                self.logger.warning(f"No data to export for {symbol}")
                return False
            
            if format.lower() == 'csv':
                df.to_csv(output_path)
            elif format.lower() == 'parquet':
                df.to_parquet(output_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.logger.info(f"Exported {len(df)} records to {output_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error exporting data: {e}")
            return False
    
    def close(self) -> None:
        """Close database connection."""
        try:
            if self.connection:
                self.connection.close()
                self.connection = None
                self.logger.info("Database connection closed")
        except Exception as e:
            self.logger.error(f"Error closing database connection: {e}")
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def __str__(self) -> str:
        """String representation."""
        return f"DatabaseManager(db_path={self.db_path})"
    
    def __repr__(self) -> str:
        """Detailed representation."""
        stats = self.get_database_stats()
        return (f"DatabaseManager(db_path={self.db_path}, "
                f"records={stats.get('total_records', 0)}, "
                f"symbols={stats.get('unique_symbols', 0)})")
    
    @classmethod
    def reset_instance(cls):
        """Reset singleton instance (primarily for testing)."""
        with cls._lock:
            if cls._instance is not None:
                try:
                    cls._instance.close()
                except:
                    pass
            cls._instance = None
            cls._initialized = False


# Convenience functions for easy access
def get_database_manager(db_path: Optional[str] = None, config_path: Optional[str] = None) -> DatabaseManager:
    """Get the singleton database manager instance."""
    return DatabaseManager(db_path, config_path)


def get_quick_data(symbol: str, timeframe: str = '5m', days: int = 30) -> pd.DataFrame:
    """Quick data retrieval for common use cases."""
    from datetime import datetime, timedelta
    
    db_manager = get_database_manager()
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    return db_manager.get_spot_data(
        symbol=symbol,
        exchange='NSE_INDEX',
        timeframe=timeframe,
        start_date=start_date.strftime('%Y-%m-%d'),
        end_date=end_date.strftime('%Y-%m-%d')
    )

def extract_strike_from_symbol(symbol: str) -> int:
    """
    Extract strike from option symbol.
    Example: NIFTY20JAN2625550PE -> 25550
    """
    symbol = symbol.upper()

    if not symbol.endswith(("CE", "PE")):
        raise ValueError(f"Invalid option symbol: {symbol}")

    core = symbol[:-2]  # remove CE / PE

    # Find last month occurrence (JAN, FEB, ...)
    months = ("JAN","FEB","MAR","APR","MAY","JUN",
              "JUL","AUG","SEP","OCT","NOV","DEC")

    month_pos = -1
    for m in months:
        pos = core.rfind(m)
        if pos > month_pos:
            month_pos = pos

    if month_pos == -1:
        raise ValueError(f"Cannot locate expiry month in {symbol}")

    # Expiry format = DDMMMYY → strike starts after that
    strike_part = core[month_pos + 5:]  # 3 (MMM) + 2 (YY)

    if not strike_part.isdigit():
        raise ValueError(f"Invalid strike section: {strike_part}")

    return int(strike_part)


# Global instance for backward compatibility
database_manager = get_database_manager()

if __name__ == "__main__":
    # Simple test of DatabaseManager functionality
    db_manager = get_database_manager()

    df = db_manager.get_ohlcv_data('BANKNIFTY', 'NSE_INDEX', '1m', '2026-01-16', '2026-01-16', instrument_type="SPOT")
    # df = db_manager.get_ohlcv_data(symbol='NIFTY20JAN2625650PE', exchange='NFO', timeframe='D', start_date='2025-12-01', end_date='2026-01-15', instrument_type="OPTIONS", underlying_symbol="NIFTY", expiry_date="20-01-2026")
    print(df.count())