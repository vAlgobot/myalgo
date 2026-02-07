"""
NSE Historical Option Data Ingestor
----------------------------------

Purpose
-------
This script downloads daily historical option data (D timeframe) from the
NSE public historical endpoint and writes normalized rows into a DuckDB
table named `options_data_master`.

Features
--------
- Uses a requests.Session with a browser-like User-Agent and Referer to
    perform a lightweight handshake with NSE and avoid trivial blocks.
- Reads the list of expiries and strikes to process from the existing
    `options_data_master` table, then fetches historical CSV/JSON data for
    each contract (both CE and PE) over a configurable lookback window.
- Normalizes numeric fields (open, high, low, close, volume, oi) with
    robust cleaning and converts timestamps to pandas datetime.
- Inserts records into DuckDB using a temporary registered dataframe and
    an INSERT ... SELECT query that guards against duplicate rows
    (ON CONFLICT DO NOTHING semantics expected on the target table).

Configuration
-------------
- `DB_PATH`: path to DuckDB database file.
- `UNDERLYING_SYMBOL`: underlying index/symbol (e.g., NIFTY).
- `LOOKBACK_DAYS`: number of days of history to request up to expiry.
- `BASE_URL` / `REFERER_URL`: NSE endpoint and referer header.

Assumptions / Requirements
--------------------------
- DuckDB is used as the local store and the `options_data_master` table
    already exists with columns: symbol, exchange, underlying, expiry,
    strike, timeframe, timestamp, open, high, low, close, volume, oi, iv.
- The script expects expiry strings in the DB in the format used by the
    repository (e.g. '02DEC25') and converts them to the format NSE
    expects for the API call.
- The NSE API occasionally blocks requests; a session handshake
    (`init_nse_session`) is included to warm up cookies before fetches.

Usage
-----
Run this script directly or import `NSEHistoricalIngestor` and call
`run()` from another orchestrator. It performs light rate limiting
between calls and logs progress to stdout.

Error handling & logging
------------------------
- Network or parsing errors are logged; the script attempts a session
    refresh on 401/403 responses.
- Failures to insert or normalize a contract are logged and do not stop
    the entire ingestion run.

Note on idempotency
-------------------
The insertion SQL uses an `ON CONFLICT DO NOTHING` clause (assumes the
underlying DuckDB table enforces a uniqueness constraint on
(symbol, exchange, timeframe, timestamp)). This keeps the run idempotent
for previously-ingested rows.
"""

import os
import logging
import requests
import duckdb
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# CONFIGURATION
# =============================================================================
DB_PATH = r"C:\Users\Srivi\myalgo_openalgo\myalgo\data\market_data.db"
UNDERLYING_SYMBOL = "NIFTY"
LOOKBACK_DAYS = 60
BASE_URL = "https://www.nseindia.com/api/historicalOR/foCPV"
REFERER_URL = "https://www.nseindia.com/"

# Optional date-range filter (inclusive). Set to None to process all dates.
# Acceptable formats: "DD-MM-YYYY" or "YYYY-MM-DD"
START_DATE: Optional[str] = "2026-01-01"
END_DATE: Optional[str] = "2026-02-06"

# MAPPING: NSE JSON Field -> DuckDB Column
# Note: symbol, exchange, timeframe, timestamp are constructed/transformed
FIELD_MAPPING = {
    "FH_OPENING_PRICE": "open",
    "FH_TRADE_HIGH_PRICE": "high",
    "FH_TRADE_LOW_PRICE": "low",
    "FH_CLOSING_PRICE": "close",
    "FH_TOT_TRADED_QTY": "volume",
    "FH_OPEN_INT": "oi"
}

# Logging Setup
def setup_logger():
    logger = logging.getLogger("nse_ingestion")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

logger = setup_logger()

class NSEHistoricalIngestor:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": REFERER_URL
        })
        self.conn = None

    def connect_db(self):
        """Establish DuckDB connection."""
        try:
            self.conn = duckdb.connect(self.db_path)
            logger.info(f"Connected to DuckDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def close_db(self):
        """Close DuckDB connection."""
        if self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed.")

    def init_nse_session(self):
        """Perform initial handshake with NSE to get cookies."""
        try:
            logger.info("Initializing NSE session (handshake)...")
            self.session.get(REFERER_URL, timeout=10)
            time.sleep(1) # Small buffer
        except Exception as e:
            logger.error(f"NSE handshake failed: {e}")
            raise

    def get_expiries_and_strikes(self, underlying: str) -> Dict[str, List[int]]:
        """Fetch unique expiries and their strikes from DB."""
        try:
            # Build optional date filter based on global START_DATE/END_DATE
            date_filter = ""
            if START_DATE and END_DATE:
                # Accept DD-MM-YYYY or YYYY-MM-DD input formats
                def _norm(dstr):
                    for fmt in ("%d-%m-%Y", "%Y-%m-%d"):
                        try:
                            return datetime.strptime(dstr, fmt).strftime("%Y-%m-%d")
                        except Exception:
                            continue
                    raise ValueError(f"Unsupported date format: {dstr}")

                try:
                    s = _norm(START_DATE)
                    e = _norm(END_DATE)
                    # Use DATE(timestamp) for safe date-only comparison in DuckDB
                    date_filter = f"AND DATE(timestamp) BETWEEN DATE '{s}' AND DATE '{e}'"
                    logger.info(f"Applying date filter: {s} -> {e}")
                except Exception as ex:
                    logger.warning(f"Invalid START_DATE/END_DATE provided: {ex} - ignoring date filter")

            query = f"""
            SELECT expiry, list(distinct strike) as strikes
            FROM options_data_master
            WHERE underlying = '{underlying}' {date_filter}
            GROUP BY expiry
            """
            result = self.conn.execute(query).fetchall()
            # Convert list of tuples to dict: {expiry: [strike1, strike2, ...]}
            data = {row[0]: sorted(row[1]) for row in result}
            logger.info(f"Discovered {len(data)} expiries from database.")
            return data
        except Exception as e:
            logger.error(f"Failed to fetch expiries/strikes: {e}")
            return {}

    def fetch_nse_data(self, expiry: str, strike: int, option_type: str) -> Optional[List[Dict]]:
        """Fetch data from NSE API for a specific contract."""
        try:
            # Parse expiry string (format in DB: DDMMMYY e.g. 02DEC25)
            expiry_dt = datetime.strptime(expiry, "%d%b%y")
            
            # Construct params
            to_date = expiry_dt.strftime("%d-%m-%Y")
            from_date = (expiry_dt - timedelta(days=LOOKBACK_DAYS)).strftime("%d-%m-%Y")
            year = expiry_dt.strftime("%Y")
            formatted_expiry_nse = expiry_dt.strftime("%d-%b-%Y").upper() # NSE expects 02-DEC-2025

            params = {
                "from": from_date,
                "to": to_date,
                "instrumentType": "OPTIDX",
                "symbol": UNDERLYING_SYMBOL,
                "year": year,
                "expiryDate": formatted_expiry_nse,
                "optionType": option_type,
                "strikePrice": str(strike)
            }

            logger.info(f"Fetching {UNDERLYING_SYMBOL} {expiry} {strike}{option_type} | Range: {from_date} to {to_date}")
            
            response = self.session.get(BASE_URL, params=params, timeout=15)
            
            if response.status_code == 401 or response.status_code == 403:
                logger.warning("NSE blocking/Unauthorized. Re-initializing session...")
                self.init_nse_session()
                response = self.session.get(BASE_URL, params=params, timeout=15)

            if response.status_code != 200:
                logger.error(f"Failed to fetch data from NSE: HTTP {response.status_code}")
                return None

            json_data = response.json()
            return json_data.get("data", [])

        except Exception as e:
            logger.error(f"Error during NSE fetch: {e}")
            return None

    @staticmethod
    def _clean_numeric(value, to_type=float):
        """Clean string values (remove commas) and convert to numeric type."""
        if value is None or value == "":
            return 0
        if isinstance(value, (int, float)):
            return to_type(value)
        try:
            # Remove commas and convert
            clean_val = str(value).replace(",", "").strip()
            return to_type(float(clean_val)) if clean_val else 0
        except (ValueError, TypeError):
            logger.warning(f"Failed to convert value '{value}' to {to_type}")
            return 0

    def process_and_insert(self, nse_data: List[Dict], expiry: str, strike: int, option_type: str):
        """Normalize NSE data and insert into DuckDB."""
        if not nse_data:
            logger.warning(f"No data points found for {strike}{option_type}")
            return

        try:
            # Prepare rows for DataFrame
            rows = []
            symbol = f"{UNDERLYING_SYMBOL}{expiry}{strike}{option_type}"
            
            for item in nse_data:
                # Map fields with robust numeric cleaning
                row = {
                    "symbol": symbol,
                    "exchange": "NFO",
                    "underlying": UNDERLYING_SYMBOL,
                    "expiry": expiry,
                    "strike": int(strike),
                    "timeframe": "D",
                    "timestamp": item.get("FH_TIMESTAMP_ORDER"), # ISO format from NSE
                    "open": self._clean_numeric(item.get("FH_OPENING_PRICE"), float),
                    "high": self._clean_numeric(item.get("FH_TRADE_HIGH_PRICE"), float),
                    "low": self._clean_numeric(item.get("FH_TRADE_LOW_PRICE"), float),
                    "close": self._clean_numeric(item.get("FH_CLOSING_PRICE"), float),
                    "volume": self._clean_numeric(item.get("FH_TOT_TRADED_QTY"), int),
                    "oi": self._clean_numeric(item.get("FH_OPEN_INT"), int),
                    "iv": 0
                }
                rows.append(row)

            df = pd.DataFrame(rows)
            
            # Ensure timestamp is datetime
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            
            # Insert into DuckDB
            self.conn.register("temp_nse_df", df)
            
            query = """
            INSERT INTO options_data_master 
            (symbol, exchange, underlying, expiry, strike, timeframe, timestamp, 
             open, high, low, close, volume, oi, iv)
            SELECT 
                symbol, exchange, underlying, expiry, strike, timeframe, timestamp, 
                open, high, low, close, volume, oi, iv
            FROM temp_nse_df
            ON CONFLICT (symbol, exchange, timeframe, timestamp) DO NOTHING;
            """
            
            self.conn.execute(query)
            inserted_count = self.conn.execute("SELECT count(*) FROM temp_nse_df").fetchone()[0]
            
            logger.info(f"✅ Logged {inserted_count} rows for {symbol}")
            self.conn.unregister("temp_nse_df")

        except Exception as e:
            logger.error(f"Failed to process and insert data: {e}")

    def run(self):
        """Main execution loop."""
        try:
            self.connect_db()
            self.init_nse_session()
            
            data_map = self.get_expiries_and_strikes(UNDERLYING_SYMBOL)
            
            if not data_map:
                logger.warning("No expiries/strikes found to process. Exiting.")
                return

            for expiry, strikes in data_map.items():
                logger.info(f"🚀 Processing Expiry: {expiry} ({len(strikes)} strikes)")
                
                for strike in strikes:
                    for opt_type in ["CE", "PE"]:
                        # Fetch
                        nse_data = self.fetch_nse_data(expiry, strike, opt_type)
                        
                        # Process & Insert
                        self.process_and_insert(nse_data, expiry, strike, opt_type)
                        
                        # Avoid aggressive rate limiting
                        time.sleep(0.1) 

            logger.info("🏁 NSE Historical Ingestion Complete.")

        except Exception as e:
            logger.error(f"Critical error in execution: {e}")
        finally:
            self.close_db()

if __name__ == "__main__":
    ingestor = NSEHistoricalIngestor(DB_PATH)
    ingestor.run()
