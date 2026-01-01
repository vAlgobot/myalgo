"""
OpenAlgo Dynamic Market Data Dispatcher
---------------------------------------

Author: OpenAlgo Core Engineering
Role  : Lead Algo Trading Systems Engineer

Supports:
- SPOT + OPTIONS
- LIVE + BACKTESTING
- DuckDB backend
- Gap validation & patching
"""

from datetime import datetime, timedelta
from typing import List, Dict
import sys
import pandas as pd
from openalgo import api
import time
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger
from db.database_main import get_database_manager

DB_MANAGER = get_database_manager()
logger = get_logger("market_data_dispatcher")

# =====================================================================
# 🔹 GLOBAL CONFIGURATION (DRIVER VARIABLES)
# =====================================================================

MODE = "LIVE"                  # LIVE | BACKTESTING
INSTRUMENT_TYPE = "OPTIONS"       # SPOT | OPTIONS
SYMBOL = ["NIFTY","BANKNIFTY","SENSEX"]  # For SPOT mode ["NIFTY","BANKNIFTY","SENSEX"]
SPOT_EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
TIMEFRAMES = ["1m", "5m", "D"]  # List of timeframes to fetch

#---Backtesting configuration-----#
START_DATE = "2024-01-01"
END_DATE = "2024-01-31"
EXCHANGE = "NSE_INDEX"

#---Openalgo api-----#
API_KEY = "6fb3e0c7b256b90192a29e6592e363510d055f70cde5aea43bed88145d00637e"
API_HOST = "https://vbot.vralgo.com/"
WS_URL = "wss://vbot.vralgo.com/ws"
MARKET_DATE = None


client = client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)  # Placeholder for your market data API client

class MarketDataDispatcher:

    def __init__(self):
        self.mode = MODE.upper()
        self.instrument_type = INSTRUMENT_TYPE.upper()
        self.symbol = SYMBOL
        self.spot_exchange = SPOT_EXCHANGE
        self.option_exchange = OPTION_EXCHANGE
        self.timeframes = TIMEFRAMES
        self.start_date = START_DATE
        self.end_date = END_DATE
        self.client = client

        # ==============================
        # 🧠 Institutional Caches
        # ==============================
        self.expiry_cache = {}   # { "NIFTY": "30DEC25", "BANKNIFTY": "30DEC25" }
        self.strike_cache = {}   # { ("NIFTY", "2025-12-01"): [22000, 22050, ...] }


        logger.info(
            f"[INIT] MODE={self.mode} | "
            f"INSTRUMENT={self.instrument_type} | "
            f"SYMBOL={self.symbol}")
    
    def retry_api_call(self, func, max_retries=3, delay=2, description="API call", *args, **kwargs):
        """
        Generic retry mechanism for API calls.
        Retries the given function up to max_retries times if it fails or returns invalid/empty data.
        """
        for attempt in range(1, max_retries + 1):
            try:
                result = func(*args, **kwargs)

                # ✅ Validate order response
                if "order" in description.lower():
                    if not result or "orderid" not in result or not result.get("status") == "success":
                        logger.warning(f"❌ Missing orderid on attempt {attempt}")
                        logger.warning(f"❌ ORDER RESPONSE {result}")
                        continue  
                    order_id = result["orderid"] 

                    # ✅ Confirm executed price using get_executed_price 
                    if "sl-order" not in description.lower():
                        exec_price = None
                        exec_price = self.get_executed_price(order_id)
                        if exec_price is not None and exec_price > 0:
                            logger.info(f"✅ Executed price confirmed (order {order_id}) = {exec_price}")    
                            result["exec_price"] = exec_price
                        else:
                            logger.error(f"❌ Executed price not confirmed on attempt {attempt}")
                            continue

                if isinstance(result, pd.DataFrame) and not result.empty:
                    logger.info(f"✅ {description} succeeded on attempt {attempt}")
                    return result

                # ✅ Validate non-empty dict response
                if isinstance(result, dict) and result.get("data"):
                    logger.info(f"✅ {description} (dict) succeeded on attempt {attempt}")
                    return result

                # ✅ Case 3: Analyze mode or success response
                if isinstance(result, dict) and result.get("status") == "success":
                    logger.info(f"✅ {description} (analyze/success mode) succeeded on attempt {attempt}")
                    return result

                logger.warning(f"⚠️ {description} returned empty on attempt {attempt}")
            except Exception as e:
                logger.warning(f"⚠️ {description} failed on attempt {attempt}: {e}")

            if attempt < max_retries:
                time.sleep(delay)

        logger.error(f"❌ {description} failed after {max_retries} attempts")
        return None
    
    def normalize_df(self, raw):
        """Normalize DataFrame"""
        if isinstance(raw, pd.DataFrame):
            df = raw.copy()
        elif isinstance(raw, dict) and "data" in raw:
            df = pd.DataFrame(raw["data"])
        else:
            df = pd.DataFrame()

        if df.empty:
            logger.warning(f"No data received for normalization")
            return df

        # Coerce data types
        for c in ["open", "high", "low", "close", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # Set timestamp index
        if "timestamp" in df.columns:
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df.set_index("timestamp", inplace=True)
            df.sort_index(inplace=True)

        return df
    
    def parse_option_symbol(self, option_symbol: str):
        """
        Parse option symbol into underlying, expiry, strike, option_type.

        Example:
        NIFTY30DEC2522000CE
        -> underlying = NIFTY
        -> expiry = 30DEC25
        -> strike = 22000
        -> type = CE
        """
        try:
            option_symbol = option_symbol.upper()

            if option_symbol.endswith("CE"):
                opt_type = "CE"
                core = option_symbol[:-2]
            elif option_symbol.endswith("PE"):
                opt_type = "PE"
                core = option_symbol[:-2]
            else:
                raise ValueError("Not an option symbol")

            # Underlying is leading alphabets
            i = 0
            while core[i].isalpha():
                i += 1

            underlying = core[:i]
            rest = core[i:]

            # Expiry = next 7 chars (e.g., 30DEC25)
            expiry = rest[:7]
            expiry_dt = datetime.strptime(expiry, "%d%b%y").strftime("%d-%m-%Y")
            strike = int(rest[7:])

            return underlying, expiry_dt, strike, opt_type

        except Exception as e:
            logger.error(f"[PARSE] Failed to parse option symbol {option_symbol}: {e}")
            return None, None, None, None

    def get_expiry_cached(
        self,
        symbol: str,
        exchange: str,
        instrumenttype: str
    ) -> str:
        """
        Return nearest weekly expiry using OpenAlgo Expiry API.
        Cache is locked per symbol per trading day to avoid expiry mixing.
        """

        today = datetime.now().strftime("%Y-%m-%d")

        # -------------------------------------------------
        # Reset cache once per trading day
        # -------------------------------------------------
        if getattr(self, "_expiry_cache_date", None) != today:
            self.expiry_cache = {}
            self._expiry_cache_date = today
            logger.info("[CACHE] Expiry cache reset for new trading day")

        # -------------------------------------------------
        # Cache key (symbol is enough, exchange/type included for safety)
        # -------------------------------------------------
        cache_key = f"{symbol.upper()}_{exchange.upper()}_{instrumenttype.lower()}"

        if cache_key in self.expiry_cache:
            return self.expiry_cache[cache_key]

        # -------------------------------------------------
        # Call OpenAlgo Expiry API
        # -------------------------------------------------
        try:
            response = self.retry_api_call(
                self.client.expiry,
                description="OpenAlgo Expiry API",
                symbol=symbol,
                exchange=exchange,
                instrumenttype=instrumenttype
            )

            if not response or "data" not in response:
                raise ValueError("Invalid expiry API response")

            expiries = response["data"]

            if not isinstance(expiries, list) or not expiries:
                raise ValueError("Empty expiry list received")

            # -------------------------------------------------
            # Filter future expiries
            # -------------------------------------------------
            today_dt = datetime.now().date()
            valid_expiries = []

            for exp in expiries:
                try:
                    exp_dt = datetime.strptime(exp, "%d-%b-%y").date()
                    if exp_dt >= today_dt:
                        valid_expiries.append(exp_dt)
                except Exception:
                    continue

            if not valid_expiries:
                raise ValueError("No valid future expiries found")

            # -------------------------------------------------
            # Pick nearest expiry
            # -------------------------------------------------
            nearest_expiry_dt = min(valid_expiries)
            nearest_expiry = nearest_expiry_dt.strftime("%d%b%y").upper()

            # -------------------------------------------------
            # Cache & return
            # -------------------------------------------------
            self.expiry_cache[cache_key] = nearest_expiry
            logger.info(
                f"[CACHE] Expiry locked for {symbol} "
                f"({exchange}, {instrumenttype}): {nearest_expiry}"
            )

            return nearest_expiry

        except Exception as e:
            # -------------------------------------------------
            # HARD FALLBACK (FAIL-SAFE)
            # -------------------------------------------------
            fallback = datetime.now().strftime("%d%b%y").upper()
            logger.error(
                f"[CACHE][EXPIRY] API failed for {symbol} "
                f"({exchange}, {instrumenttype}) | "
                f"fallback used: {fallback} | {e}"
            )

            self.expiry_cache[cache_key] = fallback
            return fallback
    
    def get_strikes_cached(self, symbol, high, low, trade_date):
        """
        Build & cache strike ladder per symbol per day.
        Ensures all timeframes use identical strikes.
        """
        key = (symbol, trade_date)

        if key in self.strike_cache:
            return self.strike_cache[key]

        interval = 50 if symbol == "NIFTY" else 100

        base_low = int(low // interval * interval)
        base_high = int(high // interval * interval)

        strikes = list(range(base_low, base_high + interval, interval))

        strikes = (
            [strikes[0] - i * interval for i in range(5, 0, -1)] +
            strikes +
            [strikes[-1] + i * interval for i in range(1, 6)]
        )

        self.strike_cache[key] = strikes

        logger.info(f"[CACHE] Strike ladder locked for {symbol} {trade_date} → {len(strikes)} strikes")
        return strikes

    def generate_option_symbols(
        self,
        symbol: str,
        day_high: float,
        day_low: float,
        expiry: str
    ) -> List[str]:
        """Generate option symbols based on day range + buffer."""
        symbol = symbol.upper()

        strike_interval = 50 if symbol == "NIFTY" else 100

        base_low = int(day_low // strike_interval * strike_interval)
        base_high = int(day_high // strike_interval * strike_interval)

        strikes = list(range(base_low, base_high + strike_interval, strike_interval))

        # Buffer
        strikes = (
            [strikes[0] - i * strike_interval for i in range(5, 0, -1)] +
            strikes +
            [strikes[-1] + i * strike_interval for i in range(1, 6)]
        )

        option_symbols = []
        for strike in strikes:
            option_symbols.append(f"{symbol}{expiry}{strike}CE")
            option_symbols.append(f"{symbol}{expiry}{strike}PE")

        logger.info(
            f"[OPTIONS] Generated {len(option_symbols)} option symbols "
            f"({symbol}, strikes={len(strikes)})"
        )

        return option_symbols

    def fetch_and_store_spot_live(self, symbols: List[str], exchange: str, timeframes: List[str], market_date: str = None):
        """Fetch and store spot market data for LIVE mode."""
        if market_date is None:
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = today
            end_date = today
        else:
            start_date = market_date
            end_date = market_date

        for symbol in symbols:
            for tf in timeframes:
                logger.info(f"[LIVE][SPOT] Fetching {symbol} {tf}")
                exchange = "BSE_INDEX" if symbol.upper() == "SENSEX" else exchange
                df = self.retry_api_call(
                    self.client.history,
                    symbol=symbol,
                    exchange=exchange,
                    interval=tf,
                    start_date=start_date,
                    end_date=end_date
                )

                if df is None or df.empty:
                    logger.warning(f"[LIVE][SPOT] Empty API data: {symbol} {tf}")
                    continue

                df = self.normalize_df(df)

                DB_MANAGER.store_ohlcv_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=tf,
                    data=df,
                    instrument_type="SPOT"
                )

                db_df = DB_MANAGER.get_ohlcv_data(
                    symbol, exchange, tf, start_date, end_date, "SPOT"
                )

                logger.info(
                    f"[LIVE][SPOT] {symbol} {tf} {start_date} | "
                    f"Inserted={len(df)} | DB={len(db_df)}"
                )

    def fetch_and_store_options_live(self, symbol: str, exchange: str, timeframes: List[str], market_date: str = None):
        """Fetch and store option market data for LIVE mode."""
        if market_date is None:
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = today
            end_date = today
        else:
            start_date = market_date
            end_date = market_date

        # ---- Step 1: Fetch daily SPOT candle
        day_df = self.retry_api_call(
            self.client.history,
            symbol=symbol,
            exchange=exchange,
            interval="D",
            start_date=start_date,
            end_date=end_date
        )

        if day_df is None or day_df.empty:
            logger.error(f"[LIVE][OPTIONS] No day candle for compute [SPOT] strike {symbol}")
            sys.exit(1)

        day_high = day_df["high"].iloc[0]
        day_low = day_df["low"].iloc[0]

        # ---- Step 2: Expiry (nearest weekly) Expiry (cached, locked)
        expiry = self.get_expiry_cached(symbol, self.option_exchange, "options")

        # # ---- Step 3: Generate symbols
        option_symbols = self.generate_option_symbols(
             symbol, day_high, day_low, expiry
        )
        opt_exchange = "BFO" if symbol.upper() == "SENSEX" else self.spot_exchange
        # ---- Step 4: Fetch option candles
        for opt_symbol in option_symbols:
            for tf in timeframes:
                logger.info(f"[LIVE][OPTIONS] {opt_symbol} {tf}")

                df = self.retry_api_call(
                    self.client.history,
                    symbol=opt_symbol,
                    exchange=opt_exchange,
                    interval=tf,
                    start_date=start_date,
                    end_date=end_date
                )

                if df is None or df.empty:
                    logger.warning(f"[LIVE][OPTIONS] Empty: {opt_symbol} {tf}")
                    continue
                
                df = self.normalize_df(df)
                #-----Get expiry strike opt_type underlying----#
                underlying, expiry_str, strike, opt_type = self.parse_option_symbol(opt_symbol)
                df["underlying"] = underlying
                df["expiry"] = expiry_str
                df["strike"] = strike
                df["opt_type"] = opt_type

                DB_MANAGER.store_ohlcv_data(
                    symbol=opt_symbol,
                    exchange=self.option_exchange,
                    timeframe=tf,
                    data=df,
                    instrument_type="OPTIONS"
                )

                db_df = DB_MANAGER.get_ohlcv_data(
                    opt_symbol, self.option_exchange, tf, start_date, end_date, "OPTIONS"
                )

                logger.info(
                    f"[LIVE][OPTIONS] {opt_symbol} {tf} {start_date} | "
                    f"Inserted={len(df)} | DB={len(db_df)}"
                )

    def backtest_data_loader_with_gap_validation(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str
    ) -> pd.DataFrame:
        """Universal backtest loader with gap detection & patching."""
        logger.info(
            f"[BACKTEST] Loading {symbol} {timeframe} "
            f"{start_date} → {end_date}"
        )

        df = DB_MANAGER.get_ohlcv_data(
            symbol, exchange, timeframe,
            start_date, end_date,
            instrument_type
        )

        # ---- Case 1: DB empty
        if df.empty:
            logger.warning(f"[BACKTEST] DB EMPTY → API FETCH")

            api_df = self.retry_api_call(
                self.client.history,
                symbol=symbol,
                exchange=exchange,
                interval=timeframe,
                start_date=start_date,
                end_date=end_date
            )

            if api_df is None or api_df.empty:
                logger.error(f"[BACKTEST][FATAL] API EMPTY → STOP BOT")
                sys.exit(1)

            api_df = self.normalize_df(api_df)

            DB_MANAGER.store_ohlcv_data(
                symbol, exchange, timeframe,
                api_df, instrument_type=instrument_type
            )

            df = DB_MANAGER.get_ohlcv_data(
                symbol, exchange, timeframe,
                start_date, end_date, instrument_type
            )

            logger.info(f"[BACKTEST] Loaded {len(df)} rows")
            return df

        # ---- Case 2: Validate gaps
        logger.info("[BACKTEST] Validating date completeness")

        api_daily = self.retry_api_call(
            self.client.history,
            symbol=symbol,
            exchange=exchange,
            interval="D",
            start_date=start_date,
            end_date=end_date
        )

        if api_daily is None or api_daily.empty:
            logger.error("[BACKTEST] Cannot validate gaps (daily API empty)")
            sys.exit(1)

        api_dates = set(pd.to_datetime(api_daily["timestamp"]).dt.date)
        db_dates = set(df.index.normalize().date)

        missing_dates = sorted(api_dates - db_dates)

        if missing_dates:
            logger.warning(f"[BACKTEST] Missing dates detected: {missing_dates}")

        for miss_date in missing_dates:
            miss_date_str = miss_date.strftime("%Y-%m-%d")

            logger.info(
                f"[BACKTEST] Patching {symbol} {timeframe} {miss_date_str}"
            )

            patch_df = self.retry_api_call(
                self.client.history,
                symbol=symbol,
                exchange=exchange,
                interval=timeframe,
                start_date=miss_date_str,
                end_date=miss_date_str
            )

            if patch_df is None or patch_df.empty:
                logger.error(
                    f"[BACKTEST][FATAL] Missing intraday data: {miss_date_str}"
                )
                sys.exit(1)

            patch_df = self.normalize_df(patch_df)

            DB_MANAGER.store_ohlcv_data(
                symbol, exchange, timeframe,
                patch_df, instrument_type=instrument_type
            )

        df = DB_MANAGER.get_ohlcv_data(
            symbol, exchange, timeframe,
            start_date, end_date, instrument_type
        )

        logger.info(f"[BACKTEST] Final rows={len(df)}")
        return df

    def run(self):
        """Main dispatcher entry point."""
        logger.info(
            f"[DISPATCHER] MODE={self.mode} | "
            f"INSTRUMENT={self.instrument_type} | SYMBOL={self.symbol}"
        )

        if self.mode == "LIVE":
            self.fetch_and_store_spot_live(
                symbols= self.symbol,
                exchange= self.spot_exchange,
                timeframes=self.timeframes,
                market_date=MARKET_DATE
                )
            for sym in self.symbol: #----Option data fetching---#
                exchange = "BSE_INDEX" if sym.upper() == "SENSEX" else exchange
                self.fetch_and_store_options_live(
                    symbol= sym,
                    exchange=exchange,
                    timeframes=self.timeframes,
                    market_date=MARKET_DATE
                    )
                
        elif self.mode == "BACKTESTING":
            if self.instrument_type == "SPOT":
                for sym in self.symbol:
                    exchange = "BSE_INDEX" if sym.upper() == "SENSEX" else self.spot_exchange
                    for tf in self.timeframes:
                        self.backtest_data_loader_with_gap_validation(
                            symbol=sym,
                            exchange=exchange,
                            timeframe=tf,
                            start_date=self.start_date,
                            end_date=self.end_date,
                            instrument_type=self.instrument_type
                        )
            elif self.instrument_type == "OPTIONS":
                for tf in self.timeframes:
                    self.backtest_data_loader_with_gap_validation(
                        symbol=self.symbol,
                        exchange=self.option_exchange,
                        timeframe=tf,
                        start_date=self.start_date,
                        end_date=self.end_date,
                        instrument_type=self.instrument_type
                    )
        else:
            raise ValueError(f"Invalid MODE: {self.mode}")

if __name__ == "__main__":
    MarketDataDispatcher().run()
    logger.info("Dispatched completed")


#                                ┌────────────────────────────┐
#                                │        __main__             │
#                                │  MarketDataDispatcher.run() │
#                                └───────────────┬────────────┘
#                                                │
#                                                ▼
#                           ┌────────────────────────────┐
#                           │       MODE SELECTOR         │
#                           │   LIVE   |   BACKTESTING    │
#                           └───────────────┬────────────┘
#                                           │
#              ┌────────────────────────────┴─────────────────────────────┐
#              │                                                            │
#              ▼                                                            ▼
# ┌──────────────────────────────┐                          ┌──────────────────────────────┐
# │          LIVE MODE            │                          │       BACKTEST MODE           │
# └───────────────┬──────────────┘                           └───────────────┬──────────────┘
#                 │                                                            │
#                 ▼                                                            ▼
#       ┌────────────────────┐                                 ┌──────────────────────────┐
#       │ Instrument Selector │                                 │   DB Data Availability    │
#       │  SPOT   |  OPTIONS   │                                 │      Checker              │
#       └──────────┬──────────┘                                 └────────────┬─────────────┘
#                  │                                                            │
#         ┌────────┴─────────┐                                       ┌─────────▼─────────┐
#         │                  │                                       │                   │
#         ▼                  ▼                                       ▼                   ▼
# ┌────────────────┐   ┌──────────────────────┐            ┌────────────────┐    ┌───────────────────┐
# │   SPOT LIVE     │   │     OPTIONS LIVE      │            │ DB Has Data     │    │ DB Is Empty       │
# └───────┬────────┘   └────────────┬──────────┘            └─────────┬────────┘    └─────────┬─────────┘
#         │                         │                                   │                         │
#         ▼                         ▼                                   ▼                         ▼
# ┌────────────────┐     ┌──────────────────────┐        ┌────────────────────┐      ┌────────────────────┐
# │ For each SYMBOL │     │ Fetch SPOT Day Candle │        │ Validate Data Gaps  │      │ Fetch Full Range   │
# │ For each TF     │     │ (Daily)               │        │ via API (Daily)     │      │ from API           │
# └───────┬────────┘     └────────────┬──────────┘        └─────────┬──────────┘      └─────────┬──────────┘
#         │                          │                                    │                          │
#         ▼                          ▼                                    ▼                          ▼
# ┌────────────────┐     ┌──────────────────────┐        ┌────────────────────┐      ┌────────────────────┐
# │ Fetch from API │     │ Compute High & Low    │        │ Missing Dates?      │      │ Normalize Data     │
# │ (retry_api)    │     └────────────┬──────────┘        └─────────┬──────────┘      └─────────┬──────────┘
# └───────┬────────┘                  │                                   │                          │
#         │                           ▼                              Yes   │   No                     ▼
#         ▼               ┌──────────────────────┐               ┌────────▼────────┐       ┌────────────────────┐
# ┌────────────────┐      │ Generate Strike Grid │               │ Patch Missing     │       │ Store into DuckDB   │
# │ Normalize Data │      │ ±5 strikes            │               │ Dates via API     │       └─────────┬──────────┘
# └───────┬────────┘      └────────────┬──────────┘               └─────────┬────────┘                 │
#         │                           │                                   │                            ▼
#         ▼                           ▼                                   ▼                   ┌────────────────────┐
# ┌────────────────┐      ┌──────────────────────┐        ┌────────────────────┐      │ Re-query Database   │
# │ Store into DB  │      │ Build Option Symbols  │        │ Insert into DB      │      │ & Return Data      │
# │ DuckDB         │      └────────────┬──────────┘        └─────────┬──────────┘      └────────────────────┘
# └───────┬────────┘                   │                                   │
#         │                            ▼                                   ▼
#         ▼                 ┌──────────────────────┐        ┌────────────────────┐
# ┌────────────────┐        │ For each Option       │        │ Re-query Final DB   │
# │ Re-query DB    │        │ For each Timeframe    │        │ Dataset             │
# └───────┬────────┘        └────────────┬──────────┘        └─────────┬──────────┘
#         │                            │                                   │
#         ▼                            ▼                                   ▼
# ┌────────────────┐        ┌──────────────────────┐        ┌────────────────────┐
# │ Log Inserted   │        │ Fetch API → Normalize │        │ Return Clean Data   │
# │ vs DB Count   │        │ → Store → Verify DB    │        │ to Strategy Engine  │
# └────────────────┘        └──────────────────────┘        └────────────────────┘
