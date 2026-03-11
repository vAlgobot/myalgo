"""
OpenAlgo Dynamic Market Data Dispatcher
---------------------------------------

- LIVE MODE MARKET_DATE IS NONE WE CAN DOWNLAOD CURRENT DAY DATA ELSE WE CAN DOWNLOAD DATA FOR THE GIVEN MARKET_DATE
"""
from datetime import datetime, timedelta
from typing import List, Dict
import sys
import pandas as pd
import pytz
from openalgo import api
import time
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger
from db.database_main import DatabaseManager

DB_MANAGER = DatabaseManager(read_only=False)
logger = get_logger("market_data_dispatcher")

# =====================================================================
# 🔹 GLOBAL CONFIGURATION (DRIVER VARIABLES)
# =====================================================================
MODE = "LIVE"                  # LIVE | BACKTESTING
INSTRUMENT_TYPE = "SPOT"       # SPOT | OPTIONS
SYMBOL = ["NIFTY"]  # For SPOT mode ["NIFTY","BANKNIFTY","SENSEX"]
SPOT_EXCHANGE = "NSE_INDEX"
OPTION_EXCHANGE = "NFO"
TIMEFRAMES = ["1m", "5m", "D"]  # List of timeframes to fetch

MARKET_DATE = None  # Optional: Specific market date for LIVE mode (YYYY-MM-DD) or None
#---Backtesting configuration-----#
START_DATE = "2026-02-24"
END_DATE = "2026-02-25"
BATCH_DATES = []
EXCHANGE = "NSE_INDEX"

#---Openalgo api-----#
API_KEY = "56ca4e1d47a566adf3dada0ab5555358431dde1cb110ebdd8ec069a308ed4e50" #live
API_HOST = "https://myalgo.vralgo.com"
WS_URL = "wss://myalgo.vralgo.com/ws"
# API_KEY = "59b70ffdda45f37e5feaa863942cdae78510b0432bc39c166c269ad816b1ccfd" #sandbox
# API_HOST = "http://127.0.0.1:5000"
# WS_URL = "ws://127.0.0.1:8765"


DAY_CANDLE_LOOKBACK_DAYS = 70
LOOKBACK_DAYS = 10          # Lookback days for LIVE mode
client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)  # Placeholder for your market data API client
IST = pytz.timezone("Asia/Kolkata")

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
        self.batch_dates = BATCH_DATES
        self.client = client

        # ==============================
        # 🧠 Institutional Caches
        # ==============================
        self.expiry_cache = {}   # { "NIFTY": "30DEC25", "BANKNIFTY": "30DEC25" }
        self.strike_cache = {}   # { ("NIFTY", "2025-12-01"): [22000, 22050, ...] }

        # ==============================
        # 📊 Execution Metrics Tracking
        # ==============================
        self.metrics = {
            "spot_data": {},      # { "NIFTY": { "1m": 650, "3m": 600, ... }, ... }
            "option_data": {},    # { "NIFTY24JAN25C24000": { "1m": 100, ... }, ... }
            "timeframe_totals": {},  # { "1m": 5000, "3m": 4800, ... }
            "gaps_detected": 0,
            "gaps_patched": 0,
            "api_calls": 0,
            "api_failures": 0,
            "start_time": datetime.now(IST),
            "end_time": None,
        }

        logger.info(
            f"🚀 [INIT] MODE={self.mode} | "
            f"INSTRUMENT={self.instrument_type} | "
            f"SYMBOLS={self.symbol}")
    
    def retry_api_call(self, func, max_retries=5, delay=10, description="API call", *args, **kwargs):
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
        logger.info(f"[CACHE] Expiry paramters:{symbol} {exchange} {instrumenttype} ")
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
    
    def generate_execution_summary(self):
        """Generate and log comprehensive execution summary."""
        self.metrics["end_time"] = datetime.now(IST)
        execution_time = (self.metrics["end_time"] - self.metrics["start_time"]).total_seconds()
        
        # Calculate totals
        total_spot_records = sum(sum(tfs.values()) for tfs in self.metrics["spot_data"].values())
        total_option_records = sum(sum(tfs.values()) for tfs in self.metrics["option_data"].values())
        total_records = total_spot_records + total_option_records
        
        # Log summary header
        logger.info("╔════════════════════════════════════════════════════════════════╗")
        logger.info("║               📊 EXECUTION SUMMARY REPORT                       ║")
        logger.info("╚════════════════════════════════════════════════════════════════╝")
        
        # 1️⃣ Symbol-wise breakdown
        if self.metrics["spot_data"]:
            logger.info("\n🎯 SPOT DATA SUMMARY")
            logger.info("─" * 60)
            for symbol, timeframes in self.metrics["spot_data"].items():
                total = sum(timeframes.values())
                tf_breakdown = " | ".join([f"{tf}:{cnt}" for tf, cnt in timeframes.items()])
                logger.info(f"  ✓ {symbol:12} → Total: {total:6} records | {tf_breakdown}")
        
        # 2️⃣ Timeframe-wise breakdown
        if self.metrics["timeframe_totals"]:
            logger.info("\n⏱️  TIMEFRAME SUMMARY")
            logger.info("─" * 60)
            for tf in self.timeframes:
                count = self.metrics["timeframe_totals"].get(tf, 0)
                bar = "█" * (count // 200) if count > 0 else ""
                logger.info(f"  ⏰ {tf:4} → {count:6} records")
        
        # 3️⃣ Options summary (if applicable)
        if self.metrics["option_data"]:
            opt_count = len(self.metrics["option_data"])
            opt_total = total_option_records
            logger.info(f"\n📈 OPTIONS DATA SUMMARY")
            logger.info("─" * 60)
            logger.info(f"  ✓ Total Option Symbols: {opt_count}")
            logger.info(f"  ✓ Total Option Records: {opt_total:,}")
        
        # 4️⃣ Data quality metrics
        logger.info(f"\n🔍 DATA QUALITY METRICS")
        logger.info("─" * 60)
        logger.info(f"  ✓ Gaps Detected: {self.metrics['gaps_detected']}")
        logger.info(f"  ✓ Gaps Patched: {self.metrics['gaps_patched']}")
        logger.info(f"  ✓ API Calls: {self.metrics['api_calls']}")
        logger.info(f"  ✓ API Failures: {self.metrics['api_failures']}")
        
        # 5️⃣ Overall summary
        logger.info(f"\n✅ OVERALL SUMMARY")
        logger.info("─" * 60)
        logger.info(f"  📦 Total Records Inserted: {total_records:,}")
        logger.info(f"    ├─ SPOT: {total_spot_records:,}")
        logger.info(f"    └─ OPTIONS: {total_option_records:,}")
        logger.info(f"  ⏱️  Execution Time: {execution_time:.2f} seconds")
        logger.info(f"  🎯 Symbols Processed: {len(self.metrics['spot_data'])}")
        logger.info(f"  📊 Timeframes: {', '.join(self.timeframes)}")
        logger.info(f"  🔧 Mode: {self.mode} | Instrument: {self.instrument_type}")
        
        # 6️⃣ Final status
        logger.info(f"\n{'='*60}")
        logger.info(f"✅ DISPATCHER EXECUTION COMPLETE AT {self.metrics['end_time'].strftime('%Y-%m-%d %H:%M:%S IST')}")
        logger.info(f"{'='*60}\n")

    def track_spot_data(self, symbol: str, timeframe: str, record_count: int):
        """Track SPOT data metrics."""
        if symbol not in self.metrics["spot_data"]:
            self.metrics["spot_data"][symbol] = {}
        self.metrics["spot_data"][symbol][timeframe] = record_count
        
        if timeframe not in self.metrics["timeframe_totals"]:
            self.metrics["timeframe_totals"][timeframe] = 0
        self.metrics["timeframe_totals"][timeframe] += record_count

    def track_option_data(self, option_symbol: str, timeframe: str, record_count: int):
        """Track OPTIONS data metrics."""
        if option_symbol not in self.metrics["option_data"]:
            self.metrics["option_data"][option_symbol] = {}
        self.metrics["option_data"][option_symbol][timeframe] = record_count
        
        if timeframe not in self.metrics["timeframe_totals"]:
            self.metrics["timeframe_totals"][timeframe] = 0
        self.metrics["timeframe_totals"][timeframe] += record_count

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
        logger.debug(f"📊 [{symbol}] Strike ladder locked: {len(strikes)} strikes ({base_low}-{base_high})")
        return strikes
        self.strike_cache[key] = strikes

        logger.debug(f"📊 [{symbol}] Strike ladder locked: {len(strikes)} strikes ({base_low}-{base_high})")
        return strikes

    def generate_option_symbols(
        self,
        symbol: str,
        day_high: float,
        day_low: float,
        expiry: str
    ) -> List[str]:
        """Generate option symbols based on day range + buffer (robust)."""

        symbol = symbol.upper()
        strike_interval = 50 if symbol == "NIFTY" else 100

        # ✅ Round LOW down, HIGH up
        base_low = int((day_low // strike_interval) * strike_interval)
        base_high = int(((day_high + strike_interval - 1) // strike_interval) * strike_interval)

        # Core strikes fully covering day range
        core_strikes = list(range(base_low, base_high + strike_interval, strike_interval))

        # Buffer ±5 strikes
        buffer = 5
        strikes = (
            [core_strikes[0] - i * strike_interval for i in range(buffer, 0, -1)] +
            core_strikes +
            [core_strikes[-1] + i * strike_interval for i in range(1, buffer + 1)]
        )

        # Generate CE & PE symbols
        option_symbols = [
            f"{symbol}{expiry}{strike}{opt_type}"
            for strike in strikes
            for opt_type in ("CE", "PE")
        ]

        logger.info(
            f"[OPTIONS] Generated {len(option_symbols)} option symbols "
            f"({symbol}, strikes={len(strikes)}, "
            f"range={base_low}-{base_high})"
        )

        return option_symbols

    def fetch_and_store_spot_live(self, symbols: List[str], exchange: str, timeframes: List[str], market_date: str = None):
        """Fetch and store spot market data for LIVE mode."""
        if market_date is None:
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = today
            end_date = today
        else:
            market_date = datetime.strptime(market_date, "%Y-%m-%d")
            start_date = market_date 
            end_date = market_date

        for symbol in symbols:
            for tf in timeframes:
                today = datetime.now(IST)
                end_date = market_date if market_date is not None else today
                if tf == "D":
                    lookback_filter_days = DAY_CANDLE_LOOKBACK_DAYS
                elif tf == "1m":
                    lookback_filter_days = 1  # Only fetch current day for 1m to ensure we get the full day's range without gaps
                else:
                    lookback_filter_days = LOOKBACK_DAYS

                start_date = end_date - timedelta(days=lookback_filter_days)                            
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")

                exchange = "BSE_INDEX" if symbol.upper() == "SENSEX" else exchange
                df = self.retry_api_call(
                    self.client.history,
                    symbol=symbol,
                    exchange=exchange,
                    interval=tf,
                    start_date=start_str,
                    end_date=end_str,
                    description=f"SPOT {symbol} {tf}"
                )

                if df is None or df.empty:
                    logger.warning(f"⚠️  [{symbol}:{tf}] No data from API")
                    continue

                df = self.normalize_df(df)
                self.metrics["api_calls"] += 1

                DB_MANAGER.store_ohlcv_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=tf,
                    data=df,
                    replace=True,
                    instrument_type="SPOT"
                )

                db_df = DB_MANAGER.get_ohlcv_data(
                    symbol, exchange, tf, start_str, end_str, "SPOT"
                )

                record_count = len(db_df)
                self.track_spot_data(symbol, tf, record_count)
                
                logger.debug(f"✓ [{symbol:12}:{tf:3}] {record_count:5} records stored")

    def fetch_and_store_options_live(self, symbol: str, exchange: str, timeframes: List[str], market_date: str = None):
        """Fetch and store option market data for LIVE mode."""
        if market_date is None:
            today = datetime.now().strftime("%Y-%m-%d")
            start_date = today
            end_date = today
        else:
            market_date =  datetime.strptime(market_date, "%Y-%m-%d")
            start_date = market_date - timedelta(days=1)  # Fetch previous day to ensure we get the day's range
            end_date = market_date
            start_date = start_date.strftime("%Y-%m-%d")
            end_date = end_date.strftime("%Y-%m-%d")

        # ---- Step 1: Fetch daily SPOT candle
        day_df = self.retry_api_call(
            self.client.history,
            symbol=symbol,
            exchange=exchange,
            interval="D",
            start_date=start_date,
            end_date=end_date,
            description=f"SPOT Daily for {symbol}"
        )

        if day_df is None or day_df.empty:
            logger.error(f"❌ [{symbol}] Cannot compute strikes - no daily candle")
            return

        day_high = day_df["high"].iloc[0]
        day_low = day_df["low"].iloc[0]
        self.metrics["api_calls"] += 1

        opt_exchange = "BFO" if symbol.upper() == "SENSEX" else self.option_exchange
        # ---- Step 2: Expiry (nearest weekly) - cached
        expiry = self.get_expiry_cached(symbol, opt_exchange, "options")

        # ---- Step 3: Generate symbols
        option_symbols = self.generate_option_symbols(symbol, day_high, day_low, expiry)
        logger.info(f"📈 [{symbol}] Generated {len(option_symbols)} option symbols for {expiry}")

        # ---- Step 4: Fetch option candles
        for opt_symbol in option_symbols:
            for tf in timeframes:
                today = datetime.now(IST)
                end_date = market_date if market_date is not None else today
                if tf == "D":
                    lookback_filter_days = DAY_CANDLE_LOOKBACK_DAYS
                elif tf == "1m":
                    lookback_filter_days = 1  # Only fetch current day for 1m to ensure we get the full day's range without gaps
                else:
                    lookback_filter_days = LOOKBACK_DAYS

                start_date = end_date - timedelta(days=lookback_filter_days)
                start_str = start_date.strftime("%Y-%m-%d")
                end_str = end_date.strftime("%Y-%m-%d")
                
                df = self.retry_api_call(
                    self.client.history,
                    symbol=opt_symbol,
                    exchange=opt_exchange,
                    interval=tf,
                    start_date=start_str,
                    end_date=end_str,
                    description=f"OPTIONS {opt_symbol} {tf}"
                )

                if df is None or df.empty:
                    continue
                
                self.metrics["api_calls"] += 1
                df = self.normalize_df(df)
                
                # Parse option symbol
                underlying, expiry_str, strike, opt_type = self.parse_option_symbol(opt_symbol)
                df["underlying"] = underlying
                df["expiry"] = expiry_str
                df["strike"] = strike
                df["opt_type"] = opt_type
                df["iv"] = "0"  # Default IV value - in a real app, this would be computed from the option data

                DB_MANAGER.store_ohlcv_data(
                    symbol=opt_symbol,
                    exchange=self.option_exchange,
                    timeframe=tf,
                    data=df,
                    replace=True,
                    instrument_type="OPTIONS"
                )

                db_df = DB_MANAGER.get_ohlcv_data(
                    opt_symbol, self.option_exchange, tf, start_str, end_str, "OPTIONS", 
                    underlying_symbol=underlying, expiry_date=expiry_str
                )

                record_count = len(db_df)
                self.track_option_data(opt_symbol, tf, record_count)
                
                logger.debug(f"✓ [{opt_symbol:20}:{tf:3}] {record_count:5} records stored")

    def backtest_data_loader_with_gap_validation(
        self,
        symbol: str,
        exchange: str,
        timeframe: str,
        start_date: str,
        end_date: str,
        instrument_type: str
    ) -> pd.DataFrame:
        """Universal backtest loader (Gap Validation Removed)."""
        logger.debug(f"📥 [{symbol}:{timeframe}] Loading backtest data")

        df = DB_MANAGER.get_ohlcv_data(
            symbol, exchange, timeframe,
            start_date, end_date,
            instrument_type
        )
        logger.debug(f"📥 [{symbol}:{timeframe}] fetching from API")

        api_df = self.retry_api_call(
            self.client.history,
            symbol=symbol,
            exchange=exchange,
            interval=timeframe,
            start_date=start_date,
            end_date=end_date,
            description=f"BACKTEST {symbol} {timeframe}"
        )

        if api_df is None or api_df.empty:
            logger.error(f"❌ [{symbol}:{timeframe}] API returned empty - STOP")
            # sys.exit(1)

        self.metrics["api_calls"] += 1
        api_df = self.normalize_df(api_df)

        DB_MANAGER.store_ohlcv_data(
            symbol, exchange, timeframe,
            api_df, instrument_type=instrument_type
        )

        df = DB_MANAGER.get_ohlcv_data(
            symbol, exchange, timeframe,
            start_date, end_date, instrument_type
        )

        self.track_spot_data(symbol, timeframe, len(df))
        logger.debug(f"✓ [{symbol}:{timeframe}] Loaded {len(df)} rows from API")
        return df

    def run(self):
        """Main dispatcher entry point."""
        logger.info(
            f"🎯 [DISPATCHER] MODE={self.mode} | "
            f"INSTRUMENT={self.instrument_type} | SYMBOLS={self.symbol}"
        )

        try:
            if self.mode == "LIVE":
                logger.info(f"📡 Starting LIVE data fetch...")
                self.fetch_and_store_spot_live(
                    symbols=self.symbol,
                    exchange=self.spot_exchange,
                    timeframes=self.timeframes,
                    market_date=MARKET_DATE
                )
                
                for sym in self.symbol:
                    exchange = "BSE_INDEX" if sym.upper() == "SENSEX" else self.spot_exchange
                    self.fetch_and_store_options_live(
                        symbol=sym,
                        exchange=exchange,
                        timeframes=self.timeframes,
                        market_date=MARKET_DATE
                    )
                    
            elif self.mode == "BACKTESTING":
                logger.info(f"🔄 Starting BACKTEST data loading...")
                
                # Use BATCH_DATES if available, else fall back to start/end range
                dates_to_process = []
                if self.batch_dates:
                    dates_to_process = [(d, d) for d in self.batch_dates]
                else:
                    dates_to_process = [(self.start_date, self.end_date)]

                for s_date, e_date in dates_to_process:
                    logger.info(f"📅 Processing range: {s_date} to {e_date}")
                    if self.instrument_type == "SPOT":
                        for sym in self.symbol:
                            exchange = "BSE_INDEX" if sym.upper() == "SENSEX" else self.spot_exchange
                            for tf in self.timeframes:
                                self.backtest_data_loader_with_gap_validation(
                                    symbol=sym,
                                    exchange=exchange,
                                    timeframe=tf,
                                    start_date=s_date,
                                    end_date=e_date,
                                    instrument_type=self.instrument_type
                                )
                    elif self.instrument_type == "OPTIONS":
                        for tf in self.timeframes:
                            self.backtest_data_loader_with_gap_validation(
                                symbol=self.symbol,
                                exchange=self.option_exchange,
                                timeframe=tf,
                                start_date=s_date,
                                end_date=e_date,
                                instrument_type=self.instrument_type
                            )
            else:
                raise ValueError(f"Invalid MODE: {self.mode}")
        
        finally:
            # Generate summary regardless of success/failure
            self.generate_execution_summary()

if __name__ == "__main__":
    try:
        dispatcher = MarketDataDispatcher()
        dispatcher.run()
    except Exception as e:
        logger.error(f"❌ DISPATCHER FAILED: {e}", exc_info=True)
        sys.exit(1)


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
