# myalgo_trading_system_integrated.py
"""
MyAlgo Trading System - Integrated with Date-Based Simulation

MODE SELECTION:
- Set SIMULATION_DATE = "DD-MM-YYYY" for automatic simulation mode
- Set SIMULATION_DATE = None for live trading mode
- MODE = "LIVE" for REALTIME  
- MODE = "BACKTESTING" for simulation particular date  

SIMULATION BEHAVIOR:
- Fetches 1-minute OHLC data for the specified date
- Replays candles as LTP ticks (fast mode, no delays)
- Triggers strategy_job() at minute % 5 == 0 (same as APScheduler in live)
- All functions (get_intraday, indicators, orders) work identically to live mode

LIVE BEHAVIOR:
- Uses real WebSocket for LTP updates
- APScheduler triggers strategy every 5 minutes
- All existing functionality preserved
"""
from dataclasses import dataclass
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import logging
from openalgo import api
import pandas as pd
from datetime import datetime, timedelta, date, time as dt_time
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
# Directly call the helper from MarketDataDispatcher
from market_data_dispatcher import MarketDataDispatcher
import pytz
import re
import sys
from dataclasses import dataclass
import os
from openalgo import ta
from typing import Dict, List, Optional
import io
import time

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger, log_trade_execution
import platform


os_name = platform.system()
MARKET_OPEN  = dt_time(9, 15)   # 09:15 AM Market open time
MARKET_CLOSE = dt_time(15, 20)  # 03:20 PM Market close time
ENTRY_CUTOFF_TIME = dt_time(14, 30) # 02:30 PM No new entry orders after this time

dispatcher = MarketDataDispatcher()

SIMULATION_DATE: Optional[str] = None           # Example: "28-11-2025" or None
MODE = "LIVE"                                    # Example: LIVE or BACKTESTING
# ----------------------------
# Configuration
# ----------------------------
logging.info(f"🔧 Setting up API client based on OS {os_name}")
if os_name.upper == "OS LINUX" or os_name.upper() == "LINUX":
    logging.info("💻 Running on Linux - using production API settings")
    API_KEY = "45428a0d1b460d2b7a29cfcc71df97d296e63f6155a3a8282a741b5879fa99d9"
    API_HOST = "https://myalgo.vralgo.com/"
    WS_URL = "wss://myalgo.vralgo.com/ws"
else:
    logging.info("🖥️ Running on Windows - using local API settings")
    API_KEY = "44594f88f9f8dec784ae99da67bcea129c794005186c0b8eb0d6af542df1a56c"
    API_HOST = "http://127.0.0.1:5000"
    WS_URL = "ws://127.0.0.1:8765"

# API_KEY = "7773b590c743c9184fb1bb74830091a379f88c2035a2203e2b40d24cb2f86711"
# API_HOST = "http://127.0.0.1:5000"
# WS_URL = "ws://127.0.0.1:8765"

# ==============================================================  
try:
    logging.info("🔌 Initializing OpenAlgo API client")
    client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL) # Initialize openalgo API client
    logging.info("✅ 🚀 OpenAlgo API client initialized successfully")
except Exception as e:
    logging.error(f"❌ 🚨 Failed to initialize OpenAlgo API client: {e}")
    sys.exit(1)

# ==============================================================   
# Instrument
SYMBOL = "NIFTY"                                  #Symbol
EXCHANGE = "NSE_INDEX"                            #Exchange                     
PRODUCT = "MIS"                                   #Product
CANDLE_TIMEFRAME = "5m"                           #Candle timeframe
LOOKBACK_DAYS = 10                                #Lookback days
SIGNAL_CHECK_INTERVAL = 5                         # minutes (use integer minutes)
MIN_TP_SEPARATION_R = 0.5                         #Minimum TP separation ratio is to avoid closer pivot with previous target
# ===============================================================
# 🔧 Dynamic LTP Breakout Confirmation Config
# ===============================================================

LTP_BREAKOUT_ENABLED = True                         # enable / disable breakout waiting
LTP_BREAKOUT_INTERVAL_MIN = 5                       # minutes to wait for LTP breakout confirmation
ENTRY_BREAKOUT_MODE = "OPTIONS"                     # "SPOT" or "OPTIONS" (For breakout validation)

# Indicators to compute
EMA_PERIODS = [9, 20, 50, 200]                      #EMA Periods
SMA_PERIODS = [9, 20, 50, 200]                      #SMA Periods    
RSI_PERIODS = [14, 21]                              #RSI Periods
CPR = ['DAILY', 'WEEKLY','MONTHLY']                 #CPR Periods

# === LTP Pivot Touch Gatekeeper (new) ===
ENABLE_LTP_PIVOT_GATE = True                        # master switch for this gatekeeper
PIVOT_TOUCH_BUFFER_PTS = 0.5                        # absolute buffer in price units (points). adjust for NIFTY ~0.5
PIVOT_TOUCH_BUFFER_PCT = None                       # optional: use percentage buffer (e.g. 0.001 for 0.1%). If set, overrides PIVOT_TOUCH_BUFFER_PTS
# --------------------------------------

# Risk settings
STOPLOSS = 0                      #Stoploss
TARGET = 0                        #Target
MAX_SIGNAL_RANGE = 50             #Maximum signal range
MIN_SL_POINTS = 5                 #Absolute minimum SL points
MAX_RISK_POINTS = 15              #Absolute safety on option premium
SL_PERCENT = 0.10                 #10% of option premium
MIN_REWARD_PCT_OF_RISK = 0.5      #At least 50% reward of risk
MIN_REWARD_FOR_SL_MOVE = 0.5      #TSL Move only if reward is at least 50% of initial target 
MIN_ABSOLUTE_REWARD = 5           #Minimum absolute reward in points
ENTRY_CONFIRM_SECONDS = 1         #Seconds to confirm entry
EXIT_CONFIRM_SECONDS = 5          #Seconds to confirm exit    
SL_BUFFER_PCT = 0.12              #12% buffer from premium automatic SL placement in broker
SL_BUFFER_POINT = 1               #Buffer in points from premium in CPR SL placement

# Trade management
MAX_TRADES_PER_DAY = 3              #Maximum trades per day
STRATEGY = "myalgo_scalping_" + SYMBOL

# Option trading config
OPTION_ENABLED = True               #Enable option trading
OPTION_EXCHANGE = "NFO"             #Option exchange
STRIKE_INTERVAL = 50                #Strike interval
OPTION_EXPIRY_TYPE = "WEEKLY"       #Option expiry type
OPTION_STRIKE_SELECTION = "OTM1"    # "ATM", "ITM1", "OTM2", etc.
EXPIRY_LOOKAHEAD_DAYS = 30          #Expiry look ahead days
LOT_QUANTITY =65                    #Lot size
LOT = 1                             #Lot size
QUANTITY = LOT * LOT_QUANTITY       #Quantity
SL_ORDER_ENABLED = True             #Enable SL order
# Websocket CONSTANTS
HEARTBEAT_INTERVAL = 10             # seconds
RECONNECT_BASE = 1.0                # seconds
RECONNECT_MAX = 60.0                # seconds

# Stoploss/Target tracking configuration
USE_SPOT_FOR_SLTP = False           # If True → uses spot price for SL/TP tracking
USE_OPTION_FOR_SLTP = True          # If True → uses option position LTP for SL/TP tracking

# SL/TP Enhancement config
# Available Methods:
# - "Signal_candle_range_SL_TP": Uses signal candle range for SL/TP with TSL
# - "CPR_range_SL_TP": Uses CPR pivot levels as dynamic targets with trailing
SL_TP_METHOD = "CPR_range_SL_TP" # current active method #CPR_range_SL_TP #Signal_candle_range_SL_TP
TSL_ENABLED = True # enable trailing stoploss behavior
TSL_METHOD = "TSL_CPR_range_SL_TP" # trailing method for this enhancement

# ===============================================================
# 🔒 STOP LOSS AUTOMATION & ENTRY RESTRICTIONS
# ===============================================================

# Entry restrictions configuration
DAY_HIGH_LOW_VALIDATION_FROM_TRADE = 5  # Apply day high/low validation from this trade count onward

# ===============================================================
# 🎯 CPR PIVOT EXCLUSION CONFIGURATION
# ===============================================================

CPR_EXCLUDE_PIVOTS = ["WEEKLY_TC",  "WEEKLY_BC","WEEKLY_CLOSE", "WEEKLY_CPR_RANGE",
     "MONTHLY_TC", "MONTHLY_BC", "MONTHLY_CLOSE", "MONTHLY_CPR_RANGE",
     "DAILY_TC", "DAILY_BC", "DAILY_CLOSE", "DAILY_CPR_RANGE" 
     ]
# ===============================================================
# 🎯 DYNAMIC RISK-REWARD CONFIGURATION
# ===============================================================
# Define your global target multiplier (RR ratio)
RISK_REWARD_RATIO = 2.0        # e.g., 1.5 = 1:1.5, 2.0 = 1:2 risk-reward
USE_DYNAMIC_TARGET = True      # Enable or disable dynamic target logic
DYNAMIC_TARGET_METHOD = "NEAREST_VALID"  # "NEAREST_VALID" or "FIRST_BEYOND"

# Define which CPR levels can be used as valid targets
VALID_TARGET_LEVELS = ["pivot", "bc", "tc", "r1", "r2", "s1", "s2"]  # CPR level names to consider

# month map for option token if needed
MONTH_MAP = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"
}

# IST timezone
IST = pytz.timezone("Asia/Kolkata")

# Enhanced Logging System
# Module-specific loggers for detailed tracking
main_logger = get_logger("trading_engine")
signal_logger = get_logger("signals") 
order_logger = get_logger("orders")
position_logger = get_logger("positions")
risk_logger = get_logger("risk_management")
indicators_logger = get_logger("indicators")
data_logger = get_logger("market_data")

# Keep original logger for backward compatibility
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("FixedSignalBot")
# =========================================================
# 🎮 SIMULATION INFRASTRUCTURE
# =========================================================

class BacktestingManager:
    """
    Manages simulation state and 1-minute historical data for replay.
    Thread-safe singleton for simulation control.
    """
    def __init__(self):
        self.active = False
        self.df: Optional[pd.DataFrame] = None
        self.sim_index: int = 0  # next row to be emitted
        self.simulate_date_str: Optional[str] = None
        self.lock = threading.RLock()
        self.pause_ticks = False

    def start(self, simulate_date_str: str, symbol: str, exchange: str, timeframe: str):
        """
        Load 1-minute OHLC for the given date using client.history(..., interval='1m').
        Normalizes timestamp column to tz-aware IST and keeps rows >= 09:15.
        """
        with self.lock:
            dt = self._parse_date(simulate_date_str)
            trade_date = dt.strftime("%Y-%m-%d")
            main_logger.info(f"🎬 Loading simulation data for {trade_date}")
            logger.info("🛰️ Fetching 1-minute OHLC for %s", trade_date)

            # -----------------------------
            # 2️⃣ DuckDB FIRST
            # -----------------------------

            df = DB_MANAGER.get_ohlcv_data(
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                start_date=trade_date,
                end_date=trade_date
                )
            # -----------------------------
            # 3️⃣ API FALLBACK (ONLY IF NEEDED)
            # -----------------------------
            if df is None or df.empty:
                main_logger.warning(
                f"⚠️ [Backtesting][Data missing] {symbol} {timeframe} {trade_date} → fetching from OpenAlgo"
                )
                try:
                    raw = client.history(symbol=symbol, exchange=exchange, interval=timeframe,
                                     start_date=trade_date, end_date=trade_date)
                except Exception as e:
                    logger.exception("❌ 🚨 client.history failed: %s", e)
                    raise RuntimeError("History fetch failed") from e
                # Normalize to DataFrame
                df_openalgo_historical = normalize_df(raw)
                # Cache to DuckDB for future use

                if df_openalgo_historical.empty:
                    raise RuntimeError("No usable 1-minute rows at/after 09:15 for " + trade_date)
                
                # DuckDB expects timestamp as column
                df_openalgo_historical = df_openalgo_historical.reset_index()
                logger.info("🛰️ Fetched %d rows from OpenAlgo for %s", len(df_openalgo_historical), trade_date)
                # Store into DuckDB
                DB_MANAGER.store_ohlcv_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=timeframe,
                    data=df_openalgo_historical,
                    replace=False
                    )
                logger.info("💾 Stored historical data into DuckDB for %s", trade_date)
                
                # Re-query DuckDB (single source of truth)
                df = DB_MANAGER.get_ohlcv_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=timeframe,
                    start_date=trade_date,
                    end_date=trade_date
                    )
                logger.info("💾 Re-queried DuckDB, got %d rows for %s", len(df), trade_date)

                if df is None or df.empty:
                    raise RuntimeError("Simulation data unavailable after DB + API fallback")
            # -----------------------------
            # Normalize DB output
            # -----------------------------
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()

            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(IST)
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert(IST)
       
            # Filter to trading start (09:15 or later)
            start_time = datetime.strptime("09:15", "%H:%M").time()
            df = df[df["timestamp"].dt.time >= start_time].reset_index(drop=True)

            if df.empty:
                raise RuntimeError("No usable 1-minute candles after 09:15")

            self.df = df
            self.sim_index = 0
            self.simulate_date_str = simulate_date_str
            self.active = True
            
            main_logger.info(f"✅ Simulation loaded: {len(df)} candles from {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info("🎬 Simulation loaded for %s | rows=%d", simulate_date_str, len(df))

    def stop(self):
        with self.lock:
            self.active = False
            self.df = None
            self.sim_index = 0
            self.simulate_date_str = None
            main_logger.info("🛑 Simulation stopped")
            logger.info("🛑 Simulation stopped")

    def _parse_date(self, dstr: str) -> datetime:
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(dstr, fmt)
            except Exception:
                continue
        raise ValueError("simulate_date format should be DD-MM-YYYY")

class DateReplayClient:
    """
    Replays BACKTESTING_MANAGER.df as LTP ticks and synchronously triggers strategy at minute%5 == 0.
    Fast-mode: no sleep between ticks.
    """
    def __init__(self, BACKTESTING_MANAGER: BacktestingManager):
        self.BACKTESTING_MANAGER = BACKTESTING_MANAGER
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
        # --- Intrabar control ---
        self._spot_intrabar_stage = 0
        self._last_spot_candle_ts = None
    def connect(self):
        logger.info("🔌 DateReplayClient.connect() (simulation mode)")

    def disconnect(self):
        logger.info("🔌 DateReplayClient.disconnect() - stopping")
        self._stop_event.set()
        if self._thread and self._thread.is_alive() and threading.current_thread() != self._thread:
            self._thread.join(timeout=1)

    def _get_spot_intrabar_ltp(self, row, stage):
        o = float(row["open"])
        h = float(row["high"])
        l = float(row["low"])
        c = float(row["close"])

        # Conservative path
        if c >= o:
            path = [o, l, h, c]
        else:
            path = [o, h, l, c]

        return path[min(stage, 3)]

    def subscribe_ltp(self, instrument=None, on_data_received=None, strategy_fn=None, 
                     instrument_type: str = "SPOT", data_df: pd.DataFrame = None):
        """
        Subscribe to LTP updates (SPOT or OPTION).
        
        Args:
            instrument: Instrument spec (for SPOT mode, legacy)
            on_data_received: Callback for tick data
            strategy_fn: Strategy function to call every 5 minutes
            instrument_type: "SPOT" or "OPTION"
            data_df: DataFrame with OPTION data (for OPTION mode)
        """
        if instrument_type == "SPOT":
            # Existing SPOT mode logic
            if not self.BACKTESTING_MANAGER.active or self.BACKTESTING_MANAGER.df is None:
                raise RuntimeError("Simulation not active or data not loaded")
            replay_df = self.BACKTESTING_MANAGER.df
        elif instrument_type == "OPTION":
            # OPTION mode: use provided data_df
            if data_df is None or data_df.empty:
                raise RuntimeError("Option data_df not provided or empty")
            replay_df = data_df
        else:
            raise ValueError(f"Unknown instrument_type: {instrument_type}")
        
        self._stop_event.clear()

        def _run():
            """
            Main simulation replay loop for SPOT or OPTION.
            Replays 1-minute data as live LTP ticks in sequence.
            """
            df = replay_df
            idx = self.BACKTESTING_MANAGER.sim_index  # Current index position

            main_logger.info(f"🎬 Replay starting ({instrument_type}) at index {idx} of {len(df)} candles")
            logger.info(f"DateReplayClient replay starting ({instrument_type}) at index {idx}")
            
            # MAIN REPLAY LOOP
            while idx < len(df) and not self._stop_event.is_set() and self.BACKTESTING_MANAGER.active:
                
                # SIMULATION PAUSE DURING EXIT
                while self.BACKTESTING_MANAGER.pause_ticks:
                     time.sleep(0.01)
                     if self._stop_event.is_set() or not self.BACKTESTING_MANAGER.active:
                        break
                if self._stop_event.is_set() or not self.BACKTESTING_MANAGER.active:
                    break

                # Extract current candle row and timestamp
                row = df.iloc[idx]

                if "timestamp" in df.columns:
                    ts = pd.to_datetime(row["timestamp"])
                else:
                    ts = df.index[idx]
                
                # Ensure timestamp is timezone-aware (IST)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(IST)
                else:
                    ts = ts.tz_convert(IST)

                # Reset intrabar upon new candle (SPOT MODE)
                if instrument_type == "SPOT":
                    if ts != self._last_spot_candle_ts:
                        self._last_spot_candle_ts = ts
                        self._spot_intrabar_stage = 0
                    
                    # Get intrabar LTP
                    ltp_val = self._get_spot_intrabar_ltp(row, self._spot_intrabar_stage)
                else:
                    # Fallback for generic/option direct replay?
                    ltp_val = float(row.get("ltp", row.get("close", 0)))

                # Build tick payload (works for both SPOT and OPTION)
                tick = {
                    "type": "tick",
                    "symbol": row.get("symbol", SYMBOL),
                    "ltp": ltp_val,
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)),
                    "timestamp": ts.isoformat()
                }
                
                # Add option-specific fields if present
                if instrument_type == "OPTION":
                    tick["strike"] = row.get("strike")
                    tick["option_type"] = row.get("option_type")
                    tick["expiry"] = row.get("expiry")
                
                # Emit LTP -> on_ltp_update
                try:
                    if on_data_received:
                        on_data_received(tick)
                except Exception:
                    logger.exception(f"❌ 🚨 on_data_received raised during {instrument_type} replay")

                # Advance simulation index (ONLY when we finish the candle)
                # But here we are emitting multiple ticks per candle for SPOT.
                # So we only increment idx when _spot_intrabar_stage >= MAX_INTRABAR_STEPS
                
                should_advance_candle = True
                if instrument_type == "SPOT":
                    self._spot_intrabar_stage += 1
                    MAX_INTRABAR_STEPS = 4
                    if self._spot_intrabar_stage < MAX_INTRABAR_STEPS:
                        should_advance_candle = False
                
                if should_advance_candle:
                    with self.BACKTESTING_MANAGER.lock:
                        self.BACKTESTING_MANAGER.sim_index = idx + 1
                    
                # Trigger 5-minute strategy scheduler (after 09:15 only)
                # -------------------------------------------------------------
                if (ts.hour == 9 and ts.minute < 25): # Skip strategy checks before 09:20 
                    pass
                else:
                    # Strategy logic validation:
                    # Run ONCE per candle (at Close step for Intrabar, or normal for others)
                    run_strategy = False
                    if instrument_type == "SPOT":
                        # Only run at the last stage (Close) -> stage was incremented above, so check if == MAX
                        if self._spot_intrabar_stage >= MAX_INTRABAR_STEPS:
                            run_strategy = True
                    else:
                        run_strategy = True

                    if run_strategy and (ts.minute % SIGNAL_CHECK_INTERVAL == 0 and ts.second == 0):
                        main_logger.info(f"⏰ Signal trigger at {ts.strftime('%H:%M:%S')}")
                        if strategy_fn:
                            try:
                               # Run the strategy and check if signal was generated
                                result = strategy_fn() 
                               # --- 🔁 Re-feed tick only if a signal is generated ---
                                signal_generated = False
                                if isinstance(result, bool) and result:
                                    signal_generated = True
                                elif hasattr(self, "signal_generated") and self.signal_generated:
                                    signal_generated = True
                                    self.signal_generated = False  # reset
                                if signal_generated and on_data_received:
                                    main_logger.info("🔁 Re-emitting tick for entry validation after signal generation")
                                    on_data_received(tick)
                            except Exception:
                                logger.exception("❌ 🚨 strategy_fn raised during replay")
                
                if should_advance_candle:
                    idx += 1 # Move to next candle only if intrabar finished
                # time.sleep(0.05)  # small delay to simulatFetching 1-minute OHLCe real ticks
        # -------------------------------------------------------------
        # LOOP END — only stop when ALL candles replayed
        # -------------------------------------------------------------
            if idx >= len(df):
                main_logger.info("🏁 Replay completed — all candles processed.")
                with self.BACKTESTING_MANAGER.lock:
                    self.BACKTESTING_MANAGER.active = False
                self._stop_event.set()
                main_logger.info("✅ Simulation completed successfully.")

                # 🔴 SIGNAL BOT TO STOP
                if hasattr(self, "bot_ref"):
                    self.bot_ref.running = False
                    self.bot_ref.stop_event.set()
                    main_logger.info("🟢 SIMULATION END → BOT STOP SIGNAL CONFIRMED")
            else:
                # ⚡ Important: Do NOT mark inactive or disconnect mid-replay
                main_logger.info("➡️ Trade exited — keeping simulation WebSocket active.")
                logger.info("🛰️ Simulation WebSocket continues streaming ticks.")
                return
        #-------------------------------------------------------------
        # Start replay in a background thread (daemon)
        # -------------------------------------------------------------
        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        logger.info("🚀 DateReplayClient started replay thread")
        
    def unsubscribe_ltp(self, instruments):
        logger.info("🔌 DateReplayClient.unsubscribe_ltp() unsubscribing")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)

def get_now():
    """
    Returns simulated current timestamp (tz-aware) if simulation active, else real now in IST.
    """
    if BACKTESTING_MANAGER.active and BACKTESTING_MANAGER.df is not None:
        idx = max(0, BACKTESTING_MANAGER.sim_index - 1)
        if idx < len(BACKTESTING_MANAGER.df):
            ts = pd.to_datetime(BACKTESTING_MANAGER.df.iloc[idx]["timestamp"])
            if ts.tzinfo is None:
                ts = ts.tz_localize(IST)
            else:
                ts = ts.tz_convert(IST)
            return ts
    # Real time fallback
    return datetime.now(IST)

def normalize_df(raw):
    # Normalize DataFrame
    if isinstance(raw, pd.DataFrame):
        df = raw.copy()
    elif isinstance(raw, dict) and "data" in raw:
        df = pd.DataFrame(raw["data"])
    else:
        df = pd.DataFrame()

    if df.empty:
        indicators_logger.warning(f"⚠️ No data received for normalization")
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

# Simulation instances for Market Spot data OHLC
if MODE == "BACKTESTING" and SIMULATION_DATE is not None:
    main_logger.info(f"🚀 Starting in BACKTESTING mode for date {SIMULATION_DATE}")
    from db.database_main import get_database_manager
    BACKTESTING_MANAGER = BacktestingManager()
    REPLAY_CLIENT = DateReplayClient(BACKTESTING_MANAGER)
    DB_MANAGER = get_database_manager()
else:
    main_logger.info("🚀 Starting in LIVE mode")
    BACKTESTING_MANAGER = BacktestingManager()  # inactive in live mode
    REPLAY_CLIENT = None
    DB_MANAGER = None
# =========================================================
# 📘 SQLAlchemy Dataclass Setup for Trade Logging
# =========================================================
Base = declarative_base()

@dataclass
class TradeLog(Base):
    __tablename__ = "my_trade_logs"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: datetime = Column(DateTime, default=datetime.now)
    instrument: str = Column(String(50), default=SYMBOL)
    spot_price: str = Column(String(50))
    symbol: str = Column(String(50))
    action: str = Column(String(10))
    quantity: float = Column(Float)
    price: float = Column(Float)
    order_id: str = Column(String(50))
    strategy: str = Column(String(50))
    leg_type: str = Column(String(20))
    reason: str = Column(String(200), nullable=True)
    pnl: float = Column(Float, default=0.0)
    leg_status: str = Column(String(20), default="open")

# ✅ Proper engine and session setup
# DB_PATH = os.path.join(os.getcwd(), "myalgo.db")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "myalgo.db")
engine = create_engine(f"sqlite:///{DB_PATH}", echo=False, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

# Create tables once at initialization
Base.metadata.create_all(engine)
# ----------------------------
# Database Logging Utility
# ----------------------------
def log_trade_db(trade_data: dict):
    """Persist trade details (entry/exit) to SQLite via SQLAlchemy ORM."""
    session = SessionLocal()
    try:
        # 🎯 Simulation: use simulated timestamp
        if 'timestamp' not in trade_data:
            trade_data['timestamp'] = get_now()  # ✅ Fixed
            
        record = TradeLog(**trade_data)
        session.add(record)
        session.commit()
        print(f"✅ Trade logged: {trade_data['symbol']} | {trade_data['action']} @ {trade_data['price']}")
    except Exception as e:
        print(f"⚠️ DB Logging Error: {e}")
        session.rollback()
    finally:
        session.close()

# =========================================================
# 🔹 CPR / Pivot Utility Functions
# =========================================================        
def merge_pivot_levels(pivots: Dict[str, Dict[str, float]]) -> List[float]:
    """Original merge function - kept for backward compatibility"""
    vals = set()
    for period, mapping in (pivots or {}).items():
        if not isinstance(mapping, dict):
            continue
        for k, v in mapping.items():
            try:
                vals.add(float(v))
            except Exception:
                continue
    return sorted(vals)

def merge_pivot_levels_with_exclusion(pivots: Dict[str, Dict[str, float]], exclude_list: List[str] = None) -> tuple:
    """
    Enhanced pivot merger with exclusion filtering and identifier mapping
    Returns: (values_list, value_to_identifier_mapping, excluded_identifiers)
    """
    if exclude_list is None:
        exclude_list = CPR_EXCLUDE_PIVOTS
        
    vals = set()
    value_mapping = {}  # {level_value: "DAILY_R1"}
    excluded_identifiers = []
    included_identifiers = []
    
    for period, mapping in (pivots or {}).items():
        if not isinstance(mapping, dict):
            continue
            
        period_upper = period.upper()
        for k, v in mapping.items():
            try:
                level_value = float(v)
                # Create identifier like "DAILY_R1", "WEEKLY_PIVOT", etc.
                identifier = f"{period_upper}_{k.upper()}"
                
                # Check if this identifier should be excluded
                if identifier in exclude_list:
                    excluded_identifiers.append(identifier)
                    continue
                    
                vals.add(level_value)
                value_mapping[level_value] = identifier
                included_identifiers.append(identifier)
                
            except Exception:
                continue
    
    return sorted(vals), value_mapping, excluded_identifiers

def expand_with_midpoints(levels: List[float]) -> List[float]:
    if not levels:
        return []
    mids = [(levels[i] + levels[i + 1]) / 2.0 for i in range(len(levels) - 1)]
    return sorted(set(levels + mids))

def market_is_open(now: datetime | None = None) -> bool:
    """
    Returns True if current time is within NSE market hours.
    """
    now = get_now()
    t = now.time()
    return MARKET_OPEN <= t < MARKET_CLOSE

# ----------------------------
# MyAlgo Trading System
# ----------------------------
class MYALGO_TRADING_BOT:
    # ------------------------
    # Initialization
    # -------------------------
    def __init__(self):
        # 🎯 Client will be set dynamically based on mode
        self.client = client

        self.position = None
        self.entry_price = 0.0
        self.stoploss_price = 0.0
        self.option_stop_loss = 0.0
        self.target_price = 0.0
        self.spot_symbol = SYMBOL
        self.spot_exchange = EXCHANGE
        self.ltp = None
        self.option_ltp = None
        self.exit_in_progress = False
        self.running = True
        self.stop_event = threading.Event()
        self.instrument = [{"exchange": EXCHANGE, "symbol": SYMBOL}]
        self.static_indicators = None
        self.static_indicators_date = None
        self.current_day_high = None
        self.current_day_low = None
        self.trade_count = 0
        self.spot_entry_price = 0.0
        self.option_entry_price = 0.0
        self.option_symbol = None
        self.scheduler = None
        self.trailing_levels = []
        self.trailing_index = 0
        self.pending_breakout = None
        self.pending_breakout_expiry = None
        self.breakout_side = None
        self._state_lock = threading.Lock()
        self.cpr_levels_sorted = []
        self.cpr_targets = []
        self.cpr_sl = 0.0
        self.cpr_side = None
        self.current_tp_index = 0
        self.cpr_tp_hit_count = 0
        self.cpr_active = False
        self.dynamic_target_info = None
        self.excluded_pivots_info = None
        self.dynamic_indicators_option = None
        self.initial_stoploss_price = 0.0
        # Gatekeeper state
        self.ltp_pivot_breakout = False        # becomes True when LTP touched a qualifying pivot
        self.ltp_pivot_info = None             # dict: {"pivot":value, "identifier":"DAILY_R1", "period":"DAILY", "matched_at": timestamp}
        self._pivot_gate_lock = threading.Lock()  # protect transitions
        self.gk_resistance_pivot = None
        self.gk_support_pivot = None
        self.option_1m_df = None
        self.ws_connected = False
        self.ws_retry_count = 0
        self.ws_should_run = True
        self.last_ws_tick_time = None
        self.ws_subscriptions = set()
        self.pending_exit_breakout = None
        self.pending_exit_breakout_expiry = None
        self.exit_breakout_side = None
        self.entry_timer_started = False
        self.entry_timer_start_ts = None
        self.entry_timer_start_ts = None
        self.cpr_tp_hits = []

        # --- Intrabar control ---
        self._option_intrabar_stage = 0
        self._last_option_candle_ts = None
    
        # 🗓️ Expiry Cache
        self.expiry_cache = {"date": None, "is_expiry": False, "expiry_dt": None}

        # 🎯 Log mode selection
        mode_str = MODE
        logger.info(f"🚀 Bot initialized in {mode_str} MODE")
        main_logger.info(f"=== ℹ️ MyAlgo Trading Bot Initialized ({mode_str} MODE) ===")
        
        if MODE == "BACKTESTING" and SIMULATION_DATE:
            main_logger.info(f"📅 Simulation Date: {BACKTESTING_MANAGER.simulate_date_str}")
        
        main_logger.info(f"🧠 Strategy: {STRATEGY} | Symbol: {SYMBOL} | Timeframe: {CANDLE_TIMEFRAME}")
        main_logger.info(f"⚖️ Risk Management: SL={STOPLOSS}, TP={TARGET} | Max Trades: {MAX_TRADES_PER_DAY}")
        main_logger.info(f"🔒 SL/TP Method: {SL_TP_METHOD} | TSL Enabled: {TSL_ENABLED}")
        main_logger.info(f"📝 Options Enabled: {OPTION_ENABLED} | Strike Selection: {OPTION_STRIKE_SELECTION}")
        main_logger.info(f"⏰ Signal Check Interval: {SIGNAL_CHECK_INTERVAL} minutes")
        
        # Validate SL/TP method configuration
        valid_methods = ["Signal_candle_range_SL_TP", "CPR_range_SL_TP"]
        if SL_TP_METHOD not in valid_methods:
            main_logger.warning(f"⚠️ Unknown SL_TP_METHOD: {SL_TP_METHOD}. Valid options: {valid_methods}")
        else:
            main_logger.info(f"✅ Valid SL/TP Method configured: {SL_TP_METHOD}")
            
        # Log CPR enhancement features
        if SL_TP_METHOD == "CPR_range_SL_TP":
            main_logger.info("=== ℹ️ CPR Enhancement Features ===")
            main_logger.info(f"📐 Pivot Exclusion: {'Enabled' if CPR_EXCLUDE_PIVOTS else 'Disabled'}")
            if CPR_EXCLUDE_PIVOTS:
                main_logger.info(f"  📐 Excluded Levels: {len(CPR_EXCLUDE_PIVOTS)} ({', '.join(CPR_EXCLUDE_PIVOTS[:3])}...)")
            main_logger.info(f"⚖️ Dynamic Risk:Reward: {'Enabled' if USE_DYNAMIC_TARGET else 'Disabled'}")
            if USE_DYNAMIC_TARGET:
                main_logger.info(f"  ⚖️ RR Ratio: 1:{RISK_REWARD_RATIO:.2f}")
                main_logger.info(f"  Target Method: {DYNAMIC_TARGET_METHOD}")
                main_logger.info(f"  Valid Target Levels: {', '.join(VALID_TARGET_LEVELS)}")
            main_logger.info("===============================")

    # =========================================================
    # 💰 Auto PnL Reconciliation Helper
    # =========================================================
    def reconcile_trade_pnl(self, exit_trade: dict):
        """
        Auto-match exit order with last open entry and update realized PnL.
        Called automatically when exit order is placed.
        """
        try:
            session = SessionLocal()
            symbol = exit_trade.get("symbol")
            strategy = exit_trade.get("strategy")

            # Find last open entry for same symbol + strategy
            open_leg = (
                session.query(TradeLog)
                .filter(TradeLog.symbol == symbol)
                .filter(TradeLog.strategy == strategy)
                .filter(TradeLog.leg_status == "open")
                .order_by(TradeLog.id.desc())
                .first()
            )

            if open_leg:
                exit_price = float(exit_trade.get("price", 0))
                entry_price = float(open_leg.price or 0)
                qty = float(exit_trade.get("quantity", 0))

                # Calculate PnL (for long vs short)
                if open_leg.action.upper() == "CALL":
                    pnl = (exit_price - entry_price) * qty
                else:
                    pnl = (entry_price - exit_price) * qty

                open_leg.pnl = pnl
                open_leg.leg_status = "closed"
                session.commit()

                order_logger.info(
                    f"💰 Trade reconciled: {symbol} | Entry:{entry_price:.2f} | Exit:{exit_price:.2f} | Qty:{qty} | PnL:{pnl:.2f}"
                )
            else:
                order_logger.warning(f"No open leg found for {symbol} ({strategy}) to reconcile.")

            session.close()

        except Exception as e:
            order_logger.error(f"❌ PnL reconciliation failed: {e}", exc_info=True)
            
    # -------------------------
    # Scheduler & Strategy job
    # -------------------------
    def init_scheduler(self):
        self.scheduler = BackgroundScheduler(timezone=IST)
        self.scheduler.add_job(
            self.strategy_job,
            CronTrigger(minute=f"*/{int(SIGNAL_CHECK_INTERVAL)}", second=5),
            id="strategy_signal_job",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("APScheduler started — signal checks every %s minute(s)", SIGNAL_CHECK_INTERVAL)

    def strategy_job(self):
        """
        Main scheduled job – runs aligned to timeframe close calls.
        🎯 In simulation: called synchronously by replay at minute%5==0
        🎯 In live: called by APScheduler every 5 minutes
        """
        try:
            # 🎯 Use get_now() for proper timestamp (simulated or real)
            current_time = get_now()
            
            if BACKTESTING_MANAGER.active:
                signal_logger.info(f"⏰ Strategy check (SIMULATED): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # If reached daily trade limit, initiate graceful shutdown
            if not self.position and self.trade_count >= MAX_TRADES_PER_DAY:
                logger.info("Max trades reached (%d). Initiating graceful shutdown.", MAX_TRADES_PER_DAY)
                self.shutdown_gracefully()
                return

            # Ensure static daily indicators are present and fresh
            if not self.static_indicators or self.static_indicators_date != current_time.date():
                ok = self.compute_static_indicators(symbol=self.spot_symbol, exchange=self.spot_exchange)
                if not ok:
                    logger.warning("Static indicators missing – skipping this tick")
                    return

            # ENTRY: only attempt if no open position
            if not self.position and not self.exit_in_progress:
                intraday = self.get_intraday()
                if intraday.empty:
                    logger.debug("No intraday data")
                else:
                    signal = self.check_entry_signal(intraday)
                    if (LTP_BREAKOUT_ENABLED and self.pending_breakout):
                        return True
                    if signal:
                        self.place_entry_order(signal, intraday)
                        return True
                    else:
                        return False

            # EXIT: if position exists, check exit conditions
            if self.position and not self.exit_in_progress:
                intraday = self.get_intraday(
                            days=LOOKBACK_DAYS, 
                            symbol=self.spot_symbol, 
                            exchange=self.spot_exchange,
                            instrument_type="SPOT")
                intraday_df_opt = self.get_intraday(
                            days=LOOKBACK_DAYS,
                            symbol=self.option_symbol,
                            exchange=OPTION_EXCHANGE,
                            instrument_type="OPTIONS"
                            )
                if not intraday.empty and intraday_df_opt is not None:
                    reason = self.check_exit_signal(intraday, intraday_df_opt)
                    if reason:
                        logger.info("Exit triggered: %s", reason)
                        self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=(reason,), daemon=True).start()

                if not market_is_open():
                    logger.info("🛑 Market closed right now. Please schedule the bot to run during market hours.")
                    self.shutdown_gracefully()
                    return

        except Exception as e:
            logger.exception("strategy_job execution failed", exc_info=e)

    def strategy_thread(self):
        """
        Strategy thread wrapper - behavior depends on mode:
        - LIVE: starts APScheduler
        - SIMULATION: no scheduler (replay handles timing)
        """
        logger.info("Strategy scheduler starting")
        
        # 🎯 Only start scheduler in LIVE mode
        if not BACKTESTING_MANAGER.active:
            self.init_scheduler()
        else:
            main_logger.info("📅 Simulation mode - APScheduler disabled (replay controls timing)")
            
        try:
            while not self.stop_event.is_set() and self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Manual stop requested")
            self.shutdown_gracefully()
        finally:
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
                logger.info("Scheduler stopped")

    def reset_pending_breakout(self):
        """Reset any active pending breakout state."""
        self.pending_breakout = None
        self.pending_breakout_expiry = None
        self.breakout_side = None
        # with self._pivot_gate_lock:
        #     self.ltp_pivot_breakout = False
        #     self.ltp_pivot_info = None

    def handle_exit_ltp_breakout(self, current_time):
            """
            Handle EXIT confirmation via LTP breakout
            (Triggered after reversal candle detection)
            """
            # No pending exit → nothing to do
            if not LTP_BREAKOUT_ENABLED:
                return False

            if not self.pending_exit_breakout or not self.position:
                self.reset_timer()
                return False

            # Expiry check
            if current_time > self.pending_exit_breakout_expiry:
                signal_logger.info("❌ Exit breakout window expired clearing.")
                self.pending_exit_breakout = None
                self.exit_breakout_side = None
                self.reset_timer()
                return False

            # We always use OPTION LTP for exit confirmation
            option_ltp = getattr(self, "option_ltp", None)
            if option_ltp is None:
                return False

            side = self.exit_breakout_side
            high = self.pending_exit_breakout.get("high")
            low = self.pending_exit_breakout.get("low")
            
            now_ts = current_time
            # -------------------------------
            # EXIT CALL → LTP breaks BELOW low
            # -------------------------------
            if side == "EXIT_BUY":
                if option_ltp < low:
                    if not self.entry_timer_started:
                        self.start_timer(now_ts)
                        return False
                    elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                    if elapsed < EXIT_CONFIRM_SECONDS:
                        signal_logger.info(
                            f"⏳ EXIT TRADE - LTP BREAKOUT DETECTED @ {option_ltp:.2f}, waiting for time breakout confirmation..."
                        )   
                        return False
                    if elapsed >= EXIT_CONFIRM_SECONDS:
                        signal_logger.info(
                            f"⏳ EXIT TRADE - TIME BREAKOUT DETECTED @ {option_ltp:.2f}, confirming..."
                        )
                        if option_ltp < low:
                            signal_logger.info(
                                f"🔴 EXIT TRADE - LTP BREAKOUT CONFIRMED AFTER {elapsed:.1f}s @ {option_ltp:.2f}"
                                )
                            # Check trigger type for appropriate exit reason
                            trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                            exit_reason = "CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_LTP_CONFIRM"
                            self._trigger_exit(exit_reason)
                            self.reset_timer()
                            return True
                        else:
                            self.reset_timer()
                            return False
                else:
                    self.reset_timer()
                    return False

            # ==============================
            # EXIT SELL → Option must stay ABOVE high
            # ==============================
            elif side == "EXIT_SELL":

                if option_ltp > high:
                    if not self.entry_timer_started:
                        self.start_timer(now_ts)
                        return False

                    elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()

                    if elapsed >= EXIT_CONFIRM_SECONDS:
                        if option_ltp > high:
                            signal_logger.info(
                                f"🔴 EXIT TRADE - PUT SIDE LTP BREAKOUT CONFIRMED AFTER {elapsed:.1f}s @ {option_ltp:.2f}"
                            )
                            # Check trigger type for appropriate exit reason
                            trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                            exit_reason = "CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_LTP_CONFIRM"
                            self._trigger_exit(exit_reason)
                            self.reset_timer()
                            return True
                        else:
                            self.reset_timer()
                            return False
                else:
                    self.reset_timer()
                    return False

            return False

    def _trigger_exit(self, reason: str):
            with self._state_lock:
                if self.exit_in_progress:
                    return
                self.exit_in_progress = True

            threading.Thread(
                target=self.place_exit_order,
                args=(reason,),
                daemon=True
            ).start()

            # Clear exit breakout state
            self.pending_exit_breakout = None
            self.exit_breakout_side = None
    
    def check_ltp_pivot_break(self):
        """
        Gatekeeper breakout logic:
        1. On first call (no stored pivots):
            - Compute nearest resistance and nearest support pivot
            - Store them
        2. On all later calls:
            - Validate breakout only against stored pivots
            - Do NOT recalculate nearest pivots dynamically
            """

        try:
            if not self.static_indicators:
                ok = self.compute_static_indicators(symbol=self.spot_symbol, exchange=self.spot_exchange)
                if not ok:
                    logger.warning("Static indicators missing for LTP pivot breakout check")
                    return False, None 

            ltp = float(self.ltp or 0.0)
        # -------------------------------------------
        # STEP 1 — IF NOT STORED → FIND & STORE ONCE
        # --------------------------------------------
            if self.gk_resistance_pivot is None and self.gk_support_pivot is None:
                pivots_dict = {}
                daily_pivot = float(self.static_indicators.get("DAILY", {}).get("pivot",0.0))
                for tf in ["DAILY", "WEEKLY", "MONTHLY"]:
                    if tf in self.static_indicators:
                        pivots_dict[tf.lower()] = self.static_indicators[tf]

                filtered_levels, level_mapping, excluded = merge_pivot_levels_with_exclusion(
                    pivots_dict,
                    CPR_EXCLUDE_PIVOTS
                )
                if not filtered_levels:
                    return False, None

                pivots = [float(p) for p in level_mapping.keys()]
                # Dynamic resistance/support detection (only once)
                resistance = [p for p in pivots if p > daily_pivot]
                support    = [p for p in pivots if p < daily_pivot]

                if resistance:
                    self.gk_resistance_pivot = min(resistance)
                if support:
                    self.gk_support_pivot = max(support)

                signal_logger.info(
                    f"[GATEKEEPER INIT] Stored Pivots → "
                    f"Resistance={self.gk_resistance_pivot}, Support={self.gk_support_pivot}"
                )

            # --------------------------------------------
            # STEP 2 — VALIDATE BREAKOUT AGAINST STORED PIVOTS
            # --------------------------------------------
            
            # --- 🚫 TIME FILTER: Skip first candle volatility (wait till 09:20) ---
            now = get_now()
            if now.time() < dt_time(9, 20):
                #  signal_logger.debug("Skipping pivot breakout check after 09:20")
                 return False, None

            # Resistance breakout
            if self.gk_resistance_pivot is not None and ltp > self.gk_resistance_pivot:
                return True, {
                    "pivot": self.gk_resistance_pivot,
                    "identifier": "RESISTANCE_BREAK",
                    "timestamp": now
                }

            # Support breakdown
            if self.gk_support_pivot is not None and ltp < self.gk_support_pivot:
                return True, {
                    "pivot": self.gk_support_pivot,
                    "identifier": "SUPPORT_BREAK",
                    "timestamp": now
                }

            return False, None

        except:
            signal_logger.exception("check_ltp_pivot_break failed")
            return False, None
    
    def handle_entry_ltp_breakout(self,current_time):
        """
        Handle ENTRY confirmation via LTP breakout
        (Triggered after reversal candle detection)
        """
        signal_logger.info(f"✅ LTP breakout check " +  
                           f"| Pending Breakout: {self.pending_breakout} " +
                           f"| Expiry: {self.pending_breakout_expiry} " +
                           f"| Side: {self.breakout_side}"
                           )    
        now_ts = current_time
        side = self.breakout_side
        high = self.pending_breakout.get("high", 0)
        low = self.pending_breakout.get("low", 0)
        # --------------------------
        # 1️⃣ Breakout expiry time next 5 mins
        # --------------------------
        if now_ts > self.pending_breakout_expiry:
            signal_logger.info("❌ Breakout window expired | clearing pending signal.")
            self.reset_timer()
            self.reset_pending_breakout()
            self.unsubscribe_option_ltp(self.option_symbol)
            return
        # --------------------------
        # 1.1 🆕 OPTION MODE BREAKOUT LOGIC
        # --------------------------
        mode = self.pending_breakout.get("mode", "SPOT")
        
        if mode == "OPTIONS":
            current_opt_ltp = getattr(self, "option_ltp", 0.0) or 0.0
            if current_opt_ltp == 0.0:
                # signal_logger.info("⚠️ OPTION LTP not available yet for breakout check")
                return
            break_level = high
            logger.info(f"🔔 OPTION MODE BREAKOUT CHECK | Side: {side} | Current Option LTP: {current_opt_ltp:.2f} | Break Level: {break_level:.2f}")
            # For Options, BOTH Calls (BUY) and Puts (SELL) must break HIGH to enter
            if current_opt_ltp > break_level:
                 logger.info(f"🔔 OPTION LTP above break level for {side} side")
                 if not self.entry_timer_started:
                    self.start_timer(now_ts)
                    return
                 elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                 if elapsed < ENTRY_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳Waiting for entry seconds breakout. current second {elapsed:.1f}s @ current_opt_ltp: {current_opt_ltp:.2f}")
                    return
                 if elapsed >= ENTRY_CONFIRM_SECONDS:
                     signal_logger.info(f"⏳ Verifying OPTION {side} breakout after {elapsed:.1f}s @ current_opt_ltp: {current_opt_ltp:.2f}")
                     if current_opt_ltp > break_level:
                         signal_logger.info(f"🚀 OPTION BREAKOUT CONFIRMED! ({side}) | LTP {current_opt_ltp:.2f} > {break_level:.2f} | Elapsed: {elapsed:.1f}s")
                         intraday = self.get_intraday()
                         # Side remains "BUY" or "SELL" to indicate CE or PE
                         self.place_entry_order(side, intraday)
                         self.reset_timer()
                         self.reset_pending_breakout()
                         return
                     else:
                         signal_logger.info("❌ OPTION breakout wick failed - resetting timer")
                         self.reset_timer() # Wick failed
                         return
            else:
                self.reset_timer() # Fell back
                return
            # Force return to skip Spot logic if in Option mode
            return
        # --------------------------
        # 2️⃣ BUY breakout logic (Call option breakout) for Spot
        # --------------------------
        if side == "BUY":
            if self.ltp > high:  # Price above breakout?
                # Start timer if first cross
                if not self.entry_timer_started:
                    self.start_timer(now_ts)
                    return 
                elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                if elapsed < ENTRY_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳ Verifying BUY breakout after {elapsed:.1f}s @ {self.ltp:.2f}")
                    return
                # Has it held long enough?
                if elapsed >= ENTRY_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳ Verifying BUY breakout after {elapsed:.1f}s @ {self.ltp:.2f}")
                    if self.ltp > high:
                        self.ltp = high
                        signal_logger.info(f"🚀 ENTRY BREAKOUT CONFIRMED AT (CALL SIDE) AFTER {elapsed:.1f}s @ {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("BUY", intraday)
                        self.reset_timer()
                        self.reset_pending_breakout()
                        return
                    else:
                        # Wick failed
                        signal_logger.info("❌ BUY breakout wick failed - resetting timer")
                        self.reset_timer()
                        return
            else:
                # Fell back below breakout → reset
                self.reset_timer()
                return
        # --------------------------
        # 3️⃣ SELL breakout logic
        # --------------------------
        if side == "SELL":
            if self.ltp < low:
                if not self.entry_timer_started:
                    self.start_timer(now_ts)
                    return
                
                elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                if elapsed < ENTRY_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳ Verifying SELL breakout after {elapsed:.1f}s @ {self.ltp:.2f}")
                    return
                if elapsed >= ENTRY_CONFIRM_SECONDS:
                    if self.ltp < low:
                        self.ltp = low
                        signal_logger.info(f"🚀 ENTRY BREAKOUT CONFIRMED AT (PUT SIDE) AFTER {elapsed:.1f}s @ {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("SELL", intraday)
                        self.reset_pending_breakout()
                        return
                    else:
                        # Wick failed
                        signal_logger.info("❌ SELL breakout wick failed - resetting timer")
                        self.reset_timer()
                        return
            else:
                # Fell back below breakout → reset
                signal_logger.info("❌ SELL breakout fell back - resetting timer")
                self.reset_timer()
                return
        # --------------------------
        # 4️⃣ Still waiting
        # --------------------------
        logger.info(f"⏳ Waiting for {side} breakout | LTP={self.ltp:.2f}")

    def start_timer(self, now_ts):
        self.entry_timer_started = True
        self.entry_timer_start_ts = now_ts
        signal_logger.info("⏱ Entry confirmation timer started at %s", now_ts.strftime("%H:%M:%S"))

    def reset_timer(self):
        if self.entry_timer_started:
            signal_logger.info("🔄 Entry timer reset (breakout rejected)")
        self.entry_timer_started = False
        self.entry_timer_start_ts = None

    def _update_option_ltp_from_df(self, current_ts):
        """
        Derive option LTP from option_1m_df
        using current simulated timestamp + intrabar logic
        """
        if self.option_1m_df is None or self.option_1m_df.empty:
            return None

        # Normalize current_ts to minute start
        ts = current_ts.replace(second=0, microsecond=0)
        
        # Reset intrabar on new candle
        if ts != self._last_option_candle_ts:
            self._last_option_candle_ts = ts
            self._option_intrabar_stage = 0

        try:
            # Look up the row
            row = self.option_1m_df.loc[ts.strftime("%Y-%m-%d %H:%M:%S")]
            
            o = float(row["open"])
            h = float(row["high"])
            l = float(row["low"])
            c = float(row["close"])

            # Conservative path logic (same as spot)
            if c >= o:
                path = [o, l, h, c]
            else:
                path = [o, h, l, c]

            stage = min(self._option_intrabar_stage, 3)
            ltp = path[stage]

            self._option_intrabar_stage += 1
            return float(ltp)

        except KeyError:
            # candle not available yet → keep last known LTP
            return self.option_ltp
    # -------------------------
    # WebSocket - Real time data LTP 
    # -------------------------  
    def on_ltp_update(self, data):
        try:
            # ----------------------------------------
            dtype = data.get("type")
            symbol = data.get("symbol") or (data.get("data") or {}).get("symbol")
            ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")
            timestamp = data.get("timestamp")
            self.option_ltp = None
            if symbol == self.option_symbol:
                self.option_ltp = float(ltp)
                # logger.info(f"Option LTP: {self.option_ltp} {timestamp}")
            elif symbol == SYMBOL:
                self.ltp = float(ltp)
            if symbol is None or ltp is None:
                position_logger.warning("No LTP data in tick data")
                return
            # 🎯 Get proper timestamp (simulated or real)
            current_time = get_now()
            now_str = current_time.strftime("%H:%M:%S")
            # 🎯 Only log LTP in SIMULATED mode
            if BACKTESTING_MANAGER.active:
                logger.debug(f"LTP: {self.ltp} {timestamp}")  # DEBUG: Tick-level data
                ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")
            else:
                self.last_ws_tick_time = time.time()
            # ----------------------------------------
            # Update Option LTP from DataFrame in SIMULATION mode
            if BACKTESTING_MANAGER.active and (self.position or self.pending_breakout) and USE_OPTION_FOR_SLTP:
                opt_ltp = self._update_option_ltp_from_df(current_time)
                if opt_ltp is not None:
                    self.option_ltp = opt_ltp
                    logger.debug(f"Option LTP: {self.option_ltp} {timestamp}")  # DEBUG: Tick-level data
                else:
                    position_logger.info(f"Option LTP not found in DataFrame for {self.option_symbol} at {current_time}")
            # Update LTP for spot breakout confirmation
            if BACKTESTING_MANAGER.active and LTP_BREAKOUT_ENABLED and self.pending_breakout:
                pass # Logic continues...
                side = self.breakout_side
                if side == "BUY":
                    self.ltp = data.get("high") 
                elif side == "SELL":
                    self.ltp = data.get("low") 
            # ----------------------------------------
            #  LTP Pivot Breakout Gatekeeper
            # ----------------------------------------
            if ENABLE_LTP_PIVOT_GATE and self.trade_count == 0:
                # Read flag safely
                with self._pivot_gate_lock:
                    gate_ok = self.ltp_pivot_breakout
              # Gate still locked → check for breakout
                if not gate_ok:
                      # 1️⃣ Call pivot-break logic (it auto-stores pivots on first call)
                    breakout, info = self.check_ltp_pivot_break()
                    if breakout:
                        with self._pivot_gate_lock:
                            self.ltp_pivot_breakout = True
                            self.ltp_pivot_info = info
                        signal_logger.info(f"[GATEKEEPER] UNLOCKED → {info}")
                    else:
                        # State-aware logging: log only once when entering wait state
                        if not hasattr(self, '_gatekeeper_waiting_logged') or not self._gatekeeper_waiting_logged:
                            signal_logger.info("[GATEKEEPER] Waiting — pivot breakout not yet happened")
                            self._gatekeeper_waiting_logged = True 
            # ----------------------------------------
            # ENTRY LTP Breakout Confirmation
            # ----------------------------------------
            if LTP_BREAKOUT_ENABLED and self.pending_breakout:
                self.handle_entry_ltp_breakout(current_time)
                return              
            # ----------------------------------------
            # EXIT LTP Breakout Confirmation
            # ----------------------------------------
            if self.handle_exit_ltp_breakout(current_time):
                return

            with self._state_lock:
                position = self.position
                exit_in_progress = self.exit_in_progress
                stoploss_price = self.stoploss_price
                target_price = self.target_price
                entry_price = self.entry_price
                option_symbol = getattr(self, 'option_symbol', None)
                spot_entry_price = getattr(self, 'spot_entry_price', None)
                option_entry_price = getattr(self, 'option_entry_price', None)
            # If no active position or still initializing, show status and skip
            if not position or exit_in_progress or stoploss_price in (None, 0.0) or target_price in (None, 0.0):
                current_time = time.time()
                if not hasattr(self, '_last_detailed_log') or current_time - self._last_detailed_log > 30:
                    print(f"\r[{now_str}] {symbol} LTP={self.ltp:.2f} | No Active Position |", end="")
                    self._last_detailed_log = current_time
                return
                
            # Choose LTP stream for monitoring (track_ltp). Do not re-read self.* after snapshot.
            track_ltp = None
            tracking_mode = "UNKNOWN"
            if USE_SPOT_FOR_SLTP:
                track_ltp = self.ltp
                tracking_mode = "SPOT"
                track_option_ltp = getattr(self, "option_ltp", None)
                if track_option_ltp is None:
                    track_option_ltp = option_entry_price if option_entry_price else entry_price
                self.option_ltp = track_option_ltp
            elif USE_OPTION_FOR_SLTP and option_symbol:
                tracking_mode = "OPTION"
                track_option_ltp = getattr(self, "option_ltp", None)
                if track_option_ltp is None:
                    # position_logger.info(f"Option LTP not available yet for {option_symbol}")
                    return
                track_ltp = track_option_ltp
                self.option_ltp = track_ltp

            if track_ltp is None:
                print(f"\r[{now_str}] Waiting for {tracking_mode} LTP updates...", end="")
                return

            # Compute P&L
            if position == "BUY":
                unreal = (track_option_ltp - (option_entry_price if option_entry_price else entry_price)) * QUANTITY
                distance_to_sl = track_ltp - stoploss_price
                distance_to_tp = target_price - track_ltp
            else:
                unreal = (track_option_ltp - (option_entry_price if option_entry_price else entry_price)) * QUANTITY
                distance_to_sl = track_ltp - stoploss_price
                distance_to_tp = target_price - track_ltp

            # Print brief status line
            # print(f"\r[{now}] {symbol} LTP={track_ltp:.2f} | {position} @ {entry_price:.2f} | P&L {unreal:.2f} | SL {stoploss_price:.2f} TG {target_price:.2f}", end="")

            # Detailed position logging occasionally
            current_time = time.time()
            if not hasattr(self, '_last_detailed_log') or current_time - self._last_detailed_log > 10:
                position_logger.info("=== Position Status Update ===")
                position_logger.info(f"Spot Entry: {position} @ {spot_entry_price:.2f}")
                position_logger.info(f"Strike: {position} @ {option_symbol}")
                position_logger.info(f"Option Entry: {position} @ {self.option_entry_price:.2f}")
                position_logger.info(f"Tracking Mode: {tracking_mode}")
                position_logger.info(f"Current Spot LTP: {self.ltp:.2f}")
                position_logger.info(f"Current Option LTP: {track_option_ltp:.2f}")
                position_logger.info(f"Unrealized P&L: {unreal:.2f}")
                position_logger.info(f"SL Level: {stoploss_price:.2f} (Distance: {distance_to_sl:.2f})")
                position_logger.info(f"TP Level: {target_price:.2f} (Distance: {distance_to_tp:.2f})")
                position_logger.info(f"Quantity: {QUANTITY}")
                self._last_detailed_log = current_time

            # --- SL/TP/TSL logic ---
            # Handle CPR Range SL/TP method
            if SL_TP_METHOD == "CPR_range_SL_TP":
                cpr_result = self.check_cpr_sl_tp_conditions(track_ltp)
                if cpr_result == "SL_HIT":
                    position_logger.warning(f"🔴 CPR STOP LOSS TRIGGERED at {track_ltp:.2f}")
                    print(f"\n[{now_str}] CPR Stoploss hit at {track_ltp:.2f}")
                    with self._state_lock:
                        self.exit_in_progress = True
                    threading.Thread(target=self.place_exit_order, args=("CPR_STOPLOSS",), daemon=True).start()
                    return
                elif cpr_result == "SL_PENDING_BREAKOUT":
                    # Waiting for LTP breakout confirmation
                    position_logger.debug(f"⏳ CPR SL detected, waiting for LTP breakout confirmation")
                    return  # Continue monitoring, handle_exit_ltp_breakout will confirm
                elif cpr_result == "FINAL_EXIT":
                    position_logger.info(f"🎯 CPR FINAL TARGET ACHIEVED at {track_ltp:.2f}")
                    print(f"\n[{now_str}] CPR Final target reached at {track_ltp:.2f}")
                    with self._state_lock:
                        self.exit_in_progress = True
                    threading.Thread(target=self.place_exit_order, args=("CPR_FINAL_TARGET",), daemon=True).start()
                    return
                elif cpr_result == "TP_HIT":
                    # TP hit handled in check_cpr_sl_tp_conditions, continue monitoring
                    return
            # If TSL is enabled and the SL_TP_METHOD is Signal_candle_range_SL_TP, handle TP hits by trailing instead of exiting immediately.
            elif TSL_ENABLED and SL_TP_METHOD == "Signal_candle_range_SL_TP":
                # TP hit -> advance TSL; SL hit -> exit
                if position == "BUY":
                    # Stoploss check (exit)
                    if track_ltp <= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} <= {stoploss_price:.2f}")
                        print(f"\n[{now_str}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return

                    # TP hit -> update trailing (do not exit)
                    if track_ltp >= target_price:
                        position_logger.info(f"🟢 TSL TP HIT: {track_ltp:.2f} >= {target_price:.2f} — advancing TSL")
                        self._handle_tsl_tp_hit(position, target_price)
                        return

                else:  # SELL
                    if track_ltp >= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} >= {stoploss_price:.2f}")
                        print(f"\n[{now_str}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return

                    if track_ltp <= target_price:
                        position_logger.info(f"🟢 TSL TP HIT: {track_ltp:.2f} <= {target_price:.2f} – advancing TSL")
                        self._handle_tsl_tp_hit(position, target_price)
                        return

            else:
                # Standard (non-TSL) behavior: exit immediately on TP or SL
                if position == "BUY":
                    if track_ltp <= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} <= {stoploss_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp >= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} >= {target_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return
                else:
                    if track_ltp >= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} >= {stoploss_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp <= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} <= {target_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return
        except Exception:
            logger.exception("on_ltp_update error")
            position_logger.error("Error in LTP update processing", exc_info=True)

    def subscribe_all_symbols(self):
        """
        subscribe all required symbols after websocket reconnect.
        """
        base_client = client
        for sym in list(self.ws_subscriptions):
            try:
                exchange = EXCHANGE if sym == SYMBOL else OPTION_EXCHANGE
                self.client.subscribe_ltp([{"exchange": exchange, "symbol": sym}], on_data_received=self.on_ltp_update)
                logger.info(f"🔁 Subscribed {sym}")
            except Exception as e:
                logger.error(f"❌ 🚨 Failed to subscribe {sym}: {e}")

    def websocket_thread(self):
        """
        Websocket/replay thread - adapts based on mode:
        🎯 SIMULATION: uses DateReplayClient
        🎯 LIVE: uses real websocket client
        """
        try:
            mode_str = "SIMULATION" if BACKTESTING_MANAGER.active else "LIVE"
            logger.info(f"🚀 Websocket thread starting ({mode_str} mode)")
            main_logger.info(f"🔌 Starting data feed ({mode_str} mode)")

            # 🎯 Choose client based on mode
            with BACKTESTING_MANAGER.lock:
                if BACKTESTING_MANAGER.active:
                    self.client = REPLAY_CLIENT
                    main_logger.info("🛰️ Using DateReplayClient for simulation")
                else:
                    self.client = client
                    main_logger.info("🛰️ Using live WebSocket client")

            if BACKTESTING_MANAGER.active:
                # 🎯 SIMULATION MODE: start replay
                self.client.connect()
                main_logger.info("🎬 Starting simulation replay...")
                REPLAY_CLIENT.bot_ref = self
                REPLAY_CLIENT.subscribe_ltp(
                    self.instrument,
                    on_data_received=self.on_ltp_update,
                    strategy_fn=self.strategy_job,
                )
                # Keep alive until sim stops
                while BACKTESTING_MANAGER.active and self.running and not self.stop_event.is_set():
                    time.sleep(0.2)
            else:
                # 🎯 LIVE MODE: robust websocket with retry
                self.ws_should_run = True
                self.ws_connected = False
                self.last_ws_tick_time = None
                self.ws_retry_count = 0
                spot_symbol = self.instrument[0]["symbol"]
                self.ws_subscriptions.add(spot_symbol) #spot instrument
                # ===============================
                # 🔁 MAIN LOOP (Market Session)
                # ===============================
                while self.ws_should_run and not self.stop_event.is_set():
                    # -------------------------------
                    # 🛑 Market closed → EXIT BOT
                    # -------------------------------
                    if not market_is_open():
                        logger.info("🛑 Market closed stopping websocket & exiting bot")
                        break
                    try:
                        # -------------------------------
                        # 🔌 Connect WebSocket
                        # -------------------------------
                        logger.info("🔌 Connecting LIVE WebSocket...")
                        self.client.connect()
                        self.ws_connected = True
                        self.ws_retry_count = 0 # reset on success
                        
                        self.subscribe_all_symbols() # subscribe all symbols                     
                        logger.info("🟢 LIVE WebSocket connected & subscribed")
                        # -------------------------------
                        # Keep alive until disconnect
                        # -------------------------------
                        while self.ws_connected and not self.stop_event.is_set():
                            time.sleep(0.5)
                            # Heartbeat check
                            if self.last_ws_tick_time and time.time() - self.last_ws_tick_time > HEARTBEAT_INTERVAL * 2:
                                logger.warning("⚠️ WebSocket heartbeat lost")
                                self.ws_connected = False
                                break
                            # Market close during session
                            if not market_is_open():
                                logger.info("🛑 Market closed during session")
                                self.ws_connected = False
                                break
                    except Exception as e:
                        logger.error(f"🔴 WebSocket error: {e}", exc_info=True)
                        self.ws_connected = False
                    # -------------------------------
                    # RECONNECT ONLY during market hours
                    # -------------------------------
                    if market_is_open():    
                        logger.info("🔁 Attempting WebSocket reconnect...")
                        self.ws_retry_count += 1
                        delay = min(RECONNECT_BASE * (2 ** self.ws_retry_count), RECONNECT_MAX)
                        logger.warning(
                            f"🔁 WebSocket reconnect attempt {self.ws_retry_count} in {delay:.1f}s"
                        )
                        time.sleep(delay)
                    else:
                        logger.info("🛑 Market closed, not reconnecting WebSocket.")
                        break                    
        except Exception:
            logger.exception("websocket error")
        finally:
            # -------------------------------
            # 🔌 CLEAN SHUTDOWN
            # -------------------------------
            if not BACKTESTING_MANAGER.active:
                try:
                    self.ws_should_run = False
                    self.ws_connected = False

                    if hasattr(self.client, "disconnect"):
                        self.client.disconnect()

                    logger.info("LIVE WebSocket disconnected")
                except Exception:
                    logger.exception("WebSocket disconnect failed")

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
                        order_logger.warning(f"❌ Missing orderid on attempt {attempt}")
                        order_logger.warning(f"❌ ORDER RESPONSE {result}")
                        continue  
                    order_id = result["orderid"] 

                    # ✅ Confirm executed price using get_executed_price 
                    if "sl-order" not in description.lower():
                        exec_price = None
                        exec_price = self.get_executed_price(order_id)
                        if exec_price is not None and exec_price > 0:
                            order_logger.info(f"✅ Executed price confirmed (order {order_id}) = {exec_price}")    
                            result["exec_price"] = exec_price
                        else:
                            order_logger.error(f"❌ Executed price not confirmed on attempt {attempt}")
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

    # ==========================================================
    # 🧠 Get intraday candle
    # ==========================================================
    # ==========================================================
    # 🧠 Get intraday candle
    # ==========================================================
    def get_intraday(self, days=LOOKBACK_DAYS, symbol=SYMBOL, exchange=EXCHANGE, instrument_type="SPOT") -> pd.DataFrame:
        """
        Fetch intraday OHLC data.
        ✅ Works identically in LIVE and SIMULATION modes
        ✅ Simulation Mode:
        - Fetches and caches 5m data once per simulated date
        - Uses cache and trims up to current simulated time
        - Computes current-day open/high/low using existing helper
        ✅ Live Mode:
        - Fetches from API every call (as before)
        - Computes current-day open/high/low via get_current_day_highlow()
        """
        try:
            # 🎯 Use get_now() for consistent behavior across live/sim
            end_date = get_now()
            start_date = end_date - timedelta(days=days)
            base_client = client  # always use base client
            
            order_logger.info(f"Fetching intraday: {symbol} {exchange} | {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # ==========================================================
            # 🧠 SIMULATION MODE
            # ==========================================================
            if BACKTESTING_MANAGER.active:
                try:
                    backtesting_day_key = end_date.date().isoformat()
                    order_logger.info(f"[SIM MODE] Fetching intraday for {backtesting_day_key}")
                    
                    # Create cache if not exists
                    if not hasattr(self, "backtesting_data_cache"):
                        self.backtesting_data_cache = {}
                        order_logger.debug("Created backtesting_data_cache")  # DEBUG: Internal detail

                    # 🔑 FIXED cache key
                    backtesting_day_key = (
                        end_date.date().isoformat(),
                        instrument_type.upper(),
                        symbol.upper(),
                        CANDLE_TIMEFRAME
                    )       
                    # Cache intraday once per simulated date
                    if backtesting_day_key not in self.backtesting_data_cache:
                        order_logger.info(f"🔁 Simulation: loading intraday data for {backtesting_day_key}")
                        df_full = self.get_historical(
                            interval=CANDLE_TIMEFRAME,
                            lookback=days,
                            symbol=symbol,
                            exchange=exchange,
                            instrument_type=instrument_type
                        )
                        if df_full is None:
                            order_logger.error(f"get_historical returned None for {backtesting_day_key}")
                            return pd.DataFrame()
                        self.backtesting_data_cache[backtesting_day_key] = df_full
                        order_logger.info(f"Cached {len(df_full)} rows for {backtesting_day_key}")
                    else:
                        order_logger.debug(f"Using cached data for {backtesting_day_key}")  # DEBUG: Internal optimization
                        df_full = self.backtesting_data_cache[backtesting_day_key]

                    if df_full.empty:
                        order_logger.warning(f"⚠️ Simulation cache empty for {backtesting_day_key}")
                        return pd.DataFrame()

                    # ⚠️ CRITICAL: Ensure df_full index is tz-aware (IST) before comparison
                    if df_full.index.tz is None:
                        # Localize tz-naive index to IST
                        try:
                            df_full.index = df_full.index.tz_localize(IST, ambiguous='raise', nonexistent='shift_forward')
                            order_logger.debug("Localized df_full.index from tz-naive to IST")
                        except Exception as e:
                            order_logger.error(f"Error localizing df_full index to IST: {e}", exc_info=True)
                            return pd.DataFrame()
                    elif df_full.index.tz != IST:
                        # Convert to IST if different timezone
                        try:
                            df_full.index = df_full.index.tz_convert(IST)
                            order_logger.debug(f"Converted df_full.index from {df_full.index.tz} to IST")
                        except Exception as e:
                            data_logger.error(f"Error converting df_full index to IST: {e}", exc_info=True)
                            return pd.DataFrame()

                    # Trim candles up to current simulated time
                    current_sim_time = get_now()
                    start_915 = pd.Timestamp(
                        year=current_sim_time.year,
                        month=current_sim_time.month,
                        day=current_sim_time.day,
                        hour=9, minute=15, second=0,
                        tz=IST
                    )
                    df = df_full[(df_full.index >= start_915) & (df_full.index <= current_sim_time)].copy()
                    df_intraday = df_full[(df_full.index <= current_sim_time)].copy()
                    
                    data_logger.debug(f"Trimmed to {len(df_intraday)} rows (up to {current_sim_time.strftime('%H:%M:%S')})")
                    
                    # ✅ Compute current-day levels using your helper
                    try:
                        day_open, day_high, day_low = self.get_current_day_highlow(df)
                        if day_open and day_high and day_low:
                            self.current_day_open = day_open
                            self.current_day_high = day_high
                            self.current_day_low = day_low
                            data_logger.info(f"📊 [SIM] Current Day Levels | O={day_open:.2f} H={day_high:.2f} L={day_low:.2f}")
                        else:
                            data_logger.warning("Day high/low computation returned None values")
                    except Exception as e:
                        data_logger.error(f"Error computing day high/low in SIM mode: {e}", exc_info=True)

                    return df_intraday
                    
                except Exception as e:
                    data_logger.error(f"Error in SIMULATION mode get_intraday: {e}", exc_info=True)
                    return pd.DataFrame()

            # ==========================================================
            # 🌐 LIVE MODE
            # ==========================================================
            else:
                try:
                    data_logger.info(f"[LIVE MODE] Fetching intraday {symbol} {exchange}")
                    
                    raw = self.retry_api_call(
                        func=base_client.history,
                        max_retries=3,
                        delay=2,
                        description="Intraday History Fetch",
                        symbol=symbol,
                        exchange=exchange,
                        interval=CANDLE_TIMEFRAME,
                        start_date=start_date.strftime("%Y-%m-%d"),
                        end_date=end_date.strftime("%Y-%m-%d")
                    )

                    if raw is None:
                        data_logger.warning(f"retry_api_call returned None for intraday fetch")
                        return pd.DataFrame()

                    df = normalize_df(raw)
                    
                    if df.empty:
                        data_logger.warning(f"normalize_df returned empty DataFrame for {symbol} {exchange}")
                        return pd.DataFrame()
                    
                    data_logger.debug(f"Fetched {len(df)} rows before trimming")

                    # Trim candles up to current live time
                    current_live_time = get_now()
                    df = df[df.index <= current_live_time]
                    
                    data_logger.debug(f"Trimmed to {len(df)} rows (up to {current_live_time.strftime('%H:%M:%S')})")

                    # ✅ Compute current-day high/low/open exactly as your system does
                    try:
                        day_open, day_high, day_low = self.get_current_day_highlow(df)
                        if day_open and day_high and day_low:
                            self.current_day_open = day_open
                            self.current_day_high = day_high
                            self.current_day_low = day_low
                            data_logger.info(f"📊 [LIVE] Current Day Levels | O={day_open:.2f} H={day_high:.2f} L={day_low:.2f}")
                        else:
                            data_logger.warning("Day high/low computation returned None values in LIVE mode")
                    except Exception as e:
                        data_logger.error(f"Error computing day high/low in LIVE mode: {e}", exc_info=True)

                    return df
                    
                except Exception as e:
                    data_logger.error(f"Error in LIVE mode get_intraday: {e}", exc_info=True)
                    return pd.DataFrame()
                    
        except Exception as e:
            data_logger.error(f"Critical error in get_intraday({symbol}, {exchange}): {e}", exc_info=True)
            logger.exception(f"get_intraday failed")
            return pd.DataFrame()
    # -------------------------
    # Static indicators 
    # -------------------------
    # -------------------------
    # Static indicators 
    # -------------------------
    def compute_static_indicators(self,symbol=SYMBOL, exchange=EXCHANGE, instrument_type="spot"):
        """
        Compute static CPR indicators for all enabled timeframes.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        try:
            # 🎯 Use get_now() for proper date handling
            current_time = get_now()
            
            self.static_indicators = {}
            indicators_logger.info("╔══════════════════════════════")
            indicators_logger.info("🔍 INITIALIZING MULTI-TIMEFRAME CPR COMPUTATION")
            indicators_logger.info(f"📐 Enabled CPR Timeframes: {', '.join(CPR)}")
            indicators_logger.info("╚══════════════════════════════")

            # DAILY CPR
            if 'DAILY' in CPR:
                dfdaily = self.get_historical("D",lookback=5, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['DAILY'] = self.compute_cpr(dfdaily, "DAILY")
                self.static_indicators_date = current_time.date()
                
            # WEEKLY CPR
            if 'WEEKLY' in CPR:
                dfweekly = self.get_historical("W",lookback=20, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['WEEKLY'] = self.compute_cpr(dfweekly, "WEEKLY")
                
            # MONTHLY CPR
            if 'MONTHLY' in CPR:
                dfmonthly = self.get_historical("M",lookback=70, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['MONTHLY'] = self.compute_cpr(dfmonthly, "MONTHLY")

            # Combined summary
            indicators_logger.info("╔═══════════════════════════")
            indicators_logger.info("📘 CPR SUMMARY OVERVIEW (All Active Timeframes)")
            for key, val in self.static_indicators.items():
                if val:
                    indicators_logger.info(
                        f"{key:<8} | Pivot:{val['pivot']:.2f} | BC:{val['bc']:.2f} | TC:{val['tc']:.2f} | Width:{val['cpr_range']:.2f}"
                    )
            indicators_logger.info("╚═══════════════════════════")
            return True

        except Exception as e:
            indicators_logger.error(f"❌ Error computing static indicators: {e}", exc_info=True)
            return False         
    # -------------------------
    # Get Historical OHLCV 
    # -------------------------
    def get_historical(
        self,
        interval="1m",
        lookback=90,
        symbol=SYMBOL,
        exchange=EXCHANGE,
        instrument_type="SPOT"
    ):
        """
        Historical data loader.
        LIVE        → API only
        BACKTEST    → DuckDB first, API fallback
        """

        try:
            end_date = get_now()
            start_date = end_date - timedelta(days=lookback)

            # Normalize interval for API
            api_interval = "D" if interval in ["W", "M"] else interval

            start_str = start_date.strftime("%Y-%m-%d")
            end_str = end_date.strftime("%Y-%m-%d")

            # ==================================================
            # 🟢 LIVE MODE → API ONLY
            # ==================================================
            if MODE == "LIVE":
                raw = self.retry_api_call(
                    func=client.history,
                    max_retries=3,
                    delay=2,
                    description=f"LIVE Historical ({interval})",
                    symbol=symbol,
                    exchange=exchange,
                    interval=api_interval,
                    start_date=start_str,
                    end_date=end_str
                )

                df = normalize_df(raw)

                if df.empty:
                    indicators_logger.warning(
                        f"[LIVE] No historical data for {symbol} {interval}"
                    )
                    return df
            # ==================================================
            # 🔵 BACKTEST / SIMULATION MODE → DB + API
            # ==================================================
            else:
                df = DB_MANAGER.get_ohlcv_data(
                    symbol=symbol,
                    exchange=exchange,
                    timeframe=api_interval,
                    start_date=start_str,
                    end_date=end_str,
                    instrument_type = instrument_type,
                    underlying_symbol= SYMBOL,
                    expiry_date= self.expiry_cache["expiry_dt"]
                    )
                # validate all the given start and end dates are covered

                if df.empty:
                    indicators_logger.warning(
                        f"[BACKTESTING][Data MISSING] {symbol} {interval} {start_str} - {end_str} → Proceed to API fetch"
                    )

                    raw = self.retry_api_call(
                        func=client.history,
                        max_retries=3,
                        delay=2,
                        description=f"BACKTEST Historical ({interval})",
                        symbol=symbol,
                        exchange=exchange,
                        interval=api_interval,
                        start_date=start_str,
                        end_date=end_str
                    )

                    df_api = normalize_df(raw)

                    if df_api.empty:
                        return df_api

                    # Store once
                    DB_MANAGER.store_ohlcv_data(
                        symbol=symbol,
                        exchange=exchange,
                        timeframe=api_interval,
                        data=df_api.reset_index(),
                        replace=False,
                        instrument_type=instrument_type
                    )

                    df = DB_MANAGER.get_ohlcv_data(
                        symbol=symbol,
                        exchange=exchange,
                        timeframe=api_interval,
                        start_date=start_str,
                        end_date=end_str,
                        instrument_type = instrument_type
                    )

            if df.empty:
                return df
            # ==================================================
            # Post-processing (Weekly / Monthly)
            # ==================================================
            if isinstance(df.index, pd.DatetimeIndex) is False:
                df = df.reset_index().set_index("timestamp")

            if interval == "W":
                df = df.resample("W").agg({
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum"
                }).dropna()

            elif interval == "M":
                df = df.resample("M").agg({
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum"
                }).dropna()

            indicators_logger.info(
                f"[HISTORICAL][{MODE}] {symbol} {interval} | "
                f"Records:{len(df)} | "
                f"{df.index.min().strftime('%d-%b-%Y')} → {df.index.max().strftime('%d-%b-%Y')}"
            )

            return df

        except Exception as e:
            logger.exception(
                f"get_historical failed [{MODE}] ({symbol} {interval}): {e}"
            )
            return pd.DataFrame()
    #--------------------------
    # CPR - Daily, Weekly, Monthly
    #--------------------------  
    def compute_cpr(self, df, label: str):
        """Generic CPR computation used by daily, weekly, monthly methods."""
        if len(df) < 1:
            indicators_logger.warning(f"{label} CPR: insufficient bars to compute.")
            return None
        if len(df) > 1:
           index_df = -2
           prev = df.iloc[index_df]
        else:
           index_df = -1
           prev = df.iloc[index_df]
        
        high, low, close = prev['high'], prev['low'], prev['close']
        pivot = (high + low + close) / 3
        bc = (high + low) / 2
        tc = 2 * pivot - bc
        cpr_range = abs(tc - bc)
        r1 = (2 * pivot) - low
        s1 = (2 * pivot) - high
        r2 = pivot + (high - low)
        s2 = pivot - (high - low)
        r3 = high + 2 * (pivot - low)
        s3 = low - 2 * (high - pivot)
        r4 = r3 + (r2 - r1)
        s4 = s3 - (s1 - s2)

        indicators_logger.info("╔═══════════════════════")
        indicators_logger.info(f"📊 {label.upper()} CPR INDICATOR LEVELS (Previous {label.title()} Data)")
        indicators_logger.info("────────────────────────")
        if label.upper() == "DAILY":
            indicators_logger.info(f"Date: {df.index[index_df].strftime('%d-%b-%Y')}")
        elif label.upper() == "WEEKLY":
            indicators_logger.info(f"Week Start: {df.index[index_df].strftime('%d-%b-%Y')}")
        else:
            indicators_logger.info(f"Month: {df.index[index_df].strftime('%b %Y')}")
        indicators_logger.info(f"High: {high:.2f} | Low: {low:.2f} | Close: {close:.2f}")
        indicators_logger.info(f"Pivot: {pivot:.2f} | BC: {bc:.2f} | TC: {tc:.2f}")
        indicators_logger.info(f"CPR Width: {cpr_range:.2f}")
        indicators_logger.info(f"R1: {r1:.2f} | R2: {r2:.2f} | R3: {r3:.2f} | R4: {r4:.2f}")
        indicators_logger.info(f"S1: {s1:.2f} | S2: {s2:.2f} | S3: {s3:.2f} | S4: {s4:.2f}")
        indicators_logger.info("╚═══════════════════════")

        return {
            "pivot": pivot,
            "bc": bc,
            "tc": tc,
            "cpr_range": cpr_range,
            "r1": r1,
            "s1": s1,
            "r2": r2,
            "s2": s2,
            "r3": r3,
            "s3": s3,
            "r4": r4,
            "s4": s4,
            "high": high,
            "low": low,
            "close": close,
            "timestamp": df.index[index_df]
        }
        
    # -------------------------
    # Dynamic indicators (every current candle timeframe)
    # -------------------------
    def get_vwap(self, intraday_df, symbol=None, exchange=None, interval=None, instrument_type="OPTIONS"):
        """
            Generic VWAP calculator.
        - Uses given symbol/exchange/interval OR falls back to global config.
        - VWAP uses OHLCV from history() with same timeframe as indicators.
        - Returns VWAP for last completed candle (-2).
        """
        try:
            logger.info("VWAP calculation starts")
            # ---------------------------
            # Use global config if not provided
            # ---------------------------
            df = intraday_df.copy()
            df["date"] = df.index.date

            tp = (df["high"] + df["low"] + df["close"]) / 3
            df["tpv"] = tp * df["volume"]

            cum_vol = df.groupby("date")["volume"].cumsum()
            cum_tpv = df.groupby("date")["tpv"].cumsum()

            df["VWAP"] = cum_tpv / cum_vol.replace(0, None)

            # Return last completed candle VWAP
            if len(df) >= 2:
                return float(df["VWAP"].iloc[-2])
            else:                     
                indicators_logger.warning("VWAP calculation skipped: Not an OPTIONS instrument")
                return None

        except Exception as e:
            indicators_logger.error(f"VWAP calculation error: {e}")
            return None
        
    def compute_dynamic_indicators(self, intraday_df):
        """
        Compute EMA, SMA, RSI on intraday_df.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        dyn = {}
        if intraday_df is None or intraday_df.empty:
            indicators_logger.warning("Empty intraday DataFrame - cannot compute dynamic indicators")
            return dyn

        df = intraday_df.copy()
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Need at least 3 candles to have prev & last (last completed) reliably
        if len(df) < 3:
            indicators_logger.warning(f"Insufficient data for indicators: {len(df)} candles (need at least 3)")
            return dyn

        indicators_logger.debug(f"Computing dynamic indicators on {len(df)} candles")

        # Compute EMAs
        for p in EMA_PERIODS:
            df[f"EMA_{p}"] = df["close"].ewm(span=p, adjust=False).mean()
            dyn[f"EMA_{p}"] = float(df[f"EMA_{p}"].iloc[-2])

        # Compute SMAs
        for p in SMA_PERIODS:
            df[f"SMA_{p}"] = df["close"].rolling(window=p, min_periods=1).mean()
            dyn[f"SMA_{p}"] = float(df[f"SMA_{p}"].iloc[-2])

        # Compute RSI
        for p in RSI_PERIODS:
            delta = df["close"].diff()
            up = delta.clip(lower=0)
            down = -1 * delta.clip(upper=0)
            roll_up = up.ewm(span=p, adjust=False).mean()
            roll_down = down.ewm(span=p, adjust=False).mean()
            rs = roll_up / roll_down.replace(0, 1e-8)
            rsi_series = 100.0 - (100.0 / (1.0 + rs))
            dyn[f"RSI_{p}"] = float(rsi_series.iloc[-2])

        # ---------------------------------------------------
        # VWAP using OHLCV of configured symbol/exchange/interval
        # ---------------------------------------------------
        try:
            vwap_value = self.get_vwap(intraday_df, symbol=self.option_symbol, exchange=OPTION_EXCHANGE, interval=CANDLE_TIMEFRAME)
            if vwap_value is not None:
                dyn["VWAP"] = vwap_value
                indicators_logger.info(f"VWAP: {vwap_value:.2f}")  # DEBUG: Calculation detail
        except Exception as e:
            indicators_logger.error(f"VWAP integration error: {e}")

        # Candles: last completed (-2) and prev (-3) and current day high low
        last_candle = df.iloc[-2].to_dict()
        prev_candle = df.iloc[-3].to_dict()
        dyn["last_candle"] = last_candle
        dyn["prev_candle"] = prev_candle

        # ✅ Compute current-day levels
        day_open, day_high, day_low = self.get_current_day_highlow(df)
        if day_open and day_high and day_low:
            dyn["current_day_open"] = day_open
            dyn["current_day_high"] = day_high
            dyn["current_day_low"]  = day_low

        self.current_day_high = float(dyn.get("current_day_high", 0.0))
        self.current_day_low = float(dyn.get("current_day_low", 0.0))
        self.current_day_open = float(dyn.get("current_day_open", 0.0))

        indicators_logger.debug(
            f"📊 Current Day Levels | O={day_open} H={day_high} L={day_low}"
            )

        # Comprehensive logging of computed indicators
        indicators_logger.debug("=== Dynamic Indicators Computed ===")
        
        # Log EMA values
        ema_values = []
        for p in EMA_PERIODS:
            if f"EMA_{p}" in dyn:
                ema_values.append(f"EMA_{p}={dyn[f'EMA_{p}']:.2f}")
        indicators_logger.debug(f"EMA Values: {', '.join(ema_values)}")
        
        # Log SMA values  
        sma_values = []
        for p in SMA_PERIODS:
            if f"SMA_{p}" in dyn:
                sma_values.append(f"SMA_{p}={dyn[f'SMA_{p}']:.2f}")
        indicators_logger.debug(f"SMA Values: {', '.join(sma_values)}")
        
        # Log RSI values
        rsi_values = []
        for p in RSI_PERIODS:
            if f"RSI_{p}" in dyn:
                rsi_values.append(f"RSI_{p}={dyn[f'RSI_{p}']:.2f}")
        indicators_logger.debug(f"RSI Values: {', '.join(rsi_values)}")

        # Log Dynamic data
        lc = dyn.get("last_candle", {})
        pc = dyn.get("prev_candle", {})
        if lc:
            indicators_logger.debug(f"Last Candle: O={lc.get('open',0):.2f}, H={lc.get('high',0):.2f}, L={lc.get('low',0):.2f}, C={lc.get('close',0):.2f}")
        if pc:
            indicators_logger.debug(f"Prev Candle: O={pc.get('open',0):.2f}, H={pc.get('high',0):.2f}, L={pc.get('low',0):.2f}, C={pc.get('close',0):.2f}")

        return dyn
    # -------------------------
    # Entry & Exit conditions check
    # -------------------------
    def check_exit_signal(self, intraday_df, intraday_df_opt):
        """
        Evaluate exit signal conditions.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        try:
            signal_logger.info("=== Evaluating Exit Signal ===")
            
            if intraday_df is None or intraday_df.empty or intraday_df_opt is None or intraday_df_opt.empty:
                signal_logger.warning("No intraday data available for exit signal evaluation")
                return None
                
            if not self.position:
                signal_logger.debug("No position to exit")
                return None
                
            # Get current LTP and position details for logging context
            current_ltp = self.ltp if self.ltp else 0.0
            signal_logger.info(f"Current Position: {self.position} @ {self.entry_price:.2f}")
            signal_logger.info(f"Current LTP: {current_ltp:.2f}")
            signal_logger.info(f"SL Level: {self.stoploss_price:.2f}, TP Level: {self.target_price:.2f}")
            
            dyn = self.compute_dynamic_indicators(intraday_df)
            dyn_option = self.compute_dynamic_indicators(intraday_df_opt)
            
            if not dyn:
                signal_logger.warning("Failed to compute dynamic indicators for exit evaluation")
                return None

            #---Get spot candle details
            spot_candle = dyn.get("last_candle", {})
            spot_prev_candle = dyn.get("prev_candle", {})
            spot_close = float(spot_candle.get("close", 0.0))
            spot_open = float(spot_candle.get("open", 0.0))
            spot_high = float(spot_candle.get("high", 0.0))
            spot_low = float(spot_candle.get("low", 0.0))
            spot_prev_close = float(spot_prev_candle.get("close", 0.0))
            spot_prev_open = float(spot_prev_candle.get("open", 0.0))
            spot_prev_high = float(spot_prev_candle.get("high", 0.0))
            spot_prev_low = float(spot_prev_candle.get("low", 0.0))
            spot_ema20 = float(dyn.get("EMA_20", 0.0))
            spot_ema9 = float(dyn.get("EMA_9", 0.0))

            #---Get option candle details
            option_candle = dyn_option.get("last_candle", {})
            option_prev_candle = dyn_option.get("prev_candle", {})
            opt_close = float(option_candle.get("close", 0.0))
            opt_open = float(option_candle.get("open", 0.0))
            opt_high = float(option_candle.get("high", 0.0))
            opt_low = float(option_candle.get("low", 0.0))
            opt_prev_close = float(option_prev_candle.get("close", 0.0))
            opt_prev_open = float(option_prev_candle.get("open", 0.0))
            opt_prev_high = float(option_prev_candle.get("high", 0.0))
            opt_prev_low = float(option_prev_candle.get("low", 0.0))
            opt_ema20 = float(dyn_option.get("EMA_20", 0.0))
            opt_ema9 = float(dyn_option.get("EMA_9", 0.0))

            
            signal_logger.info(f"Exit Signal Evaluation Data:")
            signal_logger.info(f"  Last Spot Candle Close: {spot_close:.2f}")
            signal_logger.info(f"  Spot EMA20: {spot_ema20:.2f}")
            signal_logger.info(f"  Last Option Close: {opt_close:.2f}")
            signal_logger.info(f"  Option EMA20: {opt_ema20:.2f}")

            # Calculate current P&L for context
            if self.option_ltp is None:
                signal_logger.warning("Option LTP not available for P&L calculation")
                unrealized_pnl = 0.0
            else:
                self.option_ltp = self.option_ltp if self.option_ltp else 0.0
                unrealized_pnl = 0.0    
                if self.position == "BUY":
                    unrealized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
                else:
                    unrealized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
                
            signal_logger.info(f"  Current P&L: {unrealized_pnl:.2f}")
            
           # Check Multiple exit conditions
            if self.position == "BUY":
                signal_logger.info(f"Reversal Candle Check for CALL Position")
                is_spot_reversal = spot_prev_close < spot_prev_open and spot_close < spot_prev_low and spot_close < spot_open
                is_opt_reversal = opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open
                signal_logger.info(f"CALL Exit Check Spot: spot_prev_close < spot_prev_open and spot_close < spot_prev_low and spot_close < spot_open  -> {spot_prev_close:.2f} < {spot_prev_open:.2f} and {spot_close:.2f} < {spot_prev_low:.2f} and {spot_close:.2f} < {spot_open:.2f} = {is_spot_reversal}")
                signal_logger.info(f"CALL Exit Check Option: opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open -> {opt_prev_close:.2f} < {opt_prev_open:.2f} and {opt_close:.2f} < {opt_prev_low:.2f} and {opt_close:.2f} < {opt_open:.2f} = {is_opt_reversal}")
                if is_spot_reversal or is_opt_reversal:
                    if LTP_BREAKOUT_ENABLED:
                        now = get_now()
                        self.pending_exit_breakout = {
                            "type": "EXIT_BUY",
                            "low": float(opt_low - SL_BUFFER_POINT),
                            "high": float(opt_high + SL_BUFFER_POINT)
                        }
                        minute = now.minute
                        next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                        self.pending_exit_breakout_expiry = (
                            now.replace(minute=0, second=0, microsecond=0)
                            + timedelta(minutes=next_multiple)
                        )
                        self.exit_breakout_side = "EXIT_BUY"
                        signal_logger.info(
                            f"⏳ EXIT BUY waiting for OPTION LTP breakdown < {opt_low:.2f} "
                            f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                        )
                        return None
                    signal_logger.info("🔴 EXIT: Reversal candle confirmed (CALL)")
                    return "REVERSAL_CANDLE_FORMATION"
                is_spot_ema_exit = spot_close < spot_ema20
                is_opt_ema_exit = opt_close < opt_ema20    
                signal_logger.info(f"CALL Exit Check Spot: Close < EMA20 -> {spot_close:.2f} < {spot_ema20:.2f} = {is_spot_ema_exit}; CALL Exit Check Option: Close < EMA20 -> {opt_close:.2f} < {opt_ema20:.2f} = {is_opt_ema_exit}") 
                if is_spot_ema_exit or is_opt_ema_exit:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE BELOW EMA20 (CALL Position)")
                    return f"CLOSE_BELOW_EMA20 (close={spot_close:.2f}, EMA20={spot_ema20:.2f}, option_close={opt_close:.2f}, EMA20_option={opt_ema20:.2f})"
            elif self.position == "SELL":
                signal_logger.info(f"Reversal Candle Check for PUT Position")
                is_spot_reversal = spot_prev_close > spot_prev_open and spot_close > spot_prev_high and spot_close > spot_open
                is_opt_reversal = opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open
                signal_logger.info(f"PUT Exit Check Spot: spot_prev_close > spot_prev_open and spot_close > spot_prev_high and spot_close > spot_open  -> {spot_prev_close:.2f} > {spot_prev_open:.2f} and {spot_close:.2f} > {spot_prev_high:.2f} and {spot_close:.2f} > {spot_open:.2f} = {is_spot_reversal}")
                signal_logger.info(f"PUT Exit Check Option: opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open -> {opt_prev_close:.2f} < {opt_prev_open:.2f} and {opt_close:.2f} < {opt_prev_low:.2f} and {opt_close:.2f} < {opt_open:.2f} = {is_opt_reversal}") 
                if is_spot_reversal or is_opt_reversal:
                    if LTP_BREAKOUT_ENABLED:
                        now = get_now()
                        self.pending_exit_breakout = {
                            "type": "EXIT_BUY",
                            "high": float(opt_high + SL_BUFFER_POINT),
                            "low": float(opt_low - SL_BUFFER_POINT)
                        }
                        minute = now.minute
                        next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                        self.pending_exit_breakout_expiry = (
                            now.replace(minute=0, second=0, microsecond=0)
                            + timedelta(minutes=next_multiple)
                        )
                        self.exit_breakout_side = "EXIT_BUY"
                        signal_logger.info(
                            f"⏳ EXIT SELL waiting for OPTION LTP breakout < {opt_low:.2f} "
                            f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                        )
                        return None
                    signal_logger.info("🔴 EXIT: Reversal candle confirmed (PUT)")
                    return "REVERSAL_CANDLE_FORMATION"
                is_spot_ema_exit = spot_close > spot_ema20
                is_opt_ema_exit = opt_close < opt_ema20
                signal_logger.info(f"PUT Exit Check Spot: Close > EMA20 -> {spot_close:.2f} > {spot_ema20:.2f} = {is_spot_ema_exit} ; PUT Exit Check Option: Close < EMA20 -> {opt_close:.2f} < {opt_ema20:.2f} = {is_opt_ema_exit}")
                if is_spot_ema_exit or is_opt_ema_exit:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE ABOVE EMA20 (PUT Position)")
                    return f"CLOSE_ABOVE_EMA20 (close={spot_close:.2f}, EMA20={spot_ema20:.2f}, option_close={opt_close:.2f}, EMA20_option={opt_ema20:.2f})"

            signal_logger.info("⚪ NO EXIT SIGNAL - Position maintained")
            return None
            
        except Exception:
            logger.exception("check_exit_signal failed")
            signal_logger.error("Exit signal evaluation failed", exc_info=True)
            return None

    def get_current_day_highlow(self, df):
        """
        🔍 Compute current day's intraday High, Low, and Open (till last closed candle)
    
        ✅ Works for live and backtest data
        ✅ Excludes the currently forming candle
        ✅ Returns (day_open, day_high, day_low)
        """
        if df is None or df.empty:
            return None, None, None

        # Ensure timestamp is timezone-aware
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError("DataFrame must have a datetime index")

        # Localize to IST if needed
        if df.index.tz is None:
            df.index = df.index.tz_localize(IST)
        else:
            df.index = df.index.tz_convert(IST)

    # Filter only today's data
        current_date = get_now().date()
        df_today = df[df.index.date == current_date]

    # Exclude current forming candle (latest one)
        if len(df_today) > 1:
            df_for_highlow = df_today.iloc[:-1]
        else:
            df_for_highlow = df_today.copy()

        if df_for_highlow.empty:
            return None, None, None

        # ✅ Exclude both forming candle and the last closed candle (to avoid self-comparison)
        df_for_highlow = df_today.iloc[:-3]

        if df_for_highlow.empty:
            return None, None, None
        else:
            df_for_highlow = df_today.iloc[:-3]
            
        day_open = df_for_highlow["open"].iloc[0]
        day_high = df_for_highlow["high"].max()
        day_low  = df_for_highlow["low"].min()

        return day_open, day_high, day_low

    def check_entry_signal(self, intraday_df):
        """
        Comprehensive entry signal evaluation.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        try:
            signal_logger.info(f"=== 🧠 Evaluating Entry Signal === {get_now()}")

            if intraday_df is None or intraday_df.empty:
                signal_logger.warning("⚠️ No intraday data available for signal evaluation")
                return None

            # Get current LTP for logging context
            current_ltp = self.ltp if self.ltp else 0.0
            signal_logger.info(f"🔍 Current LTP: {current_ltp:.2f}")
             # ---------------------------
            if LTP_BREAKOUT_ENABLED and self.pending_breakout:
                signal_logger.info(f"⏳ LTP breakout enabled for entry.")
                return None

            # ONLY FIRST TRADES REQUIRE PIVOT GATE
            if ENABLE_LTP_PIVOT_GATE and self.trade_count == 0:
                with self._pivot_gate_lock:
                    if not self.ltp_pivot_breakout:
                        signal_logger.info("❗ Entry BLOCKED → Pivot Breakout not happened yet for first trade.")
                        return None
            
            # Current Day High/Low Validation (Early Exit for Performance)
            if self.trade_count >= DAY_HIGH_LOW_VALIDATION_FROM_TRADE:
                signal_logger.info(f"🔍 Applying day high/low validation (trade #{self.trade_count})")
                
                if len(intraday_df) < 2:
                    signal_logger.warning("⚠️ Insufficient intraday data for signal candle validation")
                    return None

                sig_candle = intraday_df.iloc[-2]  # Last completed candle (signal candle)
                close_price = float(sig_candle.get("close", 0.0))
                
                # Pre-validate for BUY signal
                if self.current_day_high is not None:
                    buy_valid = close_price > float(self.current_day_high)
                    signal_logger.info(f"BUY pre-validation: Signal close={close_price:.2f} > Day high={self.current_day_high:.2f} = {buy_valid}")
                else:
                    buy_valid = False
                    signal_logger.warning("Current day high not available for BUY validation")
                
                # Pre-validate for SELL signal  
                if self.current_day_low is not None:
                    sell_valid = close_price < float(self.current_day_low)
                    signal_logger.info(f"SELL pre-validation: Signal close={close_price:.2f} < Day low={self.current_day_low:.2f} = {sell_valid}")
                else:
                    sell_valid = False
                    signal_logger.warning("Current day low not available for SELL validation")
                
                # If neither BUY nor SELL can be valid, skip expensive technical analysis
                if not buy_valid and not sell_valid:
                    signal_logger.info("🔍 Day high/low pre-validation failed for both BUY and SELL, skipping technical analysis")
                    return None
                    
                signal_logger.info("🔍 Day high/low pre-validation passed, proceeding with technical analysis")
            else:
                signal_logger.info(f"🔍 Day high/low validation not required yet (trade #{self.trade_count} < {DAY_HIGH_LOW_VALIDATION_FROM_TRADE})")

            # Dynamic indicators
            dyn = self.compute_dynamic_indicators(intraday_df)
                
            if not dyn:
                signal_logger.warning("Failed to compute dynamic indicators")
                return None
            
            last = dyn["last_candle"]
            prev = dyn["prev_candle"]

            close = float(last.get("close", 0.0))
            open_ = float(last.get("open", 0.0))
            high = float(last.get("high", 0.0))
            low = float(last.get("low", 0.0))
            prev_close = float(prev.get("close", 0.0))
            prev_open = float(prev.get("open", 0.0))
            prev_high = float(prev.get("high", 0.0))
            prev_low = float(prev.get("low", 0.0))
            prev_close = float(prev.get("close", 0.0))
           

            ema9 = float(dyn.get("EMA_9", 0.0))
            ema20 = float(dyn.get("EMA_20", 0.0))
            ema50 = float(dyn.get("EMA_50", 0.0))
            ema200 = float(dyn.get("EMA_200", 0.0))

            # static daily
            if not self.static_indicators:
                signal_logger.error("Daily indicators not available for signal evaluation")
                return None
            cpr_r1 = float(self.static_indicators.get("DAILY", {}).get("r1", 0.0))
            cpr_s1 = float(self.static_indicators.get("DAILY", {}).get("s1", 0.0))
            prev_day_high = float(self.static_indicators.get("DAILY", {}).get("high",0.0))
            prev_day_low = float(self.static_indicators.get("DAILY", {}).get("low",0.0))
            daiily_pivot = float(self.static_indicators.get("DAILY", {}).get("pivot",0.0))
            weekly_pivot = float(self.static_indicators.get("WEEKLY", {}).get("pivot",0.0))
            
            # Log all key values for signal evaluation
            signal_logger.info(f"📊 Signal Evaluation Data:")
            signal_logger.info(f"  📈 Current Candle: Open={open_:.2f}, Close={close:.2f}")
            signal_logger.info(f"  📉 Previous Candle: Open={prev_open:.2f}, High={prev_high:.2f}, Low={prev_low:.2f}, Close={prev_close:.2f}")
            signal_logger.info(f"  📊 EMA Values: EMA9={ema9:.2f}, EMA20={ema20:.2f}, EMA50={ema50:.2f}")
            signal_logger.info(f"  📐 CPR Levels: R1={cpr_r1:.2f}, S1={cpr_s1:.2f}")
            signal_logger.info(f"  📊 Previous Day: High={prev_day_high:.2f}, Low={prev_day_low:.2f}")

            # require current_day_high/low as additional gate (if not available, treat it as false to avoid false entries)
            current_high_ok = (self.current_day_high is not None and close > self.current_day_high)
            current_low_ok = (self.current_day_low is not None and close < self.current_day_low)

            # -----------------------------------------------------------
            # 🗓️ EXPIRY DAY TRADING PRINCIPLE
            # -----------------------------------------------------------
            expiry_allowed = True # Default for non-expiry days
            
            # 1. OPTIMIZED EXPIRY DETECTION (Cached per day)
            current_date_val = get_now().date()
            is_expiry_day = False
            
            # Check cache
            if (
                self.expiry_cache.get("date") is not None and
                self.expiry_cache.get("is_expiry") is not None and
                self.expiry_cache.get("expiry_dt") is not None
                ):           
                is_expiry_day = self.expiry_cache["is_expiry"]
            else:
                # Need to identify expiry date
                resolved_expiry_date = None
                # A) BACKTESTING MODE & LIVE MODE: Use shared helper
                # Since get_nearest_weekly_expiry now handles logic for both, just call it.
                resolved_expiry_date = self.get_nearest_weekly_expiry()
                if isinstance(resolved_expiry_date, date):
                    expiry_str = resolved_expiry_date.strftime("%d-%m-%Y")
                elif isinstance(resolved_expiry_date, str):
                    expiry_str = datetime.strptime(resolved_expiry_date, "%Y-%m-%d").strftime("%d-%m-%Y")
                else:
                    raise TypeError("Invalid expiry type") 
                # Update Cache
                if resolved_expiry_date:
                    is_expiry_day = (resolved_expiry_date == current_date_val)
                    self.expiry_cache = {
                        "date": current_date_val,
                        "is_expiry": is_expiry_day,
                        "expiry_dt": expiry_str
                    }
                    signal_logger.info(f"🗓️ Expiry Cache Updated: Today={current_date_val}, Expiry={resolved_expiry_date}, IsExpiry={is_expiry_day}")
                else:
                    signal_logger.warning("⚠️ Could not resolve expiry date. Assuming Non-Expiry.")

            # 2. APPLY RULES IF EXPIRY DAY
            if is_expiry_day:
                    signal_logger.info(f"📅 TODAY IS EXPIRY DAY - Applying prev day/low breakout must for entry")
                    
                    now_time = get_now().time()
                    expiry_cutoff = dt_time(10, 30)
                    
                    if now_time < expiry_cutoff:
                        # Rule 1: Before 10:30 AM -> Must break Previous Day High OR Low (Generic)
                        broke_pdH = (close > prev_day_high)
                        broke_pdL = (close < prev_day_low)
                        
                        if broke_pdH or broke_pdL:
                             expiry_allowed = True
                             signal_logger.info(f"✅ 🎯 Expiry Rule (<10:30): Trading Allowed (Outside PD Range). Close={close:.2f} PDH={prev_day_high:.2f} PDL={prev_day_low:.2f}")
                        else:
                             expiry_allowed = False
                             signal_logger.info(f"⛔ Expiry Rule (<10:30): Trading Blocked (Inside PD Range). Close={close:.2f}")

                    else:
                        # Rule 2: After 10:30 AM -> Must break Current Day High OR Low (Generic)
                        broke_cdH = current_high_ok # close > current_day_high
                        broke_cdL = current_low_ok  # close < current_day_low
                        
                        if broke_cdH or broke_cdL:
                             expiry_allowed = True
                             signal_logger.info(f"✅ 🎯 Expiry Rule (>10:30): Trading Allowed (New Day High/Low). Close={close:.2f}")
                        else:
                             expiry_allowed = False
                             signal_logger.info(f"⛔ Expiry Rule (>10:30): Trading Blocked (Inside Current Day Range, day high/low breakout must). Close={close:.2f}")
            # -----------------------------------------------------------

            # Evaluate Long conditions step by step
            long_cond1 = (close > weekly_pivot)
            long_cond2 = (close > daiily_pivot)
            long_cond3 = (close > ema9)
            long_cond4 = (ema9 > ema20)
            long_cond5 = (prev_close > prev_open)
            long_cond6 = (close > open_)
            long_cond7 = (close > ema50)
            long_cond8 = (close > ema200)
            long_cond9 = (current_ltp > high)
            long_cond10 = current_high_ok
            long_cond11 = (close > prev_high)

            # Evaluate Short conditions step by step
            short_cond1 = (close < weekly_pivot)
            short_cond2 = (close < daiily_pivot)
            short_cond3 = (close < ema9)
            short_cond4 = (ema9 < ema20)
            short_cond5 = (prev_close < prev_open)
            short_cond6 = (close < open_)
            short_cond7 = (close < ema50)
            short_cond8 = (close < ema200)
            short_cond9 = (current_ltp < low)
            short_cond10 = current_low_ok
            short_cond11 = (close < prev_low)
            # Final Long/Short condition aggregation

            long_cond = long_cond2 and long_cond3 and long_cond4 and long_cond5 and long_cond6 and long_cond7 and long_cond8 and long_cond11 and expiry_allowed

            short_cond = short_cond2 and short_cond3 and short_cond4 and short_cond5 and short_cond6 and short_cond7 and short_cond8 and short_cond11 and expiry_allowed  

            # long_cond = long_cond5 and long_cond6  and expiry_allowed

            # short_cond = short_cond5 and short_cond6 and expiry_allowed          
            # Log detailed condition evaluation (matching your reference)
            signal_logger.info(f"=== 🟢 LONG Signal Conditions === {long_cond}")
            signal_logger.info(f"  1. Close > Weekly Pivot: {close:.2f} > {weekly_pivot:.2f} = {long_cond1}")
            signal_logger.info(f"  2. Close > Daily Pivot: {close:.2f} > {daiily_pivot:.2f} = {long_cond2}")
            signal_logger.info(f"  3. Close > EMA9: {close:.2f} > {ema9:.2f} = {long_cond3}")
            signal_logger.info(f"  4. EMA9 > EMA20: {ema9:.2f} > {ema20:.2f} = {long_cond4}")
            signal_logger.info(f"  5. Prev Close > Prev Open: {prev_close:.2f} > {prev_open:.2f} = {long_cond5}")
            signal_logger.info(f"  6. Close > Prev High: {close:.2f} > {prev_high:.2f} = {long_cond11}")
            signal_logger.info(f"  7. Close > Open: {close:.2f} > {open_:.2f} = {long_cond6}")
            signal_logger.info(f"  8. Close > EMA50: {close:.2f} > {ema50:.2f} = {long_cond7}")
            signal_logger.info(f"  9. Close > EMA200: {close:.2f} > {ema200:.2f} = {long_cond8}")
            signal_logger.info(f"  10. LTP > last candle high: {current_ltp:.2f} > {high:.2f} = {long_cond9}")
            signal_logger.info(f"  11. Close > current day high: {close:.2f} > {self.current_day_high :.2f} = {long_cond10}")
            signal_logger.info(f"  12. Expiry Day Rule: {expiry_allowed}")
            signal_logger.info(f"  🟢 🎯 LONG Signal Valid: {long_cond} {get_now()}")

            signal_logger.info("=== 🔴 SHORT Signal Conditions ===")
            signal_logger.info(f"  1. Close < Weekly Pivot: {close:.2f} < {weekly_pivot:.2f} = {short_cond1}")
            signal_logger.info(f"  2. Close < Daily Pivot: {close:.2f} < {daiily_pivot:.2f} = {short_cond2}")
            signal_logger.info(f"  3. Close < EMA9: {close:.2f} < {ema9:.2f} = {short_cond3}")
            signal_logger.info(f"  4. EMA9 < EMA20: {ema9:.2f} < {ema20:.2f} = {short_cond4}")
            signal_logger.info(f"  5. Prev Close < Prev Open: {prev_close:.2f} < {prev_open:.2f} = {short_cond5}")
            signal_logger.info(f"  6. Close < Prev Low: {close:.2f} < {prev_low:.2f} = {short_cond11}")
            signal_logger.info(f"  7. Close < Open: {close:.2f} < {open_:.2f} = {short_cond6}")
            signal_logger.info(f"  8. Close < EMA50: {close:.2f} < {ema50:.2f} = {short_cond7}")
            signal_logger.info(f"  9. Close < EMA200: {close:.2f} < {ema200:.2f} = {short_cond8}")
            signal_logger.info(f"  10. LTP < last candle low: {current_ltp:.2f} < {low:.2f} = {short_cond9}")
            signal_logger.info(f"  11. Close < current day low: {close:.2f} < {self.current_day_low :.2f} = {short_cond10}")
            signal_logger.info(f"  12. Expiry Day Rule: {expiry_allowed}")
            signal_logger.info(f"  🔴 🎯 SHORT Signal Valid: {short_cond} {get_now()}")

            # === Breakout Filter Check ===
            if long_cond:
                last_candle = last
                if LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # --- NEW: Option-Based Breakout Setup ---
                    if ENTRY_BREAKOUT_MODE == "OPTIONS":
                        logger.info("Setting up OPTION breakout levels for BUY signal...")
                        signal_logger.info("🕵️ Resolving option for BREAKOUT setup (BUY Signal)...")
                        # 1. Resolve Option
                        res = self.resolve_option_instrument("BUY")
                        tradable_symbol, tradable_exchange, strike, opt_type, expiry = res
                        if not res:
                            signal_logger.warning("❌ Could not resolve option for breakout setup. Aborting signal.")
                            return None
                        
                        # 2. Subscribe Mechanism
                        if not BACKTESTING_MANAGER.active:
                             order_logger.info("🛒 Subscribing to Live option LTP for Breakout Tracking...")
                             self.subscribe_option_ltp()
                        else:
                             logger.info("Backtesting mode detected - skipping live subscription for option LTP.")
                             self._fetch_simulation_option_data()
                        
                        # 3. Get Option Data
                        # time.sleep(1) # small buffer for subscription
                        intraday_df_opt = self.get_intraday(days=LOOKBACK_DAYS, symbol=self.option_symbol, exchange=OPTION_EXCHANGE, instrument_type="OPTIONS")
                        
                        if intraday_df_opt is None or intraday_df_opt.empty or len(intraday_df_opt) < 2:
                             signal_logger.warning("❌ No option data available for breakout level. Aborting.")
                             return None
                        
                        opt_last = intraday_df_opt.iloc[-2] # Last completed candle
                        breakout_high = float(opt_last.get("high", 0))
                        breakout_low = float(opt_last.get("low", 0)) # Not used for logic, but logged
                        
                        self.pending_breakout = {
                            "type": "BUY",
                            "timestamp": now,
                            "high": breakout_high,
                            "low": breakout_low,
                            "mode": "OPTIONS",
                            "symbol": tradable_symbol,
                            "strike": strike,
                            "opt_type": opt_type,
                            "expiry": expiry,
                            "exchange": tradable_exchange
                            }
                        signal_logger.info(f"🪙 OPTION BREAKOUT SETUP: {self.option_symbol} | High to Break: {breakout_high:.2f} | {get_now()}")
                    else:
                        # Standard SPOT Setup
                        logger.info("Setting up SPOT breakout levels for BUY signal...")
                        self.pending_breakout = {
                            "type": "BUY",
                            "timestamp": now,
                            "high": float(last_candle.get("high", 0)),
                            "low": float(last_candle.get("low", 0)),
                            "mode": "SPOT"
                            }
    
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    ltp_breakout_time_expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.pending_breakout_expiry = ltp_breakout_time_expiry
                    self.breakout_side = "BUY"
                    
                    target_val = self.pending_breakout['high'] if self.pending_breakout.get('mode') == 'OPTIONS' else self.pending_breakout['high']
                    signal_logger.info(f"🟢 🎯 BUY signal waiting for {self.pending_breakout.get('mode', 'SPOT')} breakout 📈 > {target_val:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info(f"🟢 🎯 ENTRY SIGNAL GENERATED: BUY (no LTP breakout filter) {get_now()}")
                    return "BUY"

            if short_cond:
                last_candle = last
                if LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # --- NEW: Option-Based Breakout Setup ---
                    if ENTRY_BREAKOUT_MODE == "OPTIONS":
                        signal_logger.info("🕵️ Resolving option for BREAKOUT setup (SELL Signal)...")
                        # 1. Resolve Option
                        res = self.resolve_option_instrument("SELL")
                        tradable_symbol, tradable_exchange, strike, opt_type, expiry = res
                        if not res:
                            signal_logger.warning("❌ Could not resolve option for breakout setup. Aborting signal.")
                            return None
                        # 2. Subscribe Mechanism
                        if not BACKTESTING_MANAGER.active:
                             order_logger.info("Subscribing to Live option LTP for Breakout Tracking...")
                             self.subscribe_option_ltp()
                        else:
                             self._fetch_simulation_option_data()
                        
                        # 3. Get Option Data
                        # time.sleep(1) # small buffer
                        intraday_df_opt = self.get_intraday(days=LOOKBACK_DAYS, symbol=self.option_symbol, exchange=OPTION_EXCHANGE, instrument_type="OPTIONS")
                        
                        if intraday_df_opt is None or intraday_df_opt.empty or len(intraday_df_opt) < 2:
                             signal_logger.warning("❌ No option data available for breakout level. Aborting.")
                             return None
                        
                        opt_last = intraday_df_opt.iloc[-2]
                        breakout_high = float(opt_last.get("high", 0))
                        
                        # NOTE: For Option-Mode, "SELL" Spot signal still means BUYING a PE Option.
                        # So we watch for Breakout > High.
                        self.pending_breakout = {
                            "type": "SELL",
                            "timestamp": now,
                            "high": breakout_high, # PE Option High
                            "mode": "OPTIONS",
                            "symbol": tradable_symbol,
                            "strike": strike,
                            "opt_type": opt_type,
                            "expiry": expiry,
                            "exchange": tradable_exchange
                        }
                        signal_logger.info(f"🪙 OPTION BREAKOUT SETUP: {self.option_symbol} | High to Break: {breakout_high:.2f}")

                    else:
                        # Standard SPOT Setup
                        self.pending_breakout = {
                            "type": "SELL",
                            "timestamp": now,
                            "high": float(last_candle.get("high", 0)),
                            "low": float(last_candle.get("low", 0)),
                            "mode": "SPOT"
                        }

                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    ltp_breakout_time_expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.pending_breakout_expiry = ltp_breakout_time_expiry
                    self.breakout_side = "SELL"
                    
                    if self.pending_breakout.get('mode') == 'OPTIONS':
                         signal_logger.info(f"📉 SELL signal waiting for OPTION breakout 📈 > {self.pending_breakout['high']:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    else:
                         signal_logger.info(f"📉 SELL signal waiting for SPOT breakout < {self.pending_breakout['low']:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info(f"🔴 ENTRY SIGNAL GENERATED: SELL (no LTP breakout filter) {get_now()}")
                    return "SELL"
                                    
            signal_logger.info("⚪ NO ENTRY SIGNAL - Conditions not met")
            return None
            
        except Exception:
            logger.exception("check_entry_signal failed")
            signal_logger.error("Entry signal evaluation failed", exc_info=True)
            return None
    #--------------------------
    # TSL - Trailing Stop Loss
    #--------------------------
    def _handle_tsl_tp_hit(self, position, hit_price):
        """
        Called when a TP is hit while TSL_ENABLED. Advances SL/TP according to rules:
        Rule 1: After first TP hit -> move SL to entry (breakeven)
        Rule 2: After second TP hit -> move SL to first TP price
        Rule 3+: Continue rolling SL to last TP price and calculate next TP as current TP ± tsl_step
        """
        try:
            with self._state_lock:
                # record hit
                self.trailing_levels.append(hit_price)
                self.trailing_index = len(self.trailing_levels)

                if self.trailing_index == 1:
                    # move SL to breakeven
                    new_sl = round(self.entry_price, 2)
                    self.stoploss_price = new_sl - SL_BUFFER_POINT
                else:
                    # move SL to previous TP price
                    new_sl = round(self.trailing_levels[-2], 2)
                    self.stoploss_price = new_sl - SL_BUFFER_POINT

                # compute next TP
                prev_tp = float(hit_price)
                step = float(self.tsl_step or TARGET)
                if position == 'BUY':
                    next_tp = round(prev_tp + step, 2)
                else:
                    next_tp = round(prev_tp - step, 2)

                # set new TP
                self.target_price = next_tp

                position_logger.info(f"TSL advanced: stage={self.trailing_index} | New SL={self.stoploss_price:.2f} | Next TP={self.target_price:.2f}")
                return True
        except Exception:
            position_logger.exception("_handle_tsl_tp_hit failed")
            return False
    # -------------------------
    # Calculate Dynamic SL/TP 
    # -------------------------
    def initialize_signal_candle_sl_tp(self, signal, intraday_df, base_price=None):
        """
        Initialize stoploss, target and TSL variables using the signal candle range.
        - signal: 'BUY' or 'SELL'
        - intraday_df: dataframe of intraday candles (must contain last completed candle at -2)
        - base_price: price to calculate target from (spot/option/exec)
        """
        try:
            # defensive
                if intraday_df is None or intraday_df.empty or len(intraday_df) < 2:
                # fallback to fixed STOPLOSS/TARGET if no candle
                    if base_price is None:
                        base_price = float(self.entry_price or 0.0)
                    if signal == "BUY":
                        self.stoploss_price = round(base_price - STOPLOSS, 2)
                        self.target_price = round(base_price + TARGET, 2)
                    else:
                        self.stoploss_price = round(base_price + STOPLOSS, 2)
                        self.target_price = round(base_price - TARGET, 2)
                    self.trailing_levels = []
                    self.trailing_index = 0
                    self.tsl_step = abs(self.target_price - base_price) if self.target_price and base_price else TARGET
                    position_logger.info("Signal candle not found -> fallback SL/TP set")
                    return True

                sig_candle = intraday_df.iloc[-2]
                sig_high = float(sig_candle.get('high', 0) or 0)
                sig_low = float(sig_candle.get('low', 0) or 0)
                
                if base_price is None:
                    base_price = float(self.entry_price or 0.0)

            # --- Apply integrated enhanced SL/TP logic ---
                sl, tp, sig_range = self.calculate_signal_candle_sl_tp(signal, sig_high, sig_low)
                self.stoploss_price = round(sl, 2)
                self.target_price = round(tp, 2)

                # Initialize trailing variables
                self.trailing_levels = []
                self.trailing_index = 0
                try:
                    self.tsl_step = abs(self.target_price - base_price) if self.target_price and base_price else sig_range
                    if self.tsl_step == 0:
                        self.tsl_step = sig_range
                except Exception:
                    self.tsl_step = sig_range

                position_logger.info(
                    f"Initialized Signal Candle Range SL/TP | SignalCandle H={sig_high:.2f} L={sig_low:.2f} Range={sig_range:.2f}"
                )
                position_logger.info(
                    f"SL={self.stoploss_price:.2f}, TP={self.target_price:.2f}, TSL_step={self.tsl_step:.2f}, "
                    f"MaxRangeCap={MAX_SIGNAL_RANGE}, RR={RISK_REWARD_RATIO}"
                )
                return True
        except Exception:
                    position_logger.exception("initialize_signal_candle_sl_tp failed")
                    return False        
    # -------------------------
    # CPR_range_SL_TP Implementation
    # -------------------------
    def calculate_signal_candle_sl_tp(self, signal: str, high: float, low: float):
        """Enhanced Signal Candle Range SL/TP logic with capped target range."""
        candle_range = abs(high - low)
        # Stop Loss logic
        if signal == "BUY":
            raw_sl = low
            sl = high - MAX_SIGNAL_RANGE if (high - low) > MAX_SIGNAL_RANGE else raw_sl
            sl = low
        else:
            raw_sl = high
            sl = low + MAX_SIGNAL_RANGE if (high - low) > MAX_SIGNAL_RANGE else raw_sl
            # Target logic: if candle range < MAX_SIGNAL_RANGE, use dynamic; else cap at MAX_SIGNAL_RANGE * reward
        if candle_range < MAX_SIGNAL_RANGE:
            target_range = candle_range * RISK_REWARD_RATIO
        else:
            target_range = MAX_SIGNAL_RANGE * RISK_REWARD_RATIO

        if signal == "BUY":
            tp = high + target_range
        else:
            tp = low - target_range

        return sl, tp, candle_range
            
    def initialize_cpr_range_sl_tp_enhanced(self, signal, intraday_df, base_price=None, pivots=None):
        """
        Enhanced CPR initialization with exclusion filtering and dynamic risk:reward targeting
        Combines both CPR pivot exclusion and dynamic target selection features
        """
        try:
            position_logger.info(f"Initializing Enhanced CPR Range SL/TP for {signal} signal")
            position_logger.info(f"Features: Exclusion Filter={'ON' if CPR_EXCLUDE_PIVOTS else 'OFF'}, Dynamic Target={'ON' if USE_DYNAMIC_TARGET else 'OFF'}")
            position_logger.info("Option premimum: {:.2f}".format(float(base_price or 0.0)))
            # Get signal candle data for initial SL
            if intraday_df is None:
                position_logger.warning("No signal candle data available for enhanced CPR initialization")
                return False
                
            sig_candle = intraday_df.iloc[-2]
            signal_candle_low = float(sig_candle.get('low', 0) or 0)
            signal_candle_high = float(sig_candle.get('high', 0) or 0)
            signal_candle_range = abs(signal_candle_high - signal_candle_low)
            # Merge pivot levels with exclusion filtering
                
            pivots_dict = pivots.copy() if pivots else self.static_indicators.copy()
            
            # for timeframe in ['DAILY', 'WEEKLY', 'MONTHLY']:
            #     if timeframe in self.static_indicators:
            #         pivots_dict[timeframe.lower()] = self.static_indicators[timeframe]
                    
            # Get filtered pivot levels with exclusion
            filtered_levels, level_mapping, excluded_ids = merge_pivot_levels_with_exclusion(pivots_dict, CPR_EXCLUDE_PIVOTS)
            
            # Store and log exclusion results
            self.excluded_pivots_info = excluded_ids
            if excluded_ids:
                position_logger.info(f"Excluded {len(excluded_ids)} unreliable pivot levels")
                
            expanded_levels = expand_with_midpoints(filtered_levels)
            self.cpr_levels_sorted = sorted(expanded_levels)
            
            if not self.cpr_levels_sorted:
                position_logger.warning("No CPR levels available after filtering")
                return False
                
            # Set entry price and initial SL
            if base_price is None:
                base_price = float(self.option_entry_price or 0.0)
            self.entry_price = float(base_price)
            self.cpr_side = signal.upper()
            
            # Calculate initial SL from signal candle
            position_logger.info(f"Calculating initial SL using Option signal candle range: Low={signal_candle_low:.2f}, High={signal_candle_high:.2f}, Range={signal_candle_range:.2f}")
            premium_based_range = self.entry_price * SL_PERCENT
            percent = SL_PERCENT * 100
            position_logger.info(f"{percent:.2f}% SL from entry premium {self.entry_price:.2f}: {premium_based_range:.2f}")
            position_logger.info(f"Minumum SL points: {MIN_SL_POINTS}")
            position_logger.info(f"Option Signal candle range: {signal_candle_range:.2f}")
            max_allowed_range = max(MIN_SL_POINTS, min(signal_candle_range, premium_based_range, MAX_RISK_POINTS))
            position_logger.info(f"Calculated max allowed SL range: {max_allowed_range:.2f}, (SignalCandleRange={signal_candle_range:.2f}, PremiumBasedRange={premium_based_range:.2f})")
            if self.cpr_side == "BUY":
                initial_sl = round(self.entry_price - (max_allowed_range + SL_BUFFER_POINT), 2)
                # initial_sl = signal_candle_high - MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else raw_sl
            else:  # SELL
                initial_sl = round(self.entry_price + max_allowed_range, 2)
                # initial_sl = signal_candle_low + MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else raw_sl  
            self.cpr_sl = initial_sl
            self.stoploss_price = initial_sl
            self.initial_stoploss_price = initial_sl
            
            # Enhanced target selection with dynamic risk:reward
            if USE_DYNAMIC_TARGET:
                # Use dynamic calculation with filtered CPR levels
                dynamic_target, risk_dist, reward_dist, target_key, rationale = self.calculate_dynamic_cpr_target(
                    signal, base_price, initial_sl, self.cpr_levels_sorted, level_mapping
                )
                
                # Set the dynamic target as initial target
                self.target_price = round(dynamic_target, 2)
                
                # Create CPR targets list starting from the selected dynamic target
                if self.cpr_side == "BUY":
                    self.cpr_targets = [lvl for lvl in self.cpr_levels_sorted if lvl >= dynamic_target]
                else:  # SELL
                    self.cpr_targets = [lvl for lvl in sorted(self.cpr_levels_sorted, reverse=True) if lvl <= dynamic_target]
                    
                # Store dynamic calculation results for logging
                self.dynamic_target_info = {
                    "risk": risk_dist,
                    "reward_distance": reward_dist,
                    "target_key": target_key,
                    "rationale": rationale,
                    "rr_ratio": RISK_REWARD_RATIO
                }
                maximum_loss = abs(self.entry_price - self.cpr_sl)  
                maximum_loss = round(maximum_loss, 2) * QUANTITY
                position_logger.info(f"Dynamic Target Selected: {target_key} @ {dynamic_target:.2f}")
                position_logger.info(f"Maximum loss for this trade: {maximum_loss}")
                position_logger.info(f"Risk: {risk_dist:.2f} | Reward: {reward_dist:.2f} | RR: 1:{RISK_REWARD_RATIO:.2f}")
                position_logger.info(f"Rationale: {rationale}")
                
            else:
                # Use original CPR target selection (all levels in direction)
                if self.cpr_side == "BUY":
                    self.cpr_targets = [lvl for lvl in self.cpr_levels_sorted if lvl > base_price]
                else:  # SELL
                    self.cpr_targets = [lvl for lvl in sorted(self.cpr_levels_sorted, reverse=True) if lvl < base_price]
                    
                # Set first target as initial TP
                if self.cpr_targets:
                    self.target_price = round(self.cpr_targets[0], 2)
                else:
                    # Fallback if no targets
                    if signal == "BUY":
                        self.target_price = round(base_price + TARGET, 2)
                    else:
                        self.target_price = round(base_price - TARGET, 2)
                        
                self.dynamic_target_info = None
                position_logger.info(f"Standard CPR Target: {self.target_price:.2f}")
                
            # Initialize tracking variables
            self.current_tp_index = 0 if self.cpr_targets else -1
            self.cpr_tp_hit_count = 0
            self.cpr_active = True
            
            # Comprehensive initialization summary
            position_logger.info(f"Enhanced CPR Range SL/TP Initialized Successfully:")
            position_logger.info(f"  Side: {self.cpr_side}")
            position_logger.info(f"  Entry Price: {self.entry_price:.2f}")
            position_logger.info(f"  Initial SL: {self.cpr_sl:.2f}")
            position_logger.info(f"  Signal Candle: Low={signal_candle_low:.2f}, High={signal_candle_high:.2f}")
            position_logger.info(f"  Total CPR Levels (after filtering): {len(self.cpr_levels_sorted)}")
            position_logger.info(f"  Available Targets: {len(self.cpr_targets)}")
            position_logger.info(f"  Next Target: {self.target_price:.2f}")
            position_logger.info(f"  Exclusions Applied: {len(excluded_ids)} levels")
            position_logger.info(f"  Dynamic Targeting: {'Enabled' if USE_DYNAMIC_TARGET else 'Disabled'}")
            
            return True
            
        except Exception as e:
            position_logger.error(f"Error initializing Enhanced CPR Range SL/TP: {e}", exc_info=True)
            self.cpr_active = False
            return False
            
    def get_current_cpr_tp(self):
        """Get the current CPR target price"""
        if not getattr(self, "cpr_active", False) or not self.cpr_targets:
            return None
        idx = getattr(self, "current_tp_index", -1)
        if 0 <= idx < len(self.cpr_targets):
            return float(self.cpr_targets[idx])
        return None
        
    def handle_cpr_tp_hit(self, hit_price):
        """
        Handle CPR target hit with institutional SL logic.
        Returns:
            "IGNORED"     → weak CPR hit (no SL / TP advance)
            "SL_MOVED"    → SL progressed
            "FINAL_EXIT"  → no more targets
            """
        try:
            with self._state_lock:
                prev_idx = self.current_tp_index 

                # --- Risk / Reward calculation ---
                risk = abs(self.entry_price - self.initial_stoploss_price)
                reward = abs(hit_price - self.entry_price)
                reward_r = reward / max(risk, 1e-6)

                position_logger.info(f"risk {risk:.2f} | reward {reward:.2f} | RR={reward_r:.2f}")
                position_logger.info(
                    f"CPR HIT @ {hit_price:.2f} | RewardR={reward_r:.2f}"
                )
                def _advance_tp():
                    next_idx = prev_idx + 1
                    if next_idx >= len(self.cpr_targets):
                        position_logger.info("Final CPR target crossed")
                        return "FINAL_EXIT"
        
                    self.current_tp_index = next_idx
                    self.target_price = round(float(self.cpr_targets[next_idx]), 2)
                    position_logger.info(f"Next CPR Target: {self.target_price:.2f}")
                    return None
                #--- RULE 1: First TP check CPR reactions closer levels ---
                if self.cpr_tp_hit_count == 0:
                    if reward_r < MIN_REWARD_FOR_SL_MOVE:
                        position_logger.info(
                                f"Weak CPR hit ignored (reward {reward_r:.2f}R < {MIN_REWARD_FOR_SL_MOVE}R)"
                            )
                        res = _advance_tp()
                        if res:
                            return res
                        
                        return "IGNORED"
                # ---------------------------
                # RULE 2: TOO CLOSE TO PREVIOUS TP
                # ---------------------------

                if self.cpr_tp_hit_count > 0 and prev_idx > 0 and self.cpr_tp_hits:
                    prev_tp = self.cpr_tp_hits[-1]
                    separation = abs(hit_price - prev_tp)
                    separation_r = separation / max(risk, 1e-6)

                    position_logger.info(
                    f"TP Separation Check: hit price: {hit_price:.2f} - prev tp: {prev_tp:.2f} , separation:{separation:.2f} {separation_r:.2f}R"
                    )

                    if separation_r < MIN_TP_SEPARATION_R:
                        position_logger.info(
                        f"CPR hit ignored → too close to previous TP "
                        f"({separation_r:.2f}R < {MIN_TP_SEPARATION_R}R)"
                        )
                        res = _advance_tp()
                        if res:
                            return res
                        
                        return "IGNORED"

                # --- Meaningful move → allow SL progression ---
                self.cpr_tp_hit_count += 1
                self.cpr_tp_hits.append(hit_price)

                if self.cpr_tp_hit_count == 1:
                    # First CPR hit → Move SL to breakeven
                    self.cpr_sl = round(float(self.entry_price), 2)
                    position_logger.info(
                        f"First CPR hit → SL moved to Breakeven {self.cpr_sl:.2f}"
                    )
                elif self.cpr_tp_hit_count > 1:
                    # Subsequent meaningful hits → SL to previous TP
                    previous_hit = self.cpr_tp_hits[-2]
                    self.cpr_sl = round(float(previous_hit), 2)
                    position_logger.info(
                        f"CPR hit #{self.cpr_tp_hit_count} → SL moved to prev TP {self.cpr_sl:.2f}"
                    )

                # Update SL
                self.stoploss_price = self.cpr_sl - SL_BUFFER_POINT

                # Advance target index ONLY on meaningful move
                self.current_tp_index = prev_idx + 1

                # Final target reached
                if self.current_tp_index >= len(self.cpr_targets):
                    self.current_tp_index = -1
                    position_logger.info("Final CPR target reached - exit next update")
                    return "FINAL_EXIT"

                # Set next target
                next_tp = self.cpr_targets[self.current_tp_index]
                self.target_price = round(float(next_tp), 2)
                position_logger.info(f"Next CPR Target: {self.target_price:.2f}")

                self._log_cpr_tp_hit(hit_price)
                return "SL_MOVED"

        except Exception as e:
            position_logger.error(f"Error handling CPR TP hit: {e}", exc_info=True)
            return "FINAL_EXIT"

            
    def check_cpr_sl_tp_conditions(self, current_ltp):
        """
        Check CPR SL/TP conditions in real-time
        Returns: 'SL_HIT', 'TP_HIT', 'FINAL_EXIT', or None
        """
        if not getattr(self, "cpr_active", False):
            return None
            
        try:
            side = self.cpr_side
            current_tp = self.get_current_cpr_tp()
            
            # Check SL conditions with LTP breakout confirmation
            if side == "BUY" and current_ltp <= self.cpr_sl:
                position_logger.warning(f"CPR BUY | SL Hit: {current_ltp:.2f} <= {self.cpr_sl:.2f}")
                
                # Setup LTP breakout confirmation using CPR SL level
                if LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # Breakout level is the CPR SL itself
                    # handle_exit_ltp_breakout will confirm if option LTP breaks below this
                    self.pending_exit_breakout = {
                        "type": "CPR_SL_BUY",
                        "low": float(self.cpr_sl - SL_BUFFER_POINT),
                        "high": float(self.cpr_sl + SL_BUFFER_POINT),
                        "trigger": "CPR_SL"
                    }
                    
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    self.pending_exit_breakout_expiry = (
                        now.replace(minute=0, second=0, microsecond=0)
                        + timedelta(minutes=next_multiple)
                    )
                    self.exit_breakout_side = "EXIT_BUY"
                    
                    position_logger.info(
                        f"⏳ CPR SL waiting for OPTION LTP breakdown < {self.cpr_sl - SL_BUFFER_POINT:.2f} "
                        f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                    )
                    return "SL_PENDING_BREAKOUT"
                else:
                    # Immediate exit if LTP breakout disabled
                    return "SL_HIT"
                    
            elif side == "SELL" and current_ltp >= self.cpr_sl:
                position_logger.warning(f"CPR SELL | SL Hit: {current_ltp:.2f} >= {self.cpr_sl:.2f}")
                return "SL_HIT"
                
            # Check TP conditions
            if current_tp is not None:
                if side == "BUY" and current_ltp >= current_tp:
                    position_logger.info(f"🟢 CPR BUY TP Hit: {current_ltp:.2f} >= {current_tp:.2f}")     
                    # Check TSL_ENABLED to determine behavior
                    if not TSL_ENABLED:
                        position_logger.info("TSL disabled - sending FINAL_EXIT signal on TP hit")
                        return "FINAL_EXIT"
                    else:
                        # TSL enabled - proceed with trailing logic
                        tp_result = self.handle_cpr_tp_hit(current_tp)
                        if tp_result == "FINAL_EXIT":
                            return "FINAL_EXIT"
                        elif tp_result == "SL_MOVED":
                            return "TP_HIT"
                        elif tp_result == "IGNORED":
                            position_logger.info("Weak CPR hit ignored - continuing trade")
                            return None     
                        
                elif side == "SELL" and current_ltp <= current_tp:
                    position_logger.info(f"CPR SELL TP Hit: {current_ltp:.2f} <= {current_tp:.2f}")
                    
                    # Check TSL_ENABLED to determine behavior
                    if not TSL_ENABLED:
                        position_logger.info("TSL disabled - sending FINAL_EXIT signal on TP hit")
                        return "FINAL_EXIT"
                    else:
                        # TSL enabled - proceed with trailing logic
                        tp_result = self.handle_cpr_tp_hit(current_tp)
                        if tp_result == "FINAL_EXIT":
                            return "FINAL_EXIT"
                        elif tp_result == "SL_MOVED":
                            return "TP_HIT"
                        elif tp_result == "IGNORED":
                            position_logger.info("Weak CPR hit ignored - continuing trade")
                            return None  
                    
            return None
            
        except Exception as e:
            position_logger.error(f"Error checking CPR SL/TP conditions: {e}", exc_info=True)
            return "SL_HIT"  # Exit on error
            
    def _log_cpr_tp_hit(self, target_level):
        """Log CPR TP hit to database"""
        try:
            if self.option_ltp is None:
                self.option_ltp = self.option_entry_price
                
            # Calculate P&L at TP hit
            if self.position == "BUY":
                unrealized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
                
            data = {
                "timestamp": get_now(),
                "symbol": self.option_symbol if hasattr(self, 'option_symbol') else SYMBOL,
                "spot_price": self.ltp if self.ltp else 0.0,
                "action": f"CPR_TP_{self.cpr_tp_hit_count}",
                "quantity": QUANTITY,
                "price": float(self.option_ltp),
                "order_id": "",
                "strategy": STRATEGY,
                "leg_type": "CPR_TP_HIT",
                "reason": f"CPR TP {target_level:.2f} hit",
                "pnl": unrealized_pnl,
                "leg_status": "partial"
            }
            log_trade_db(data)
            position_logger.info(f"CPR TP hit logged: Level={target_level:.2f}, P&L={unrealized_pnl:.2f}")
            
        except Exception as e:
            position_logger.error(f"Error logging CPR TP hit: {e}", exc_info=True)
            
    def calculate_dynamic_cpr_target(self, signal, entry_price, sl_price, available_levels, level_mapping):
        """
        Calculate optimal target and filtered CPR levels
        Returns: (target_price, risk, reward_distance, selected_level_key, rationale)
        """
        try:
            signal = signal.upper()
            entry_price = float(entry_price)
            sl_price = float(sl_price)
            
            # Calculate risk distance
            risk = abs(entry_price - sl_price)
            if risk <= 0:
                position_logger.warning("Invalid risk distance; using fallback target calculation")
                fallback_target = entry_price + TARGET if signal == "BUY" else entry_price - TARGET
                risk = abs(entry_price - MIN_SL_POINTS)
            
            # Compute required reward distance
            reward_distance = risk 
            
            # Determine required minimum target price
            if signal == "BUY":
                required_target = entry_price + reward_distance
            else:  # SELL
                required_target = entry_price - reward_distance
                
            position_logger.info(f"Dynamic Target Calculation:")
            position_logger.info(f"  Signal: {signal} | Entry: {entry_price:.2f} | SL: {sl_price:.2f} | Risk: {risk:.2f}")
            
            if not available_levels:
                position_logger.warning("No CPR levels available; using computed target")
                return required_target, risk, reward_distance, "COMPUTED", "No CPR levels available"
            
            # Filter CPR levels based on direction and minimum distance requirement
            valid_candidates = []
            
            for level_value in available_levels:
                level_id = level_mapping.get(level_value, f"MIDPOINT_{level_value:.2f}")
                
                if signal == "BUY" and level_value > entry_price:
                    # For BUY: level must be above entry AND meet minimum reward requirement
                    # if level_value > entry_price and level_value >= required_target:
                    reward = level_value - entry_price
                    if reward >= max(risk * MIN_REWARD_PCT_OF_RISK, MIN_ABSOLUTE_REWARD):
                        valid_candidates.append((level_value, level_id, reward))
                elif signal == "SELL" and level_value < entry_price:  # SELL
                    # For SELL: level must be below entry AND meet minimum reward requirement  
                    # if level_value < entry_price and level_value <= required_target:
                    reward = entry_price - level_value
                    if reward >= max(risk * MIN_REWARD_PCT_OF_RISK, MIN_ABSOLUTE_REWARD):
                        valid_candidates.append((level_value, level_id, reward))

            if not valid_candidates:
                position_logger.warning("No valid CPR levels meet minimum reward requirement; using computed target")
                return required_target, risk, reward_distance, "COMPUTED", "No CPR levels meet minimum reward"
            
            # Select target based on method
            if DYNAMIC_TARGET_METHOD == "NEAREST_VALID":
                # Select the nearest level that meets requirements
                selected = min(valid_candidates, key=lambda x: x[2])  # Sort by distance from entry
                target_price, target_key, distance = selected
                rationale = f"Nearest valid CPR level ({distance:.2f} from entry)"
                
            elif DYNAMIC_TARGET_METHOD == "FIRST_BEYOND":
                # Select the first level beyond the required target
                if signal == "BUY":
                    # Sort by price ascending, take first
                    selected = min(valid_candidates, key=lambda x: x[0])
                else:
                    # Sort by price descending, take first  
                    selected = max(valid_candidates, key=lambda x: x[0])
                target_price, target_key, distance = selected
                rationale = f"First CPR level beyond required reward ({distance:.2f} from entry)"
            else:
                # Fallback to nearest
                selected = min(valid_candidates, key=lambda x: x[2])
                target_price, target_key, distance = selected
                rationale = f"Default nearest CPR level ({distance:.2f} from entry)"
            
            # Log selection details
            position_logger.info(f"  Available target levels: {len(valid_candidates)}")
            for level_val, level_key, dist in valid_candidates[:10]:  # Log first 10
                position_logger.info(f"    {level_key}: {level_val:.2f} (dist: {dist:.2f})")
            if len(valid_candidates) > 10:
                position_logger.info(f"    ... and {len(valid_candidates) - 10} more")
                
            position_logger.info(f"  🎯 SELECTED TARGET: {target_key} @ {target_price:.2f}")
            position_logger.info(f"   Risk Reward Ratio: 1:{(target_price - entry_price if signal == 'BUY' else entry_price - target_price) / risk:.2f}")
            
            return target_price, risk, reward_distance, target_key, rationale
            
        except Exception as e:
            position_logger.error(f"Error in dynamic CPR target calculation: {e}", exc_info=True)
            fallback_target = entry_price + TARGET if signal == "BUY" else entry_price - TARGET
            return fallback_target, 0, 0, "ERROR_FALLBACK", f"Calculation error: {str(e)}"
    # -------------------------
    # Option functions - Identify Option Strike, Expiry, Symbol
    # -------------------------
    def get_option_strike(self, strike_spec, spot_ltp, option_side):
        """
        strike_spec: "ATM", "ITM2", "OTM1", "+1%", "-0.5%" etc.
        option_side: "CE" or "PE"
        """
        try:
            spot = float(spot_ltp)
            atm = int(round(spot / STRIKE_INTERVAL) * STRIKE_INTERVAL)
            s = str(strike_spec).strip().upper()
            # ATM
            if s == "ATM":
                return atm
            # ITM/OTM pattern
            m = re.match(r'^(ITM|OTM)(\d+)$', s)
            if m:
                side, n = m.group(1), int(m.group(2))
                if side == "ITM":
                    # For Calls, ITM -> strike < ATM. For Puts, ITM -> strike > ATM
                    if option_side == "CE":
                        return int(atm - n * STRIKE_INTERVAL)
                    else:
                        return int(atm + n * STRIKE_INTERVAL)
                else:  # OTM
                    if option_side == "CE":
                        return int(atm + n * STRIKE_INTERVAL)
                    else:
                        return int(atm - n * STRIKE_INTERVAL)
            # percentage pattern: +1%, -0.5%
            m2 = re.match(r'^([+-]?\d+(\.\d+)?)%$', s)
            if m2:
                pct = float(m2.group(1))
                strike = spot * (1.0 + pct / 100.0)
                return int(round(strike / STRIKE_INTERVAL) * STRIKE_INTERVAL)
            # fallback numeric
            try:
                return int(float(s))
            except Exception:
                return atm
        except Exception:
            logger.exception("get_option_strike get_option_strike failed")
            return None

    def get_nearest_weekly_expiry(self):
        """Pick nearest expiry date (YYYY-MM-DD). In Backtesting, calculates Next Tuesday."""
        try:
            # 🟢 BACKTESTING MODE: Calculate "Next Tuesday"
            if BACKTESTING_MANAGER.active:
                current_date_val = get_now().date()
                # Find upcoming Tuesday (including today)
                # Weekday: Mon=0, Tue=1, Wed=2, Thu=3, Fri=4, Sat=5, Sun=6
                today_weekday = current_date_val.weekday()
                target_weekday = 1 # Tuesday
                
                if today_weekday <= target_weekday:
                    days_ahead = target_weekday - today_weekday
                else:
                    days_ahead = 7 - (today_weekday - target_weekday)
                    
                resolved_expiry_date = current_date_val + timedelta(days=days_ahead)
                # order_logger.info(f"🗓️ [SIMULATION] Calculated Next Tuesday Expiry: {resolved_expiry_date}")
                return resolved_expiry_date

            # 🟢 LIVE MODE: API Call
            base_client = client
            resp = base_client.expiry(symbol=SYMBOL, exchange=OPTION_EXCHANGE, instrumenttype="options")
            data = resp.get("data") if isinstance(resp, dict) else resp
            if not data:
                return None
            parsed = []
            for ex in data:
                try:
                    # accept various formats: try YYYY-MM-DD first
                    dt = datetime.strptime(ex, "%Y-%m-%d").date()
                except Exception:
                    # try alternative formats
                    try:
                        # some providers return 'DD-MON-YY'
                        dt = datetime.strptime(ex, "%d-%b-%y").date()
                    except Exception:
                        continue
                if dt >= date.today():
                    parsed.append(dt)
            parsed.sort()
            return parsed[0] if parsed else None
        except Exception:
            logger.exception("get_nearest_weekly_expiry failed")
            return None

    def get_option_symbol_via_search_api(self, expiry_date, strike, opt_type):
        """
        Construct a candidate and use client.search(query=..., exchange="NFO") like earlier reference.
        Candidate formats vary by provider; we try common one: SYMBOL + DD + MON + YY + STRIKE + CE/PE
        Example: NIFTY30OCT25 21000CE  -> but some providers have other naming. We'll attempt a search.
        """
        try:
            if expiry_date is None or strike is None:
                return None

            expiry_token = f"{expiry_date.day:02d}{MONTH_MAP[expiry_date.month]}{str(expiry_date.year)[-2:]}"
            suffix = "CE" if opt_type.upper() == "CE" else "PE"
            candidate = f"{SYMBOL}{expiry_token}{strike}{suffix}"

            # 🟢 BACKTESTING MODE: Direct Construction
            if BACKTESTING_MANAGER.active:
                 # In simulation, we trust the constructed symbol matches the data
                 # logger.info(f"📋 [SIMULATION] Constructed Option Symbol: {candidate}")
                 return candidate

            # 🟢 LIVE MODE: Search API
            # 🔹 Step 1: search candidate
            base_client = client
            resp = base_client.search(query=candidate, exchange=OPTION_EXCHANGE)
            if isinstance(resp, dict) and resp.get("data"):
                for d in resp["data"]:
                    sym = (d.get("symbol") or d.get("tradingsymbol") or "").upper()
                # ✅ Only return if it *starts* with the correct symbol
                    if sym.startswith(SYMBOL.upper()) and suffix in sym and str(strike) in sym:
                        return sym

            # 🔹 Step 2: fallback to broader expiry search
            resp2 = base_client.search(query=f"{SYMBOL}{expiry_token}", exchange=OPTION_EXCHANGE)
            if isinstance(resp2, dict) and resp2.get("data"):
                rows = resp2["data"]
                # Find strike and CE/PE match
                for r in rows:
                    name = (r.get("symbol") or r.get("tradingsymbol") or "").upper()
                    if name.startswith(SYMBOL.upper()) and str(strike) in name and suffix in name:
                        return r.get("symbol") or r.get("tradingsymbol")

                # If nothing exact, fallback to first *valid* match that starts with SYMBOL
                for r in rows:
                    name = (r.get("symbol") or r.get("tradingsymbol") or "").upper()
                    if name.startswith(SYMBOL.upper()):
                        return r.get("symbol") or r.get("tradingsymbol")

            logger.warning(f"No valid {SYMBOL} {suffix} found for {expiry_token} {strike}")
            return None

        except Exception:
            logger.exception("resolve_option_symbol_via_search failed")
            return None
    # -------------------------
    # Order functions - Place entry/exit order - Get existing order price
    # -------------------------
    def get_executed_price(self, order_id):
        """
        Poll order status up to 3 times (2s interval) until executed price is available.
        Returns float price or None if never filled.
        """
        for attempt in range(1):
            time.sleep(2)
            try:
                base_client = client
                resp = base_client.orderstatus(order_id=order_id, strategy=STRATEGY)
                if not isinstance(resp, dict):
                    continue

                if resp.get("status") != "success":
                    continue

                data = resp.get("data", {})
                order_status = str(data.get("order_status", "")).lower()

                if order_status in ("complete", "completed", "filled"):
                    avg_price = float(data.get("average_price", 0) or 0)
                    if avg_price > 0:
                        order_logger.info(f"✅ Executed price fetched on attempt {attempt+1}: {avg_price}")
                        return avg_price
            except Exception as e:
                order_logger.warning(f"⚠️ get_executed_price attempt {attempt+1} failed: {e}")

        order_logger.error(f"❌ Executed price still None after 5 retries for order {order_id}")
        return None


    def place_order_with_execution_retry(
        self,
        strategy,
        symbol,
        exchange,
        action,
        quantity,
        position_size=0,
        product="MIS",
        price_type="MARKET",
        max_order_attempts=3,
        max_exec_retries=3,
        retry_delay=2,
        ):
        """
        Places an order with full retry and execution confirmation.
        🎯 In SIMULATION mode: simulates order execution with current LTP
        🎯 In LIVE mode: uses real API calls
        """

        # 🎯 SIMULATION MODE: Mock order execution
        if BACKTESTING_MANAGER.active:
            order_logger.info(f"📋 [SIMULATION] Placing {action} order: {symbol} x {quantity}")
            # Generate mock_order_id with timestamp
            mock_order_id = f"SIGNAL_{get_now().strftime('%H%M%S')}_{action[:1]}"
            
            if not self._fetch_simulation_option_data():
                return False
                
            # Use current LTP as execution price
            exec_price = self._update_option_ltp_from_df(get_now())
            order_logger.info(f"✅ 🎯 [SIMULATION] Order executed: ID={mock_order_id} | Price={exec_price:.2f}")
            return mock_order_id, exec_price
        # 🎯 LIVE MODE: Real order placement
        else: 
            base_client = client
            description = f"Placing {action} order ({symbol} x {quantity})"
            # --- Step 1: Try placing order via retry_api_call
            try:
                if "BUY" in action or "SELL" in action:
                    order_resp = self.retry_api_call(
                        func=base_client.placesmartorder,
                        max_retries=max_order_attempts,
                        delay=retry_delay,
                        description=description,
                        strategy=strategy,
                        symbol=symbol,
                        exchange=exchange,
                        action=action,
                        quantity=quantity,
                        position_size=position_size,
                        price_type=price_type,
                        product=product,
                        )

            except Exception as e:
                order_logger.warning(f"placeorder failed: {e}")
                return None, None
            # --- Step 2: Validate order response
            if not order_resp or "orderid" not in order_resp:
                order_logger.warning(f"❌ Missing orderid....")
                return None, None
            if (not order_resp)  or ("exec_price" not in order_resp and "sell" not in action):
                order_logger.warning(f"❌ Missing executed price....")
                return None, None        
            order_id = order_resp["orderid"]    
            order_logger.info(f"✅ Order response: {order_resp}")
            order_logger.info(f"✅ 🎯 Order placed successfully ID={order_id}. Confirming executed price...")
            # --- Step 3: Confirm executed price using get_executed_price
            exec_price = order_resp["exec_price"] 
            # exec_price = self.get_executed_price(order_id)
            if exec_price is not None and exec_price > 0:
                order_logger.info(f"✅ Executed price confirmed (order {order_id}) = {exec_price}")
                return order_id, exec_price    
            else:
                order_logger.error(f"❌ Executed price not confirmed after {max_exec_retries} polls")
                return None, None   
    
    def compute_pct_sl(self, exec_price: float, side: str) -> float:
        """
        Compute stop loss ORDER PRICE using:
        - % based SL
        - Minimum SL points
        - Maximum risk cap
        """

        # 1️⃣ % based SL points
        premium_based_range = exec_price * SL_BUFFER_PCT

        # 2️⃣ Clamp SL range between MIN and MAX
        sl_points = max(
            MIN_SL_POINTS,
            min(premium_based_range, MAX_RISK_POINTS)
        )

        # 3️⃣ Convert SL points → SL price
        if side.upper() == "BUY":
            sl_order_price = round(exec_price - sl_points, 2)
        else:  # SELL
            sl_order_price = round(exec_price + sl_points, 2)

        logger.info(
            f"Computed SL Order Price: {sl_order_price:.2f} | "
            f"Exec Price: {exec_price:.2f} | "
            f"SL Points: {sl_points:.2f} | "
            f"SL %: {SL_BUFFER_PCT*100:.1f}%"
        )

        return sl_order_price

    def place_and_confirm_stoploss(self, exit_symbol: str, exit_exchange: str, entry_side: str, sl_price: float) -> Optional[str]:
        """
        Place stop loss order and confirm execution with retry mechanism.
        🎯 In SIMULATION mode: skips SL order (monitoring handles it)
        """
        # 🎯 SIMULATION MODE: Skip SL order placement
        if BACKTESTING_MANAGER.active:
            order_logger.info(f"📋 [SIMULATION] SL order skipped (handled by monitoring)")
            return "SIM_SL_SKIP"

         # 🎯 LIVE MODE: Real SL placement
        sl_action = "SELL" 
         
        order_logger.info(f"📋 Placing SL order -> Action={sl_action} | Trigger/SL={sl_price:.2f} | Symbol={exit_symbol}")
        # -------------------------------
        # 🧩SL (Stop Loss Limit)
        # -------------------------------
        try:
            TICK_SIZE = 0.05  # ✅ Angel One NSE tick size = ₹0.05
            base_client = client
            max_order_attempts = 3
            retry_delay = 2
            description = "SL-Order"
            def round_to_tick(price: float) -> float:
                """Round price to nearest 5 paise tick."""
                return round(round(price / TICK_SIZE) * TICK_SIZE, 2)

                # 🧮 Compute valid trigger & limit prices
            trigger_price = round_to_tick(sl_price)
            logger.info(f"Computed SL Trigger Price: {trigger_price:.2f}")
            if sl_action.upper() == "SELL":
                limit_price = round_to_tick(trigger_price - TICK_SIZE)
            else:  # BUY SL
                limit_price = round_to_tick(trigger_price + TICK_SIZE)
            logger.info(f"Computed SL Limit Price: {limit_price:.2f}")
            order_resp = self.retry_api_call(
                    func=base_client.placeorder,
                    max_retries=max_order_attempts,
                    delay=retry_delay,
                    description=description,
                    strategy=STRATEGY,
                    symbol=exit_symbol,
                    exchange=exit_exchange,
                    action=sl_action,
                    quantity=QUANTITY,
                    price_type="SL",
                    product=PRODUCT,
                    trigger_price=str(trigger_price),
                    price=str(limit_price)
                    )

            order_logger.info(f"🟡 SL-Limit response: {order_resp}")
            # Wait for order confirmation
            if isinstance(order_resp, dict) and order_resp.get("status") == "success":
                sl_order_id = order_resp.get("orderid")
                order_logger.info(f"✅ 🎯 SL-Limit order placed successfully | ID={sl_order_id} | Trigger={sl_price:.2f} | Limit={limit_price:.2f}")
                return sl_order_id
            order_logger.error(f"❌ SL-Limit order failed: {order_resp}")
                
        except Exception as e:
                order_logger.exception(f"⚠️ Exception while placing SL order: {e}")
        
        order_logger.error("🚨 Failed to place any SL order (SL-M or SL).")
        return None
             
# PATCH: place_entry_order - clear exit_in_progress after initialization
# File: nifty_myalgo_trading_system.py
# This is a drop-in replacement for the place_entry_order method.
# Apply by replacing the existing method in your file with this version.

    def resolve_option_instrument(self, signal):
        """
        Reusable method to resolve option symbol/strike/expiry.
        Returns (tradable_symbol, tradable_exchange, strike, opt_type) or None on failure.
        Sets self.option_symbol as side effect (to maintain compatibility).
        """
        if not OPTION_ENABLED:
            return None

        order_logger.info("=== 🕵️ OPTION SYMBOL RESOLUTION ===")
        opt_type = "CE" if signal == "BUY" else "PE"
        
        current_ltp = self.ltp if self.ltp else 0.0
        try:
            spot_ltp = current_ltp
            order_logger.info(f"Fetched Spot LTP: {spot_ltp:.2f}")
        except Exception:
            spot_ltp = self.ltp or 0.0
            order_logger.warning(f"Failed to fetch spot LTP, using cached: {spot_ltp:.2f}")
            return None

        strike = self.get_option_strike(OPTION_STRIKE_SELECTION, spot_ltp, opt_type)
        order_logger.info(f"Strike price: {strike} (selection: {OPTION_STRIKE_SELECTION})")

        tradable_symbol = None
        
        # 🟢 Resolved via smart helper methods (Live vs Backtest handled internally)
        expiry = self.get_nearest_weekly_expiry() 
        order_logger.info(f"Selected Expiry: {expiry}")
        
        tradable_symbol = self.get_option_symbol_via_search_api(expiry, strike, opt_type)
        if not tradable_symbol:
            order_logger.error(f"Failed to resolve option symbol: expiry={expiry}, strike={strike}, type={opt_type}")
            logger.warning("Could not resolve option symbol for expiry=%s strike=%s type=%s", expiry, strike, opt_type)
            return None

        tradable_exchange = OPTION_EXCHANGE
        self.option_symbol = tradable_symbol
        order_logger.info(f"Final Option Symbol: {tradable_symbol}")
        order_logger.info(f"Option Exchange: {tradable_exchange}")
        
        return tradable_symbol, tradable_exchange, strike, opt_type, expiry

    def place_entry_order(self, signal, intraday_df):
        """
        Enhanced entry order placement with:
        1. Stop Loss Automation (25% rule)
        2. Option symbol resolution and SL/TP initialization

        BUGFIX: ensure self.exit_in_progress is cleared after SL/TP initialization
        so that on_ltp_update() will begin monitoring the newly opened position.
        """
        try:
            order_logger.info("=== 🛒 PLACING ENTRY ORDER ===")
            signal_side = "CALL" if signal == "BUY" else "PUT"
            order_logger.info(f"Buy Signal: {signal_side}")
            
            # 🛑 TIME CHECK: strict cutoff for new entries
            current_time = get_now().time()
            if current_time > ENTRY_CUTOFF_TIME:
                order_logger.warning(f"⛔ Entry skipped: Current time {current_time.strftime('%H:%M:%S')} > Cutoff {ENTRY_CUTOFF_TIME.strftime('%H:%M:%S')}")
                return False

            if self.trade_count >= MAX_TRADES_PER_DAY:
                order_logger.warning(f"Daily trade limit reached ({self.trade_count}/{MAX_TRADES_PER_DAY})")
                logger.info("Daily trade limit reached (%d), will shutdown.", MAX_TRADES_PER_DAY)
                threading.Thread(target=self.shutdown_gracefully, daemon=True).start()
                return False
            current_ltp = self.ltp if self.ltp else 0.0
            order_logger.info(f"Current {SYMBOL} LTP: {current_ltp:.2f}")

            tradable_symbol = SYMBOL
            tradable_exchange = EXCHANGE
            opt_type = None
            strike = None

            if OPTION_ENABLED and ENTRY_BREAKOUT_MODE=="OPTIONS" and self.option_symbol is not None:
                tradable_symbol = self.option_symbol
                tradable_exchange = self.pending_breakout.get("exchange", OPTION_EXCHANGE)
                strike = self.pending_breakout.get("strike")
                expiry = self.pending_breakout.get("expiry")
                opt_type = self.pending_breakout.get("type")
                logger.info("✅ Placing %s option order after breakout -> %s (%s) strike=%s expiry=%s", signal_side, tradable_symbol, tradable_exchange, strike, expiry)
            else:
                resolution = self.resolve_option_instrument(signal)
                if not resolution:
                     return False
                tradable_symbol, tradable_exchange, strike, opt_type, expiry = resolution
                logger.info("✅ Placing %s option order -> %s (%s) strike=%s expiry=%s", signal_side, tradable_symbol, tradable_exchange, strike, expiry)
            
            # --- 🧩 PLACE SMART ENTRY ORDER ---
            entry_order_id, exec_price = self.place_order_with_execution_retry(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
                                      action="BUY", quantity=QUANTITY, position_size = QUANTITY, price_type="MARKET", product=PRODUCT)
            
            if entry_order_id is None or exec_price is None:
                order_logger.error(f"Entry order failed")
                return False
            order_logger.info(f"✅ 🎯 Order placed successfully. dynamic SL/TP calculation started {get_now()}")
            order_logger.info(f"✅ 🎯 Entry Order Executed | ID={entry_order_id} |")
            order_logger.info(f"📈 Trade Count: {self.trade_count +1}/{MAX_TRADES_PER_DAY}")
            self.trade_start_time = get_now()
            self.trade_count += 1
            self.option_entry_price = exec_price
            # --- 🧩 PLACE STOP LOSS ORDER after confirmed entry ---
            if SL_ORDER_ENABLED:
                try:
                    # Determine which price to use for SL placement
                    exit_symbol = tradable_symbol
                    exit_exchange = tradable_exchange
                    if USE_OPTION_FOR_SLTP:
                        entry_side = "BUY"
                    else:
                        entry_side = signal.upper()

                    # Use existing computed 10% stoploss_price for SL order
                    sl_price = self.compute_pct_sl(exec_price, entry_side)

                    order_logger.info(f"Attempting to place linked SL order after entry confirmation...")
                    sl_order_id = self.place_and_confirm_stoploss(exit_symbol, exit_exchange, entry_side, sl_price)

                    if sl_order_id:
                        self.sl_order_id = sl_order_id
                        order_logger.info(f"✅ 🎯 Stop Loss Order Placed Successfully | ID={sl_order_id} | SL={sl_price:.2f}")
                    else:
                        order_logger.warning("⚠️ Stop Loss order placement failed after entry confirmation.")
                except Exception:
                    order_logger.exception("Error placing SL order after entry execution")
            else:
                order_logger.exception("SL order not enabled")
                
           # --- Subscribe to option LTP for SL/TP tracking ---
            if not BACKTESTING_MANAGER.active:
                order_logger.info("Subscribing to Live option LTP for SL/TP tracking...")
                self.subscribe_option_ltp()
            else:
                order_logger.info("📋 [SIMULATION] Option LTP from Duck db for current simulation date")
                #Write code to fetch option ltp from duckdb for current simulation date and subscribe similar to market OHLC data
                # Load full-day option candles from DB
                  
            
            # mark that we are initializing (prevent concurrent monitoring during setup)
            with self._state_lock:
                self.exit_in_progress = True
                self.position = signal
                # determine base_price (spot/option/entry)
                base_price = self.spot_entry_price
                if USE_SPOT_FOR_SLTP:
                    try:
                        # q = self.client.quotes(symbol=SYMBOL, exchange=EXCHANGE)
                        self.spot_entry_price = self.ltp
                        base_price = self.spot_entry_price
                        risk_logger.info(f"SL/TP tracking via SPOT | Spot Entry = {base_price:.2f}")
                    except Exception:
                        self.spot_entry_price = self.ltp or self.entry_price
                        base_price = self.spot_entry_price
                        risk_logger.warning(f"Failed to fetch option ltp for SL/TP, using cached: {base_price:.2f}")
                elif USE_OPTION_FOR_SLTP:
                    self.spot_entry_price = self.ltp
                    base_price = exec_price
                    # option entry price will be set after execution retrieval; temporarily keep base_price
                    risk_logger.info("SL/TP tracking via OPTION; option exec price to be recorded after execution")

                # Initialize SL/TP using configured method
                if SL_TP_METHOD == "Signal_candle_range_SL_TP":
                    self.initialize_signal_candle_sl_tp(signal, intraday_df, base_price=base_price)
                elif SL_TP_METHOD == "CPR_range_SL_TP":
                    risk_logger.info("CPR-based SL/TP calculation started")
                    cpr_success = False
                    if USE_OPTION_FOR_SLTP: 
                        spot_cpr = self.static_indicators.copy()
                        risk_logger.info("Calculating CPR for SL/TP")
                        self.compute_static_indicators(
                        symbol = self.option_symbol,
                        exchange = OPTION_EXCHANGE
                        )
                        self.static_indicators_option = self.static_indicators.copy()
                        self.static_indicators = spot_cpr
                    else:
                        self.static_indicators_option = None
                    # Calculate CPR-based SL/TP
                    if USE_OPTION_FOR_SLTP and self.static_indicators_option:
                        risk_logger.info("CPR SL/TP calculation enabled")                  
                        if not cpr_success:
                            risk_logger.info("OPTION CPR SL/TP initialization successfully")
                            intraday_df_opt = self.get_intraday(
                            days=LOOKBACK_DAYS,
                            symbol=self.option_symbol,
                            exchange=OPTION_EXCHANGE,
                            instrument_type="OPTIONS"
                            )
                            if intraday_df_opt is not None and not intraday_df_opt.empty:
                                cpr_source = self.static_indicators_option
                                cpr_success = self.initialize_cpr_range_sl_tp_enhanced("BUY", intraday_df_opt, base_price=exec_price, pivots=cpr_source)
                                # self.dynamic_indicators_option = self.compute_dynamic_indicators(intraday_df_opt)
                            else:
                                self.dynamic_indicators_option = None 
                                cpr_success = False
                                risk_logger.warning("Failed to fetch option intraday data for dynamic indicators after OPTION CPR SL")        
                    if USE_SPOT_FOR_SLTP and not cpr_success:
                        risk_logger.info("SPOT CPR SL/TP calculation enabled")
                        cpr_source =  self.static_indicators
                        cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal, intraday_df, base_price=base_price, pivots=cpr_source)
                    if not cpr_success:
                        self.static_indicators = spot_cpr
                        risk_logger.info("Option CPR initialization failed, Falling back to SPOT CPR")
                        cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal, intraday_df, base_price=base_price, pivots=cpr_source)
                    if not cpr_success:
                        self.initialize_signal_candle_sl_tp(signal, intraday_df, base_price=base_price)
                else:
                    # fallback: fixed TP/SL
                    base_price = self.spot_entry_price
                    if signal == "BUY":
                        self.stoploss_price = round(base_price - STOPLOSS, 2)
                        self.target_price = round(base_price + TARGET, 2)
                    else:
                        self.stoploss_price = round(base_price + STOPLOSS, 2)
                        self.target_price = round(base_price - TARGET, 2)

                # finalize basic state vars
                self.entry_price = round(base_price, 2)
            
            # BUGFIX: Re-enable websocket-based monitoring now that SL/TP and position are initialized.
            with self._state_lock:
                self.exit_in_progress = False

            order_logger.info(f"Order ID: {entry_order_id}")

            # final logging and DB record
            order_logger.info("=== POSITION ESTABLISHED ===")
            order_logger.info(f"Position: {self.position}")
            order_logger.info(f"Entry Price: {self.entry_price:.2f}")
            order_logger.info(f"Stop Loss: {self.stoploss_price:.2f}")
            order_logger.info(f"Target: {self.target_price:.2f}")
            order_logger.info(f"Quantity: {QUANTITY}")
            order_logger.info(f"Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")

            # log trade to DB (existing helper)
            trade_data = {
                "timestamp": self.trade_start_time,
                "symbol": tradable_symbol,
                "spot_price": getattr(self, 'spot_entry_price', 0.0),
                "action": "CALL" if signal.upper() == "BUY" else "PUT",
                "quantity": QUANTITY,
                "price": getattr(self, 'option_entry_price', self.entry_price),
                "order_id": entry_order_id,
                "strategy": STRATEGY,
                "leg_type": "ENTRY",
                "leg_status": "open"
            }
            log_trade_db(trade_data)

            logger.info("Entry executed: %s @ %.2f | SL @ %.2f | TP @ %.2f | trade_count=%d",
                    self.position, self.entry_price, self.stoploss_price, self.target_price, self.trade_count)
            return True

        except Exception as e:
            logger.exception(f"place_entry_order failed: {e}")
            order_logger.error("Failed to place enhanced entry order", exc_info=True)
            # ensure flag cleared on error so bot can continue
            with self._state_lock:
                self.exit_in_progress = False
            return False

    def subscribe_option_ltp(self):
        """Subscribe dynamically to option LTP once order is placed."""
        if not getattr(self, "option_symbol", None):
            return
        
        # Avoid duplicate subscription
        if self.option_symbol in self.ws_subscriptions:
            logger.info(f"Option LTP already subscribed: {self.option_symbol}")     
            return
        try:
            base_client = client
            base_client.subscribe_ltp(
                [
                    {"exchange": OPTION_EXCHANGE, "symbol": self.option_symbol}
                ],
                on_data_received=self.on_ltp_update
            )
            signal_logger.info(f"🧩 Subscribed to Option LTP: {self.option_symbol}")
            self.ws_subscriptions.add(self.option_symbol)
        except Exception as e:
            signal_logger.warning(f"⚠️ Failed to subscribe option symbol: {e}")
    
    def _fetch_simulation_option_data(self):
        """
        Fetch 1-minute option data for backtesting mode.
        Checks if data is already loaded (similar to ws_subscriptions in live mode).
        Returns True if successful, False otherwise.
        """
        if not getattr(self, "option_symbol", None):
            logger.warning("Cannot fetch simulation option data: option_symbol not set")
            return False
        
        # ✅ Check if already subscribed (like live mode)
        if self.option_symbol in self.ws_subscriptions:
            logger.info(f"Option data already loaded for backtesting: {self.option_symbol}")
            return True
        
        try:
            # Fetch 1-minute option data from database
            logger.info(f"Fetching 1-minute option data for backtesting: {self.option_symbol}")
            
            # Get simulation date range
            sim_date = get_now().date()
            start_date = sim_date.strftime("%Y-%m-%d")
            end_date = sim_date.strftime("%Y-%m-%d")
            
            # Fetch from database
            df = DB_MANAGER.get_ohlcv_data(
                symbol=self.option_symbol,
                exchange=OPTION_EXCHANGE,
                timeframe="1m",
                start_date=start_date,
                end_date=end_date,
                instrument_type="OPTIONS",
                underlying_symbol=SYMBOL,
                expiry_date=self.expiry_cache.get("expiry_dt")
            )
            
            if df is None or df.empty:
                logger.warning(f"No 1-minute option data found for {self.option_symbol}")
                return False
            
            # Store in option_1m_df for intrabar LTP updates
            if isinstance(df.index, pd.DatetimeIndex):
                self.option_1m_df = df
            else:
                df_indexed = df.set_index("timestamp")
                self.option_1m_df = df_indexed
            
            # ✅ Mark as subscribed to prevent redundant fetches
            self.ws_subscriptions.add(self.option_symbol)
            logger.info(f"✅ Option data loaded and tracked for backtesting: {self.option_symbol} ({len(df)} candles)")
            return True
            
        except Exception as e:
            logger.error(f"Failed to fetch simulation option data: {e}", exc_info=True)
            return False
    
    def unsubscribe_option_ltp(self, option_symbol):
        if option_symbol in self.ws_subscriptions:
            self.ws_subscriptions.remove(option_symbol)
        try:
            client.unsubscribe_ltp([
                        {"exchange": OPTION_EXCHANGE, "symbol": option_symbol}])
        except Exception:
            pass
    
    def place_exit_order(self, reason="Manual"):
        try:
            order_logger.info("=== 🛒 PLACING EXIT ORDER ===")
            order_logger.info(f"🚪 Exit Reason: {reason}")
            
            if not self.position:
                order_logger.warning("No position to exit")
                self.exit_in_progress = False
                return False
            if BACKTESTING_MANAGER.active:
                BACKTESTING_MANAGER.pause_ticks = True
                
            # Get current LTP and P&L for logging
            spot_ltp = self.ltp if self.ltp else 0.00
            option_ltp = self.option_ltp if self.ltp else 0.00
            
            # Calculate P&L before exit
            try:
                if self.position == "BUY":
                    unrealized_pnl = (option_ltp - self.option_entry_price) * QUANTITY
                else:
                    unrealized_pnl = (option_ltp - self.option_entry_price) * QUANTITY
            except Exception:
                unrealized_pnl = 00.00  
                option_ltp = 00.00

            # Calculate trade duration
            trade_start = getattr(self, 'trade_start_time', get_now())
            trade_end_time = get_now()
            trade_duration = trade_end_time - trade_start
            
            order_logger.info(f"📈 Trade Spot Position: {self.position} @ {self.spot_entry_price:.2f}")
            order_logger.info(f"📈 Current Spot LTP: {spot_ltp:.2f}")
            order_logger.info(f"📈 Trade Option Position: {self.position} @ {self.option_entry_price:.2f}")
            order_logger.info(f"📈 Current Option LTP: {option_ltp:.2f}")
            order_logger.info(f"💰 Unrealized P&L: {unrealized_pnl:.2f}")
            order_logger.info(f"⏳ Trade Duration: {trade_duration}")
            
            action = "SELL" 
            
            # Use option symbol if available, otherwise fall back to spot
            exit_symbol = self.option_symbol if hasattr(self, 'option_symbol') and self.option_symbol else SYMBOL
            exit_exchange = OPTION_EXCHANGE if hasattr(self, 'option_symbol') and self.option_symbol else EXCHANGE

            if exit_symbol is None:
                order_logger.warning("❌ No option_symbol found. Cannot exit.")
                self.exit_in_progress = False
                return False
                 # ---- Fetch current position quantity for real orders----
            exit_qty = 0
            if not BACKTESTING_MANAGER.active:
                try:
                    pos = client.positionbook()
                    if isinstance(pos, dict) and pos.get("status") == "success":
                        for p in pos.get("data", []):
                            if p.get("symbol") == exit_symbol and p.get("exchange") == exit_exchange:
                                netqty = int(float(p.get("quantity", 0)))
                                exit_qty = abs(netqty)
                                break
                    ob2 = client.orderbook()
                    if isinstance(ob2, dict) and ob2.get("status") == "success":
                        for o in ob2.get("data", {}).get("orders", []):
                            if o.get("symbol") == exit_symbol and o.get("exchange") == exit_exchange and str(o.get("order_status", "")).upper() in ("TRIGGER PENDING","PENDING", "OPEN"):
                                try:
                                    client.cancelorder(order_id=o.get("orderid"), strategy=STRATEGY)
                                    order_logger.info(f"Cancelled pending order {o.get('orderid')}")
                                except Exception:
                                    order_logger.debug(f"Could not cancel order {o.get('orderid')} (non-fatal)")
                except Exception:
                    order_logger.error("orderbook fetch/cancel failed (non-fatal)") 
                    exit_order_id = None
                # ---- Place exit order ----
                if exit_qty == 0:
                    order_logger.warning(f"❌ No net quantity found to exit for {exit_symbol}.might have been already exited.")
                    tb = client.tradebook()
                    trades = []
                    if isinstance(tb, dict) and tb.get("status") == "success":
                        trades = tb.get("data", [])
                    # Filter SELL trades for the symbol
                    sell_trades = []
                    for t in trades:
                        if (
                            t.get("symbol") == exit_symbol
                            and t.get("exchange") == exit_exchange
                            and t.get("action") == "SELL"
                        ):
                            ts = t.get("timestamp")
                            if not ts:
                                continue

                            try:
                                datetime.strptime(ts, "%H:%M:%S")
                                sell_trades.append(t)
                            except ValueError:
                                continue

                    if sell_trades:
                    # Pick latest SELL trade
                        latest_sell = max(
                            sell_trades,
                            key=lambda x: datetime.strptime(x["timestamp"], "%H:%M:%S")
                        )   
                        exit_order_id = latest_sell.get("orderid", None)
                        exit_price = float(latest_sell.get("average_price", 0))
                        exit_qty = abs(int(float(latest_sell.get("quantity", 0))))
                        order_logger.info(
                            f"✅ Found latest SELL from tradebook | "
                            f"OrderID={exit_order_id}, Qty={exit_qty}, AvgPrice={exit_price}"
                            )
                else:
                    exit_qty = QUANTITY
                    exit_order_id, exit_price = self.place_order_with_execution_retry(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
                                        action="SELL", quantity=exit_qty, position_size=0, price_type="MARKET", product=PRODUCT) 
                
            # # Place exit order
            # resp = self.client.placeorder(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
            #                               action=action, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            # exit_order_id = resp["orderid"]
            # exit_price = self.get_executed_price(exit_order_id)
            # ------------------ Cancel all pending orders for this symbol in real time ------------------    
            order_logger.info(f"Exit Symbol: {exit_symbol}")
            order_logger.info(f"Exit Exchange: {exit_exchange}")
            order_logger.info(f"Exit Action: {action}")
            order_logger.info(f"Exit Quantity: {exit_qty}")
                     
            if BACKTESTING_MANAGER.active:
                order_logger.info("📋 [SIMULATION] Exit order simulated in backtesting mode.")
                exit_order_id = f"SIM_EXIT_{get_now().strftime('%H%M%S')}"
                exit_price = self._update_option_ltp_from_df(get_now())

            if (exit_order_id is not None or exit_price is not None):
                order_logger.info(f"Exit Order Success: {exit_order_id}")
                try:
                    if not BACKTESTING_MANAGER.active:
                        self.unsubscribe_option_ltp(exit_symbol)
                    else:
                        order_logger.warning(f"Option LTP not required to unsubscribe in SIM mode")
                except Exception as e:
                    order_logger.error(f"Unable to unsubscribe option ltp")
                    pass
                
                if self.option_ltp is None:
                    self.option_ltp = self.option_entry_price

                if exit_price:
                    # Calculate actual P&L
                    if self.position == "BUY":
                        realized_pnl = (exit_price - self.option_entry_price) * QUANTITY
                    else:
                        realized_pnl = (exit_price - self.option_entry_price) * QUANTITY
                    
                    order_logger.info(f"EXIT EXECUTED: Price = {exit_price:.2f}")
                    order_logger.info(f"Realized P&L: {realized_pnl:.2f}")
                    
                    # Comprehensive trade summary
                    order_logger.info(f"=== TRADE {self.trade_count} COMPLETED ===") 
                    order_logger.info(f"Trade Start Time: {self.trade_start_time}")
                    order_logger.info(f"Spot Entry: {self.position} @ {self.spot_entry_price:.2f}")
                    order_logger.info(f"Spot Exit: {action} @ {spot_ltp}")
                    order_logger.info(f"Option Symbol: {self.option_symbol}")
                    order_logger.info(f"Option Entry: {self.position} @ {self.option_entry_price:.2f}")
                    order_logger.info(f"Option Exit: {action} @ {self.option_ltp:.2f}")
                    order_logger.info(f"Trade End Time: {trade_end_time}")
                    order_logger.info(f"P&L: {realized_pnl:.2f}")
                    order_logger.info(f"Reason: {reason}")
                    order_logger.info(f"Duration: {trade_duration}")

                    log_trade_db({
                    "timestamp": trade_end_time,
                    "symbol": self.option_symbol,
                    "action": "PUT" if self.position == "SELL" else "CALL",
                    "spot_price": spot_ltp,
                    "quantity": QUANTITY,
                    "price": option_ltp,
                    "order_id": exit_order_id,
                    "strategy": STRATEGY,
                    "leg_type": "EXIT",
                    "reason": reason,
                    "leg_status": "closed"
                    })

                    self.reconcile_trade_pnl({
                    "symbol": self.option_symbol,
                    "strategy": STRATEGY,
                    "price": self.option_ltp,
                    "quantity": QUANTITY
                    })
                   
                else:
                    order_logger.warning("Could not confirm exit execution price or unable to log db ")                    
            else:
                order_logger.error(f"Exit order failed")
            
            # Clear position data
            previous_position = self.position
            previous_entry = self.entry_price
            # Reset all position-related attributes
            self.position = None
            self.entry_price = 0.0
            self.stoploss_price = 0.0
            self.target_price = 0.0
            self.exit_in_progress = False
            self.option_symbol = None
            self.spot_entry_price = 0.0
            self.option_entry_price = 0.0
            self.option_stop_loss = 0.0
            # Reset enhanced CPR attributes
            self.cpr_active = False
            self.cpr_levels_sorted = []
            self.cpr_targets = []
            self.cpr_sl = 0.0
            self.cpr_side = None
            self.current_tp_index = 0
            self.cpr_tp_hit_count = 0
            self.cpr_active = False
            self.cpr_sl = None
            self.target_price = None
            self.cpr_targets = []
            self.current_tp_index = -1
            self.cpr_tp_hit_count = 0
            self.excluded_pivots_info = None
            self.dynamic_target_info = None
            self.stoploss_price = None
            self.entry_price = None 
            self.static_indicators_option = None
            self.option_entry_price = None
            #  Reset dynamic pivot attributes
            self.dynamic_target_info = None
            self.excluded_pivots_info = None
            self.gk_resistance_pivot = None
            self.gk_support_pivot = None
            # Reset LTP pivot breakout state
            with self._pivot_gate_lock:
                self.ltp_pivot_breakout = False
                self.ltp_pivot_info = None
            self.reset_pending_breakout()
             
            order_logger.info("Position cleared and reset (including CPR state)")
            main_logger.info(f"Position {previous_position} @ {previous_entry:.2f} exited due to: {reason}")
            
            return True
        except Exception:
            logger.exception("place_exit_order failed")
            order_logger.error(f"Failed to place exit order for reason: {reason}", exc_info=True)
            self.exit_in_progress = False
            return False
        finally:
            if BACKTESTING_MANAGER.active:
                BACKTESTING_MANAGER.pause_ticks = False

    def exit_all_positions(self):
        """
        Universal position cleanup method that:
        1. Fetches current open positions via API
        2. Places market exit orders for all positions
        3. Cancels all pending SL orders for each symbol
        4. Provides comprehensive logging for audit trail
        
        This prevents orphaned SL orders and ensures clean position state.
        Can be called manually or programmatically for emergency cleanup.
        """
        try:
            order_logger.info("=== UNIVERSAL POSITION CLEANUP INITIATED ===")
            main_logger.info("Starting exit_all_positions() - Universal cleanup")

              # 🎯 SIMULATION MODE: Skip API calls, just clear state
            if BACKTESTING_MANAGER.active:
                order_logger.info("📋 [SIMULATION] Skipping position cleanup API calls")
                return
            
            # 1️⃣ Get current open positions from broker
            try:
                base_client = client
                positions_resp = base_client.positionbook()
                if not isinstance(positions_resp, dict) or positions_resp.get("status") != "success":
                    order_logger.error(f"Failed to fetch positions: {positions_resp}")
                    return False
                    
                positions_data = positions_resp.get("data", [])
                open_positions = [p for p in positions_data if float(p.get("netqty", 0)) != 0]
                
            except Exception as e:
                order_logger.exception(f"Error fetching positions: {e}")
                return False

            if not open_positions:
                order_logger.info("✅ No open positions found.")
                main_logger.info("No open positions to clean up")
                return True

            order_logger.info(f"Found {len(open_positions)} open position(s) to close")
            main_logger.info(f"Processing {len(open_positions)} open positions for cleanup")

            # 2️⃣ Process each open position
            for pos in open_positions:
                try:
                    symbol = pos.get("symbol", "")
                    exchange = pos.get("exchange", "")
                    net_qty = float(pos.get("netqty", 0))
                    qty = abs(int(net_qty))
                    action = "SELL" 
                    product = pos.get("product", "MIS")
                    
                    order_logger.info(f"Processing position: {symbol} | Net Qty: {net_qty} | Action: {action}")

                    if qty == 0:
                        order_logger.warning(f"Skipping {symbol} - zero quantity")
                        continue

                    # 3️⃣ Place market exit order
                    try:
                        close_resp = base_client.placeorder(
                            strategy=STRATEGY,
                            symbol=symbol,
                            exchange=exchange,
                            action=action,
                            quantity=qty,
                            price_type="MARKET",
                            product=product
                        )
                        
                        if isinstance(close_resp, dict) and close_resp.get("status") == "success":
                            order_logger.info(f"✅ Exit order placed for {symbol}: {close_resp.get('orderid', 'N/A')}")
                            main_logger.info(f"Closed position: {symbol} {action} {qty}")
                        else:
                            order_logger.error(f"❌ Exit order failed for {symbol}: {close_resp}")
                            
                    except Exception as e:
                        order_logger.exception(f"Failed to place exit order for {symbol}: {e}")

                    # Cancel pending SL orders for this symbol
                    try:
                        orders_resp = base_client.orderbook()
                        if isinstance(orders_resp, dict) and orders_resp.get("status") == "success":
                            orders_data = orders_resp.get("data", [])
                            
                            # Find pending SL orders for this symbol
                            sl_orders = [
                                o for o in orders_data
                                if (o.get("symbol", "") == symbol and 
                                    o.get("status", "").upper() in ["PENDING", "OPEN"] and 
                                    o.get("pricetype", "").upper().startswith("SL"))
                            ]
                            
                            if sl_orders:
                                order_logger.info(f"Found {len(sl_orders)} pending SL order(s) for {symbol}")
                                
                                for sl in sl_orders:
                                    try:
                                        order_id = sl.get("orderid", "")
                                        cancel_resp = base_client.cancelorder(
                                            order_id=order_id, 
                                            strategy=STRATEGY
                                        )
                                        
                                        if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "success":
                                            order_logger.info(f"✅ Cancelled SL order {order_id} for {symbol}")
                                        else:
                                            order_logger.warning(f"⚠️ SL cancel failed for {order_id}: {cancel_resp}")
                                            
                                    except Exception as e:
                                        order_logger.exception(f"Error cancelling SL order {sl.get('orderid', 'N/A')}: {e}")
                            else:
                                order_logger.info(f"No pending SL orders found for {symbol}")
                                
                        else:
                            order_logger.warning(f"Failed to fetch orders for SL cleanup: {orders_resp}")
                            
                    except Exception as e:
                        order_logger.exception(f"Error during SL cleanup for {symbol}: {e}")

                except Exception as e:
                    order_logger.exception(f"Error processing position {pos}: {e}")
                    continue

            # 5️⃣ Clear internal position state
            with self._state_lock:
                self.position = None
                self.entry_price = 0.0
                self.stoploss_price = 0.0
                self.target_price = 0.0
                self.exit_in_progress = False
                self.option_symbol = None
                self.spot_entry_price = 0.0
                self.option_entry_price = 0.0
                
                # Reset CPR state
                self.cpr_active = False
                self.cpr_levels_sorted = []
                self.cpr_targets = []
                self.cpr_sl = 0.0
                self.cpr_side = None
                self.current_tp_index = 0
                self.cpr_tp_hit_count = 0
                
                # Reset enhanced CPR attributes
                self.dynamic_target_info = None
                self.excluded_pivots_info = None

            order_logger.info("✅ Universal position cleanup completed successfully")
            main_logger.info("=== All positions closed and SL orders cancelled safely ===")
            return True

        except Exception as e:
            order_logger.exception(f"exit_all_positions failed: {e}")
            main_logger.error(f"Universal cleanup failed: {e}")
            return False
    # -------------------------
    # Graceful shutdown
    # -------------------------
    def shutdown_gracefully(self):
        """
        Immediate, safe shutdown for both SIMULATION and LIVE modes.
        Ensures we don't attempt to join the current thread and that
        replay/websocket/scheduler are signalled to stop before joining.
        """
        main_logger.info("=== 🛑 GRACEFUL SHUTDOWN INITIATED ===")
        logger.info("🛑 Graceful shutdown started")

        # Log final system status (safe guarded)
        try:
            main_logger.info(f"📈 Final Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
            main_logger.info(f"💼 Current Position: {self.position if self.position else 'None'}")

            if self.position:
                current_ltp = getattr(self, "option_ltp", 0.0) or 0.0
                if self.position == "BUY":
                    unrealized_pnl = (current_ltp - getattr(self, "option_entry_price", 0.0)) * QUANTITY
                else:
                    unrealized_pnl = (getattr(self, "option_entry_price", 0.0) - current_ltp) * QUANTITY

                main_logger.info(
                    "Final Position Details: %s @ %.2f, LTP: %.2f, P&L: %.2f"
                    % (
                        self.position,
                        getattr(self, "entry_price", 0.0),
                        current_ltp,
                        unrealized_pnl,
                    )
                )
            # Attempt to cancel all orders and close position via API (best-effort)
            logger.info("Attempting final order cancellations and position close via API")
            client.cancelallorders(strategy=STRATEGY)
            client.closeposition(strategy=STRATEGY)
        except Exception:
            logger.exception("Error while logging final status")

        # ---------------------------
        # 0) Immediate in-memory shutdown flags (do this first)
        # ---------------------------
        try:
            # Prevent further processing in loops that check these flags
            self.running = False
            self.stop_event.set()
            # clear any exit_in_progress to avoid waiting during shutdown
            self.exit_in_progress = False
            # clear position in memory (we only clear in-memory state; ledger persists)
            # careful: do not persist changes to DB here if you need them for audit
            self.position = None

            main_logger.info("Stop flags set — blocking further activity")
        except Exception:
            logger.exception("Error setting shutdown flags")

        # ---------------------------
        # 1) Signal Replay Client to stop (simulation)
        # ---------------------------
        try:
            # Ensure BACKTESTING_MANAGER is marked inactive so replay loops can detect it
            try:
                BACKTESTING_MANAGER.active = False
            except Exception:
                # if BACKTESTING_MANAGER not present or any error, continue
                pass

            # Signal replay internal stop event if available
            try:
                if hasattr(REPLAY_CLIENT, "_stop_event"):
                    REPLAY_CLIENT._stop_event.set()
            except Exception:
                pass

            # Join the replay thread only if it exists, is alive and we are not the same thread
            try:
                replay_thread = getattr(REPLAY_CLIENT, "_thread", None)
                if replay_thread and replay_thread.is_alive():
                    if threading.current_thread() is not replay_thread:
                        replay_thread.join(timeout=1)
                    else:
                        logger.debug("Skipping join on replay thread because shutdown called from replay thread")
            except Exception:
                logger.debug("Replay thread join skipped or failed")

            # Finally, attempt a controlled disconnect of the replay client
            try:
                if hasattr(REPLAY_CLIENT, "disconnect"):
                    REPLAY_CLIENT.disconnect()
                    main_logger.info("Replay client disconnected")
            except Exception:
                logger.exception("Replay client disconnect error")

        except Exception:
            logger.exception("Replay client stop error")

        # ---------------------------
        # 2) Stop scheduler (if any)
        # ---------------------------
        try:
            if getattr(self, "scheduler", None):
                try:
                    self.scheduler.shutdown(wait=False)
                    main_logger.info("Scheduler shutdown completed")
                except Exception:
                    # best-effort shutdown
                    logger.exception("Scheduler shutdown error")
        except Exception:
            logger.exception("Scheduler object error")

        # ---------------------------
        # 3) Force WebSocket unsubscribe & disconnect (safe)
        # ---------------------------
        try:
            # If the client object exists, try to unsubscribe and disconnect safely.
            if hasattr(self, "client") and self.client:
                try:
                    # In simulation mode you might want to avoid unsubscribe; but since a hard stop requested, do best-effort
                    try:
                    # some clients require instrument param, others don't; call defensively
                        if hasattr(self.client, "unsubscribe_ltp"):
                            try:
                                # Only attempt unsubscribe if not inside that same client's thread (defensive)
                                self.client.unsubscribe_ltp(getattr(self, "instrument", None))
                            except TypeError:
                                # fallback: call without args
                                try:
                                    self.client.unsubscribe_ltp()
                                except Exception:
                                    pass
                    except Exception:
                        pass
                except Exception:
                    pass

                # Disconnect websocket client
                try:
                    if hasattr(self.client, "disconnect"):
                        self.client.disconnect()
                        main_logger.info("WebSocket disconnected")
                except Exception:
                    logger.exception("Error disconnecting websocket client")

        except Exception:
            logger.exception("WebSocket stop error")

        # ---------------------------
        # 4) Final safety join for replay thread if still alive and safe to join
        # ---------------------------
        try:
            replay_thread = getattr(REPLAY_CLIENT, "_thread", None)
            if replay_thread and replay_thread.is_alive() and threading.current_thread() is not replay_thread:
                try:
                    replay_thread.join(timeout=2)
                except Exception:
                    logger.debug("Final replay join skipped")
        except Exception:
            logger.debug("Final replay join check failed")

        # ---------------------------
        # 5) Final logs & return
        # ---------------------------
        try:
            main_logger.info("=== ✅ 🛑 GRACEFUL SHUTDOWN COMPLETED — BOT STOPPED ===")
            main_logger.info("👋 Bot session ended")
        except Exception:
            pass

    
    def shutdown_gracefully_old(self):
        main_logger.info("=== GRACEFUL SHUTDOWN INITIATED ===")
        logger.info("Graceful shutdown started")
        
        # Log final system status
        main_logger.info(f"Final Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
        main_logger.info(f"Current Position: {self.position if self.position else 'None'}")
        
        if self.position:
            current_ltp = self.option_ltp if self.option_ltp else 0.0
            if self.position == "BUY":
                unrealized_pnl = (current_ltp - self.option_entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.option_entry_price - current_ltp) * QUANTITY
            main_logger.info(f"Final Position Details: {self.position} @ {self.entry_price:.2f}, LTP: {current_ltp:.2f}, P&L: {unrealized_pnl:.2f}")
        # ---------------------------------
        # 2️⃣ Stop Replay Client (Simulation Mode)
        # ---------------------------------
        try:
            if BACKTESTING_MANAGER.active:
                BACKTESTING_MANAGER.active = False
                if REPLAY_CLIENT._thread and REPLAY_CLIENT._thread.is_alive():
                    REPLAY_CLIENT._stop_event.set()
                    REPLAY_CLIENT._thread.join(timeout=1)
                REPLAY_CLIENT.disconnect()
                main_logger.info("Replay client stopped")
        except Exception:
            logger.exception("Replay client stop error")
            # set flags
            self.stop_event.set()
            self.running = False
            main_logger.info("Shutdown flags set")
            self.exit_in_progress = False
            self.position = None

        main_logger.info("Stop flags set — blocking further activity")


        # ---------------------------------
        # 3️⃣ Stop Scheduler (Live Mode)
        # ---------------------------------
        try:
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
                main_logger.info("Scheduler stopped")
        except Exception:
            logger.exception("Scheduler stop error")

        # ---------------------------------
        # 2️⃣ Stop Replay Client (Simulation Mode)
        # ---------------------------------
        try:
            if BACKTESTING_MANAGER.active:
                BACKTESTING_MANAGER.active = False
                if REPLAY_CLIENT._thread and REPLAY_CLIENT._thread.is_alive():
                    REPLAY_CLIENT._stop_event.set()
                    REPLAY_CLIENT._thread.join(timeout=1)
                REPLAY_CLIENT.disconnect()
                main_logger.info("Replay client stopped")
        except Exception:
            logger.exception("Replay client stop error")

        try:
            if REPLAY_CLIENT._thread and REPLAY_CLIENT._thread.is_alive():
                REPLAY_CLIENT._thread.join(timeout=2)
        except Exception:
            logger.debug("Replay thread join skipped")
        
        # stop scheduler if running
        try:
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
                main_logger.info("Scheduler shutdown completed")
                logger.info("Scheduler shutdown")
        except Exception:
            logger.exception("Error shutting down scheduler")
            main_logger.error("Error shutting down scheduler", exc_info=True)
            
        # ---------------------------------
        # 4️⃣ Force WebSocket Disconnect
        # ---------------------------------
        try:
            if hasattr(self, "client"):
                try:
                    self.client.unsubscribe_ltp(self.instrument)
                except Exception:
                    pass
            try:
                self.client.disconnect()
            except Exception:
                pass
            main_logger.info("WebSocket disconnected")
        except Exception:
            logger.exception("WebSocket stop error")

        main_logger.info("=== GRACEFUL SHUTDOWN COMPLETED — BOT STOPPED ===")
                
        main_logger.info("=== GRACEFUL SHUTDOWN COMPLETED ===")
        main_logger.info("Bot session ended")
    # -------------------------
    # Run
    # -------------------------
    def run(self):
        main_logger.info("=== 🚀 STARTING MYALGO TRADING SESSION ===")
        session_start_time = datetime.now()
        main_logger.info(f"⏰ Session Start Time: {session_start_time}")
        
        logger.info("🚀 Starting bot")
        
        try:
            # websocket thread
            main_logger.info(f"Starting WebSocket thread for {MODE} data feed")
            ws_thread = threading.Thread(target=self.websocket_thread, daemon=True)
            ws_thread.start()
            time.sleep(1)
            
            # strategy thread (contains scheduler)
            main_logger.info("Starting strategy thread with signal processing")
            strat_thread = threading.Thread(target=self.strategy_thread, daemon=True)
            strat_thread.start()
            
            main_logger.info("All threads started successfully - entering main loop")
            main_logger.info(f"System ready for trading | Max trades: {MAX_TRADES_PER_DAY} | Signal interval: {SIGNAL_CHECK_INTERVAL}min")
            
            try:
                while not self.stop_event.is_set() and self.running:
                    time.sleep(1)
            except KeyboardInterrupt:
                main_logger.info("Manual interruption detected (Ctrl+C)")
                logger.info("KeyboardInterrupt -> shutting down")
                self.shutdown_gracefully()
        except Exception:
            main_logger.error("Critical error in main run loop", exc_info=True)
            logger.exception("Critical error in run method")
        finally:
            # join threads
            main_logger.info("Waiting for threads to complete...")
            ws_thread.join(timeout=3)
            strat_thread.join(timeout=3)
            try:
                if REPLAY_CLIENT._thread and REPLAY_CLIENT._thread.is_alive():
                    REPLAY_CLIENT._thread.join(timeout=2)
            except Exception:
                logger.debug("Replay thread join skipped")
            # Calculate session statistics
            session_end_time = datetime.now()
            session_duration = session_end_time - session_start_time
            main_logger.info("=== TRADING SESSION COMPLETED ===")
            main_logger.info(f"Session Start: {session_start_time}")
            main_logger.info(f"Session End: {session_end_time}")
            main_logger.info(f"Total Duration: {session_duration}")
            main_logger.info(f"Total Trades Executed: {self.trade_count}")
            main_logger.info(f"Max Trades Allowed: {MAX_TRADES_PER_DAY}")
            main_logger.info(f"Final Status: {'Max trades reached' if self.trade_count >= MAX_TRADES_PER_DAY else 'Session ended normally'}")
            
            logger.info("Bot stopped")
            sys.exit()
# -------------------------
# main
# -------------------------
if __name__ == "__main__":
    # Auto start simulation if configured
    if MODE == "BACKTESTING" and SIMULATION_DATE:
        try:
            BACKTESTING_MANAGER.start(SIMULATION_DATE, symbol=SYMBOL, exchange=EXCHANGE, timeframe="1m")
            main_logger.info(f"🎬 Simulation Mode Activated for {SIMULATION_DATE}")
        except Exception:
            main_logger.exception("❌ 🚨 Simulation startup failed; exiting")
            sys.exit(1)
    else:
        main_logger.info("🚀 Live Mode Activated")
        if not market_is_open():
            logger.info("🛑 Market closed right now. Please schedule the bot to run during market hours.")
            sys.exit(1)
    bot = MYALGO_TRADING_BOT()
    bot.run()
