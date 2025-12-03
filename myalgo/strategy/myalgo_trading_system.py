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
from datetime import datetime, timedelta, date
import time
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import pytz
import re
import sys
from dataclasses import dataclass
import os
from typing import Dict, List, Optional
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
# from openalgo.services import telegram_alert_service

# telegram_alert_service.initialize(
#     bot_token=os.getenv("TELEGRAM_BOT_TOKEN"),
#     username=os.getenv("TELEGRAM_USER"),
#     chat_id=os.getenv("TELEGRAM_CHAT_ID")
# )

# # ✅ Notify Telegram that the bot has started
# telegram_alert_service.send_message("🔁 OpenAlgo Python Bot is running. ✅")

# =========================================================
# 🎯 SIMULATION MODE CONFIGURATION
# =========================================================
# Set to "DD-MM-YYYY" for simulation, None for live trading
SIMULATION_DATE: Optional[str] = "None" # Example: "28-11-2025" or None
MODE = "LIVE" # Example: LIVE or BACKTESTING
# ----------------------------
# Configuration
# ----------------------------
API_KEY = "7773b590c743c9184fb1bb74830091a379f88c2035a2203e2b40d24cb2f86711"
API_HOST = "http://127.0.0.1:5000"
WS_URL = "ws://127.0.0.1:8765"

client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

# Instrument
SYMBOL = "NIFTY"
EXCHANGE = "NSE_INDEX"
FUTURE_SYMBOL = "NIFTY30DEC25FUT"
FUTURE_EXCHANGE = "NFO"
PRODUCT = "MIS"
CANDLE_TIMEFRAME = "5m"
LOOKBACK_DAYS = 3
SIGNAL_CHECK_INTERVAL = 5# minutes (use integer minutes)
# ===============================================================
# 🔧 Dynamic LTP Breakout Confirmation Config
# ===============================================================
LTP_BREAKOUT_ENABLED = False          # enable / disable breakout waiting
LTP_BREAKOUT_INTERVAL_MIN = 5        # minutes to wait for LTP breakout confirmatio

# Indicators to compute
EMA_PERIODS = [9, 20, 50, 200]
SMA_PERIODS = [9, 20, 50, 200]
RSI_PERIODS = [14, 21]
CPR = ['DAILY', 'WEEKLY','MONTHLY']

# === LTP Pivot Touch Gatekeeper (new) ===
ENABLE_LTP_PIVOT_GATE = True      # master switch for this gatekeeper
PIVOT_TOUCH_BUFFER_PTS = 0.5      # absolute buffer in price units (points). adjust for NIFTY ~0.5
PIVOT_TOUCH_BUFFER_PCT = None     # optional: use percentage buffer (e.g. 0.001 for 0.1%). If set, overrides PIVOT_TOUCH_BUFFER_PTS
# --------------------------------------

# Risk settings
STOPLOSS = 0
TARGET = 5
MAX_SIGNAL_RANGE = 50
# Trade management
MAX_TRADES_PER_DAY = 3
STRATEGY = "myalgo_scalping"

# Option trading config
OPTION_ENABLED = True
OPTION_EXCHANGE = "NFO"
STRIKE_INTERVAL = 50
OPTION_EXPIRY_TYPE = "WEEKLY"
OPTION_STRIKE_SELECTION = "OTM10"  # "ATM", "ITM1", "OTM2", etc.
EXPIRY_LOOKAHEAD_DAYS = 30
LOT_QUANTITY =75
LOT = 1
QUANTITY = LOT * LOT_QUANTITY
SL_ORDER_ENABLED = True
# Websocket CONSTANTS
HEARTBEAT_INTERVAL = 10        # seconds
OPTION_POLL_INTERVAL = 1.0     # seconds (or higher if rate-limited)
RECONNECT_BASE = 1.0           # seconds
RECONNECT_MAX = 60.0           # seconds


# Stoploss/Target tracking configuration
USE_SPOT_FOR_SLTP = True      # If True → uses spot price for SL/TP tracking
USE_OPTION_FOR_SLTP = False   # If True → uses option position LTP for SL/TP tracking

# SL/TP Enhancement config
# Available Methods:
# - "Signal_candle_range_SL_TP": Uses signal candle range for SL/TP with TSL
# - "CPR_range_SL_TP": Uses CPR pivot levels as dynamic targets with trailing
SL_TP_METHOD = "CPR_range_SL_TP" # current active method #CPR_range_SL_TP #Signal_candle_range_SL_TP
TSL_ENABLED = False # enable trailing stoploss behavior
TSL_METHOD = "TSL_CPR_range_SL_TP" # trailing method for this enhancement

# ===============================================================
# 🔒 ENHANCEMENT: STOP LOSS AUTOMATION & ENTRY RESTRICTIONS
# ===============================================================
# Stop Loss automation constants
MAX_SL_RETRIES = 1              # Maximum retry attempts for SL confirmation
SL_RETRY_DELAY = 2              # Seconds between SL retry attempts
SL_BUFFER_PCT = 0.25            # 25% buffer for automatic SL placement

# Entry restrictions configuration
DAY_HIGH_LOW_VALIDATION_FROM_TRADE = 2  # Apply day high/low validation from this trade count onward

# ===============================================================
# 🎯 CPR PIVOT EXCLUSION CONFIGURATION
# ===============================================================
# Reference below pivot levels from CPR targets similar you can update other levels in CPR_EXCLUDE_PIVOTS
# CPR_EXCLUDE_PIVOTS = [
#     "DAILY_S3", "DAILY_S4", "DAILY_R3", "DAILY_R4",      # Extreme daily levels
#     "WEEKLY_S4", "WEEKLY_R4", "WEEKLY_TC",  "WEEKLY_BC", # Far weekly levels  
#     "MONTHLY_S3", "MONTHLY_S4", "MONTHLY_R3", "MONTHLY_R4"  # Extreme monthly levels
# ]

CPR_EXCLUDE_PIVOTS = [     # Extreme daily levels
     "WEEKLY_TC",  "WEEKLY_BC","WEEKLY_CLOSE", 
     "MONTHLY_TC", "MONTHLY_BC", "MONTHLY_CLOSE", 
     "DAILY_TC", "DAILY_BC", "DAILY_CLOSE"  # Extreme monthly levels
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
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from logger import get_logger, log_trade_execution

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

class SimulationManager:
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

    def start(self, simulate_date_str: str):
        """
        Load 1-minute OHLC for the given date using client.history(..., interval='1m').
        Normalizes timestamp column to tz-aware IST and keeps rows >= 09:15.
        """
        with self.lock:
            dt = self._parse_date(simulate_date_str)
            api_date = dt.strftime("%Y-%m-%d")
            main_logger.info(f"🎬 Loading simulation data for {api_date}")
            logger.info("Fetching 1-minute OHLC for %s", api_date)
            
            try:
                raw = client.history(symbol=SYMBOL, exchange=EXCHANGE, interval="1m",
                                     start_date=api_date, end_date=api_date)
            except Exception as e:
                logger.exception("client.history failed: %s", e)
                raise RuntimeError("History fetch failed") from e

            # Normalize to DataFrame
            if isinstance(raw, dict) and raw.get("data"):
                df = pd.DataFrame(raw["data"])
            elif isinstance(raw, pd.DataFrame):
                df = raw.copy()
            else:
                try:
                    df = pd.DataFrame(raw)
                except Exception:
                    logger.error("Unexpected history response type: %s", type(raw))
                    raise RuntimeError("Unexpected history response format")

            if df.empty:
                raise RuntimeError(f"No 1-minute data returned for {api_date}")

            # Handle DataFrame index
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
                if 'index' in df.columns and 'timestamp' not in df.columns:
                    df.rename(columns={'index': 'timestamp'}, inplace=True)

            # Accept common timestamp column names
            if "timestamp" not in df.columns:
                for alt in ("time", "datetime", "date"):
                    if alt in df.columns:
                        df.rename(columns={alt: "timestamp"}, inplace=True)
                        break

            if "timestamp" not in df.columns:
                raise RuntimeError("History data missing timestamp column")

            # Convert to datetime and ensure timezone-awareness (IST)
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(IST)
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert(IST)

            # Filter to trading start (09:15 or later)
            start_time = datetime.strptime("09:15", "%H:%M").time()
            df = df[df["timestamp"].dt.time >= start_time].reset_index(drop=True)
            
            if df.empty:
                raise RuntimeError("No usable 1-minute rows at/after 09:15 for " + api_date)

            self.df = df
            self.sim_index = 0
            self.simulate_date_str = simulate_date_str
            self.active = True
            
            main_logger.info(f"✅ Simulation loaded: {len(df)} candles from {df['timestamp'].min()} to {df['timestamp'].max()}")
            logger.info("Simulation loaded for %s | rows=%d", simulate_date_str, len(df))

    def stop(self):
        with self.lock:
            self.active = False
            self.df = None
            self.sim_index = 0
            self.simulate_date_str = None
            main_logger.info("Simulation stopped")
            logger.info("Simulation stopped")

    def _parse_date(self, dstr: str) -> datetime:
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(dstr, fmt)
            except Exception:
                continue
        raise ValueError("simulate_date format should be DD-MM-YYYY")

class DateReplayClient:
    """
    Replays SIM_MANAGER.df as LTP ticks and synchronously triggers strategy at minute%5 == 0.
    Fast-mode: no sleep between ticks.
    """
    def __init__(self, sim_manager: SimulationManager):
        self.sim_manager = sim_manager
        self._thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()
    def connect(self):
        logger.info("DateReplayClient.connect() - noop")

    def disconnect(self):
        logger.info("DateReplayClient.disconnect() - stopping")
        self._stop_event.set()
        if self._thread and self._thread.is_alive() and threading.current_thread() != self._thread:
            self._thread.join(timeout=1)

    def subscribe_ltp(self, instruments, on_data_received=None, strategy_fn=None):
        if not self.sim_manager.active or self.sim_manager.df is None:
            raise RuntimeError("Simulation not active or data not loaded")
        self._stop_event.clear()

        def _run():
            """
            Main simulation replay loop.
            Replays 1-minute historical data as live LTP ticks in sequence,
            while triggering the strategy scheduler every 5 minutes (just like live mode).
            """
            df = self.sim_manager.df # Retrieve the simulated DataFrame (historical OHLC data)
            idx = self.sim_manager.sim_index  # Current index position (resume point for replay)
            main_logger.info(f"🎬 Replay starting at index {idx} of {len(df)} candles")
            logger.info("DateReplayClient replay starting at index %d", idx)
            # -------------------------------------------------------------
            # MAIN REPLAY LOOP
            # -------------------------------------------------------------
            # Continue until all candles are replayed OR stop event is triggered
            while idx < len(df) and not self._stop_event.is_set() and self.sim_manager.active:
                
                # ⏸️ SIMULATION PAUSE DURING EXIT
                while self.sim_manager.pause_ticks:
                     time.sleep(0.01)
                     # If shutdown happens while paused, exit loop
                     if self._stop_event.is_set() or not self.sim_manager.active:
                        break
                if self._stop_event.is_set() or not self.sim_manager.active:
                    break

                # Extract current candle row and timestamp
                row = df.iloc[idx]
                ts = pd.to_datetime(row["timestamp"])
                
                # Ensure timestamp is timezone-aware (IST)
                if ts.tzinfo is None:
                    ts = ts.tz_localize(IST)
                else:
                    ts = ts.tz_convert(IST)

                # Build tick payload matching live format
                tick = {
                    "type": "tick",
                    "symbol": row.get("symbol", SYMBOL),
                    "ltp": float(row.get("ltp", row.get("close", 0))),
                    "open": float(row.get("open", 0)),
                    "high": float(row.get("high", 0)),
                    "low": float(row.get("low", 0)),
                    "close": float(row.get("close", 0)),
                    "volume": float(row.get("volume", 0)),
                    "timestamp": ts.isoformat()
                    }
                # Emit LTP -> on_ltp_update # Call on_ltp_update() so that all downstream logic (like SL/TP checks)
                try:
                    if on_data_received:
                        on_data_received(tick) # runs exactly as it would during live market ticks
                except Exception:
                    logger.exception("on_data_received raised during replay")

                # Advance simulation index # The below index acts as a pointer for get_now() and other functions
                with self.sim_manager.lock:
                    self.sim_manager.sim_index = idx + 1 # to know which candle timestamp represents 'current simulated time'.    
                # -------------------------------------------------------------              
                # Trigger 5-minute strategy scheduler (after 09:15 only)
                # -------------------------------------------------------------
                if (ts.hour == 9 and ts.minute < 25): # Skip strategy checks before 09:20 to allow warm-up candle (09:15–09:19)
                    # Ignore all strategy triggers before 09:25 (First Two candles)
                    pass
                # Trigger strategy at exact live scheduler points: minute % 5 == 0
                # Every 5th minute (like live APScheduler job), invoke strategy_fn().
                else:
                    if (ts.minute % SIGNAL_CHECK_INTERVAL == 0 and ts.second == 0):
                        main_logger.info(f"⏰ Strategy trigger at {ts.strftime('%H:%M:%S')}")
                        if strategy_fn:
                            try:
                               # Run the strategy and check if signal was generated
                                result = strategy_fn() # This ensures indicators and signal generation align with live candle timing.
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
                                logger.exception("strategy_fn raised during replay")
                idx += 1 # Move to next candle/tick
                # time.sleep(0.05)  # small delay to simulate real ticks
        # -------------------------------------------------------------
        # LOOP END — only stop when ALL candles replayed
        # -------------------------------------------------------------
            if idx >= len(df):
                main_logger.info("🏁 Replay completed — all candles processed.")
                with self.sim_manager.lock:
                    self.sim_manager.active = False
                self._stop_event.set()
                main_logger.info("✅ Simulation completed successfully.")
            else:
                # ⚡ Important: Do NOT mark inactive or disconnect mid-replay
                main_logger.info("➡️ Trade exited — keeping simulation WebSocket active.")
                logger.info("Simulation WebSocket continues streaming ticks.")
                return
        #-------------------------------------------------------------
        # Start replay in a background thread (daemon)
        # -------------------------------------------------------------
        self._thread = threading.Thread(target=_run, daemon=True)
        self._thread.start()
        logger.info("DateReplayClient started replay thread")
        
    def unsubscribe_ltp(self, instruments):
        logger.info("DateReplayClient.unsubscribe_ltp() unsubscribing")
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=1)


# Global simulation instances
SIM_MANAGER = SimulationManager()
REPLAY_CLIENT = DateReplayClient(SIM_MANAGER)

def get_now():
    """
    Returns simulated current timestamp (tz-aware) if simulation active, else real now in IST.
    """
    if SIM_MANAGER.active and SIM_MANAGER.df is not None:
        idx = max(0, SIM_MANAGER.sim_index - 1)
        if idx < len(SIM_MANAGER.df):
            ts = pd.to_datetime(SIM_MANAGER.df.iloc[idx]["timestamp"])
            if ts.tzinfo is None:
                ts = ts.tz_localize(IST)
            else:
                ts = ts.tz_convert(IST)
            return ts
    # Real time fallback
    return datetime.now(IST)
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
DB_PATH = os.path.join(os.getcwd(), "myalgo.db")
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
        # Gatekeeper state
        self.ltp_pivot_breakout = False        # becomes True when LTP touched a qualifying pivot
        self.ltp_pivot_info = None             # dict: {"pivot":value, "identifier":"DAILY_R1", "period":"DAILY", "matched_at": timestamp}
        self._pivot_gate_lock = threading.Lock()  # protect transitions
        self.gk_resistance_pivot = None
        self.gk_support_pivot = None

        
        # 🎯 Log mode selection
        mode_str = "SIMULATION" if SIM_MANAGER.active else "LIVE"
        logger.info(f"Bot initialized in {mode_str} MODE")
        main_logger.info(f"=== MyAlgo Trading Bot Initialized ({mode_str} MODE) ===")
        
        if SIM_MANAGER.active:
            main_logger.info(f"📅 Simulation Date: {SIM_MANAGER.simulate_date_str}")
        
        main_logger.info(f"Strategy: {STRATEGY} | Symbol: {SYMBOL} | Timeframe: {CANDLE_TIMEFRAME}")
        main_logger.info(f"Risk Management: SL={STOPLOSS}, TP={TARGET} | Max Trades: {MAX_TRADES_PER_DAY}")
        main_logger.info(f"SL/TP Method: {SL_TP_METHOD} | TSL Enabled: {TSL_ENABLED}")
        main_logger.info(f"Options Enabled: {OPTION_ENABLED} | Strike Selection: {OPTION_STRIKE_SELECTION}")
        main_logger.info(f"Signal Check Interval: {SIGNAL_CHECK_INTERVAL} minutes")
        
        # Validate SL/TP method configuration
        valid_methods = ["Signal_candle_range_SL_TP", "CPR_range_SL_TP"]
        if SL_TP_METHOD not in valid_methods:
            main_logger.warning(f"Unknown SL_TP_METHOD: {SL_TP_METHOD}. Valid options: {valid_methods}")
        else:
            main_logger.info(f"✅ Valid SL/TP Method configured: {SL_TP_METHOD}")
            
        # Log CPR enhancement features
        if SL_TP_METHOD == "CPR_range_SL_TP":
            main_logger.info("=== CPR Enhancement Features ===")
            main_logger.info(f"Pivot Exclusion: {'Enabled' if CPR_EXCLUDE_PIVOTS else 'Disabled'}")
            if CPR_EXCLUDE_PIVOTS:
                main_logger.info(f"  Excluded Levels: {len(CPR_EXCLUDE_PIVOTS)} ({', '.join(CPR_EXCLUDE_PIVOTS[:3])}...)")
            main_logger.info(f"Dynamic Risk:Reward: {'Enabled' if USE_DYNAMIC_TARGET else 'Disabled'}")
            if USE_DYNAMIC_TARGET:
                main_logger.info(f"  RR Ratio: 1:{RISK_REWARD_RATIO:.2f}")
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
            
            if SIM_MANAGER.active:
                signal_logger.info(f"⏰ Strategy check (SIMULATED): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # If reached daily trade limit, initiate graceful shutdown
            if not self.position and self.trade_count >= MAX_TRADES_PER_DAY:
                logger.info("Max trades reached (%d). Initiating graceful shutdown.", MAX_TRADES_PER_DAY)
                self.shutdown_gracefully()
                return

            # Ensure static daily indicators are present and fresh
            if not self.static_indicators or self.static_indicators_date != current_time.date():
                ok = self.compute_static_indicators()
                if not ok:
                    logger.warning("Daily indicators missing – skipping this tick")
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
                intraday = self.get_intraday()
                if not intraday.empty:
                    reason = self.check_exit_signal(intraday)
                    if reason:
                        logger.info("Exit triggered: %s", reason)
                        self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=(reason,), daemon=True).start()

        except Exception:
            logger.exception("strategy_job execution failed")

    def strategy_thread(self):
        """
        Strategy thread wrapper - behavior depends on mode:
        - LIVE: starts APScheduler
        - SIMULATION: no scheduler (replay handles timing)
        """
        logger.info("Strategy scheduler starting")
        
        # 🎯 Only start scheduler in LIVE mode
        if not SIM_MANAGER.active:
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
        LTP_BREAKOUT_ENABLED = False
        with self._pivot_gate_lock:
                self.ltp_pivot_breakout = False
                self.ltp_pivot_info = None
    
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
                ok = self.compute_static_indicators()
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
            # Resistance breakout
            if self.gk_resistance_pivot is not None and ltp > self.gk_resistance_pivot:
                return True, {
                    "pivot": self.gk_resistance_pivot,
                    "identifier": "RESISTANCE_BREAK",
                    "timestamp": get_now()
                }

            # Support breakdown
            if self.gk_support_pivot is not None and ltp < self.gk_support_pivot:
                return True, {
                    "pivot": self.gk_support_pivot,
                    "identifier": "SUPPORT_BREAK",
                    "timestamp": get_now()
                }

            return False, None

        except:
            signal_logger.exception("check_ltp_pivot_break failed")
            return False, None

    # -------------------------
    # WebSocket - Real time data LTP 
    # -------------------------  
    def on_ltp_update(self, data):
        """
        Handle incoming websocket ticks/quote updates.
        🎯 Works identically in both LIVE and SIMULATION modes
        """
        try:
            dtype = data.get("type")
            symbol = data.get("symbol") or (data.get("data") or {}).get("symbol")
            ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")
            timestamp = data.get("timestamp")
            
            if symbol == self.option_symbol:
                self.option_ltp = float(ltp)
                return
            elif symbol == SYMBOL:
                self.ltp = float(ltp)
            if symbol is None or ltp is None:
                position_logger.debug("No LTP data in tick")
                return
            # Update raw LTP
            self.ltp = float(ltp)
            # 🎯 Get proper timestamp (simulated or real)
            current_time = get_now()
            now_str = current_time.strftime("%H:%M:%S")
            # 🎯 Only log LTP in SIMULATED mode
            if SIM_MANAGER.active:
                logger.info(f"LTP: {self.ltp} {timestamp}")
                ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")

            if SIM_MANAGER.active and LTP_BREAKOUT_ENABLED and self.pending_breakout:
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
                        signal_logger.info("[GATEKEEPER] Waiting — pivot breakout not yet happened") 

            # Check LTP pending breakout confirmations
            if LTP_BREAKOUT_ENABLED and self.pending_breakout:
                signal_logger.info(f"✅ LTP breakout check")
                now_ts = current_time
                side = self.breakout_side
                high = self.pending_breakout.get("high", 0)
                low = self.pending_breakout.get("low", 0)

                if now_ts > self.pending_breakout_expiry:
                    signal_logger.info("❌ Breakout window expired – clearing pending signal.")
                    self.reset_pending_breakout()
                else:
                    if side == "BUY" and self.ltp > high:
                        self.ltp = high
                        signal_logger.info(f"✅ LTP breakout confirmed (BUY) at {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("BUY", intraday)
                        self.reset_pending_breakout()
                    elif side == "SELL" and self.ltp < low:
                        self.ltp = low
                        signal_logger.info(f"✅ LTP breakout confirmed (SELL) at {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("SELL", intraday)
                        self.reset_pending_breakout()
                    else:
                    # still waiting
                        print(f"\r⏳ Waiting for {side} breakout | LTP={self.ltp:.2f}", end="")
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
                print(f"\r[{now_str}] {symbol} LTP={self.ltp:.2f} | No Active Position", end="")
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
                track_ltp = getattr(self, "option_ltp", None)
                tracking_mode = "OPTION"
                self.option_ltp = track_ltp

            if track_ltp is None:
                print(f"\r[{now_str}] Waiting for LTP updates...", end="")
                return

            # Compute P&L
            if position == "BUY":
                unreal = (track_option_ltp - (option_entry_price if option_entry_price else entry_price)) * QUANTITY
                distance_to_sl = track_ltp - stoploss_price
                distance_to_tp = target_price - track_ltp
            else:
                unreal = ((option_entry_price if option_entry_price else entry_price) - track_option_ltp) * QUANTITY
                distance_to_sl = stoploss_price - track_ltp
                distance_to_tp = track_ltp - target_price

            # Print brief status line
            # print(f"\r[{now}] {symbol} LTP={track_ltp:.2f} | {position} @ {entry_price:.2f} | P&L {unreal:.2f} | SL {stoploss_price:.2f} TG {target_price:.2f}", end="")

            # Detailed position logging occasionally
            current_time = time.time()
            if not hasattr(self, '_last_detailed_log') or current_time - self._last_detailed_log > 10:
                position_logger.info("=== Position Status Update ===")
                position_logger.info(f"Spot Entry: {position} @ {spot_entry_price:.2f}")
                position_logger.info(f"Option Entry: {position} @ {self.option_entry_price:.2f}")
                position_logger.info(f"Tracking Mode: {tracking_mode}")
                position_logger.info(f"Current Spot LTP: {track_ltp:.2f}")
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

    def websocket_thread(self):
        """
        Websocket/replay thread - adapts based on mode:
        🎯 SIMULATION: uses DateReplayClient
        🎯 LIVE: uses real websocket client
        """
        try:
            mode_str = "SIMULATION" if SIM_MANAGER.active else "LIVE"
            logger.info(f"Websocket thread starting ({mode_str} mode)")
            main_logger.info(f"🔌 Starting data feed ({mode_str} mode)")
            
            # 🎯 Choose client based on mode
            with SIM_MANAGER.lock:
                if SIM_MANAGER.active:
                    self.client = REPLAY_CLIENT
                    main_logger.info("Using DateReplayClient for simulation")
                else:
                    self.client = client
                    main_logger.info("Using live WebSocket client")
            
            try:
                self.client.connect()
            except Exception:
                logger.warning("Client connect failed (non-fatal)")

            if SIM_MANAGER.active:
                # 🎯 SIMULATION MODE: start replay
                main_logger.info("Starting simulation replay...")
                REPLAY_CLIENT.subscribe_ltp(
                    self.instrument, 
                    on_data_received=self.on_ltp_update, 
                    strategy_fn=self.strategy_job
                )      
                # Keep alive until sim stops
                while SIM_MANAGER.active and self.running and not self.stop_event.is_set():
                    time.sleep(0.2)
                    
                # ✅ Keep simulation WebSocket running until all candles replayed
                main_logger.info("➡️ Trade exited — keeping simulation WebSocket active (no disconnect).")
                logger.info("Simulation continues streaming ticks like live mode.")
                return
                # DO NOT early-return. Allow finally{} cleanup to manage unsubscribe/disconnect
            else:
                # 🎯 LIVE MODE: use real websocket
                try:
                    self.client.subscribe_quote(self.instrument, on_data_received=self.on_ltp_update)
                except TypeError:
                    self.client.subscribe_ltp(self.instrument, self.on_ltp_update)
                    
                logger.info("Subscribed to LTP")
                
                while not self.stop_event.is_set() and self.running:
                    time.sleep(0.5)
                    
        except Exception:
            logger.exception("websocket error")
        finally:
            try:
                self.client.unsubscribe_ltp(self.instrument)
            except Exception:
                pass
            try:
                self.client.disconnect()
            except Exception:
                pass
            logger.info("websocket closed")
            try:
                REPLAY_CLIENT.disconnect()
            except Exception:
                pass
    
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
    
    def normalize_df(self, raw):
            if raw is None:
                return pd.DataFrame()
            if isinstance(raw, dict) and "data" in raw:
                df_local = pd.DataFrame(raw["data"])
            elif isinstance(raw, pd.DataFrame):
                df_local = raw.copy()
            else:
                try:
                    df_local = pd.DataFrame(raw)
                except Exception:
                    return pd.DataFrame()

            if df_local.empty:
                return df_local

            # Ensure timestamp exists
            if isinstance(df_local.index, pd.DatetimeIndex):
                df_local = df_local.reset_index()
                if "index" in df_local.columns and "timestamp" not in df_local.columns:
                    df_local.rename(columns={"index": "timestamp"}, inplace=True)

            if "timestamp" not in df_local.columns:
                for alt in ("time", "datetime", "date"):
                    if alt in df_local.columns:
                        df_local.rename(columns={alt: "timestamp"}, inplace=True)
                        break

            if "timestamp" not in df_local.columns:
                logger.warning("get_intraday(): timestamp column not found")
                return pd.DataFrame()

            # Normalize numeric columns
            for c in ["open", "high", "low", "close", "ltp", "volume"]:
                if c in df_local.columns:
                    df_local[c] = pd.to_numeric(df_local[c], errors="coerce")

            # Localize timestamp to IST
            df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], errors="coerce")
            if df_local["timestamp"].dt.tz is None:
                df_local["timestamp"] = df_local["timestamp"].dt.tz_localize(IST)
            else:
                df_local["timestamp"] = df_local["timestamp"].dt.tz_convert(IST)
            df_local.set_index("timestamp", inplace=True)
            df_local.sort_index(inplace=True)
            return df_local
    # ==========================================================
    # 🧠 Get intraday candle
    # ==========================================================
    def get_intraday(self, days=LOOKBACK_DAYS):
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

        # 🎯 Use get_now() for consistent behavior across live/sim
        end_date = get_now()
        start_date = end_date - timedelta(days=days)
        base_client = client  # always use base client

        # ----------------------------------------------------------
        # 🧩 Helper: Normalize API response → clean DataFrame
        # ----------------------------------------------------------
        def _normalize_df(raw):
            if raw is None:
                return pd.DataFrame()
            if isinstance(raw, dict) and "data" in raw:
                df_local = pd.DataFrame(raw["data"])
            elif isinstance(raw, pd.DataFrame):
                df_local = raw.copy()
            else:
                try:
                    df_local = pd.DataFrame(raw)
                except Exception:
                    return pd.DataFrame()

            if df_local.empty:
                return df_local

            # Ensure timestamp exists
            if isinstance(df_local.index, pd.DatetimeIndex):
                df_local = df_local.reset_index()
                if "index" in df_local.columns and "timestamp" not in df_local.columns:
                    df_local.rename(columns={"index": "timestamp"}, inplace=True)

            if "timestamp" not in df_local.columns:
                for alt in ("time", "datetime", "date"):
                    if alt in df_local.columns:
                        df_local.rename(columns={alt: "timestamp"}, inplace=True)
                        break

            if "timestamp" not in df_local.columns:
                logger.warning("get_intraday(): timestamp column not found")
                return pd.DataFrame()

            # Normalize numeric columns
            for c in ["open", "high", "low", "close", "ltp", "volume"]:
                if c in df_local.columns:
                    df_local[c] = pd.to_numeric(df_local[c], errors="coerce")

            # Localize timestamp to IST
            df_local["timestamp"] = pd.to_datetime(df_local["timestamp"], errors="coerce")
            if df_local["timestamp"].dt.tz is None:
                df_local["timestamp"] = df_local["timestamp"].dt.tz_localize(IST)
            else:
                df_local["timestamp"] = df_local["timestamp"].dt.tz_convert(IST)
            df_local.set_index("timestamp", inplace=True)
            df_local.sort_index(inplace=True)
            return df_local

        # ==========================================================
        # 🧠 SIMULATION MODE
        # ==========================================================
        if SIM_MANAGER.active:
            sim_day_key = end_date.date().isoformat()

            # Create cache if not exists
            if not hasattr(self, "_sim_data_cache"):
                self._sim_data_cache = {}

            # Cache intraday once per simulated date
            if sim_day_key not in self._sim_data_cache:
                main_logger.info(f"🔁 Simulation: caching 5m intraday data for {sim_day_key}")

                raw = self.retry_api_call(
                    func=base_client.history,
                    max_retries=3,
                    delay=2,
                    description=f"Intraday History Cache ({sim_day_key})",
                    symbol=SYMBOL,
                    exchange=EXCHANGE,
                    interval=CANDLE_TIMEFRAME,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d")
                )
                df_full = _normalize_df(raw)
                self._sim_data_cache[sim_day_key] = df_full
            else:
                df_full = self._sim_data_cache[sim_day_key]

            if df_full.empty:
                logger.warning(f"⚠️ Simulation cache empty for {sim_day_key}")
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

            # ✅ Compute current-day levels using your helper
            day_open, day_high, day_low = self.get_current_day_highlow(df)
            if day_open and day_high and day_low:
                self.current_day_open = day_open
                self.current_day_high = day_high
                self.current_day_low = day_low

            data_logger.info(
                f"📊 [SIM] Current Day Levels | O={day_open} H={day_high} L={day_low}"
            )

            return df

        # ==========================================================
        # 🌐 LIVE MODE (unchanged)
        # ==========================================================
        raw = self.retry_api_call(
            func=base_client.history,
            max_retries=3,
            delay=2,
            description="Intraday History Fetch",
            symbol=SYMBOL,
            exchange=EXCHANGE,
            interval=CANDLE_TIMEFRAME,
            start_date=start_date.strftime("%Y-%m-%d"),
            end_date=end_date.strftime("%Y-%m-%d")
        )

        if raw is None:
            return pd.DataFrame()

        df = _normalize_df(raw)

        # Trim candles up to current live time
        current_live_time = get_now()
        df = df[df.index <= current_live_time]

        # ✅ Compute current-day high/low/open exactly as your system does
        day_open, day_high, day_low = self.get_current_day_highlow(df)
        if day_open and day_high and day_low:
            self.current_day_open = day_open
            self.current_day_high = day_high
            self.current_day_low = day_low

        data_logger.info(
            f"📊 [LIVE] Current Day Levels | O={day_open} H={day_high} L={day_low}"
        )

        return df
    # -------------------------
    # Static indicators 
    # -------------------------
    def compute_static_indicators(self):
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
            indicators_logger.info(f"Enabled CPR Timeframes: {', '.join(CPR)}")
            indicators_logger.info("╚══════════════════════════════")

            # DAILY CPR
            if 'DAILY' in CPR:
                dfdaily = self.get_historical("D",lookback=5)
                self.static_indicators['DAILY'] = self.compute_cpr(dfdaily, "DAILY")
                self.static_indicators_date = current_time.date()
                
            # WEEKLY CPR
            if 'WEEKLY' in CPR:
                dfweekly = self.get_historical("W",lookback=20)
                self.static_indicators['WEEKLY'] = self.compute_cpr(dfweekly, "WEEKLY")
                
            # MONTHLY CPR
            if 'MONTHLY' in CPR:
                dfmonthly = self.get_historical("M",lookback=70)
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
    def get_historical(self, interval="D", lookback=90):
        """
        Dynamic reusable function to fetch historical OHLC data.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        try:
            # 🎯 Use get_now() for proper date handling
            end_date = get_now()

            if interval == "5m":
                start_date = end_date - timedelta(days=lookback)
            elif interval in ["D", "W", "M"]:
                start_date = end_date - timedelta(days=lookback)
            else:
                raise ValueError(f"Unsupported interval: {interval}")

            # 🎯 Always use base client for data fetch
            base_client = client
            
            description = f"Historical Fetch ({interval})"
            raw = self.retry_api_call(
                func=base_client.history,
                max_retries=3,
                delay=2,
                description=description,
                symbol=SYMBOL,
                exchange=EXCHANGE,
                interval="D" if interval in ["W", "M"] else interval,
                start_date=start_date.strftime("%Y-%m-%d"),
                end_date=end_date.strftime("%Y-%m-%d")
            )

            # Normalize DataFrame
            if isinstance(raw, pd.DataFrame):
                df = raw.copy()
            elif isinstance(raw, dict) and "data" in raw:
                df = pd.DataFrame(raw["data"])
            else:
                df = pd.DataFrame()

            if df.empty:
                indicators_logger.warning(f"No data received for {SYMBOL} {EXCHANGE} {interval}")
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

            # Resampling for Weekly and Monthly
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

            # Logging summary
            indicators_logger.info(
                f"[HISTORICAL] Interval:{interval} | Records:{len(df)} | "
                f"Range:{df.index.min().strftime('%d-%b-%Y')} → {df.index.max().strftime('%d-%b-%Y')}"
            )

            return df

        except Exception as e:
            logger.exception(f"get_historical failed for interval {interval}: {e}")
            return pd.DataFrame()
    #--------------------------
    # CPR - Daily, Weekly, Monthly
    #--------------------------  
    def compute_cpr(self, df, label: str):
        """Generic CPR computation used by daily, weekly, monthly methods."""
        if len(df) < 2:
            indicators_logger.warning(f"{label} CPR: insufficient bars to compute.")
            return None
        prev = df.iloc[-2]
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
            indicators_logger.info(f"Date: {df.index[-2].strftime('%d-%b-%Y')}")
        elif label.upper() == "WEEKLY":
            indicators_logger.info(f"Week Start: {df.index[-2].strftime('%d-%b-%Y')}")
        else:
            indicators_logger.info(f"Month: {df.index[-2].strftime('%b %Y')}")
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
            "timestamp": df.index[-2]
        }
        
    # -------------------------
    # Dynamic indicators (every current candle timeframe)
    # -------------------------
    def get_vwap(self, intraday_df, symbol=None, exchange=None, interval=None):
        """
            Generic VWAP calculator.
        - Uses given symbol/exchange/interval OR falls back to global config.
        - VWAP uses OHLCV from history() with same timeframe as indicators.
        - Returns VWAP for last completed candle (-2).
        """
        try:
            # ---------------------------
            # Use global config if not provided
            # ---------------------------
            symbol = symbol or FUTURE_SYMBOL
            exchange = exchange or FUTURE_EXCHANGE
            interval = interval or CANDLE_TIMEFRAME

            if intraday_df is None or intraday_df.empty:
                return None

            start_date = intraday_df.index[0].strftime("%Y-%m-%d")
            end_date   = intraday_df.index[-1].strftime("%Y-%m-%d")

            # ---------------------------
            # Fetch OHLCV for selected symbol
            # ---------------------------
            raw = self.retry_api_call(
                func=client.history,
                max_retries=3,
                delay=2,
                description=f"{symbol} VWAP Data",
                symbol=FUTURE_SYMBOL,
                exchange=FUTURE_EXCHANGE,
                interval=interval,
                start_date=start_date,
                end_date=end_date
            )

            # df = pd.DataFrame(raw.get("data", [])) if isinstance(raw, dict) else pd.DataFrame()
            df = self.normalize_df(raw) 
            # ---------------------------
            # Validation
            # ---------------------------
            if df.empty or "volume" not in df.columns:
                # Simulation-safe: silently skip instead of warning spam
                if SIM_MANAGER.active:
                    return None

                indicators_logger.warning(f"{symbol} OHLCV missing volume → VWAP skipped")
                return None
            # ---------------------------
            # VWAP Calculation
            # ---------------------------
            tp = (df["high"] + df["low"] + df["close"]) / 3
            df["tpv"] = tp * df["volume"]

            df["VWAP"] = df["tpv"].cumsum() / df["volume"].cumsum()

            # Return last completed candle VWAP
            if len(df) >= 2:
                return float(df["VWAP"].iloc[-2])

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
            vwap_value = self.get_vwap(intraday_df=df)
            if vwap_value is not None:
                dyn["VWAP"] = vwap_value
                indicators_logger.info(f"VWAP: {vwap_value:.2f}")
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
    def check_exit_signal(self, intraday_df):
        """
        Evaluate exit signal conditions.
        🎯 Works identically in LIVE and SIMULATION modes
        """
        try:
            signal_logger.info("=== Evaluating Exit Signal ===")
            
            if intraday_df is None or intraday_df.empty:
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
            if not dyn:
                signal_logger.warning("Failed to compute dynamic indicators for exit evaluation")
                return None
                
            last = dyn.get("last_candle", {})
            last_close = float(last.get("close", 0.0))
            ema20 = float(dyn.get("EMA_20", 0.0))
            ema50 = float(dyn.get("EMA_20", 0.0))
            ema200 = float(dyn.get("EMA_20", 0.0))
            
            signal_logger.info(f"Exit Signal Evaluation Data:")
            signal_logger.info(f"  Last Candle Close: {last_close:.2f}")
            signal_logger.info(f"  EMA20: {ema20:.2f}")
            
            # Calculate current P&L for context
            if self.option_ltp is None:
                self.option_ltp = self.option_entry_price
            if self.position == "BUY":
                unrealized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.option_entry_price - self.option_ltp) * QUANTITY
                
            signal_logger.info(f"  Current P&L: {unrealized_pnl:.2f}")
            
           # Check EMA20 crossover exit conditions
            if self.position == "BUY":
                ema_exit_condition = last_close < ema20
                signal_logger.info(f"BUY Exit Check: Close < EMA20 -> {last_close:.2f} < {ema20:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE BELOW EMA20 (BUY Position)")
                    return f"CLOSE_BELOW_EMA20 (close={last_close:.2f}, EMA20={ema20:.2f})"
                    
            elif self.position == "SELL":
                ema_exit_condition = last_close > ema20
                signal_logger.info(f"SELL Exit Check: Close > EMA20 -> {last_close:.2f} > {ema20:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE ABOVE EMA20 (SELL Position)")
                    return f"CLOSE_ABOVE_EMA20 (close={last_close:.2f}, EMA20={ema20:.2f})"
            
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
        current_date = datetime.now(IST).date()
        df_today = df[df.index.date == current_date]

    # Exclude current forming candle (latest one)
        if len(df_today) > 1:
            df_for_highlow = df_today.iloc[:-1]
        else:
            df_for_highlow = df_today.copy()

        if df_for_highlow.empty:
            return None, None, None

        # ✅ Exclude both forming candle and the last closed candle (to avoid self-comparison)
        df_for_highlow = df_today.iloc[:-2]

        if df_for_highlow.empty:
            return None, None, None
        else:
            df_for_highlow = df_today.iloc[:-1]
            
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
            signal_logger.info("=== Evaluating Entry Signal ===")

            if intraday_df is None or intraday_df.empty:
                signal_logger.warning("No intraday data available for signal evaluation")
                return None

            # Get current LTP for logging context
            current_ltp = self.ltp if self.ltp else 0.0
            signal_logger.info(f"Current LTP: {current_ltp:.2f}")
             # ---------------------------
                # Handle any pending breakout first
            # ---------------------------
            if LTP_BREAKOUT_ENABLED and self.pending_breakout:
                signal_logger.info(f"⏳ LTP breakout enabled.")
                return None

            # ONLY FIRST TRADES REQUIRE PIVOT GATE
            if ENABLE_LTP_PIVOT_GATE and self.trade_count == 0:
                with self._pivot_gate_lock:
                    if not self.ltp_pivot_breakout:
                        signal_logger.info("Entry BLOCKED → Pivot Breakout not happened yet")
                        return None
            
            # Current Day High/Low Validation (Early Exit for Performance)
            if self.trade_count >= DAY_HIGH_LOW_VALIDATION_FROM_TRADE:
                signal_logger.info(f"Applying day high/low validation (trade #{self.trade_count})")
                
                if len(intraday_df) < 2:
                    signal_logger.warning("Insufficient intraday data for signal candle validation")
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
                    signal_logger.info("Day high/low pre-validation failed for both BUY and SELL, skipping technical analysis")
                    return None
                    
                signal_logger.info("Day high/low pre-validation passed, proceeding with technical analysis")
            else:
                signal_logger.info(f"Day high/low validation not required yet (trade #{self.trade_count} < {DAY_HIGH_LOW_VALIDATION_FROM_TRADE})")

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
            signal_logger.info(f"Signal Evaluation Data:")
            signal_logger.info(f"  Current Candle: Open={open_:.2f}, Close={close:.2f}")
            signal_logger.info(f"  Previous Candle: Open={prev_open:.2f}, High={prev_high:.2f}, Low={prev_low:.2f}, Close={prev_close:.2f}")
            signal_logger.info(f"  EMA Values: EMA9={ema9:.2f}, EMA20={ema20:.2f}, EMA50={ema50:.2f}")
            signal_logger.info(f"  CPR Levels: R1={cpr_r1:.2f}, S1={cpr_s1:.2f}")
            signal_logger.info(f"  Previous Day: High={prev_day_high:.2f}, Low={prev_day_low:.2f}")

            # require current_day_high/low as additional gate (if not available, treat it as false to avoid false entries)
            current_high_ok = (self.current_day_high is not None and close > self.current_day_high)
            current_low_ok = (self.current_day_low is not None and close < self.current_day_low)

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
             
            # long_cond = long_cond6
            long_cond = long_cond1 and long_cond2 and long_cond3 and long_cond4 and long_cond5 and long_cond6 and long_cond7 and long_cond8 and long_cond11
            # long_cond = long_cond6 
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

            short_cond =  short_cond1 and short_cond2 and  short_cond3 and short_cond4 and short_cond5 and short_cond6 and short_cond7 and short_cond8 and short_cond11
            # short_cond = short_cond6 
            # Log detailed condition evaluation
            signal_logger.info("=== LONG Signal Conditions ===")
            signal_logger.info(f"  1. Close > Prev Day High: {close:.2f} > {prev_day_high:.2f} = {long_cond1}")
            signal_logger.info(f"  2. Close > CPR R1: {close:.2f} > {cpr_r1:.2f} = {long_cond2}")
            signal_logger.info(f"  3. Close > EMA9: {close:.2f} > {ema9:.2f} = {long_cond3}")
            signal_logger.info(f"  4. EMA9 > EMA20: {ema9:.2f} > {ema20:.2f} = {long_cond4}")
            signal_logger.info(f"  5. Prev Close > Prev Open: {prev_close:.2f} > {prev_open:.2f} = {long_cond5}")
            signal_logger.info(f"  6. Close > Prev High: {close:.2f} > {prev_high:.2f} = {long_cond11}")
            signal_logger.info(f"  7. Close > Open: {close:.2f} > {open_:.2f} = {long_cond6}")
            signal_logger.info(f"  8. Close > EMA50: {close:.2f} > {ema50:.2f} = {long_cond7}")
            signal_logger.info(f"  9. Close > EMA200: {close:.2f} > {ema200:.2f} = {long_cond8}")
            signal_logger.info(f"  10. LTP > last candle high: {current_ltp:.2f} > {high:.2f} = {long_cond9}")
            signal_logger.info(f"  11. Close > current day high: {close:.2f} > {self.current_day_high :.2f} = {long_cond10}")
            signal_logger.info(f"  LONG Signal Valid: {long_cond}")

            signal_logger.info("=== SHORT Signal Conditions ===")
            signal_logger.info(f"  1. Close < Prev Day Low: {close:.2f} < {prev_day_low:.2f} = {short_cond1}")
            signal_logger.info(f"  2. Close < CPR S1: {close:.2f} < {cpr_s1:.2f} = {short_cond2}")
            signal_logger.info(f"  3. Close < EMA9: {close:.2f} < {ema9:.2f} = {short_cond3}")
            signal_logger.info(f"  4. EMA9 < EMA20: {ema9:.2f} < {ema20:.2f} = {short_cond4}")
            signal_logger.info(f"  5. Prev Close < Prev Open: {prev_close:.2f} < {prev_open:.2f} = {short_cond5}")
            signal_logger.info(f"  6. Close < Prev Low: {close:.2f} < {prev_low:.2f} = {long_cond11}")
            signal_logger.info(f"  7. Close < Open: {close:.2f} < {open_:.2f} = {short_cond6}")
            signal_logger.info(f"  8. Close < EMA50: {close:.2f} < {ema50:.2f} = {short_cond7}")
            signal_logger.info(f"  9. Close < EMA200: {close:.2f} < {ema200:.2f} = {short_cond8}")
            signal_logger.info(f"  10. LTP < last candle low: {current_ltp:.2f} < {low:.2f} = {short_cond9}")
            signal_logger.info(f"  11 Close < current day low: {close:.2f} < {self.current_day_low :.2f} = {short_cond10}")
            signal_logger.info(f"  SHORT Signal Valid: {short_cond}")

            #  ✅ Compute current day's high/low from intraday candles
            # === Breakout Filter Check ===
            if long_cond:
                last_candle = last
                if LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    self.pending_breakout = {
                        "type": "BUY",
                        "timestamp": now,
                        "high": float(last_candle.get("high", 0)),
                        "low": float(last_candle.get("low", 0))
                        }
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.pending_breakout_expiry = expiry
                    self.breakout_side = "BUY"
                    signal_logger.info(f"🟢 BUY signal waiting for LTP breakout 📈 > {self.pending_breakout['high']:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info("🟢 ENTRY SIGNAL GENERATED: BUY (no LTP breakout filter)")
                    return "BUY"

            if short_cond:
                last_candle = last
                if LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    self.pending_breakout = {
                        "type": "SELL",
                        "timestamp": now,
                        "high": float(last_candle.get("high", 0)),
                        "low": float(last_candle.get("low", 0))
                    }
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.pending_breakout_expiry = expiry
                    self.breakout_side = "SELL"
                    signal_logger.info(f"📉 SELL signal waiting for LTP breakout < {self.pending_breakout['low']:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info("🔴 ENTRY SIGNAL GENERATED: SELL (no LTP breakout filter)")
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
                    self.stoploss_price = new_sl
                else:
                    # move SL to previous TP price
                    new_sl = round(self.trailing_levels[-2], 2)
                    self.stoploss_price = new_sl

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
            
    def initialize_cpr_range_sl_tp_enhanced(self, signal, intraday_df, base_price=None):
        """
        Enhanced CPR initialization with exclusion filtering and dynamic risk:reward targeting
        Combines both CPR pivot exclusion and dynamic target selection features
        """
        try:
            position_logger.info(f"Initializing Enhanced CPR Range SL/TP for {signal} signal")
            position_logger.info(f"Features: Exclusion Filter={'ON' if CPR_EXCLUDE_PIVOTS else 'OFF'}, Dynamic Target={'ON' if USE_DYNAMIC_TARGET else 'OFF'}")
            
            # Get signal candle data for initial SL
            if intraday_df is None:
                position_logger.warning("No signal candle data available for enhanced CPR initialization")
                return False
                
            sig_candle = intraday_df.iloc[-2]
            signal_candle_low = float(sig_candle.get('low', 0) or 0)
            signal_candle_high = float(sig_candle.get('high', 0) or 0)
            signal_candle_range = abs(signal_candle_high - signal_candle_low)
            # Merge pivot levels with exclusion filtering
            if not self.static_indicators:
                position_logger.error("No static indicators available for CPR levels")
                return False
                
            pivots_dict = {}
            for timeframe in ['DAILY', 'WEEKLY', 'MONTHLY']:
                if timeframe in self.static_indicators:
                    pivots_dict[timeframe.lower()] = self.static_indicators[timeframe]
                    
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
                base_price = float(self.entry_price or 0.0)
            self.entry_price = float(base_price)
            self.cpr_side = signal.upper()
            
            # Calculate initial SL from signal candle
            if self.cpr_side == "BUY":
                raw_sl = round(signal_candle_low - STOPLOSS, 2)
                initial_sl = signal_candle_high - MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else raw_sl
            else:  # SELL
                raw_sl = round(signal_candle_high + STOPLOSS, 2)
                initial_sl = signal_candle_low + MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else raw_sl
                
            self.cpr_sl = initial_sl
            self.stoploss_price = initial_sl
            
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
                
                position_logger.info(f"Dynamic Target Selected: {target_key} @ {dynamic_target:.2f}")
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
        Handle CPR target hit - advance trailing stop loss:
        Rule 1: After first TP hit -> move SL to entry (breakeven)
        Rule 2: After second TP hit -> move SL to first TP price
        Rule 3+: Continue rolling SL to last TP price
        """
        try:
            with self._state_lock:
                self.cpr_tp_hit_count += 1
                prev_idx = self.current_tp_index
                
                position_logger.info(f"CPR TP Hit #{self.cpr_tp_hit_count}: {hit_price:.2f}")
                
                # Advance SL based on TP hit count
                if self.cpr_tp_hit_count == 1:
                    # First TP hit -> move SL to breakeven (entry price)
                    self.cpr_sl = round(float(self.entry_price), 2)
                    position_logger.info(f"First CPR TP hit - SL moved to breakeven: {self.cpr_sl:.2f}")
                elif prev_idx >= 0 and prev_idx < len(self.cpr_targets):
                    # Subsequent TP hits -> move SL to previous TP price
                    self.cpr_sl = round(float(self.cpr_targets[prev_idx]), 2)
                    position_logger.info(f"CPR TP hit #{self.cpr_tp_hit_count} - SL moved to previous TP: {self.cpr_sl:.2f}")
                
                # Move to next target
                self.current_tp_index = prev_idx + 1
                
                # Update WebSocket monitoring variables
                self.stoploss_price = self.cpr_sl
                
                if self.current_tp_index >= len(self.cpr_targets):
                    # No more targets - final exit
                    self.current_tp_index = -1
                    position_logger.info("Final CPR target reached - will exit on next update")
                    return True  # Signal for final exit
                else:
                    # Set next target
                    next_tp = self.cpr_targets[self.current_tp_index]
                    self.target_price = round(float(next_tp), 2)
                    position_logger.info(f"Next CPR Target: {self.target_price:.2f}")
                    
                    # Log CPR TP hit to database
                    self._log_cpr_tp_hit(hit_price)
                    
                return False  # Continue trading
                    
        except Exception as e:
            position_logger.error(f"Error handling CPR TP hit: {e}", exc_info=True)
            return True  # Exit on error
            
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
            
            # Check SL conditions
            if side == "BUY" and current_ltp <= self.cpr_sl:
                position_logger.warning(f"CPR BUY | SL Hit: {current_ltp:.2f} <= {self.cpr_sl:.2f}")
                return "SL_HIT"
            elif side == "SELL" and current_ltp >= self.cpr_sl:
                position_logger.warning(f"CPR SELL | SL Hit: {current_ltp:.2f} >= {self.cpr_sl:.2f}")
                return "SL_HIT"
                
            # Check TP conditions
            if current_tp is not None:
                if side == "BUY" and current_ltp >= current_tp:
                    position_logger.info(f"CPR BUY TP Hit: {current_ltp:.2f} >= {current_tp:.2f}")
                    
                    # Check TSL_ENABLED to determine behavior
                    if not TSL_ENABLED:
                        position_logger.info("TSL disabled - sending FINAL_EXIT signal on TP hit")
                        return "FINAL_EXIT"
                    else:
                        # TSL enabled - proceed with trailing logic
                        final_exit = self.handle_cpr_tp_hit(current_tp)
                        return "FINAL_EXIT" if final_exit else "TP_HIT"
                        
                elif side == "SELL" and current_ltp <= current_tp:
                    position_logger.info(f"CPR SELL TP Hit: {current_ltp:.2f} <= {current_tp:.2f}")
                    
                    # Check TSL_ENABLED to determine behavior
                    if not TSL_ENABLED:
                        position_logger.info("TSL disabled - sending FINAL_EXIT signal on TP hit")
                        return "FINAL_EXIT"
                    else:
                        # TSL enabled - proceed with trailing logic
                        final_exit = self.handle_cpr_tp_hit(current_tp)
                        return "FINAL_EXIT" if final_exit else "TP_HIT"
                    
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
                unrealized_pnl = (self.option_entry_price - self.option_ltp) * QUANTITY
                
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
        Calculate optimal target using risk:reward ratio and filtered CPR levels
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
                return fallback_target, 0, 0, "FALLBACK", "Invalid risk distance"
            
            # Compute required reward distance
            reward_distance = risk 
            
            # Determine required minimum target price
            if signal == "BUY":
                required_target = entry_price + reward_distance
            else:  # SELL
                required_target = entry_price - reward_distance
                
            position_logger.info(f"Dynamic Target Calculation:")
            position_logger.info(f"  Signal: {signal} | Entry: {entry_price:.2f} | SL: {sl_price:.2f}")
            position_logger.info(f"  Risk: {risk:.2f} | RR Ratio: {RISK_REWARD_RATIO:.2f} | Required Reward: {reward_distance:.2f}")
            position_logger.info(f"  Required Target: {required_target:.2f}")
            
            if not available_levels:
                position_logger.warning("No CPR levels available; using computed target")
                return required_target, risk, reward_distance, "COMPUTED", "No CPR levels available"
            
            # Filter CPR levels based on direction and minimum distance requirement
            valid_candidates = []
            
            for level_value in available_levels:
                level_id = level_mapping.get(level_value, f"UNKNOWN_{level_value:.2f}")
                
                if signal == "BUY":
                    # For BUY: level must be above entry AND meet minimum reward requirement
                    if level_value > entry_price and level_value >= required_target:
                        distance_from_entry = level_value - entry_price
                        valid_candidates.append((level_value, level_id, distance_from_entry))
                else:  # SELL
                    # For SELL: level must be below entry AND meet minimum reward requirement  
                    if level_value < entry_price and level_value <= required_target:
                        distance_from_entry = entry_price - level_value
                        valid_candidates.append((level_value, level_id, distance_from_entry))
            
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
            position_logger.info(f"  Available Candidates: {len(valid_candidates)}")
            for level_val, level_key, dist in valid_candidates[:5]:  # Log first 5
                position_logger.info(f"    {level_key}: {level_val:.2f} (dist: {dist:.2f})")
            if len(valid_candidates) > 5:
                position_logger.info(f"    ... and {len(valid_candidates) - 5} more")
                
            position_logger.info(f"  🎯 SELECTED TARGET: {target_key} @ {target_price:.2f}")
            position_logger.info(f"  Actual RR Ratio: 1:{(target_price - entry_price if signal == 'BUY' else entry_price - target_price) / risk:.2f}")
            
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
        """Pick nearest expiry date (YYYY-MM-DD) returned by expiry API that's >= today."""
        try:
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

    # ===============================================================
    # 🔒 ENHANCEMENT: HELPER FUNCTIONS FOR STOP LOSS AUTOMATION & ENTRY RESTRICTIONS
    # ===============================================================
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
        if SIM_MANAGER.active:
            order_logger.info(f"📋 [SIMULATION] Placing {action} order: {symbol} x {quantity}")
            # Generate mock order ID with timestamp
            mock_order_id = f"SIGNAL_{get_now().strftime('%H%M%S')}_{action[:1]}"
            # Use current LTP as execution price
            exec_price = self.ltp if self.ltp else 0.0
            order_logger.info(f"✅ [SIMULATION] Order executed: ID={mock_order_id} | Price={exec_price:.2f}")
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
            order_logger.info(f"✅ Order placed successfully ID={order_id}. Confirming executed price...")
            # --- Step 3: Confirm executed price using get_executed_price
            exec_price = order_resp["exec_price"] 
            # exec_price = self.get_executed_price(order_id)
            if exec_price is not None and exec_price > 0:
                order_logger.info(f"✅ Executed price confirmed (order {order_id}) = {exec_price}")
                return order_id, exec_price    
            else:
                order_logger.error(f"❌ Executed price not confirmed after {max_exec_retries} polls")
                return None, None   
    
    def compute_25pct_sl(self, exec_price: float, side: str) -> float:
        """Compute 25% stop loss price based on executed price and side"""
        if side.upper() == "BUY":
            return round(exec_price * (1 - SL_BUFFER_PCT), 2)
        else:
            return round(exec_price * (1 + SL_BUFFER_PCT), 2)

    def place_and_confirm_stoploss(self, exit_symbol: str, exit_exchange: str, entry_side: str, sl_price: float) -> Optional[str]:
        """
        Place stop loss order and confirm execution with retry mechanism.
        🎯 In SIMULATION mode: skips SL order (monitoring handles it)
        """
        # 🎯 SIMULATION MODE: Skip SL order placement
        if SIM_MANAGER.active:
            order_logger.info(f"📋 [SIMULATION] SL order skipped (handled by monitoring)")
            return "SIM_SL_SKIP"

         # 🎯 LIVE MODE: Real SL placement
        sl_action = "SELL" 
        order_logger.info(f"Placing SL order -> Action={sl_action} | Trigger/SL={sl_price:.2f} | Symbol={exit_symbol}")
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

            if sl_action.upper() == "SELL":
                limit_price = round_to_tick(trigger_price - TICK_SIZE)
            else:  # BUY SL
                limit_price = round_to_tick(trigger_price + TICK_SIZE)

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
                order_logger.info(f"✅ SL-Limit order placed successfully | ID={sl_order_id} | Trigger={sl_price:.2f} | Limit={limit_price:.2f}")
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

    def place_entry_order(self, signal, intraday_df):
        """
        Enhanced entry order placement with:
        1. Stop Loss Automation (25% rule)
        2. Option symbol resolution and SL/TP initialization

        BUGFIX: ensure self.exit_in_progress is cleared after SL/TP initialization
        so that on_ltp_update() will begin monitoring the newly opened position.
        """
        try:
            order_logger.info("=== PLACING ENTRY ORDER ===")
            order_logger.info(f"Signal: {signal}")

            if self.trade_count >= MAX_TRADES_PER_DAY:
                order_logger.warning(f"Daily trade limit reached ({self.trade_count}/{MAX_TRADES_PER_DAY})")
                logger.info("Daily trade limit reached (%d), will shutdown.", MAX_TRADES_PER_DAY)
                threading.Thread(target=self.shutdown_gracefully, daemon=True).start()
                return False

            order_logger.info(f"Trade Count: {self.trade_count +1}/{MAX_TRADES_PER_DAY}")
            current_ltp = self.ltp if self.ltp else 0.0
            order_logger.info(f"Current {SYMBOL} LTP: {current_ltp:.2f}")

            tradable_symbol = SYMBOL
            tradable_exchange = EXCHANGE
            opt_type = None
            strike = None

            if OPTION_ENABLED:
                order_logger.info("=== OPTION SYMBOL RESOLUTION ===")
                opt_type = "CE" if signal == "BUY" else "PE"
                try:
                    spot_ltp = current_ltp
                    order_logger.info(f"Fetched Spot LTP: {spot_ltp:.2f}")
                except Exception:
                    spot_ltp = self.ltp or 0.0
                    order_logger.warning(f"Failed to fetch spot LTP, using cached: {spot_ltp:.2f}")
                strike = self.get_option_strike(OPTION_STRIKE_SELECTION, spot_ltp, opt_type)
                order_logger.info(f"Resolved Strike: {strike} (selection: {OPTION_STRIKE_SELECTION})")
                expiry = self.get_nearest_weekly_expiry()
                order_logger.info(f"Selected Expiry: {expiry}")
                opt_symbol = self.get_option_symbol_via_search_api(expiry, strike, opt_type)
                if not opt_symbol:
                    order_logger.error(f"Failed to resolve option symbol: expiry={expiry}, strike={strike}, type={opt_type}")
                    logger.warning("Could not resolve option symbol for expiry=%s strike=%s type=%s", expiry, strike, opt_type)
                    return False
                tradable_symbol = opt_symbol
                tradable_exchange = OPTION_EXCHANGE
                self.option_symbol = opt_symbol
                order_logger.info(f"Final Option Symbol: {tradable_symbol}")
                order_logger.info(f"Option Exchange: {tradable_exchange}")
                logger.info("Placing %s option order -> %s (%s) strike=%s", opt_type, tradable_symbol, tradable_exchange, strike)
            
            # resp = self.client.placeorder(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
            #                           action=signal, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)
            # order_logger.info(f"Order placed successfully. dynamic SL/TP calculation started {resp}")
            entry_order_id, exec_price = self.place_order_with_execution_retry(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
                                      action="BUY", quantity=QUANTITY, position_size = QUANTITY, price_type="MARKET", product=PRODUCT)

            # resp = self.client.placeorder(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
            #                           action=signal, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            if entry_order_id is None or exec_price is None:
                order_logger.error(f"Entry order failed")
                return False
            order_logger.info(f"Order placed successfully. dynamic SL/TP calculation started")
            self.trade_start_time = get_now()
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
                        self.subscribe_option_ltp()
                        risk_logger.info(f"SL/TP tracking via SPOT | Spot Entry = {base_price:.2f}")
                    except Exception:
                        self.spot_entry_price = self.ltp or self.entry_price
                        base_price = self.spot_entry_price
                        risk_logger.warning(f"Failed to fetch spot for SL/TP, using cached: {base_price:.2f}")
                elif USE_OPTION_FOR_SLTP:
                    # option entry price will be set after execution retrieval; temporarily keep base_price
                    risk_logger.info("SL/TP tracking via OPTION; option exec price to be recorded after execution")

                # Initialize SL/TP using configured method
                if SL_TP_METHOD == "Signal_candle_range_SL_TP":
                    self.initialize_signal_candle_sl_tp(signal, intraday_df, base_price=base_price)
                elif SL_TP_METHOD == "CPR_range_SL_TP":
                    if CPR_EXCLUDE_PIVOTS or USE_DYNAMIC_TARGET:
                        risk_logger.info("Using Enhanced CPR method with exclusions/dynamic targeting")
                        cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal, intraday_df, base_price=base_price)
                    if not cpr_success:
                        risk_logger.warning("CPR initialization failed, falling back to Signal Candle method")
                        self.initialize_signal_candle_sl_tp(signal, intraday_df, base_price=base_price)
                else:
                    # fallback: fixed TP/SL
                    if signal == "BUY":
                        self.stoploss_price = round(base_price - STOPLOSS, 2)
                        self.target_price = round(base_price + TARGET, 2)
                    else:
                        self.stoploss_price = round(base_price + STOPLOSS, 2)
                        self.target_price = round(base_price - TARGET, 2)

                # finalize basic state vars
                self.trade_count += 1
                self.entry_price = round(base_price, 2)

            if exec_price is not None:
                exec_price = float(exec_price)
                #self.entry_price = exec_price
                self.option_entry_price = exec_price
            
            # --- 🧩 PLACE STOP LOSS ORDER after confirmed entry ---
            if SL_ORDER_ENABLED:
                try:
                    # Determine which price to use for SL placement
                    exit_symbol = tradable_symbol
                    exit_exchange = tradable_exchange
                    entry_side = signal.upper()

                    # Use existing computed stoploss_price
                    sl_price = self.compute_25pct_sl(exec_price, entry_side)

                    order_logger.info(f"Attempting to place linked SL order after entry confirmation...")
                    sl_order_id = self.place_and_confirm_stoploss(exit_symbol, exit_exchange, entry_side, sl_price)

                    if sl_order_id:
                        self.sl_order_id = sl_order_id
                        order_logger.info(f"✅ Stop Loss Order Placed Successfully | ID={sl_order_id} | SL={sl_price:.2f}")
                    else:
                        order_logger.warning("⚠️ Stop Loss order placement failed after entry confirmation.")
                except Exception:
                    order_logger.exception("Error placing SL order after entry execution")
            else:
                order_logger.exception("SL order not enabled")
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

            logger.info("Entry executed: %s @ %.2f | SL @ %.2f | trade_count=%d",
                    self.position, self.entry_price, self.stoploss_price, self.trade_count)
            return True

        except Exception:
            logger.exception("place_entry_order failed")
            order_logger.error("Failed to place enhanced entry order", exc_info=True)
            # ensure flag cleared on error so bot can continue
            with self._state_lock:
                self.exit_in_progress = False
            return False

    def subscribe_option_ltp(self):
        """Subscribe dynamically to option LTP once order is placed."""
        if not getattr(self, "option_symbol", None):
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
        except Exception as e:
            signal_logger.warning(f"⚠️ Failed to subscribe option symbol: {e}")


    def place_exit_order(self, reason="Manual"):
        try:
            order_logger.info("=== PLACING EXIT ORDER ===")
            order_logger.info(f"Exit Reason: {reason}")
            
            if not self.position:
                order_logger.warning("No position to exit")
                self.exit_in_progress = False
                return False
            if SIM_MANAGER.active:
                SIM_MANAGER.pause_ticks = True
                
            # Get current LTP and P&L for logging
            spot_ltp = self.ltp if self.ltp else 0.0
            option_ltp = self.option_ltp if self.ltp else 0.0
            
            # Calculate P&L before exit
            if self.position == "BUY":
                unrealized_pnl = (option_ltp - self.option_entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.option_entry_price - option_ltp) * QUANTITY
                
            # Calculate trade duration
            trade_start = getattr(self, 'trade_start_time', get_now())
            trade_end_time = get_now()
            trade_duration = trade_end_time - trade_start
            
            order_logger.info(f"Trade Spot Position: {self.position} @ {self.spot_entry_price:.2f}")
            order_logger.info(f"Current Spot LTP: {spot_ltp:.2f}")
            order_logger.info(f"Trade Option Position: {self.position} @ {self.option_entry_price:.2f}")
            order_logger.info(f"Current Option LTP: {option_ltp:.2f}")
            order_logger.info(f"Unrealized P&L: {unrealized_pnl:.2f}")
            order_logger.info(f"Trade Duration: {trade_duration}")
            
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
            if not SIM_MANAGER.active:
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

                if exit_qty == 0:
                    order_logger.warning(f"❌ No net quantity found to exit for {exit_symbol}")
                    self.exit_in_progress = False
                    return False

                order_logger.info(f"Exit Symbol: {exit_symbol}")
                order_logger.info(f"Exit Exchange: {exit_exchange}")
                order_logger.info(f"Exit Action: {action}")
                order_logger.info(f"Exit Quantity: {exit_qty}")
            else:
                exit_qty = QUANTITY
                
            # # Place exit order
            # resp = self.client.placeorder(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
            #                               action=action, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            # exit_order_id = resp["orderid"]
            # exit_price = self.get_executed_price(exit_order_id)
            # ------------------ Cancel all pending orders for this symbol in real time ------------------    

            exit_order_id, exit_price = self.place_order_with_execution_retry(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
                                        action="SELL", quantity=exit_qty, position_size=0, price_type="MARKET", product=PRODUCT)          

            if (exit_order_id is not None or exit_price is not None):
                order_logger.info(f"Exit Order Success: {exit_order_id}")
                try:
                    if not SIM_MANAGER.active:
                        client.unsubscribe_ltp([
                        {"exchange": OPTION_EXCHANGE, "symbol": exit_symbol}])
                    else:
                        order_logger.error(f"Option LTP not required to unsubscribe in SIM mode")
                except Exception as e:
                    order_logger.error(f"Unable to unsubscribe option ltp")
                    pass
                
                
                if self.option_ltp is None:
                    self.option_ltp = self.option_entry_price

                if exit_price:
                    # Calculate actual P&L
                    if self.position == "BUY":
                        realized_pnl = (self.option_ltp - self.option_entry_price) * QUANTITY
                    else:
                        realized_pnl = (self.option_entry_price - self.option_ltp) * QUANTITY
                    
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
            
            self.position = None
            self.entry_price = 0.0
            self.stoploss_price = 0.0
            self.target_price = 0.0
            self.exit_in_progress = False
            self.option_symbol = None
            self.spot_entry_price = 0.0
            self.option_entry_price = 0.0
            self.option_stop_loss = 0.0
            
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
            self.gk_resistance_pivot = None
            self.gk_support_pivot = None

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
            if SIM_MANAGER.active:
                SIM_MANAGER.pause_ticks = False

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
            if SIM_MANAGER.active:
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

    def cancel_pending_sl_orders(self, exit_symbol):
        """Cancel any pending Stop Loss (SL) orders for the specified symbol.
        Safely handles API responses and logs all actions."""
        try:
            base_client = client
            orders_resp = base_client.orderbook()

            if isinstance(orders_resp, dict) and orders_resp.get("status") == "success":
                orders_data = orders_resp.get("data", [])
            if len(orders_data) == 0:
                print("No existing SL orders for this symbol exit_symbol")
                return     # 🔍 Find pending SL orders for this symbol
                sl_orders = [
                    o for o in orders_data
                    if (
                        o.get("symbol", "") == exit_symbol and 
                        o.get("status", "").upper() in ["PENDING", "OPEN"] and 
                        o.get("pricetype", "").upper().startswith("SL")
                    )
                ]
                if sl_orders:
                    order_logger.info(f"Found {len(sl_orders)} pending SL order(s) for {exit_symbol}")

                    for sl in sl_orders:
                        try:
                            order_id = sl.get("orderid", "")
                            cancel_resp = base_client.cancelorder(
                                order_id=order_id, 
                                strategy=STRATEGY
                            )

                            if isinstance(cancel_resp, dict) and cancel_resp.get("status") == "success":
                                order_logger.info(f"✅ Cancelled SL order {order_id} for {exit_symbol}")
                            else:
                                order_logger.warning(f"⚠️ SL cancel failed for {order_id}: {cancel_resp}")

                        except Exception as e:
                            order_logger.exception(
                                f"Error cancelling SL order {sl.get('orderid', 'N/A')} for {exit_symbol}: {e}"
                            )
                else:
                    order_logger.info(f"No pending SL orders found for {exit_symbol}")
            else:
                order_logger.warning(f"Failed to fetch orders for SL cleanup: {orders_resp}")

        except Exception as e:
            order_logger.exception(f"Error during SL cleanup for {exit_symbol}: {e}")
    # -------------------------
    # Graceful shutdown
    # -------------------------
    def shutdown_gracefully(self):
        """
        Immediate, safe shutdown for both SIMULATION and LIVE modes.
        Ensures we don't attempt to join the current thread and that
        replay/websocket/scheduler are signalled to stop before joining.
        """
        main_logger.info("=== GRACEFUL SHUTDOWN INITIATED ===")
        logger.info("Graceful shutdown started")

        # Log final system status (safe guarded)
        try:
            main_logger.info(f"Final Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
            main_logger.info(f"Current Position: {self.position if self.position else 'None'}")

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
            # Ensure SIM_MANAGER is marked inactive so replay loops can detect it
            try:
                SIM_MANAGER.active = False
            except Exception:
                # if SIM_MANAGER not present or any error, continue
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
            main_logger.info("=== GRACEFUL SHUTDOWN COMPLETED — BOT STOPPED ===")
            main_logger.info("Bot session ended")
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
            if SIM_MANAGER.active:
                SIM_MANAGER.active = False
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
            if SIM_MANAGER.active:
                SIM_MANAGER.active = False
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
        main_logger.info("=== STARTING MYALGO TRADING SESSION ===")
        session_start_time = datetime.now()
        main_logger.info(f"Session Start Time: {session_start_time}")
        
        logger.info("Starting bot")
        
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
            SIM_MANAGER.start(SIMULATION_DATE)
            main_logger.info(f"Simulation Mode Activated for {SIMULATION_DATE}")
        except Exception:
            main_logger.exception("Simulation startup failed; fallback to LIVE mode")
    bot = MYALGO_TRADING_BOT()
    bot.run()
