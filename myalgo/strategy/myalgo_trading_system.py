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
from sqlalchemy import Boolean
import logging
from openalgo import api
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date, time as dt_time
import time
import threading
import sqlite3
from apscheduler.schedulers.background import BackgroundScheduler  # used for backtesting/local
from apscheduler.schedulers.blocking import BlockingScheduler 
from apscheduler.executors.pool import ThreadPoolExecutor       # used for live on Linux (no extra thread)
from apscheduler.triggers.cron import CronTrigger
# Directly call the helper from MarketDataDispatcher
# from market_data_dispatcher import MarketDataDispatcher
import pytz
import re
import sys
from dataclasses import dataclass
import os
from openalgo import ta
from typing import Dict, List, Optional, Tuple
import io
import time
import platform
import requests
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ==============================================================
# MULTI-STRATEGY CONFIG INJECTION
# --------------------------------------------------------------
# apply_config(cfg) is the ONLY place where injectable params
# are mapped. All keys are optional — omitted keys keep the
# engine's hardcoded defaults below unchanged.
#
# run(config) is the public entry point called by strategy files.
# Calling it with no arguments is identical to the old __main__.
# ==============================================================
import json as _json

def apply_config(cfg: dict) -> None:
    """
    Inject a strategy CONFIG dict into engine globals.
    Must be called BEFORE any engine logic runs (handled by run()).
    Every parameter is listed here — this block is the documentation.
    """
    global MODE, LOGGING_MODE, STRATETGY_MODE
    global SYMBOL, EXCHANGE, PRODUCT
    global CANDLE_TIMEFRAME, LOOKBACK_DAYS, SIGNAL_CHECK_INTERVAL
    global START_DATE, END_DATE, SIMULATION_DATE, CLEAR_TRADE_TABLE
    global MAX_TRADES_PER_DAY, ENTRY_CONFIRM_SECONDS, EXIT_CONFIRM_SECONDS
    global ENABLE_SECONDS_CONFIRMATION_LIVE, DAY_HIGH_LOW_VALIDATION_FROM_TRADE
    global EXPIRY_DAY_HL_BREAKOUT, TWOCANDLEBREAKOUT
    global SL_PERCENT, RISK_REWARD_RATIO, MIN_SL_POINTS, MAX_RISK_POINTS
    global MAX_SIGNAL_RANGE, MIN_TP_SEPARATION_R, MIN_REWARD_PCT_OF_RISK
    global MIN_REWARD_FOR_SL_MOVE, MIN_ABSOLUTE_REWARD
    global SL_BUFFER_PCT, SL_BUFFER_POINT, USE_DYNAMIC_TARGET, DYNAMIC_TARGET_METHOD
    global RSI_STRATEGY, RSI_LEVELS, RSI_LOOKBACK
    global GAP_FILTER_ENABLED, GAP_THRESHOLD_PERCENT
    global CPR_MIDPOINTS_ENABLED, ENABLE_LTP_PIVOT_GATE, CANDLE_BREAKOUT_1M_ENABLED
    global TSL_ENABLED, SL_TP_METHOD
    global ENTRY_LTP_BREAKOUT_ENABLED, ENTRY_BREAKOUT_MODE
    global EXIT_LTP_BREAKOUT_ENABLED, EXIT_LTP_BREAKOUT_MODE
    global EXIT_BREAKOUT_CONFIRM_CANDLES, ENTRY_BREAKOUT_CONFIRM_CANDLES
    global REVERSAL_MODE, LTP_BREAKOUT_INTERVAL_MIN
    global OPTION_ENABLED, OPTION_EXCHANGE, STRIKE_INTERVAL
    global OPTION_EXPIRY_TYPE, OPTION_STRIKE_SELECTION, EXPIRY_LOOKAHEAD_DAYS
    global LOT_QUANTITY, LOT, QUANTITY, SL_ORDER_ENABLED
    global USE_SPOT_FOR_SLTP, USE_OPTION_FOR_SLTP
    global PIVOT_TOUCH_BUFFER_PTS, PIVOT_TOUCH_BUFFER_PCT
    global STRATEGY

    # ── shorthand section refs ─────────────────────────────────
    i   = cfg.get("instrument",        {})
    m   = cfg.get("mode",              {})
    tf  = cfg.get("timeframe",         {})
    r   = cfg.get("risk",              {})
    tm  = cfg.get("trade_management",  {})
    st  = cfg.get("strategy",          {})
    br  = cfg.get("breakout",          {})
    opt = cfg.get("options",           {})
    pv  = cfg.get("pivot",             {})

    # ── identity ──────────────────────────────────────────────
    _name = cfg.get("strategy_name")
    if _name:
        STRATEGY = f"{_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.environ["STRATEGY"] = STRATEGY

    # ── instrument ────────────────────────────────────────────
    SYMBOL   = i.get("symbol",   SYMBOL)
    EXCHANGE = i.get("exchange", EXCHANGE)
    PRODUCT  = i.get("product",  PRODUCT)

    # ── mode ──────────────────────────────────────────────────
    MODE            = m.get("run_mode",       MODE)
    LOGGING_MODE    = m.get("logging_mode",   LOGGING_MODE)
    STRATETGY_MODE  = m.get("strategy_mode",  STRATETGY_MODE)
    START_DATE      = m.get("start_date",     START_DATE)
    END_DATE        = m.get("end_date",       END_DATE)
    SIMULATION_DATE = m.get("simulation_date",SIMULATION_DATE)
    CLEAR_TRADE_TABLE = m.get("clear_trade_table", CLEAR_TRADE_TABLE)
    TWOCANDLEBREAKOUT = m.get("twocandlebreakout", TWOCANDLEBREAKOUT)

    # ── timeframe ─────────────────────────────────────────────
    CANDLE_TIMEFRAME      = tf.get("candle",                CANDLE_TIMEFRAME)
    LOOKBACK_DAYS         = tf.get("lookback_days",         LOOKBACK_DAYS)
    SIGNAL_CHECK_INTERVAL = tf.get("signal_check_interval", SIGNAL_CHECK_INTERVAL)

    # ── risk ──────────────────────────────────────────────────
    SL_PERCENT             = float(r.get("sl_percent",            SL_PERCENT))
    RISK_REWARD_RATIO      = float(r.get("risk_reward_ratio",     RISK_REWARD_RATIO))
    MIN_SL_POINTS          = r.get("min_sl_points",          MIN_SL_POINTS)
    MAX_RISK_POINTS        = r.get("max_risk_points",        MAX_RISK_POINTS)
    MAX_SIGNAL_RANGE       = r.get("max_signal_range",       MAX_SIGNAL_RANGE)
    MIN_TP_SEPARATION_R    = r.get("min_tp_separation_r",    MIN_TP_SEPARATION_R)
    MIN_REWARD_PCT_OF_RISK = r.get("min_reward_pct_of_risk", MIN_REWARD_PCT_OF_RISK)
    MIN_REWARD_FOR_SL_MOVE = r.get("min_reward_for_sl_move", MIN_REWARD_FOR_SL_MOVE)
    MIN_ABSOLUTE_REWARD    = r.get("min_absolute_reward",    MIN_ABSOLUTE_REWARD)
    SL_BUFFER_PCT          = float(r.get("sl_buffer_pct",        SL_BUFFER_PCT))
    SL_BUFFER_POINT        = r.get("sl_buffer_point",        SL_BUFFER_POINT)
    USE_DYNAMIC_TARGET     = r.get("use_dynamic_target",     USE_DYNAMIC_TARGET)
    DYNAMIC_TARGET_METHOD  = r.get("dynamic_target_method",  DYNAMIC_TARGET_METHOD)

    # ── trade management ──────────────────────────────────────
    MAX_TRADES_PER_DAY                 = tm.get("max_trades_per_day",                MAX_TRADES_PER_DAY)
    ENTRY_CONFIRM_SECONDS              = tm.get("entry_confirm_seconds",             ENTRY_CONFIRM_SECONDS)
    EXIT_CONFIRM_SECONDS               = tm.get("exit_confirm_seconds",              EXIT_CONFIRM_SECONDS)
    ENABLE_SECONDS_CONFIRMATION_LIVE   = tm.get("enable_seconds_confirmation_live",  ENABLE_SECONDS_CONFIRMATION_LIVE)
    DAY_HIGH_LOW_VALIDATION_FROM_TRADE = tm.get("day_high_low_validation_from_trade",DAY_HIGH_LOW_VALIDATION_FROM_TRADE)
    EXPIRY_DAY_HL_BREAKOUT             = tm.get("expiry_day_hl_breakout",            EXPIRY_DAY_HL_BREAKOUT)

    # ── strategy toggles ──────────────────────────────────────
    RSI_STRATEGY              = st.get("rsi_enabled",               RSI_STRATEGY)
    RSI_LEVELS                = st.get("rsi_levels",                RSI_LEVELS)
    RSI_LOOKBACK              = st.get("rsi_lookback",              RSI_LOOKBACK)
    GAP_FILTER_ENABLED        = st.get("gap_filter_enabled",        GAP_FILTER_ENABLED)
    GAP_THRESHOLD_PERCENT     = st.get("gap_threshold_percent",     GAP_THRESHOLD_PERCENT)
    CPR_MIDPOINTS_ENABLED     = st.get("cpr_midpoints_enabled",     CPR_MIDPOINTS_ENABLED)
    ENABLE_LTP_PIVOT_GATE     = st.get("enable_ltp_pivot_gate",     ENABLE_LTP_PIVOT_GATE)
    CANDLE_BREAKOUT_1M_ENABLED= st.get("candle_breakout_1m_enabled",CANDLE_BREAKOUT_1M_ENABLED)
    TSL_ENABLED               = st.get("tsl_enabled",               TSL_ENABLED)
    SL_TP_METHOD              = st.get("sl_tp_method",              SL_TP_METHOD)

    # ── breakout ──────────────────────────────────────────────
    ENTRY_LTP_BREAKOUT_ENABLED     = br.get("entry_ltp_breakout_enabled",     ENTRY_LTP_BREAKOUT_ENABLED)
    ENTRY_BREAKOUT_MODE            = br.get("entry_breakout_mode",            ENTRY_BREAKOUT_MODE)
    EXIT_LTP_BREAKOUT_ENABLED      = br.get("exit_ltp_breakout_enabled",      EXIT_LTP_BREAKOUT_ENABLED)
    EXIT_LTP_BREAKOUT_MODE         = br.get("exit_ltp_breakout_mode",         EXIT_LTP_BREAKOUT_MODE)
    EXIT_BREAKOUT_CONFIRM_CANDLES  = br.get("exit_breakout_confirm_candles",  EXIT_BREAKOUT_CONFIRM_CANDLES)
    ENTRY_BREAKOUT_CONFIRM_CANDLES = br.get("entry_breakout_confirm_candles", ENTRY_BREAKOUT_CONFIRM_CANDLES)
    REVERSAL_MODE                  = br.get("reversal_mode",                  REVERSAL_MODE)
    LTP_BREAKOUT_INTERVAL_MIN      = br.get("ltp_breakout_interval_min",      LTP_BREAKOUT_INTERVAL_MIN)

    # ── options ───────────────────────────────────────────────
    OPTION_ENABLED          = opt.get("enabled",               OPTION_ENABLED)
    OPTION_EXCHANGE         = opt.get("exchange",              OPTION_EXCHANGE)
    STRIKE_INTERVAL         = opt.get("strike_interval",       STRIKE_INTERVAL)
    OPTION_EXPIRY_TYPE      = opt.get("expiry_type",           OPTION_EXPIRY_TYPE)
    OPTION_STRIKE_SELECTION = opt.get("strike_selection",      OPTION_STRIKE_SELECTION)
    EXPIRY_LOOKAHEAD_DAYS   = opt.get("expiry_lookahead_days", EXPIRY_LOOKAHEAD_DAYS)
    LOT_QUANTITY            = opt.get("lot_quantity",          LOT_QUANTITY)
    LOT                     = opt.get("lot",                   LOT)
    SL_ORDER_ENABLED        = opt.get("sl_order_enabled",      SL_ORDER_ENABLED)
    USE_SPOT_FOR_SLTP       = opt.get("use_spot_for_sltp",     USE_SPOT_FOR_SLTP)
    USE_OPTION_FOR_SLTP     = opt.get("use_option_for_sltp",   USE_OPTION_FOR_SLTP)
    QUANTITY = LOT * LOT_QUANTITY  # always recompute after either changes

    # ── pivot ─────────────────────────────────────────────────
    PIVOT_TOUCH_BUFFER_PTS = pv.get("touch_buffer_pts", PIVOT_TOUCH_BUFFER_PTS)
    PIVOT_TOUCH_BUFFER_PCT = pv.get("touch_buffer_pct", PIVOT_TOUCH_BUFFER_PCT)

    # ── sync env flags (logging setup already ran at import,
    #    but we update so any re-import or test run is correct) ─
    if MODE in ("BACKTESTING", "BACKTESTING_RANGE"):
        os.environ["BACKTEST_MODE"] = "true"
        os.environ["FAST_BACKTEST"] = "true" if LOGGING_MODE == "fast" else "false"
    else:
        os.environ["BACKTEST_MODE"] = "false"
        os.environ["FAST_BACKTEST"] = "false"


def run(config: dict = None) -> None:
    """
    Public entry point — called by strategy files.
    With no arguments behaves identically to running the script directly.

    Usage from a strategy file:
        import myalgo_trading_system as engine
        engine.run(config=CONFIG)
    """
    if config:
        apply_config(config)
    _run_trading_engine()


# ==============================================================  
# MODE CONFIGURATION
# ==============================================================
# Available modes:
# - "LIVE": Real-time trading with WebSocket
# - "BACKTESTING": Single-day simulation
# - "BACKTESTING_RANGE": Multi-day simulation (NEW)
MODE = "BACKTESTING_RANGE"  # Change this to switch modes # Example: LIVE or BACKTESTING or BACKTESTING_RANGE
LOGGING_MODE = "fast"  # ← CHANGE THIS ONE VARIABLE TO SWITCH LOGGING DETAIL LEVEL IN BACKTESTING MODES
# ─────────────────────────────────────────────────────────────
# BACKTESTING_RANGE Configuration (NEW)
# ─────────────────────────────────────────────────────────────
STRATETGY_MODE = "SCALPING"  # "SCALPING" or "LIVE_TESTING" (can be used in strategy logic to differentiate behavior)
CLEAR_TRADE_TABLE = True                       # Clear trade logs table before backtesting (if True, only in BACKTESTING modes)  
CANDLE_BREAKOUT_1M_ENABLED = True               # Clear trade logs table before backtesting (if True, only in BACKTESTING modes)
ENABLE_LTP_PIVOT_GATE = True                    # master switch for this gatekeeper
TWOCANDLEBREAKOUT = False                       # If True, require previous candle bullish/bearish + current close breakout above/below prev high/low
RSI_STRATEGY = False # Set to True to enable RSI-based entry conditions in strategy_job()
RSI_LEVELS = [50]   
RSI_LOOKBACK = 5  # Lookback candles for RSI V setup validation
START_DATE: Optional[str] = "01-01-2024"  # DD-MM-YYYY format
END_DATE: Optional[str] = "25-03-2026"    # DD-MM-YYYY format
now = datetime.now()
SYMBOL = "NIFTY"
STRATEGY = "Scalping_LIVE_v1_Dsabled_Broker_SL" + now.strftime("%Y%m%d_%H%M%S")  # Strategy name for logging and tracking
CPR_MIDPOINTS_ENABLED = True # Enable midpoint levels for target consideration (CPR method)
# === Gap Filter Configuration ===
GAP_FILTER_ENABLED = True           # If True, gap-up/gap-down days require 15-min candle breakout before entry
GAP_THRESHOLD_PERCENT = 0.3         # Minimum gap % to classify a day as gap_up or gap_down (e.g. 1.0 = 1%)
# Risk settings
EXPIRY_DAY_HL_BREAKOUT = True     # If True, on expiry day entry requires breakout of previous day high/low (more conservative)
STOPLOSS = 0                      #Stoploss
TARGET = 0                        #Target
MAX_SIGNAL_RANGE = 30             #Maximum signal range
MIN_SL_POINTS = 10                #Absolute minimum SL points
MAX_RISK_POINTS = 30              #Absolute safety on option premium
SL_PERCENT = 0.25                 #12% of option premium
MIN_TP_SEPARATION_R = 0.5         #Minimum TP separation ratio is to avoid closer pivot with previous target                    
MIN_REWARD_PCT_OF_RISK = 0.5      #At least 50% reward of risk
MIN_REWARD_FOR_SL_MOVE = 0.5      #TSL Move only if reward is at least 50% of initial target 
MIN_ABSOLUTE_REWARD = 15          #Minimum absolute reward in points
ENTRY_CONFIRM_SECONDS = 0         #Seconds to confirm entry
EXIT_CONFIRM_SECONDS = 0          #Seconds to confirm exit    
ENABLE_SECONDS_CONFIRMATION_LIVE = False     # For live mode, require confirmation seconds for entry
SL_BUFFER_PCT = 0.25              #20% buffer from premium automatic SL placement in broker
SL_BUFFER_POINT = 5    
DAY_HIGH_LOW_BREAKOUT_START_TIME = "15:00"        #Buffer in points from premium in CPR SL placement
# Backtesting confirmation candles for exit LTP breakout (set to 1 for immediate confirmation)
EXIT_BREAKOUT_CONFIRM_CANDLES = 0  # Backtesting default
# Backtesting confirmation candles for entry LTP breakout (set to 1 for immediate confirmation)
ENTRY_BREAKOUT_CONFIRM_CANDLES = 0  # Backtesting default, can be higher to simulate more conservative entry
# Trade management
MAX_TRADES_PER_DAY = 5            #Maximum trades per day
DAY_HIGH_LOW_VALIDATION_FROM_TRADE = 2  
# "SPOT" or "OPTIONS" or "SPOT_OPTIONS" (For exit breakout validation)
EXIT_LTP_BREAKOUT_MODE = "SPOT"    
EXIT_LTP_BREAKOUT_ENABLED = True    # Enable separate exit breakout confirmation
REVERSAL_MODE = "NONE"      # "SPOT" | "OPTIONS" | "SPOT_OPTIONS" | "NONE" (For reversal candle exit)
# ============================================================================
# 🎛️ LOGGING MODE CONFIGURATION
# ============================================================================
# Change LOGGING_MODE to switch between modes:
#   "fast"   → Only warnings/errors (fastest, production)
#   "normal" → INFO + warnings (development, debugging)
#   "debug"  → Everything (deep troubleshooting)
# ============================================================================
# ============================================================================

# Auto-configure logging based on LOGGING_MODE
if MODE in ["BACKTESTING", "BACKTESTING_RANGE"]:
    if LOGGING_MODE == "fast":
        os.environ['BACKTEST_MODE'] = 'true'
        os.environ['FAST_BACKTEST'] = 'true'
        print("🚀 FAST MODE: Minimal logs | Speed: ⚡⚡⚡")
        
    elif LOGGING_MODE == "normal":
        os.environ['BACKTEST_MODE'] = 'true'
        os.environ['FAST_BACKTEST'] = 'false'
        print("📊 NORMAL MODE: INFO logs | Speed: ⚡⚡")
        
    elif LOGGING_MODE == "debug":
        os.environ['BACKTEST_MODE'] = 'false'
        os.environ['FAST_BACKTEST'] = 'false'
        print("🔍 DEBUG MODE: All logs | Speed: ⚡")
    else:
        print("⚠️  Unknown LOGGING_MODE. Using FAST mode.")
        os.environ['BACKTEST_MODE'] = 'true'
        os.environ['FAST_BACKTEST'] = 'true'
        
elif MODE == "LIVE":
    os.environ['BACKTEST_MODE'] = 'false'
    os.environ['FAST_BACKTEST'] = 'false'
    print("🔴 LIVE MODE: Full logging enabled")

print(f"🔧 Environment: BACKTEST_MODE={os.getenv('BACKTEST_MODE')}, FAST_BACKTEST={os.getenv('FAST_BACKTEST')}")

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# Ensure strategy name is available to the logger before it initialises
os.environ.setdefault('STRATEGY', STRATEGY)
from logger import get_logger, log_trade_execution, set_strategy_name
from db.expiry_list import EXPIRY_LIST_DDMMYYYY
set_strategy_name(STRATEGY)
# Try importing talib

TALIB_AVAILABLE = False
    
# ----------------------------
# Global Constants
os_name = platform.system()
MARKET_OPEN  = dt_time(9, 15)   # 09:15 AM Market open time
MARKET_CLOSE = dt_time(15, 19)  # 03:19 PM Market close time
ENTRY_CUTOFF_TIME = dt_time(15, 00) # 03:00 PM No new entry orders after this time
EXPIRY_CUTOFF_TIME = dt_time(15, 00) # 02:45 PM on expiry day for more conservative exit conditions
IB_CUTOFF_TIME = dt_time(10, 20) # 10:20 AM - use IB high/low for the day instead of actual high/low for entry conditions to simulate more conservative behavior (can be set to market open time for strictest)
TRADE_START_TIME = dt_time(9, 25) # 09:25 AM - start looking for trades from this time (allows for initial volatility to settle)
TSL_CUTOFF_TIME = dt_time(14, 45) # 02:45 PM - stop trailing SL after this time to avoid end-of-day volatilit
# dispatcher = MarketDataDispatcher()

# ─────────────────────────────────────────────────────────────
# BACKTESTING (Single Day) Configuration (EXISTING)
# ─────────────────────────────────────────────────────────────
SIMULATION_DATE: Optional[str] = "13-12-2025"  # DD-MM-YYYY or None       
# ----------------------------
# Configuration
# ----------------------------
logging.info(f"🔧 Setting up API client based on OS {os_name}")
if os_name.upper == "OS LINUX" or os_name.upper() == "LINUX":
    logging.info("💻 Running on Linux - using production API settings")
    API_KEY = "f5d15a5720a8f036b0a24958f354c61b6f73db81acb4e640ca9884bead2eb684"
    API_HOST = "https://v.vralgo.com"
    WS_URL = "wss://v.vralgo.com/ws"
else:
    logging.info("🖥️ Running on Windows - using local API settings")
    API_KEY = "f5d15a5720a8f036b0a24958f354c61b6f73db81acb4e640ca9884bead2eb684"
    API_HOST = "https://v.vralgo.com/"
    WS_URL = "wss://v.vralgo.com/ws"

# API_KEY = "7773b590c743c9184fb1bb74830091a379f88c2035a2203e2b40d24cb2f86711"
# API_HOST = "http://127.0.0.1:5000"
# WS_URL = "ws://127.0.0.1:8765"
# ==============================================================  
try:
    if MODE == "LIVE":
        logging.info("🔌 Initializing OpenAlgo API client")
        client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL) # Initialize openalgo API client
        logging.info("✅ 🚀 OpenAlgo API client initialized successfully")
    elif MODE in ["BACKTESTING", "BACKTESTING_RANGE"]:
        logging.info("🔌 Backtesting mode - OPENALGO API client not required")
        client = None
except Exception as e:
    logging.error(f"❌ 🚨 Failed to initialize OpenAlgo API client: {e}")
    sys.exit(1)

# ==============================================================
# Telegram Alert Helper
# ==============================================================
def send_telegram_alert(message: str, username: str = "valgo_live", priority: int = 5) -> dict:
    """
    Send a Telegram alert via the OpenAlgo notify endpoint.

    Args:
        message  : Alert text to send.
        username : OpenAlgo username to notify (default: valgo_live).
        priority : Alert priority 1-10 (default: 5).

    Returns:
        dict with the API response, or an error dict on failure.
    """
    try:
        url = f"{API_HOST.rstrip('/')}/api/v1/telegram/notify"
        payload = {
            "apikey": API_KEY,
            "username": username,
            "message": message,
            "priority": priority,
        }
        response = requests.post(url, json=payload, timeout=10)
        result = response.json()
        logging.info(f"📨 Telegram alert sent | status={result}")
        return result
    except Exception as e:
        logging.error(f"❌ Telegram alert failed: {e}")
        return {"status": "error", "message": str(e)}

# ==============================================================   
# Instrument
SYMBOL = "NIFTY"                                  #Symbol
EXCHANGE = "NSE_INDEX"                            #Exchange                     
PRODUCT = "MIS"                                   #Product
CANDLE_TIMEFRAME = "5m"                           #Candle timeframe
LOOKBACK_DAYS = 10                                 #Lookback days (3 days = ~225 5m candles, enough for EMA100)
SIGNAL_CHECK_INTERVAL = 5                         #minutes (use integer minutes)
                        #Minimum TP separation ratio is to avoid closer pivot with previous target
# ===============================================================
# 🔧 Dynamic LTP Breakout Configuration
# ===============================================================

# --- ENTRY BREAKOUT CONFIG ---
LTP_BREAKOUT_ENABLED = True                         # [LEGACY - transitioning to ENTRY_LTP_BREAKOUT_ENABLED]
ENTRY_LTP_BREAKOUT_ENABLED = True                   # Enable separate entry breakout confirmation
ENTRY_BREAKOUT_MODE = "SPOT"                        # "SPOT" | "OPTIONS" | "SPOT_OPTIONS"
LTP_BREAKOUT_INTERVAL_MIN = 5                       # minutes to wait for LTP breakout confirmation

# --- EXIT BREAKOUT CONFIG ---

# --- 1M CANDLE BREAKOUT CONFIG ---   # If True, use 1m candle close instead of LTP for entry/exit breakout validation
                                    # If False, existing LTP-based breakout logic is used unchanged

# Indicators to compute
EMA_PERIODS = [9, 20, 50, 100]                      #EMA Periods
SMA_PERIODS = [9, 20, 50, 100]                      #SMA Periods    
RSI_PERIODS = [14, 21]                              #RSI Periods
CPR = ['DAILY', 'WEEKLY','MONTHLY']                 #CPR Periods

# === LTP Pivot Touch Gatekeeper (new) ===
PIVOT_TOUCH_BUFFER_PTS = 0.5                        # absolute buffer in price units (points). adjust for NIFTY ~0.5
PIVOT_TOUCH_BUFFER_PCT = None                       # optional: use percentage buffer (e.g. 0.001 for 0.1%). If set, overrides PIVOT_TOUCH_BUFFER_PTS
# --------------------------------------

# Option trading config
OPTION_ENABLED = True               #Enable option trading
OPTION_EXCHANGE = "NFO"             #Option exchange
STRIKE_INTERVAL = 50                #Strike interval
OPTION_EXPIRY_TYPE = "WEEKLY"       #Option expiry type
OPTION_STRIKE_SELECTION = "ATM"    # "ATM", "ITM1", "OTM1", etc.
EXPIRY_LOOKAHEAD_DAYS = 30          #Expiry look ahead days
LOT_QUANTITY = 65                    #Lot size
LOT = 1                             #Lot size
QUANTITY = LOT * LOT_QUANTITY       #Quantity
SL_ORDER_ENABLED = True             #Enable SL order
# Websocket CONSTANTS
HEARTBEAT_INTERVAL = 10             # seconds
RECONNECT_BASE = 1.0                # seconds
RECONNECT_MAX = 60.0                # seconds

# Stoploss/Target tracking configuration
USE_SPOT_FOR_SLTP = True           # If True → uses spot price for SL/TP tracking
USE_OPTION_FOR_SLTP = False          # If True → uses option position LTP for SL/TP tracking

# SL/TP Enhancement config
# Available Methods:
# - "Signal_candle_range_SL_TP": Uses signal candle range for SL/TP with TSL
# - "CPR_range_SL_TP": Uses CPR pivot levels as dynamic targets with trailing
SL_TP_METHOD = "CPR_range_SL_TP" # current active method #CPR_range_SL_TP #Signal_candle_range_SL_TP
TSL_ENABLED = False # enable trailing stoploss behavior
TSL_METHOD = "TSL_CPR_range_SL_TP" # trailing method for this enhancement

# ===============================================================
# 🔒 STOP LOSS AUTOMATION & ENTRY RESTRICTIONS
# ===============================================================

# Entry restrictions configuration
# Apply day high/low validation from this trade count onward

# ===============================================================
# 🎯 CPR PIVOT EXCLUSION CONFIGURATION
# ===============================================================

CPR_EXCLUDE_PIVOTS = ["WEEKLY_TC",  "WEEKLY_BC","WEEKLY_CLOSE", "WEEKLY_CPR_RANGE",
     "MONTHLY_TC", "MONTHLY_BC", "MONTHLY_CLOSE", "MONTHLY_CPR_RANGE",
     "DAILY_TC", "DAILY_BC", "DAILY_CLOSE", "DAILY_CPR_RANGE", "DAILY_PREV_LOW", "DAILY_PREV_HIGH", "DAILY_PREV_TOP", "DAILY_PREV_CENTER",
     "WEEKLY_PREV_LOW", "WEEKLY_PREV_HIGH", "WEEKLY_PREV_TOP", "WEEKLY_PREV_CENTER"]  # Example: exclude all except pivot and mid
     
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
# Include strategy name in logfile (if provided) so filenames include strategy

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

# ==============================================================  
def rsi_v_setup(rsi_series, level: float, lookback: int) -> bool:
    """
    Detect bullish RSI V setup:
    - RSI was above level
    - RSI dipped below level
    - Current candle reclaims level
    """
    if len(rsi_series) < lookback + 2:
        return False

    # recent = rsi_series.iloc[-lookback:]
    recent = rsi_series.iloc[:-2].tail(lookback)
    prev_rsi = rsi_series.iloc[-3]
    prev_rsi_2 = rsi_series.iloc[-4]
    curr_rsi = rsi_series.iloc[-2]

    # Current reclaim
    reclaim0 = prev_rsi_2  < level <= curr_rsi
    reclaim1 = prev_rsi  < level <= curr_rsi
    reclaim = reclaim0 or reclaim1
    if not reclaim:
        return False

    # Structure validation
    was_above = (recent > level).any() and reclaim
    # was_below = (recent > level).any() and reclaim

    logger.debug(f"RSI V Setup Check: Prev2={prev_rsi_2:.2f}, Prev1={prev_rsi:.2f}, Curr={curr_rsi:.2f}, Reclaim={reclaim}, WasAbove={was_above}")

    return was_above and reclaim

def rsi_inverse_v_setup(rsi_series, level: float, lookback: int) -> bool:
    """
    Detect bearish inverse V:
    - RSI was below level
    - RSI moved above level
    - Current candle crosses back below level
    """
    if len(rsi_series) < lookback + 2:
        return False

    # recent = rsi_series.iloc[-lookback:]
    recent = rsi_series.iloc[:-2].tail(lookback)

    prev_rsi = rsi_series.iloc[-3]
    prev_rsi_2 = rsi_series.iloc[-4]
    curr_rsi = rsi_series.iloc[-2]

    reclaim_down0 = prev_rsi_2 > level >= curr_rsi
    reclaim_down1 = prev_rsi > level >= curr_rsi 
    reclaim_down = reclaim_down0 or reclaim_down1
    if not reclaim_down:
        return False

    was_below = (recent < level).any() and reclaim_down
    logger.debug(f"RSI Inverse V Setup Check: Prev2={prev_rsi_2:.2f}, Prev1={prev_rsi:.2f}, Curr={curr_rsi:.2f}, ReclaimDown={reclaim_down}, WasBelow={was_below}")
    return was_below and reclaim_down

# ==============================================================  
def clear_trade_logs():
    """
    Clears the `my_trade_logs` table in the local SQLite database.
    This ensures a clean slate for backtesting.
    """
    try:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(base_dir, "myalgo.db")
        
        # Connect and clear
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM my_trades;")
        conn.commit()
        conn.close()
        
        main_logger.info(f"🧹 Trade logs cleared from {db_path} for new backtest.")
    except Exception as e:
        main_logger.error(f"❌ Failed to clear trade logs: {e}")

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
        self.df_strategy_5m: Optional[pd.DataFrame] = None  # Cache for strategy
        self.option_cache: Dict[tuple, pd.DataFrame] = {}   # Key: (symbol, timeframe) -> DF
        self.sim_index: int = 0  # next row to be emitted
        self.simulate_date_str: Optional[str] = None
        self.lock = threading.RLock()
        self.lock = threading.RLock()
        self.pause_ticks = False
        self.day_indices: Dict[date, Tuple[int, int]] = {} # Map date -> (start_idx, end_idx)
        self.sim_end_index: int = 0  # current day end index

    def start(self, simulate_date_str: str, symbol: str, exchange: str, timeframe: str):
        """
        Load 1-minute OHLC for the given date (Replay) AND 5-minute historical data (Strategy).
        """
        with self.lock:
            dt = self._parse_date(simulate_date_str)
            trade_date = dt.strftime("%Y-%m-%d")
            main_logger.info(f"🎬 Loading simulation data for {trade_date}")
            
            # ─────────────────────────────────────────────────────────────
            # 1. LOAD REPLAY DATA (1-minute, Single Day)
            # ─────────────────────────────────────────────────────────────
            logger.info("🛰️ [Replay] Fetching 1-minute OHLC for %s", trade_date)
            # 2️⃣ DuckDB FIRST
            df = DB_MANAGER.get_ohlcv_data(
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                start_date=trade_date,
                end_date=trade_date
                )
            # 3️⃣ API FALLBACK
            if df is None or df.empty:
                try:
                    raw = client.history(symbol=symbol, exchange=exchange, interval=timeframe,
                                     start_date=trade_date, end_date=trade_date)
                    df_openalgo_historical = normalize_df(raw)
                    if df_openalgo_historical.empty:
                        raise RuntimeError("No usable 1-minute rows at/after 09:15 for " + trade_date)
                    # Store into DuckDB
                    df_openalgo_historical = df_openalgo_historical.reset_index()
                    DB_MANAGER.store_ohlcv_data(
                        symbol=symbol,
                        exchange=exchange,
                        timeframe=timeframe,
                        data=df_openalgo_historical,
                        replace=False
                    )
                    # Re-query
                    df = DB_MANAGER.get_ohlcv_data(
                        symbol=symbol,
                        exchange=exchange,
                        timeframe=timeframe,
                        start_date=trade_date,
                        end_date=trade_date
                    )
                except Exception as e:
                    logger.exception("❌ [Replay] Data fetch failed: %s", e)
                    raise

            # Normalize
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(IST)
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert(IST)
       
            start_time = datetime.strptime("09:15", "%H:%M").time()
            df = df[df["timestamp"].dt.time >= start_time].reset_index(drop=True)

            if df.empty:
                raise RuntimeError("No usable 1-minute candles after 09:15")

            self.df = df
            self.sim_index = 0
            
            # ─────────────────────────────────────────────────────────────
            # 2. LOAD STRATEGY DATA (5-minute, History + Today)
            # ─────────────────────────────────────────────────────────────
            # Determine start date for history (using LOOKBACK_DAYS)
            history_start_dt = dt - timedelta(days=LOOKBACK_DAYS)
            history_start_str = history_start_dt.strftime("%Y-%m-%d")
            
            logger.info(f"🛰️ [Strategy] Caching {CANDLE_TIMEFRAME} data: {history_start_str} to {trade_date}")
            
            df_5m = DB_MANAGER.get_ohlcv_data(
                symbol=symbol,
                exchange=exchange,
                timeframe=CANDLE_TIMEFRAME,
                start_date=history_start_str,
                end_date=trade_date
            )
            
            # normalize 5m cache
            if df_5m is not None and not df_5m.empty:
                if isinstance(df_5m.index, pd.DatetimeIndex):
                    df_5m = df_5m.reset_index()
                df_5m["timestamp"] = pd.to_datetime(df_5m["timestamp"])
                if df_5m["timestamp"].dt.tz is None:
                    df_5m["timestamp"] = df_5m["timestamp"].dt.tz_localize(IST)
                else:
                    df_5m["timestamp"] = df_5m["timestamp"].dt.tz_convert(IST)
                
                # Make index accessible for searching if needed, or sort
                df_5m = df_5m.sort_values("timestamp").reset_index(drop=True)
                self.df_strategy_5m = df_5m
                logger.info(f"💾 [Strategy] Cached {len(df_5m)} rows ({CANDLE_TIMEFRAME}) for fast slicing")
            else:
                logger.warning("⚠️ [Strategy] Could not preload 5m history from DB. get_intraday() may fail or fallback.")
                self.df_strategy_5m = pd.DataFrame()

            self.simulate_date_str = simulate_date_str
            self.active = True
            main_logger.info(f"✅ Simulation loaded: {len(df)} ticks (1m) | {len(self.df_strategy_5m)} candles (5m)")

    def start_range(self, start_date_str: str, end_date_str: str, symbol: str, exchange: str, timeframe: str):
        """
        Load 1-minute OHLC for the ENTIRE range and precompute day indices.
        Also loads strategy data for the full range + lookback.
        """
        with self.lock:
            start_dt = self._parse_date(start_date_str)
            end_dt = self._parse_date(end_date_str)
            main_logger.info(f"🎬 Loading simulation data for RANGE: {start_date_str} to {end_date_str}")
            
            # ─────────────────────────────────────────────────────────────
            # 1. LOAD REPLAY DATA (1-minute, Full Range)
            # ─────────────────────────────────────────────────────────────
            start_date_fmt = start_dt.strftime("%Y-%m-%d")
            end_date_fmt = end_dt.strftime("%Y-%m-%d")
            
            logger.info(f"🛰️ [Replay] Fetching 1-minute OHLC for range {start_date_fmt} - {end_date_fmt}")
            
            # 2️⃣ DuckDB FIRST
            df = DB_MANAGER.get_ohlcv_data(
                symbol=symbol,
                exchange=exchange,
                timeframe=timeframe,
                start_date=start_date_fmt,
                end_date=end_date_fmt
                )
            
            # 3️⃣ API FALLBACK REMOVED
            # User is 100% sure data is in DB. If not, stop.
            if df is None or df.empty:
                 main_logger.error(f"❌ No 1-minute data found in DB for range {start_date_fmt} to {end_date_fmt}")
                 raise RuntimeError(f"Data missing in DB for range {start_date_fmt} to {end_date_fmt} (API fallback disabled)")

            # Normalize
            if isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            if df["timestamp"].dt.tz is None:
                df["timestamp"] = df["timestamp"].dt.tz_localize(IST)
            else:
                df["timestamp"] = df["timestamp"].dt.tz_convert(IST)

            # Filter market hours for safety
            start_time = datetime.strptime("09:15", "%H:%M").time()
            df = df[df["timestamp"].dt.time >= start_time].reset_index(drop=True)
            
            if df.empty:
                raise RuntimeError("No usable 1-minute candles after filtering market hours")

            self.df = df
            
            # ─────────────────────────────────────────────────────────────
            # 2. PRECOMPUTE DAY INDICES
            # ─────────────────────────────────────────────────────────────
            self.day_indices.clear()
            # Group by date to find start/end indices
            # df is sorted by timestamp
            df['date'] = df['timestamp'].dt.date
            
            # Find first and last index for each date
            # This is much faster than filtering dataframe every day
            day_groups = df.groupby('date').indices
            
            for d, indices in day_groups.items():
                if len(indices) > 0:
                    start_idx = int(indices[0])
                    end_idx = int(indices[-1]) + 1 # Exclusive end for slicing/range
                    self.day_indices[d] = (start_idx, end_idx)
            
            main_logger.info(f"✅ Indexed {len(self.day_indices)} trading days from {len(df)} candles")

            # ─────────────────────────────────────────────────────────────
            # 3. LOAD STRATEGY DATA (5-minute, Full Range + Lookback)
            # ─────────────────────────────────────────────────────────────
            # Lookback from START date
            history_start_dt = start_dt - timedelta(days=LOOKBACK_DAYS)
            history_start_str = history_start_dt.strftime("%Y-%m-%d")
            
            logger.info(f"🛰️ [Strategy] Caching {CANDLE_TIMEFRAME} data: {history_start_str} to {end_date_fmt}")
            
            df_5m = DB_MANAGER.get_ohlcv_data(
                symbol=symbol,
                exchange=exchange,
                timeframe=CANDLE_TIMEFRAME,
                start_date=history_start_str,
                end_date=end_date_fmt
            )
            
            # normalize 5m cache
            if df_5m is not None and not df_5m.empty:
                if isinstance(df_5m.index, pd.DatetimeIndex):
                    df_5m = df_5m.reset_index()
                df_5m["timestamp"] = pd.to_datetime(df_5m["timestamp"])
                if df_5m["timestamp"].dt.tz is None:
                    df_5m["timestamp"] = df_5m["timestamp"].dt.tz_localize(IST)
                else:
                    df_5m["timestamp"] = df_5m["timestamp"].dt.tz_convert(IST)
                
                df_5m = df_5m.sort_values("timestamp").reset_index(drop=True)
                self.df_strategy_5m = df_5m
                logger.info(f"💾 [Strategy] Cached {len(df_5m)} rows ({CANDLE_TIMEFRAME}) for fast slicing")
            else:
                 logger.warning("⚠️ [Strategy] Could not preload 5m history. get_intraday() may fail.")
                 self.df_strategy_5m = pd.DataFrame()

            self.active = True
            
    def set_day(self, trade_date: date) -> bool:
        """
        Prepare BacktestingManager for a specific day from the preloaded range.
        Returns True if data exists for the day, False otherwise.
        """
        with self.lock:
            if not self.active or self.df is None:
                return False
                
            if trade_date not in self.day_indices:
                main_logger.warning(f"⚠️ No data indexed for {trade_date}")
                return False
                
            start_idx, end_idx = self.day_indices[trade_date]
            self.sim_index = start_idx
            self.sim_end_index = end_idx # Exclusive
            self.simulate_date_str = trade_date.strftime("%d-%m-%Y")
            
            main_logger.info(f"🔄 Switched to day {trade_date} | Rows {start_idx} to {end_idx} ({end_idx - start_idx} ticks)")
            return True

    def get_data_slice(self, current_time: datetime) -> pd.DataFrame:
        """
        Return cached 5m data up to current_time (inclusive).
        Optimized for speed (in-memory filtering).
        """
        if self.df_strategy_5m is None or self.df_strategy_5m.empty:
            return pd.DataFrame()
            
        # Ensure current_time is tz-aware IST
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=IST)
            
        mask = self.df_strategy_5m["timestamp"] <= current_time
        sliced_df = self.df_strategy_5m[mask].copy()
        
        # Set index to timestamp as expected by strategy
        sliced_df.set_index("timestamp", inplace=True)
        return sliced_df

    def get_option_data_slice(self, symbol: str, timeframe: str, current_time: datetime) -> Optional[pd.DataFrame]:
        """
        Return cached option data up to current_time.
        Returns None if not in cache (signaling caller to fetch & cache).
        """
        with self.lock:
            key = (symbol, timeframe)
            if key not in self.option_cache:
                return None
            
            df = self.option_cache[key]
            
        # Ensure current_time is tz-aware IST
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=IST)
            
        # Filter (df is already sorted and normalized)
        mask = df["timestamp"] <= current_time
        sliced_df = df[mask].copy()
        sliced_df.set_index("timestamp", inplace=True)
        return sliced_df

    def cache_option_data(self, symbol: str, timeframe: str, df: pd.DataFrame):
        """
        Store option dataframe in cache.
        Expects normalized df with 'timestamp' column.
        """
        with self.lock:
            if df is None or df.empty:
                return
            
            # Ensure timestamp logic
            if "timestamp" not in df.columns and isinstance(df.index, pd.DatetimeIndex):
                df = df.reset_index()
            
            if "timestamp" in df.columns:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                if df["timestamp"].dt.tz is None:
                    df["timestamp"] = df["timestamp"].dt.tz_localize(IST)
                else:
                    df["timestamp"] = df["timestamp"].dt.tz_convert(IST)
                
                df = df.sort_values("timestamp").reset_index(drop=True)
                key = (symbol, timeframe)
                self.option_cache[key] = df
                main_logger.info(f"💾 [BacktestCache] Cached option {symbol} ({timeframe}): {len(df)} rows")

    def stop(self):
        with self.lock:
            self.active = False
            self.df = None
            self.df_strategy_5m = None
            self.option_cache.clear()
            self.sim_index = 0
            self.sim_end_index = 0
            self.day_indices.clear()
            self.simulate_date_str = None
            main_logger.info("🛑 Simulation stopped")

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
            # Use sim_end_index if set (Range Mode), else end of df (Single Day Mode)
            end_limit = getattr(self.BACKTESTING_MANAGER, 'sim_end_index', 0)
            if end_limit == 0:
                 end_limit = len(df)
            
            while idx < end_limit and not self._stop_event.is_set() and self.BACKTESTING_MANAGER.active:
                
                # SIMULATION PAUSE DURING EXIT
                while self.BACKTESTING_MANAGER.pause_ticks:
                    #  time.sleep(0.01)
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
                        # main_logger.debug(f"⏰ Signal trigger at {ts.strftime('%H:%M:%S')}")
                        
                        # 🔧 PRE-CHECK: Validate breakout expiry BEFORE strategy evaluation
                        if strategy_fn and hasattr(strategy_fn, '__self__'):
                            bot = strategy_fn.__self__
                            
                            # Check ENTRY breakout expiry
                            if hasattr(bot, 'pending_breakout_expiry') and bot.pending_breakout_expiry:
                                if ts >= bot.pending_breakout_expiry:
                                    # signal_logger.info("❌ Entry breakout window expired (pre-check) | clearing pending signal.")
                                    
                                    # Unsubscribe from option LTP if subscribed
                                    if hasattr(bot, 'option_symbol') and bot.option_symbol:
                                        if bot.option_symbol in bot.ws_subscriptions:
                                            bot.ws_subscriptions.discard(bot.option_symbol)
                                            main_logger.info(f"🔌 Unsubscribed from option LTP: {bot.option_symbol}")
                                    
                                    bot.reset_timer()
                                    bot.reset_pending_breakout()
                            
                            # Check EXIT breakout expiry
                            if hasattr(bot, 'pending_exit_breakout_expiry') and bot.pending_exit_breakout_expiry:
                                if ts >= bot.pending_exit_breakout_expiry:
                                    # signal_logger.info("❌ Exit breakout window expired (pre-check) | clearing.")
                                    bot.pending_exit_breakout = None
                                    bot.exit_breakout_side = None
                                    bot.reset_timer()
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
                # time.sleep(0.05)  # small delay to simulate fetching 1-minute OHLCe real ticks
        # -------------------------------------------------------------
        # LOOP END — only stop when ALL candles replayed
        # -------------------------------------------------------------
            # LOOP END — only stop when ALL candles replayed for the current day
        # -------------------------------------------------------------
            if idx >= end_limit:
                main_logger.info("🏁 Replay completed — all candles processed.")
                
                # [FIX] In Range Mode, do NOT kill the bot/manager here.
                # Let replay_range handle the loop day-by-day.
                if MODE == "BACKTESTING_RANGE":
                     main_logger.info("🏁 Day completed (Range Mode). Thread exiting naturally.")
                     return

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

    def replay_range(self, trading_days: List[date], bot):
        """
        Drive the multi-day backtest loop from within the same process/thread.
        """
        logger.info(f"🚀 DateReplayClient starting RANGE replay for {len(trading_days)} days")
        
        self._stop_event.clear()
        
        for idx, trade_date in enumerate(trading_days, 1):
            if self._stop_event.is_set():
                break
                
            day_str = trade_date.strftime("%d-%m-%Y")
            main_logger.info("")
            main_logger.warning(f"📆 DAY {idx}/{len(trading_days)}: {day_str}")
            
            # 1. Reset Bot State
            bot.reset_daily_state()
            
            # 2. Set Data Range
            has_data = self.BACKTESTING_MANAGER.set_day(trade_date)
            if not has_data:
                continue
                
            # 3. Simulate Day
            self._run_day_sync(bot)
            
            # 4. EOD Cleanup (Force close positions if any remain)
            # Strategy usually auto-exits at 15:15 or 15:20, but we ensure here
            if bot.position:
                main_logger.info(f"⚠️ EOD Forced Exit needed for {day_str}")
                bot.exit_all_positions()
                
        main_logger.info("🏁 Range Replay Completed.")
        # Do not shut down bot here, let run() method handle finalization or return
        
    def _run_day_sync(self, bot):
        """
        Run the replay loop synchronously for the current day setup in Manager.
        This effectively replaces `_run` but runs in the main flow for range backtest.
        """
        # Re-use the existing _run logic but we need to call it or extract it.
        # Since _run is a local function in subscribe_ltp, we need to adapt structure.
        # Ideally, refactor _run to be a method. 
        # For now, I will use subscribe_ltp which starts a thread, and join it.
        # But wait, subscribe_ltp expects `on_data_received`.
        
        # In Range Mode, we call subscribe_ltp ONCE at the start?
        # No, because subscribe_ltp starts a thread.
        # Better approach: We need a synchronous version or we wait for the thread.
        
        # Let's rely on the existing subscribe_ltp structure but "Wait" for it to finish the day.
        
        # Start the thread for THIS day
        self.subscribe_ltp(
            instrument_type="SPOT",
            on_data_received=bot.on_ltp_update,
            strategy_fn=bot.strategy_job
        )
        
        # Join the thread to wait for day completion
        if self._thread and self._thread.is_alive():
             self._thread.join()
        
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
if MODE in ["BACKTESTING", "BACKTESTING_RANGE"] and SIMULATION_DATE is not None:
    main_logger.info(f"🚀 Starting in BACKTESTING mode for date {SIMULATION_DATE}")
    from db.database_main import DatabaseManager
    BACKTESTING_MANAGER = BacktestingManager()
    REPLAY_CLIENT = DateReplayClient(BACKTESTING_MANAGER)
    DB_MANAGER = DatabaseManager(read_only=True)
else:
    main_logger.info("🚀 Starting in LIVE mode")
    from db.database_main import DatabaseManager
    BACKTESTING_MANAGER = BacktestingManager()  # inactive in live mode
    REPLAY_CLIENT = None
    DB_MANAGER = None
# =========================================================
# 📘 SQLAlchemy Dataclass Setup for Trade Logging
# =========================================================
Base = declarative_base()

@dataclass
class TradeLog(Base):
    __tablename__ = "my_trades"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    strategy: str = Column(String(50))
    timestamp: datetime = Column(DateTime, default=datetime.now)
    expiry_day: bool = Column(Boolean, default=False, nullable=False)       # True or False as string for simplicity
    instrument: str = Column(String(50), default=SYMBOL)
    symbol: str = Column(String(50))
    leg_type: str = Column(String(20))
    spot_price: float = Column(Float)
    option_price: float = Column(Float) 
    stoploss: float = Column(Float)
    target: float = Column(Float)
    action: str = Column(String(10))
    option_type: str = Column(String(10))
    quantity: float = Column(Float)
    order_id: str = Column(String(50))
    reason: str = Column(String(200), nullable=True)
    pnl: float = Column(Float, default=0.0)
    leg_status: str = Column(String(20), default="open")
    daily_cpr_type: str = Column(String(20), nullable=True)   # e.g. 'narrow', 'wide' — set on first trade of day
    weekly_cpr_type: str = Column(String(20), nullable=True)  # e.g. 'normal', 'extreme_wide'
    gap_type: str = Column(String(20), nullable=True)         # gap_up, gap_down, no_gap
    cpr_relation: str = Column(String(30), nullable=True)     # higher_value, lower_value, overlapping, inside
    open_location: str = Column(String(50), nullable=True)    # dynamic open zone e.g. between_r1_r2, above_pdh
    open_location: str = Column(String(50), nullable=True)    # dynamic open zone e.g. between_r1_r2, above_pdh
    hourly_vs_weekly_cpr: str = Column(String(30), nullable=True)  # above_weekly_cpr, below_weekly_cpr, inside_weekly_cpr
    current_rtp: bool = Column(Boolean, nullable=True)        # True if EMA9 > EMA20 > EMA50 > EMA200 (or vice versa)
    rtp_9_30: bool = Column(Boolean, nullable=True)           # Snapshot of RTP at 09:30 candle
    gap_pct: float = Column(Float, nullable=True)             # Exact Gap percentage
    rsi_level: float = Column(Float, nullable=True)           # RSI level at entry
    close_location: str = Column(String(50), nullable=True)   # Current candle close CPR location
    first_15m_candle_type: str = Column(String(50), nullable=True) # Strong/Weak Bullish/Bearish
    current_candle_type: str = Column(String(50), nullable=True)   # Strong/Normal/Weak Bullish/Bearish

@dataclass
class CprMarketData(Base):
    """Stores CPR type classification per trading date for analysis."""
    __tablename__ = "cpr_market_data"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    trade_date: str = Column(String(20))          # "YYYY-MM-DD"
    symbol: str = Column(String(20))
    daily_cpr_type: str = Column(String(20), nullable=True)
    weekly_cpr_type: str = Column(String(20), nullable=True)
    # Current Day CPR (built from yesterday's OHLC)
    cd_top: float = Column(Float, nullable=True)
    cd_center: float = Column(Float, nullable=True)
    cd_low: float = Column(Float, nullable=True)
    # Previous Day CPR (built from day-before-yesterday's OHLC)
    pd_top: float = Column(Float, nullable=True)
    pd_center: float = Column(Float, nullable=True)
    pd_low: float = Column(Float, nullable=True)
    # Current Week CPR (built from last week's OHLC)
    cw_top: float = Column(Float, nullable=True)
    cw_center: float = Column(Float, nullable=True)
    cw_low: float = Column(Float, nullable=True)
    # Previous Week CPR (built from week-before-last's OHLC)
    pw_top: float = Column(Float, nullable=True)
    pw_center: float = Column(Float, nullable=True)
    pw_low: float = Column(Float, nullable=True)
    created_at: datetime = Column(DateTime, default=datetime.now)

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
        # logger.info(f"✅ Trade logged: {trade_data['symbol']} | {trade_data['action']} @ {trade_data['price']}")
    except Exception as e:
        logger.info(f"⚠️ DB Logging Error: {e}")
        session.rollback()
    finally:
        session.close()

def log_cpr_market_data(data: dict):
    """
    Upsert CPR type data for a given trade_date + symbol into cpr_market_data table.
    If a row already exists for that date+symbol it is replaced.
    """
    session = SessionLocal()
    try:
        trade_date = data.get("trade_date")
        symbol = data.get("symbol")
        # Delete existing row for same date+symbol (upsert pattern)
        session.query(CprMarketData).filter(
            CprMarketData.trade_date == trade_date,
            CprMarketData.symbol == symbol
        ).delete(synchronize_session=False)
        record = CprMarketData(**data)
        session.add(record)
        session.commit()
        logger.debug(f"✅ CPR market data logged: {trade_date} | {symbol}")
    except Exception as e:
        logger.warning(f"⚠️ CPR DB Logging Error: {e}")
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
        self.entry_breakout_candle_count = 0  # 🎯 Track consecutive candles for entry breakout
        self.exit_breakout_candle_count = 0   # 🎯 Track consecutive candles for exit breakout
        self.pending_exit_breakout = None
        self.pending_exit_breakout_expiry = None
        self.exit_breakout_side = None
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
        self.last_ltp_time = {}  # {symbol: last_tick_timestamp}
        self.ws_subscription_lock = threading.Lock()
        self.ib_high = None
        self.ib_low = None
        self.broker_sl = None          # Broker-level SL price; set after SL order placement (live only)

        # Gap filter state (reset daily)
        self.gap_type_cache = None      # 'gap_up', 'gap_down', 'no_gap' — cached per day
        self.first_15m_high = None      # High of the 09:15–09:30 first candle
        self.first_15m_low  = None      # Low  of the 09:15–09:30 first candle

        # Backtesting-configurable required candles for exit breakout confirmation
        self.exit_breakout_required_candles = EXIT_BREAKOUT_CONFIRM_CANDLES
        # Backtesting-configurable required candles for entry breakout confirmation
        self.entry_breakout_required_candles = ENTRY_BREAKOUT_CONFIRM_CANDLES

        # --- Intrabar control ---
        self._option_intrabar_stage = 0
        self._last_option_candle_ts = None
    
        # 🗓️ Expiry Cache
        self.expiry_cache = {"date": None, "is_expiry": False, "expiry_dt": None}
        # Instance-level TSL flag (per-trade) and cached dynamic indicators
        self.tsl_enabled = False
        self.cached_dyn_spot = None
        self.cached_dyn_option = None
        self.dynamic_indicators_cache = {}  # Cache for pre-calculated strategy indicators
        self._precalc_rsi_arrays = {}  # 🎯 Cache RSI arrays for RSI_STRATEGY series reconstruction
        self.cached_last_close = None

        # 🎯 Log mode selection
        mode_str = MODE
        logger.info(f"🚀 Bot initialized in {mode_str} MODE")
        main_logger.info(f"=== ℹ️ MyAlgo Trading Bot Initialized ({mode_str} MODE) ===")
        
        if MODE == "BACKTESTING" and SIMULATION_DATE:
            main_logger.info(f"📅 Simulation Date: {BACKTESTING_MANAGER.simulate_date_str}")
        
        main_logger.info(f"🧠 Strategy: {STRATEGY} | Symbol: {SYMBOL} | Timeframe: {CANDLE_TIMEFRAME}")
        main_logger.info(f"⚖️ Risk Management: SL={STOPLOSS}, TP={TARGET} | Max Trades: {MAX_TRADES_PER_DAY}")
        main_logger.info(f"🔒 SL/TP Method: {SL_TP_METHOD} | TSL Enabled: {self.tsl_enabled}")
        main_logger.info(f"📝 Options Enabled: {OPTION_ENABLED} | Strike Selection: {OPTION_STRIKE_SELECTION}")
        main_logger.info(f"⏰ Signal Check Interval: {SIGNAL_CHECK_INTERVAL} minutes")
        
        # Validate SL/TP method configuration
        valid_methods = ["Signal_candle_range_SL_TP", "CPR_range_SL_TP"]
        if SL_TP_METHOD not in valid_methods:
            main_logger.warning(f"⚠️ Unknown SL_TP_METHOD: {SL_TP_METHOD}. Valid options: {valid_methods}")
        else:
            main_logger.debug(f"✅ Valid SL/TP Method configured: {SL_TP_METHOD}")
            
        # Log CPR enhancement features
        if SL_TP_METHOD == "CPR_range_SL_TP":
            main_logger.debug("=== ℹ️ CPR Enhancement Features ===")
            main_logger.debug(f"📐 Pivot Exclusion: {'Enabled' if CPR_EXCLUDE_PIVOTS else 'Disabled'}")
            if CPR_EXCLUDE_PIVOTS:
                main_logger.debug(f"  📐 Excluded Levels: {len(CPR_EXCLUDE_PIVOTS)} ({', '.join(CPR_EXCLUDE_PIVOTS[:3])}...)")
            main_logger.debug(f"⚖️ Dynamic Risk:Reward: {'Enabled' if USE_DYNAMIC_TARGET else 'Disabled'}")
            if USE_DYNAMIC_TARGET:
                main_logger.debug(f"  ⚖️ RR Ratio: 1:{RISK_REWARD_RATIO:.2f}")
                main_logger.debug(f"  Target Method: {DYNAMIC_TARGET_METHOD}")
                main_logger.debug(f"  Valid Target Levels: {', '.join(VALID_TARGET_LEVELS)}")
            main_logger.debug("===============================")

    def reset_daily_state(self):
        """
        Reset state for a new trading day in BACKTESTING_RANGE mode.
        """
        main_logger.info("🔄 [Daily Reset] Clearing bot state for new day")
        with self._state_lock:
            # 1. Trade State
            self.trade_count = 0
            self.position = None
            self.entry_price = 0.0
            self.spot_entry_price = 0.0
            self.option_entry_price = 0.0
            self.stoploss_price = 0.0
            self.target_price = 0.0
            self.option_stop_loss = 0.0
            self.initial_stoploss_price = 0.0
            self.exit_in_progress = False
            self.option_symbol = None
            self.ltp = None
            self.option_ltp = None

            # 2. Breakout State
            self.pending_breakout = None
            self.pending_breakout_expiry = None
            self.breakout_side = None
            self.entry_breakout_candle_count = 0
            self.exit_breakout_candle_count = 0
            self.pending_exit_breakout = None
            self.pending_exit_breakout_expiry = None
            self.exit_breakout_side = None
            
            # 3. Timers
            self.reset_timer()

            # 4. CPR / Strategy State
            self.cpr_active = False
            self.cpr_levels_sorted = []
            self.cpr_targets = []
            self.cpr_tp_hit_count = 0
            self.cpr_sl = 0.0
            self.cpr_side = None
            self.current_tp_index = 0
            self.cpr_tp_hits = []
            self.dynamic_target_info = None
            self.excluded_pivots_info = None
            
            # 5. Gatekeeper
            self.ltp_pivot_breakout = False
            self.ltp_pivot_info = None
            self.gk_resistance_pivot = None
            self.gk_support_pivot = None
            
            # 6. Trailing
            self.trailing_levels = []
            self.trailing_index = 0
            
            # 7. Data Cache
            self.static_indicators = None
            self.static_indicators_date = None
            self.current_day_high = None
            self.current_day_low = None
            self.option_1m_df = None
            self.expiry_cache = {"date": None, "is_expiry": False, "expiry_dt": None}
            self.cached_dyn_spot = None
            self.cached_dyn_option = None
            self.cached_last_close = None
            self.dynamic_indicators_option = None
            self.current_day_open = None
            self.prev_day_close = None
            self.ib_high = None
            self.ib_low = None
            self.rtp_9_30 = None
            self.gap_type_cache = None
            self.gap_percent = None
            self.first_15m_candle_type = None
            
            # 8. Intrabar
            self._option_intrabar_stage = 0
            self._last_option_candle_ts = None
            self.last_ltp_time = {}

            # 9. Gap filter (clear daily cache)
            self.gap_type_cache = None
            self.first_15m_high = None
            self.first_15m_low  = None
            
        # Reset threading events
        self.stop_event.clear()
        self.running = True

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
                .filter(TradeLog.leg_status == "Open")
                .order_by(TradeLog.id.desc())
                .first()
            )

            if open_leg:
                exit_price = float(exit_trade.get("option_price", 0))
                entry_price = float(open_leg.option_price or 0)
                qty = float(exit_trade.get("quantity", 0))
            
                # Calculate PnL (for long vs short)
                if open_leg.action.upper() == "CALL":
                    pnl = (exit_price - entry_price) * qty
                else:
                    pnl = (exit_price - entry_price) * qty

                
                if self.cpr_tp_hit_count == 1 and  pnl < 100 and pnl > -100:
                    reason = "Breakeven"
                else:
                    reason = "TP Hit" if pnl > 0 else "SL Hit" if pnl < 0 else "Breakeven Exit"

                open_leg.pnl = float(f"{pnl:.2f}")
                open_leg.leg_status = "Closed"
                open_leg.reason = reason
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
    def _breakout_1m_job(self):
        """
        Runs every minute (LIVE only). Refreshes cached 1m candle closes for
        breakout validation. Only runs when there is an active signal or position.
        """
        try:
            if not CANDLE_BREAKOUT_1M_ENABLED:
                return
            if not self.pending_breakout and not self.pending_exit_breakout and not self.position:
                return  # Nothing to validate — skip API call

            now = get_now()
            current_minute_start = now.replace(second=0, microsecond=0)

            # ── Spot ──────────────────────────────────────────────────
            df_spot = self.get_historical(interval="1m", lookback=1,
                                          symbol=SYMBOL, exchange=EXCHANGE,
                                          instrument_type="SPOT")
            if df_spot is not None and not df_spot.empty:
                if not isinstance(df_spot.index, pd.DatetimeIndex):
                    df_spot = df_spot.set_index("timestamp")
                if df_spot.index.tz is None:
                    df_spot.index = df_spot.index.tz_localize(IST)
                df_closed = df_spot[df_spot.index < current_minute_start]
                if not df_closed.empty:
                    self._cached_1m_close_spot = float(df_closed.iloc[-1]["close"])
                    self._cached_1m_candle_ts = df_closed.index[-1]
                    signal_logger.info(
                        f"📊 [1M SCHED] Spot 1m cached: "
                        f"candle@{self._cached_1m_candle_ts.strftime('%H:%M')} "
                        f"close={self._cached_1m_close_spot:.2f}"
                    )

            # ── Option (only if needed) ────────────────────────────────
            mode = (self.pending_breakout or {}).get("mode", "") or \
                   (self.pending_exit_breakout or {}).get("mode", "")
            if mode.upper() in ("OPTIONS", "SPOT_OPTIONS") and getattr(self, "option_symbol", None):
                df_opt = self.get_historical(interval="1m", lookback=1,
                                             symbol=self.option_symbol, exchange=OPTION_EXCHANGE,
                                             instrument_type="OPTION")
                if df_opt is not None and not df_opt.empty:
                    if not isinstance(df_opt.index, pd.DatetimeIndex):
                        df_opt = df_opt.set_index("timestamp")
                    if df_opt.index.tz is None:
                        df_opt.index = df_opt.index.tz_localize(IST)
                    df_closed_opt = df_opt[df_opt.index < current_minute_start]
                    if not df_closed_opt.empty:
                        self._cached_1m_close_option = float(df_closed_opt.iloc[-1]["close"])
                        signal_logger.info(
                            f"📊 [1M SCHED] Option 1m cached: "
                            f"candle@{df_closed_opt.index[-1].strftime('%H:%M')} "
                            f"close={self._cached_1m_close_option:.2f}"
                        )

            # After refreshing cache — immediately run breakout check
            if ENTRY_LTP_BREAKOUT_ENABLED and self.pending_breakout:
                self.handle_entry_ltp_breakout(now)
            if self.pending_exit_breakout:
                self.handle_exit_ltp_breakout(now)

        except Exception:
            signal_logger.exception("_breakout_1m_job failed")

    def init_scheduler(self):
        """
        Use BlockingScheduler on Linux to avoid RuntimeError: can't start new thread.

        WHY:
        ----
        BackgroundScheduler.start() ALWAYS creates a new internal OS thread for its
        event loop — even with a ThreadPoolExecutor limiting job runners, the scheduler
        itself still needs 1 extra thread. On Linux (inside Flask/gunicorn), the process
        thread pool is already exhausted, so ANY attempt to start a new thread fails.

        BlockingScheduler.start() runs the event loop directly in the CURRENT thread
        (strategy_thread, which is already a daemon thread). Zero extra threads created.
        shutdown() from shutdown_gracefully() stops the blocking loop cleanly.
        """
        if self.scheduler and self.scheduler.running:
            return
        self.scheduler = BlockingScheduler(timezone=IST)
        self.scheduler.add_job(
            self.strategy_job,
            CronTrigger(minute=f"*/{int(SIGNAL_CHECK_INTERVAL)}", second=5),
            id="strategy_signal_job",
            replace_existing=True,
        )
        if CANDLE_BREAKOUT_1M_ENABLED:
            self.scheduler.add_job(
                self._breakout_1m_job,
                CronTrigger(minute="*", second=3),  # fires at HH:MM:03
                id="breakout_1m_job",
                replace_existing=True,
            )
            logger.info("📊 1m breakout validation job registered (fires every minute at :03)")
        logger.info("APScheduler (BlockingScheduler) starting — signal checks every %s minute(s)", SIGNAL_CHECK_INTERVAL)
        self.scheduler.start()  # ← blocks strategy_thread; returns when shutdown() is called

    def strategy_job(self):
        """
        Main scheduled job – runs aligned to timeframe close calls.
        🎯 In simulation: called synchronously by replay at minute%5==0
        🎯 In live: called by APScheduler every 5 minutes
        """
        try:
            # 🎯 Use get_now() for proper timestamp (simulated or real)
            current_time = get_now()
            
            # if BACKTESTING_MANAGER.active:
                # signal_logger.debug(f"⏰ Strategy check (SIMULATED): {current_time.strftime('%Y-%m-%d %H:%M:%S')}")
            
            # If reached daily trade limit, initiate graceful shutdown
            if not self.position and self.trade_count >= MAX_TRADES_PER_DAY:
                
                # FIX: In Range Mode, do not kill the bot/process. Just stop taking new trades.
                if MODE == "BACKTESTING_RANGE":
                    #  logger.debug(f"Max trades reached ({self.trade_count}). Idling for remainder of day (Range Mode).")
                     return

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
                intraday = self.get_intraday(
                            days=LOOKBACK_DAYS, 
                            symbol=self.spot_symbol, 
                            exchange=self.spot_exchange,
                            timeframe=CANDLE_TIMEFRAME,
                            instrument_type="SPOT")
                if intraday.empty:
                    logger.warning("No intraday data")
                else:
                    signal = self.check_entry_signal(intraday)
                    if (LTP_BREAKOUT_ENABLED and self.pending_breakout):
                        signal_logger.info(f"⏳ LTP breakout enabled for entry.")
                        return True # Wait for breakout confirmation before placing order
                    if signal:
                        self.place_entry_order(signal)
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
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order(reason)
                        else:
                            threading.Thread(target=self.place_exit_order, args=(reason,), daemon=True).start()

            if not market_is_open():
                reason = "Market closed"    
                logger.info("🛑 Market closed. close all active positions and shutdown a system")
                self.exit_in_progress = True
                if BACKTESTING_MANAGER.active:
                    self.place_exit_order(reason)
                else:
                    self.place_exit_order(reason)
                    # threading.Thread(target=self.place_exit_order, args=(reason,), daemon=True).start()
                
                # FIX: In BACKTESTING_RANGE, do not kill the bot on market close. 
                # Let the replay client finish the day's ticks and loop to the next day.
                if MODE == "BACKTESTING_RANGE":
                    return

                self.shutdown_gracefully()
                return

        except Exception as e:
            logger.exception("strategy_job execution failed", exc_info=e)

    def strategy_thread(self):
        """
        Strategy thread wrapper - behavior depends on mode:
        - LIVE: starts BlockingScheduler (blocks this thread's loop — no extra thread needed)
        - SIMULATION: no scheduler (replay handles timing)

        WHY BlockingScheduler?
        ─────────────────────
        BackgroundScheduler.start() spawns a new OS thread internally.
        On Linux (AWS/gunicorn), this fails with:
            RuntimeError: can't start new thread
        when the process thread limit is exhausted.
        BlockingScheduler.start() runs the event loop directly in the
        calling thread (strategy_thread), requiring ZERO extra threads.
        """
        logger.info("Strategy scheduler starting")

        # 🎯 Only start scheduler in LIVE mode
        if not BACKTESTING_MANAGER.active:
            # init_scheduler() calls BlockingScheduler.start() which blocks here.
            # The thread stays alive inside the scheduler loop until shutdown().
            try:
                self.init_scheduler()
            except KeyboardInterrupt:
                logger.info("Manual stop requested (scheduler)")
                self.shutdown_gracefully()
            except Exception as e:
                logger.exception("Scheduler error: %s", e)
            finally:
                logger.info("Strategy thread finally block reached")
                # Shut down scheduler if not already stopped by shutdown_gracefully()
                if self.scheduler and self.scheduler.running:
                    try:
                        self.scheduler.shutdown(wait=False)
                        logger.info("Scheduler stopped")
                    except Exception as ex:
                        logger.debug("Scheduler shutdown in finally block failed (likely already stopped): %s", ex)
        else:
            main_logger.info("📅 Simulation mode - APScheduler disabled (replay controls timing)")
            # In simulation mode keep the thread alive while the replay runs
            try:
                while not self.stop_event.is_set() and self.running:
                    time.sleep(0.1)
            except KeyboardInterrupt:
                logger.info("Manual stop requested")
                self.shutdown_gracefully()

    def reset_pending_breakout(self):
        """Reset any active pending breakout state."""
        self.pending_breakout = None
        self.pending_breakout_expiry = None
        self.breakout_side = None
        self.entry_breakout_candle_count = 0
        self.exit_breakout_candle_count = 0
        # with self._pivot_gate_lock:
        #     self.ltp_pivot_breakout = False
        #     self.ltp_pivot_info = None

    def handle_exit_ltp_breakout(self, current_time):
            """
            Handle EXIT confirmation via LTP breakout
            (Triggered after reversal candle detection)
            """
            # No pending exit → nothing to do
            if not EXIT_LTP_BREAKOUT_ENABLED:
                return False

            if not self.pending_exit_breakout or not self.position:
                self.reset_timer()
                return False

            # Expiry check
            if current_time >= self.pending_exit_breakout_expiry:
                signal_logger.info("❌ Exit breakout window expired clearing.")
                self.pending_exit_breakout = None
                self.exit_breakout_side = None
                self.reset_timer()
                return False

            # Read configured mode for exit breakout validation
            # Priority: Payload-specific mode > Global Configuration
            mode = self.pending_exit_breakout.get("mode", EXIT_LTP_BREAKOUT_MODE).upper()

            # Acquire Price Sources for Exit Breakout Evaluation
            # ─────────────────────────────────────────────────────────────────
            # When CANDLE_BREAKOUT_1M_ENABLED=True  → use latest CLOSED 1m candle
            # When CANDLE_BREAKOUT_1M_ENABLED=False → use live LTP (original behavior)
            # ─────────────────────────────────────────────────────────────────
            if CANDLE_BREAKOUT_1M_ENABLED:
                spot_1m = self._get_1m_close_for_breakout(
                    current_time, symbol=SYMBOL, exchange=EXCHANGE, instrument_type="SPOT"
                )
                spot_ltp = spot_1m  # may be None

                option_1m = None
                if mode in ["OPTIONS", "SPOT_OPTIONS"] and self.option_symbol:
                    option_1m = self._get_1m_close_for_breakout(
                        current_time, symbol=self.option_symbol,
                        exchange=OPTION_EXCHANGE, instrument_type="OPTION"
                    )
                option_ltp = option_1m  # may be None
            else:
                # Original LTP-based sources
                spot_ltp = getattr(self, "ltp", None)
                option_ltp = getattr(self, "option_ltp", None)

            side = self.exit_breakout_side
            now_ts = current_time

            # Extract thresholds from pending payload (support both old and new keys)
            spot_high = self.pending_exit_breakout.get("high")
            spot_low = self.pending_exit_breakout.get("low")
            opt_high = self.pending_exit_breakout.get("option_high") if "option_high" in self.pending_exit_breakout else self.pending_exit_breakout.get("high")
            opt_low = self.pending_exit_breakout.get("option_low") if "option_low" in self.pending_exit_breakout else self.pending_exit_breakout.get("low")

            # Helper to evaluate condition depending on side
            def eval_spot_condition():
                if spot_ltp is None:
                    return False
                if CANDLE_BREAKOUT_1M_ENABLED:
                    if side == "EXIT_BUY":
                        result = spot_ltp < (spot_low or 0)
                        signal_logger.debug(
                            f"Spot 1m-Candle Exit Check (EXIT_BUY): 1m_close={spot_ltp:.2f} < "
                            f"spot_low={spot_low or 0:.2f} → {result}"
                        )
                    else:  # EXIT_SELL
                        result = spot_ltp > (spot_high or 0)
                        signal_logger.debug(
                            f"Spot 1m-Candle Exit Check (EXIT_SELL): 1m_close={spot_ltp:.2f} > "
                            f"spot_high={spot_high or 0:.2f} → {result}"
                        )
                    return result
                else:
                    if side == "EXIT_BUY":
                        return spot_ltp < (spot_low or 0)
                    else:  # EXIT_SELL
                        return spot_ltp > (spot_high or 0)

            def eval_option_condition():
                if option_ltp is None:
                    return False
                if CANDLE_BREAKOUT_1M_ENABLED:
                    # Options: check 1m close < opt_low for both EXIT_BUY and EXIT_SELL
                    result = option_ltp < (opt_low or 0)
                    signal_logger.debug(
                        f"Option 1m-Candle Exit Check ({side}): 1m_close={option_ltp:.2f} < "
                        f"opt_low={opt_low or 0:.2f} → {result}"
                    )
                    return result
                else:
                    if side == "EXIT_BUY":
                        # EXIT_BUY -> option must break below option_low
                        return option_ltp < (opt_low or 0)
                    else:  # EXIT_SELL -> per new rule validate low side for options
                        return option_ltp < (opt_low or 0)

            # Determine which conditions to check based on mode
            if mode == "SPOT":
                cond = eval_spot_condition()
            elif mode == "OPTIONS":
                cond = eval_option_condition()
            else:  # SPOT_OPTIONS (both)
                cond = eval_spot_condition() and eval_option_condition()


            # Backtesting: require N-candle confirmation (configurable via EXIT_BREAKOUT_CONFIRM_CANDLES)
            if cond:
                signal_logger.info(f"🔔 Exit breakout condition met for side={side} mode={mode}")
                if BACKTESTING_MANAGER.active:
                    # Use instance override if present else global constant
                    req = getattr(self, 'exit_breakout_required_candles', EXIT_BREAKOUT_CONFIRM_CANDLES)
                    # If only 1 candle required, confirm immediately
                    if req <= 1:
                        trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                        exit_reason = self.pending_exit_breakout.get("reason") or ("CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_CANDLE_BREAKOUT")
                        signal_logger.info(f"🔴 EXIT TRADE - {req}-CANDLE CONFIRMATION | mode={mode} side={side}")
                        self._trigger_exit(exit_reason)
                        self.reset_timer()
                        self.exit_breakout_candle_count = 0
                        self.reset_pending_breakout()
                        return True
                    # Increment counter and confirm when reached
                    self.exit_breakout_candle_count += 1
                    signal_logger.info(f"🕯️ CANDLE {self.exit_breakout_candle_count} DETECTED for exit breakout (need {req})")
                    if self.exit_breakout_candle_count >= req:
                        trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                        exit_reason = self.pending_exit_breakout.get("reason") or ("CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_CANDLE_BREAKOUT")
                        signal_logger.info(f"🔴 EXIT TRADE - {req}-CANDLE CONFIRMATION | mode={mode} side={side}")
                        self._trigger_exit(exit_reason)
                        self.reset_timer()
                        self.exit_breakout_candle_count = 0
                        return True
                # Live/time-based confirmation
                if not ENABLE_SECONDS_CONFIRMATION_LIVE:
                    trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                    exit_reason = self.pending_exit_breakout.get("reason") or ("CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_CANDLE_BREAKOUT")
                    signal_logger.info(f"🔴 EXIT TRADE - BREAKOUT CONFIRMATION | mode={mode} side={side} | reason={exit_reason}")
                    self._trigger_exit(exit_reason)
                    self.reset_timer()
                    return True
                if not self.entry_timer_started:
                    self.start_timer(now_ts)
                    return False
                elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                if elapsed < EXIT_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳ EXIT TRADE - breakout detected, waiting {EXIT_CONFIRM_SECONDS - elapsed:.1f}s")
                    return False
                # final check at timeout
                if cond:
                    trigger = self.pending_exit_breakout.get("trigger", "REVERSAL")
                    # Prefer explicit reason from pending payload; fallback to legacy mapping
                    exit_reason = self.pending_exit_breakout.get("reason")
                    if not exit_reason:
                        exit_reason = "CPR_SL_LTP_CONFIRM" if trigger == "CPR_SL" else "REVERSAL_CANDLE_BREAKOUT"
                    signal_logger.info(f"🔴 EXIT TRADE - TIME CONFIRMED | mode={mode} side={side} | reason={exit_reason}")
                    self._trigger_exit(exit_reason)
                    self.reset_timer()
                    return True
                else:
                    self.reset_timer()
                    return False
            else:
                # Condition not met -> reset counters/timers
                if BACKTESTING_MANAGER.active and self.exit_breakout_candle_count > 0:
                    self.exit_breakout_candle_count = 0
                    signal_logger.info(f"❌ Exit breakout failed - resetting {getattr(self,'exit_breakout_required_candles', EXIT_BREAKOUT_CONFIRM_CANDLES)}-candle counter")
                self.reset_timer()
                return False

    def _trigger_exit(self, reason: str):
            with self._state_lock:
                if self.exit_in_progress:
                    return
                self.exit_in_progress = True

            if BACKTESTING_MANAGER.active:
                self.place_exit_order(reason)
            else:
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
                tc_og = float(self.static_indicators.get("DAILY", {}).get("tc",0.0))
                bc_og = float(self.static_indicators.get("DAILY", {}).get("bc",0.0))
                tc, bc = max(tc_og, bc_og), min(tc_og, bc_og)
                if tc <= bc:
                        position_logger.warning(
                        f"Invalid CPR values detected → TC: {tc_og}, BC: {bc_og} | Adjusting to TC: {tc}, BC: {bc}"
                        )
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
                resistance = [p for p in pivots if p > tc]
                support    = [p for p in pivots if p < bc]

                if resistance:
                    self.gk_resistance_pivot = min(resistance)
                if support:
                    self.gk_support_pivot = max(support)

                signal_logger.info(
                    f"✅ [GATEKEEPER INIT] Stored Pivots → "
                    f"Resistance={self.gk_resistance_pivot:.2f}, Support={self.gk_support_pivot:.2f}"
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
    
    def _get_1m_close_for_breakout(self, current_time, symbol=SYMBOL, exchange=EXCHANGE, instrument_type="SPOT"):
        """
        Get the latest CLOSED 1-minute candle close price for breakout validation.

        Backtesting: reads from BACKTESTING_MANAGER.df (spot) or option_cache (option).
        Live mode  : fetches via self.get_historical('1m').

        A candle is considered 'closed' when its timestamp is strictly before
        the current_time (i.e. the candle that opened at HH:MM closed at HH:MM+1).
        The currently-forming candle is NEVER returned.

        Returns float close price, or None if no closed candle is available.
        """
        try:
            now = current_time
            if BACKTESTING_MANAGER.active:
                # ── Backtesting: read from in-memory 1m data ──────────────────
                now = current_time + timedelta(minutes=1)  # Ensure we only consider candles that fully closed before current_time
                if instrument_type == "SPOT":
                    df_1m = BACKTESTING_MANAGER.df
                else:
                    # Option 1m data stored in option_cache keyed by (symbol, "1m")
                    key = (symbol, "1m")
                    df_1m = BACKTESTING_MANAGER.option_cache.get(key)

                if df_1m is None or df_1m.empty:
                    signal_logger.debug(
                        f"[1M CANDLE] No 1m data available for {symbol} in backtest cache"
                    )
                    return None

                # Work on the timestamp column (df may or may not have it as index)
                if "timestamp" in df_1m.columns:
                    ts_series = df_1m["timestamp"]
                elif isinstance(df_1m.index, pd.DatetimeIndex):
                    ts_series = df_1m.index.to_series()
                else:
                    signal_logger.warning("[1M CANDLE] Cannot resolve timestamp from 1m DataFrame")
                    return None

                # Make tz-aware for comparison
                if ts_series.dt.tz is None:
                    ts_series = ts_series.dt.tz_localize(IST)
                if now.tzinfo is None:
                    now = now.replace(tzinfo=IST)

                # Only rows whose candle has FULLY closed (timestamp strictly < now)
                mask = ts_series < now
                completed = df_1m[mask]
                if completed.empty:
                    signal_logger.debug("[1M CANDLE] No completed 1m candle found before current_time")
                    return None

                close_val = float(completed.iloc[-1]["close"])
                last_ts = ts_series[mask].iloc[-1]
                signal_logger.info(
                    f"📊 [1M CANDLE][{instrument_type}] {symbol} | "
                    f"Candle @ {last_ts.strftime('%H:%M')} | Close={close_val:.2f}"
                )
                return close_val

            else:
                # ── Live mode: fetch via cache ──────────────────────────────────
                # Use cached value from _breakout_1m_job (set once per minute, not per tick)
                if instrument_type == "SPOT":
                    cached = getattr(self, "_cached_1m_close_spot", None)
                else:
                    cached = getattr(self, "_cached_1m_close_option", None)

                if cached is None:
                    signal_logger.debug(
                        f"[1M CANDLE][LIVE] Cache not populated yet for {symbol} — 1m job hasn't fired"
                    )
                return cached

        except Exception:
            signal_logger.exception(f"_get_1m_close_for_breakout failed for {symbol}")
            return None

    def handle_entry_ltp_breakout(self,current_time):

        """
        Handle ENTRY confirmation via LTP breakout
        (Triggered after reversal candle detection)
        """
        # 1. Basic Validity Checks
        if not self.pending_breakout:
            return

        signal_logger.debug(f"✅ LTP breakout check " +  
                           f"| Pending Breakout: {self.pending_breakout.get('type')} " +
                           f"| Expiry: {self.pending_breakout_expiry} " +
                           f"| Mode: {self.pending_breakout.get('mode')} " +
                           f"| Side: {self.breakout_side}"
                           )    
        now_ts = current_time
        side = self.breakout_side
        
        # 2. Check Expiry
        if now_ts >= self.pending_breakout_expiry:
            signal_logger.info("❌ Breakout window expired | clearing pending signal.")
            self.reset_timer()
            self.reset_pending_breakout()
            self.unsubscribe_option_ltp(self.option_symbol)
            return

        # 3. Retrieve Mode and Levels
        mode = self.pending_breakout.get("mode", "SPOT")
        spot_high = self.pending_breakout.get("spot_high", 0.0)
        spot_low = self.pending_breakout.get("spot_low", 0.0)
        option_high = self.pending_breakout.get("option_high", 0.0)
        
        # 4. Acquire Price Sources for Breakout Evaluation
        # ─────────────────────────────────────────────────────────────────────
        # When CANDLE_BREAKOUT_1M_ENABLED=True  → use latest CLOSED 1m candle
        # When CANDLE_BREAKOUT_1M_ENABLED=False → use live LTP (original behavior)
        # ─────────────────────────────────────────────────────────────────────
        if CANDLE_BREAKOUT_1M_ENABLED:
            # Spot: always fetch
            spot_price = self._get_1m_close_for_breakout(
                current_time, symbol=SYMBOL, exchange=EXCHANGE, instrument_type="SPOT"
            )
            if spot_price is None:
                # No closed 1m candle yet — skip this tick silently
                signal_logger.debug("[1M ENTRY] No closed 1m spot candle yet, skipping breakout check")
                return
            spot_ltp = spot_price  # alias for section 5 log messages

            # Option: only fetch when mode requires it
            option_price = None
            if mode in ["OPTIONS", "SPOT_OPTIONS"] and self.option_symbol:
                option_price = self._get_1m_close_for_breakout(
                    current_time, symbol=self.option_symbol,
                    exchange=OPTION_EXCHANGE, instrument_type="OPTION"
                )
            option_ltp = option_price if option_price is not None else (getattr(self, "option_ltp", 0.0) or 0.0)
        else:
            # Original LTP-based price sources
            spot_ltp = getattr(self, "ltp", 0.0) or 0.0
            option_ltp = getattr(self, "option_ltp", 0.0) or 0.0
            spot_price = spot_ltp
            option_price = option_ltp if option_ltp > 0 else None

        # 5. Evaluate Breakout Conditions
        spot_condition_met = False
        option_condition_met = False

        # 5.1 SPOT Validation
        if mode in ["SPOT", "SPOT_OPTIONS"]:
            if CANDLE_BREAKOUT_1M_ENABLED:
                if side == "BUY":
                    spot_condition_met = spot_price > spot_high
                elif side == "SELL":
                    spot_condition_met = spot_price < spot_low
                if mode == "SPOT" or spot_condition_met:
                    signal_logger.debug(
                        f"Spot 1m-Candle Check ({side}): "
                        f"1m_close={spot_price:.2f} vs "
                        f"{'spot_high' if side=='BUY' else 'spot_low'}="
                        f"{spot_high if side=='BUY' else spot_low:.2f} → {spot_condition_met}"
                    )
            else:
                if side == "BUY":
                    spot_condition_met = spot_ltp > spot_high
                elif side == "SELL":
                    spot_condition_met = spot_ltp < spot_low
                # Log only if mode is pure SPOT or if condition met (reduced noise)
                if mode == "SPOT" or spot_condition_met:
                    signal_logger.debug(f"Spot Check ({side}): LTP {spot_ltp:.2f} vs {spot_high if side=='BUY' else spot_low:.2f} -> {spot_condition_met}")

        # 5.2 OPTIONS Validation
        if mode in ["OPTIONS", "SPOT_OPTIONS"]:
            if CANDLE_BREAKOUT_1M_ENABLED:
                if option_price is not None and option_price > 0:
                    # Options are always long (Call/Put), check close > option_high
                    option_condition_met = option_price > option_high
                    if mode == "OPTIONS" or option_condition_met:
                        signal_logger.debug(
                            f"Option 1m-Candle Check ({side}): "
                            f"1m_close={option_price:.2f} > option_high={option_high:.2f} → {option_condition_met}"
                        )
                else:
                    signal_logger.debug("[1M ENTRY] No closed 1m option candle yet, skipping option check")
            else:
                if option_ltp > 0:
                    # Options are always BUY (Long Call or Long Put)
                    # So we always check if LTP > Option High
                    option_condition_met = option_ltp > option_high
                    
                    if mode == "OPTIONS" or option_condition_met:
                        signal_logger.debug(f"Option Check ({side}): LTP {option_ltp:.2f} > {option_high:.2f} -> {option_condition_met}")
                else:
                    # signal_logger.debug("Option LTP not available yet")
                    pass


        # 6. Determine Final Breakout Status
        breakout_confirmed = False
        
        if mode == "SPOT":
            breakout_confirmed = spot_condition_met
        elif mode == "OPTIONS":
            breakout_confirmed = option_condition_met
        elif mode == "SPOT_OPTIONS":
            breakout_confirmed = spot_condition_met and option_condition_met

        # 7. Execute Confirmation Logic (Backtest / Live)
        if breakout_confirmed:
             logger.info(f"🔔 {mode} ENTRY BREAKOUT TRIGGERED for {side}")
             
             if BACKTESTING_MANAGER.active:
                # Configurable N-candle confirmation for backtesting
                req = getattr(self, 'entry_breakout_required_candles', ENTRY_BREAKOUT_CONFIRM_CANDLES)
                
                # Immediate entry if req <= 1
                if req <= 1:
                    signal_logger.info(f"🚀 {mode} ENTRY BREAKOUT CONFIRMED! ({side}) | {req}-Candle Confirmation")
                    self.place_entry_order(side)
                    self.reset_timer()
                    self.reset_pending_breakout()
                    self.entry_breakout_candle_count = 0
                    return

                # Multi-candle confirmation logic
                if self.entry_breakout_candle_count == 0:
                    self.entry_breakout_candle_count = 1
                    signal_logger.info(f"🕯️ CANDLE 1 DETECTED | {mode} Breakout initiated")
                    return
                
                # Increment counter on subsequent confirmations
                self.entry_breakout_candle_count += 1
                signal_logger.info(f"🕯️ CANDLE {self.entry_breakout_candle_count} CONFIRMED | Remaining in breakout zone")
                
                if self.entry_breakout_candle_count >= req:
                    signal_logger.info(f"🚀 {mode} ENTRY BREAKOUT CONFIRMED! ({side}) | {req}-Candle Confirmation Achieved")
                    self.place_entry_order(side)
                    self.entry_breakout_candle_count = 0
                    self.reset_timer()
                    self.reset_pending_breakout()
                    return
                return
             
             else: # LIVE MODE
                 if not ENABLE_SECONDS_CONFIRMATION_LIVE:
                     signal_logger.info(f"🚀 {mode} ENTRY BREAKOUT CONFIRMED! ({side}) | LIVE MODE - No seconds confirmation required")
                     self.place_entry_order(side)
                     self.reset_timer()
                     self.reset_pending_breakout()
                     return
                 
                 if not self.entry_timer_started:
                    self.start_timer(now_ts)
                    return
                 
                 elapsed = (now_ts - self.entry_timer_start_ts).total_seconds()
                 
                 if elapsed < ENTRY_CONFIRM_SECONDS:
                    signal_logger.info(f"⏳ Waiting for entry seconds breakout. Current: {elapsed:.1f}s / {ENTRY_CONFIRM_SECONDS}s")
                    return
                 
                 if elapsed >= ENTRY_CONFIRM_SECONDS:
                     signal_logger.info(f"🚀 {mode} ENTRY BREAKOUT CONFIRMED! ({side}) | Elapsed: {elapsed:.1f}s")
                     self.place_entry_order(side)
                     self.reset_timer()
                     self.reset_pending_breakout()
                     return

        else:
            # Fell back below breakout -> reset
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
        self.exit_breakout_candle_count = 0
        self.entry_breakout_candle_count = 0

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

        except Exception as e:
            # candle not available yet → keep last known LTP
            logger.error(f"Option LTP not found in DataFrame for {self.option_symbol} at {ts}: {e}")
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
            
            if symbol is None or ltp is None:
                position_logger.warning("No LTP data in tick data")
                return

            # Track last tick time for health monitoring
            self.last_ltp_time[symbol] = time.time()

            if symbol == self.option_symbol:
                self.option_ltp = float(ltp)
            elif symbol == SYMBOL:
                self.ltp = float(ltp)

            # 🎯 Get proper timestamp (simulated or real)
            current_time = get_now()
            now_str = current_time.strftime("%H:%M:%S")
            # 🎯 Only log LTP in SIMULATED mode
            if BACKTESTING_MANAGER.active:
                # logger.debug(f"LTP: {self.ltp} {timestamp}")  # DEBUG: Tick-level data (DISABLED for performance)
                ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")
            else:
                self.last_ws_tick_time = time.time()
            # ----------------------------------------
            # Update Option LTP from DataFrame in SIMULATION mode
            if BACKTESTING_MANAGER.active and (self.position or ENTRY_BREAKOUT_MODE in "SPOT_OPTIONS") and self.option_symbol:
                opt_ltp = self._update_option_ltp_from_df(current_time)
                if opt_ltp is not None:
                    self.option_ltp = opt_ltp
                    # logger.debug(f"Option LTP: {self.option_ltp} {timestamp}")  # DEBUG: Tick-level data (DISABLED for performance)
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
            if ENTRY_LTP_BREAKOUT_ENABLED and self.pending_breakout:
                # In live 1m-candle mode: handled by _breakout_1m_job (once/minute), skip per-tick
                if not (CANDLE_BREAKOUT_1M_ENABLED and not BACKTESTING_MANAGER.active):
                    self.handle_entry_ltp_breakout(current_time)
                    return              
            # ----------------------------------------
            # EXIT LTP Breakout Confirmation
            # ----------------------------------------
            if self.pending_exit_breakout:
                # In live 1m-candle mode: handled by _breakout_1m_job, skip per-tick
                if not (CANDLE_BREAKOUT_1M_ENABLED and not BACKTESTING_MANAGER.active):
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

            # ─── Broker SL Hit Detection (ALL MODES) ─────────────────────────────────
            if (position
                    and not exit_in_progress
                    and self.broker_sl is not None
                    and self.option_ltp is not None
                    and self.option_ltp < self.broker_sl):
                position_logger.warning(
                    f"⚠️ BROKER SL HIT DETECTED: option_ltp={self.option_ltp:.2f} < broker_sl={self.broker_sl:.2f} "
                    f"| Triggering exit as BROKER_SL_HIT"
                )
                with self._state_lock:
                    self.exit_in_progress = True
                
                if BACKTESTING_MANAGER.active:
                    self.place_exit_order("BROKER_SL_HIT")
                else:
                    threading.Thread(
                        target=self.place_exit_order, args=("BROKER_SL_HIT",), daemon=True
                    ).start()
                return
            # ─────────────────────────────────────────────────────────────────────────

            # If no active position or still initializing, show status and skip
            if not position or exit_in_progress or stoploss_price in (None, 0.0) or target_price in (None, 0.0):
                current_time = time.time()
                if not hasattr(self, '_last_detailed_log') or current_time - self._last_detailed_log > 30:
                    signal_logger.info(f"[{now_str}] {symbol} LTP={self.ltp:.2f} | No Active Position")
                    # logger.info(f"[{now_str}] {symbol} LTP={self.ltp:.2f} | No Active Position")
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
                logger.info(f"[{now_str}] Waiting for {tracking_mode} LTP updates...")
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

            # logger.info brief status line
            # logger.info(f"\r[{now}] {symbol} LTP={track_ltp:.2f} | {position} @ {entry_price:.2f} | P&L {unreal:.2f} | SL {stoploss_price:.2f} TG {target_price:.2f}", end="")

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
                    logger.info(f"\n[{now_str}] CPR Stoploss hit at {track_ltp:.2f}")
                    with self._state_lock:
                        self.exit_in_progress = True
                    threading.Thread(target=self.place_exit_order, args=("CPR_STOPLOSS",), daemon=True).start()
                    return
                elif cpr_result == "SL_PENDING_BREAKOUT":
                    # Waiting for LTP breakout confirmation
                    position_logger.debug(f"⏳ CPR SL detected, waiting for LTP breakout confirmation")
                    current_time = get_now()
                    if self.handle_exit_ltp_breakout(current_time):
                        return  # Continue handle_exit_ltp_breakoutonitoring, handle_exit_ltp_breakout will confirm
                elif cpr_result == "FINAL_EXIT":
                    position_logger.info(f"🎯 CPR FINAL TARGET ACHIEVED at {track_ltp:.2f}")
                    logger.info(f"\n[{now_str}] CPR Final target reached at {track_ltp:.2f}")
                    with self._state_lock:
                        self.exit_in_progress = True
                    if BACKTESTING_MANAGER.active:
                        self.place_exit_order("CPR_FINAL_TARGET")
                    else:
                        threading.Thread(target=self.place_exit_order, args=("CPR_FINAL_TARGET",), daemon=True).start()
                    return
                elif cpr_result == "TP_HIT":
                    # TP hit handled in check_cpr_sl_tp_conditions, continue monitoring
                    return
            # If TSL is enabled and the SL_TP_METHOD is Signal_candle_range_SL_TP, handle TP hits by trailing instead of exiting immediately.
            elif getattr(self, 'tsl_enabled', False) and SL_TP_METHOD == "Signal_candle_range_SL_TP":
                # TP hit -> advance TSL; SL hit -> exit
                if position == "BUY":
                    # Stoploss check (exit)
                    if track_ltp <= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} <= {stoploss_price:.2f}")
                        logger.info(f"\n[{now_str}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("STOPLOSS")
                        else:
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
                        logger.info(f"\n[{now_str}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("STOPLOSS")
                        else:
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
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("STOPLOSS")
                        else:
                            threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp >= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} >= {target_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("TARGET")
                        else:
                            threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return
                else:
                    if track_ltp >= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} >= {stoploss_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("STOPLOSS")
                        else:
                            threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp <= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} <= {target_price:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        if BACKTESTING_MANAGER.active:
                            self.place_exit_order("TARGET")
                        else:
                            threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return
        except Exception:
            logger.exception("on_ltp_update error")
            position_logger.error("Error in LTP update processing", exc_info=True)

    def subscribe_all_symbols(self):
        """
        Subscribe all required symbols after websocket reconnect with verification.
        Enhanced with immediate retry on subscribe call failures.
        """
        symbols_to_subscribe = list(self.ws_subscriptions)
        if not symbols_to_subscribe:
            return

        successfully_attempted = []  # Track which symbols to verify
        
        for sym in symbols_to_subscribe:
            max_retries = 3
            subscribed = False
            
            for attempt in range(max_retries):
                try:
                    exchange = EXCHANGE if sym == SYMBOL else OPTION_EXCHANGE
                    self.client.subscribe_ltp(
                        [{"exchange": exchange, "symbol": sym}], 
                        on_data_received=self.on_ltp_update
                    )
                    logger.info(f"📡 Subscribed {sym} (attempt {attempt+1})")
                    subscribed = True
                    successfully_attempted.append(sym)
                    break
                    
                except Exception as e:
                    logger.error(f"❌ Subscribe call failed for {sym} (attempt {attempt+1}/{max_retries}): {e}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
            
            if not subscribed:
                logger.error(f"🚨 Failed to subscribe {sym} after {max_retries} attempts")
                with self.ws_subscription_lock:
                    self.ws_subscriptions.discard(sym)
        
        # ✅ Only verify symbols that were successfully subscribed
        if successfully_attempted:
            threading.Thread(
                target=self._verify_subscriptions_async, 
                args=(successfully_attempted,),
                daemon=True
            ).start()

    def _verify_subscriptions_async(self, symbols, timeout=10, retries=5):
        """Verify that ticks are actually arriving for the given symbols."""
        time.sleep(2) # Initial wait for first ticks
        for attempt in range(retries):
            missing = []
            now = time.time()
            for sym in symbols:
                last_time = self.last_ltp_time.get(sym, 0)
                if now - last_time > timeout:
                    missing.append(sym)
            
            if not missing:
                logger.info(f"✅ All symbols verified and receiving ticks: {symbols}")
                return True
            
            logger.warning(f"⚠️ Missing ticks for {missing} (attempt {attempt+1}/{retries}). Retrying subscription...")
            for sym in missing:
                exchange = EXCHANGE if sym == SYMBOL else OPTION_EXCHANGE
                try:
                    self.client.subscribe_ltp([{"exchange": exchange, "symbol": sym}], on_data_received=self.on_ltp_update)
                except Exception as e:
                    logger.error(f"❌ Retry subscription failed for {sym}: {e}")
            time.sleep(timeout)
        
        logger.error(f"❌ FAILED to receive ticks for {missing} after {retries} retries")
        return False

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
                
                if MODE == "BACKTESTING_RANGE":
                     # Clear logs before starting
                     if CLEAR_TRADE_TABLE == True:
                        clear_trade_logs()
                     # Use the pre-indexed days from Manager
                     # Use the pre-indexed days from Manager
                     days = sorted(BACKTESTING_MANAGER.day_indices.keys())
                     if not days:
                         main_logger.error("❌ No days indexed for BACKTESTING_RANGE")
                         return
                     main_logger.warning(f"🔄 Backtesting  {len(days)} days")
                     REPLAY_CLIENT.replay_range(days, self)
                     main_logger.info("✅ Multi-day backtesting range completed. Stopping bot.")
                     self.running = False 
                     self.stop_event.set()
                else:
                    # Single day
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
                        max_initial_retries = 3
                        for attempt in range(max_initial_retries):
                            try:
                                logger.info(f"🔌 Connecting LIVE WebSocket (attempt {attempt+1}/{max_initial_retries})...")
                                self.client.connect()
                                self.ws_connected = True
                                break
                            except Exception as e:
                                logger.error(f"❌ Initial connection failed (attempt {attempt+1}): {e}")
                                if attempt < max_initial_retries - 1:
                                    time.sleep(2 ** attempt)  # Exponential backoff
                                else:
                                    logger.error("🚨 Failed to establish initial WebSocket connection after retries")
                                    return
                                
                        self.ws_connected = True
                        self.ws_retry_count = 0 # reset on success
                        self.last_ws_tick_time = time.time() # IMPORTANT: Reset heartbeat timer on reconnect!
                        
                        self.subscribe_all_symbols() # subscribe all symbols                     
                        logger.info("🟢 LIVE WebSocket connected & subscribed")
                        # -------------------------------
                        # Keep alive until disconnect
                        # -------------------------------
                        while self.ws_connected and not self.stop_event.is_set():
                            time.sleep(0.1)
                            now = time.time()
                            # Heartbeat check (global WebSocket health)
                            if self.last_ws_tick_time and now - self.last_ws_tick_time > HEARTBEAT_INTERVAL * 2:
                                logger.warning("⚠️ WebSocket heartbeat lost")
                                self.ws_connected = False
                                break
                            
                            # Individual subscription health check (Stall Detection)
                            if round(now) % 30 == 0: # Check every ~30 seconds
                                missing = []
                                active_symbols = list(self.ws_subscriptions)
                                for sym in active_symbols:
                                    last_time = self.last_ltp_time.get(sym, 0)
                                    if now - last_time > 60: # No tick for 60 seconds
                                        missing.append(sym)
                                
                                if missing:
                                    logger.warning(f"⚠️ Subscription stall detected for {missing}. Re-subscribing...")
                                    for sym in missing:
                                        exchange = EXCHANGE if sym == SYMBOL else OPTION_EXCHANGE
                                        try:
                                            self.client.subscribe_ltp([{"exchange": exchange, "symbol": sym}], on_data_received=self.on_ltp_update)
                                        except Exception as e:
                                            logger.error(f"❌ Re-subscription failed for {sym}: {e}")

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
            if MODE == "LIVE":
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
                        time.sleep(delay) 
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
                            time.sleep(delay) 
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

                logger.warning(f"⚠️ {description} returned empty on attempt {attempt} - retrying...response: {result}")
                time.sleep(delay+2) # Slightly longer delay for empty response cases
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
    def _talib_ema_apply_func(self, close: np.ndarray, period: int) -> np.ndarray:
        """
        Implementation of EMA matching pandas ewm(span=period, adjust=False).
        """
        close = np.asarray(close, dtype=np.float64).flatten()
        period = int(period)
        alpha = 2 / (period + 1)
        ema = np.empty_like(close)
        
        if len(close) > 0:
            ema[0] = close[0]
            for i in range(1, len(close)):
                ema[i] = alpha * close[i] + (1 - alpha) * ema[i - 1]
        return ema

    def _calculate_sma_numpy(self, close: np.ndarray, period: int) -> np.ndarray:
        """Vectorized SMA calculation using numpy."""
        ret = np.cumsum(close, dtype=float)
        ret[period:] = ret[period:] - ret[:-period]
        return ret[period - 1:] / period

    def _calculate_rsi_numpy(self, close: np.ndarray, period: int) -> np.ndarray:
        """Vectorized RSI calculation matches standard logic."""
        delta = np.diff(close)
        gain = np.where(delta > 0, delta, 0.0)
        loss = np.where(delta < 0, -delta, 0.0)

        avg_gain = np.full_like(close, np.nan)
        avg_loss = np.full_like(close, np.nan)

        if len(gain) >= period:
            avg_gain[period] = np.mean(gain[:period])
            avg_loss[period] = np.mean(loss[:period])

            # Use simple loop for wilder's smoothing to match standard RSI
            for i in range(period + 1, len(close)):
                avg_gain[i] = (avg_gain[i - 1] * (period - 1) + gain[i - 1]) / period
                avg_loss[i] = (avg_loss[i - 1] * (period - 1) + loss[i - 1]) / period

        rs = avg_gain / np.where(avg_loss == 0, 1e-10, avg_loss)
        rsi = 100 - (100 / (1 + rs))
        return rsi

    def _precalculate_strategy_indicators(self):
        """
        Pre-calculate strategy indicators (EMA, SMA, RSI) for the entire BACKTESTING_RANGE.
        Stores results in self.dynamic_indicators_cache keyed by the timestamp of the last completed candle.
        """
        if not BACKTESTING_MANAGER.active:
            return

        # Access the strategy timeframe data (generic naming as requested)
        # Using 5m as default source per existing logic, but generalizing naming
        df = BACKTESTING_MANAGER.df_strategy_5m
        
        if df is None or df.empty:
            logger.warning("No strategy data (5m) available for pre-calculation.")
            return
            
        # Ensure TALIB_AVAILABLE is accessible or defined
        global TALIB_AVAILABLE 

        logger.info("⚡ Pre-calculating strategy indicators for backtesting...")
        
        # Convert to numpy arrays
        close = df['close'].to_numpy(dtype=np.float64)
        open_ = df['open'].to_numpy(dtype=np.float64)
        high = df['high'].to_numpy(dtype=np.float64)
        low = df['low'].to_numpy(dtype=np.float64)
        if isinstance(df.index, pd.DatetimeIndex):
            timestamps = df.index
        else:
            # If index is not datetime (e.g. range index), try 'timestamp' column or convert
            if 'timestamp' in df.columns:
                 timestamps = pd.to_datetime(df['timestamp'])
            else:
                 # Attempt to convert index
                 timestamps = pd.to_datetime(df.index)

        cache = {}
        
        # Pre-calculate indicators using vectorized functions
        indicators = {}
        
        # EMAs
        for p in EMA_PERIODS:
            try:
                # Use talib if available
                if TALIB_AVAILABLE:
                    indicators[f"EMA_{p}"] = self._talib_ema_apply_func(close, p)
                else:
                    # Fallback to pandas/numpy if desired, but _talib_ema_apply_func is currently implemented 
                    # with specific logic. Assuming it handles fallback or calculation.
                    # Actually _talib_ema_apply_func likely USES talib if available or pandas.
                    # Let's just call it.
                     indicators[f"EMA_{p}"] = self._talib_ema_apply_func(close, p)
            except Exception as e:
                logger.error(f"Error pre-calculating EMA_{p}: {e}")

        # SMAs
        for p in SMA_PERIODS:
            sma = np.full_like(close, np.nan)
            cs = np.cumsum(close)
            # Fix: use 'p' instead of 'period'
            cs[p:] = cs[p:] - cs[:-p]
            sma[p-1:] = cs[p-1:] / p
            indicators[f"SMA_{p}"] = sma

        # RSIs
        for p in RSI_PERIODS:
            indicators[f"RSI_{p}"] = self._calculate_rsi_numpy(close, p)

        # 🎯 STORE RSI ARRAYS FOR RSI_STRATEGY (full history for series reconstruction)
        if RSI_STRATEGY:
            self._precalc_rsi_arrays = indicators.copy()

        # Previous Candle OHLC (for logic that uses 'previous candle' relative to the *last completed*)
        # compute_dynamic_indicators returns "last_candle": df.iloc[-2].to_dict()
        # So for a row at index `i`, "last_candle" is the candle at `i`.
        # Wait, compute_dynamic_indicators(df) where df is sliced to current time.
        # It takes `df.iloc[-2]`.
        # If we are looking up the cache for `target_ts`, that `target_ts` IS the timestamp of `df.iloc[-2]`.
        # So the values we store for `target_ts` should be the values calculated AT `target_ts`.
        # Correct.

        # Populate cache
        # Each row i corresponds to a timestamp T. 
        # The indicators calculated at index i are valid for that completed candle T.
        
        current_date_tracker = None
        # Delayed trackers to match legacy `iloc[:-3]` logic (Excludes Signal Candle AND Prev Candle)
        day_high_current = -float('inf')
        day_low_current = float('inf')
        
        day_high_prev = -float('inf')
        day_low_prev = float('inf')
        
        day_open = 0.0

        for i, ts in enumerate(timestamps):
             # 1. Tracker for Previous Candle
             if i > 0:
                 prev_row = {
                     "open": float(open_[i-1]),
                     "high": float(high[i-1]),
                     "low": float(low[i-1]),
                     "close": float(close[i-1]),
                     "volume": 0,
                     "date": timestamps[i-1]
                 }
             else:
                 prev_row = {} 
             
             # 2. Tracker for Day High/Low
             t_date = ts.date()
             if t_date != current_date_tracker:
                  current_date_tracker = t_date
                  # Reset for new day
                  day_high_current = -float('inf')
                  day_low_current = float('inf')
                  day_high_prev = -float('inf')
                  day_low_prev = float('inf')
                  day_open = float(open_[i])
             
             # Capture stats BEFORE updating (using PREV state to achieve lag)
             # Matches `iloc[:-3]` on DF [..., Prev, Signal, Forming]
             # Which excludes Signal (i) and Prev (i-1).
             # So we use stats from 0..i-2.
             
             dh_val = day_high_prev
             dl_val = day_low_prev
             
             row_data = {
                 "last_candle": {
                    "open": float(open_[i]),
                    "high": float(high[i]),
                    "low": float(low[i]),
                    "close": float(close[i]),
                    "volume": 0,
                    "date": ts
                 },
                 "prev_candle": prev_row,
                 "current_day_open": day_open,
                 "current_day_high": dh_val if dh_val != -float('inf') else None,
                 "current_day_low": dl_val if dl_val != float('inf') else None
             }

             # Update running stats
             # 1. Push current state to prev (for next iteration's use)
             day_high_prev = day_high_current
             day_low_prev = day_low_current
             
             # 2. Update current state with this candle i
             day_high_current = max(day_high_current, float(high[i]))
             day_low_current = min(day_low_current, float(low[i]))

             # Add calculated indicators
             for name, arr in indicators.items():
                val = arr[i]
                if not np.isnan(val):
                    row_data[name] = float(val)
                else:
                    row_data[name] = None

             # Mirror compute_dynamic_indicators: RSI_PREV_{p} should represent the candle
             # immediately before the cached "last_candle" timestamp.
             for p in RSI_PERIODS:
                 prev_val = indicators[f"RSI_{p}"][i - 4] if i > 0 else np.nan
                 if not np.isnan(prev_val):
                     row_data[f"RSI_PREV_{p}"] = float(prev_val)
                 else:
                     row_data[f"RSI_PREV_{p}"] = None
            
             cache[ts] = row_data

        self.dynamic_indicators_cache = cache
        logger.info(f"✅ Pre-calculation complete. Cached {len(cache)} candles.")

    def _build_aligned_rsi_series_from_precalc(self, period: int, target_index: pd.Index) -> pd.Series:
        """
        Build RSI series aligned to target_index from pre-calculated RSI arrays.
        This keeps RSI history capped to the current intraday context instead of
        taking values from the end of the full backtest range.
        """
        if target_index is None or len(target_index) == 0:
            return pd.Series(dtype=float)

        base = pd.Series(np.nan, index=target_index, dtype=float)
        rsi_key = f"RSI_{period}"

        if not hasattr(self, "_precalc_rsi_arrays") or rsi_key not in self._precalc_rsi_arrays:
            return base

        try:
            rsi_arr = np.asarray(self._precalc_rsi_arrays[rsi_key], dtype=float)
            source_df = BACKTESTING_MANAGER.df_strategy_5m

            if source_df is None or source_df.empty or "timestamp" not in source_df.columns:
                # Fallback: align from the start (never from tail).
                n = min(len(rsi_arr), len(base))
                if n > 0:
                    base.iloc[:n] = rsi_arr[:n]
                return base

            source_index = pd.DatetimeIndex(pd.to_datetime(source_df["timestamp"], errors="coerce"))

            n = min(len(source_index), len(rsi_arr))
            if n == 0:
                return base

            source_index = source_index[:n]
            rsi_arr = rsi_arr[:n]

            valid_mask = ~source_index.isna()
            if not np.all(valid_mask):
                source_index = source_index[valid_mask]
                rsi_arr = rsi_arr[valid_mask]

            if isinstance(base.index, pd.DatetimeIndex):
                if base.index.tz is not None:
                    if source_index.tz is None:
                        source_index = source_index.tz_localize(base.index.tz)
                    else:
                        source_index = source_index.tz_convert(base.index.tz)
                elif source_index.tz is not None:
                    source_index = source_index.tz_convert(None)

            full_series = pd.Series(rsi_arr, index=source_index, dtype=float)
            full_series = full_series[~full_series.index.duplicated(keep="last")]
            return full_series.reindex(base.index)

        except Exception as e:
            indicators_logger.warning(f"Failed to align RSI_{period} from pre-calc cache: {e}")
            try:
                rsi_arr = np.asarray(self._precalc_rsi_arrays[rsi_key], dtype=float)
                n = min(len(rsi_arr), len(base))
                if n > 0:
                    base.iloc[:n] = rsi_arr[:n]
            except Exception:
                pass
            return base

    def get_intraday(self, days=LOOKBACK_DAYS, symbol=SYMBOL, exchange=EXCHANGE, timeframe=CANDLE_TIMEFRAME, instrument_type="SPOT") -> pd.DataFrame:
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
            
            order_logger.info(f"Fetching intraday [{MODE}]: {symbol} | {exchange} | {instrument_type} | {timeframe} | {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
            
            # ==========================================================
            # 🧠 SIMULATION MODE
            # ==========================================================
            if BACKTESTING_MANAGER.active:
                try:
                    # 🚀 OPTIMIZATION (Phase 1): Serve SPOT data from RAM
                    if instrument_type == "SPOT":
                        df_slice = BACKTESTING_MANAGER.get_data_slice(end_date)
                        
                        if not df_slice.empty:
                            # ✅ Compute current-day levels using helper
                            try:
                                day_open, day_high, day_low = self.get_current_day_highlow(df_slice)
                                if day_open and day_high and day_low:
                                    self.current_day_open = day_open
                                    self.current_day_high = day_high
                                    self.current_day_low = day_low
                                    data_logger.info(f"📊 [SIM-RAM] Current Day Levels | O={day_open:.2f} H={day_high:.2f} L={day_low:.2f}")
                            except Exception as e:
                                data_logger.error(f"Error computing day high/low in SIM-RAM mode: {e}", exc_info=True)
                                
                            # data_logger.debug(f"⚡ Served {len(df_slice)} rows from RAM slice")
                            return df_slice
                        else:
                            data_logger.warning("RAM slice empty, falling back to DB...")

                    # 🚀 OPTIMIZATION (Phase 3): Serve OPTION data from JIT Cache
                    if instrument_type == "OPTIONS":
                        sim_date = get_now()
                        # Checks BacktestingManager.option_cache (supports JIT)
                        cached_opt = BACKTESTING_MANAGER.get_option_data_slice(symbol, CANDLE_TIMEFRAME, sim_date)
                        
                        if cached_opt is not None and not cached_opt.empty:
                            # data_logger.info(f"⚡ [SIM-RAM] Served Option {symbol} from cache")
                            return cached_opt
                        else:
                            # Fallback: Process will continue to typical fetch, but we MUST cache the result
                            pass 

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
                    # 🚀 OPTIMIZATION (Phase 3): Cache the fetched Result for Options
                    if instrument_type == "OPTIONS":
                         BACKTESTING_MANAGER.cache_option_data(symbol, CANDLE_TIMEFRAME, df_full)

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
                    # data_logger.info(f"[LIVE MODE] Fetching intraday {symbol} {exchange} {instrument_type} {timeframe} | {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
                    
                    raw = self.retry_api_call(
                        func=base_client.history,
                        max_retries=10,
                        delay=10,
                        description="Intraday History Fetch",
                        symbol=symbol,
                        exchange=exchange,
                        interval=timeframe,
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
    def compute_static_indicators(self, symbol=SYMBOL, exchange=EXCHANGE, instrument_type="spot"):
        """
        Compute static CPR indicators for all enabled timeframes.
        🎯 Works identically in LIVE and SIMULATION modes
        CPR type classification (narrow/wide/etc.) is computed only for instrument_type='spot'.
        """
        try:
            # 🎯 Use get_now() for proper date handling
            current_time = get_now().time()
             # 🛑 TIME CHECK: strict cutoff for new entries
            if current_time < MARKET_OPEN:
                # indicators_logger.warning(f"⛔ Indicator calculation skipped: Current time {current_time.strftime('%H:%M:%S')} < Trade Start_time {TRADE_START_TIME.strftime('%H:%M:%S')}")
                return False
            current_time = get_now()
            self.static_indicators = {}
            indicators_logger.info("╔══════════════════════════════")
            indicators_logger.info("🔍 INITIALIZING MULTI-TIMEFRAME CPR COMPUTATION")
            indicators_logger.info(f"📐 Enabled CPR Timeframes: {', '.join(CPR)}")
            indicators_logger.info("╚══════════════════════════════")

            # DAILY CPR
            if 'DAILY' in CPR:
                dfdaily = self.get_historical("D", lookback=10, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['DAILY'] = self.compute_cpr(dfdaily, "DAILY")
                self.static_indicators_date = current_time.date()
                
            # WEEKLY CPR
            if 'WEEKLY' in CPR:
                dfweekly = self.get_historical("W", lookback=20, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['WEEKLY'] = self.compute_cpr(dfweekly, "WEEKLY")
                
            # MONTHLY CPR
            if 'MONTHLY' in CPR:
                dfmonthly = self.get_historical("M", lookback=70, symbol=symbol, exchange=exchange, instrument_type=instrument_type)
                self.static_indicators['MONTHLY'] = self.compute_cpr(dfmonthly, "MONTHLY")

            # ── CPR Type Classification (SPOT only) ──────────────────────────────
            if instrument_type.lower() == "spot":
                daily_ind  = self.static_indicators.get('DAILY')
                weekly_ind = self.static_indicators.get('WEEKLY')

                daily_cpr_type  = None
                weekly_cpr_type = None

                # Daily CPR type
                if daily_ind and daily_ind.get('prev_top') is not None:
                    try:
                        daily_cpr_type = self.get_cpr_type(
                            pdt=daily_ind['prev_top'],
                            pdc=daily_ind['prev_center'],
                            pdl=daily_ind['prev_low'],
                            cdt=daily_ind['tc'],
                            cdc=daily_ind['pivot'],
                            cdl=daily_ind['bc'],
                        )
                        daily_ind['cpr_type'] = daily_cpr_type
                        indicators_logger.info(
                            f"📊 DAILY CPR Type: {daily_cpr_type} "
                            f"(CDT={daily_ind['tc']:.2f}, CDC={daily_ind['pivot']:.2f}, CDL={daily_ind['bc']:.2f} "
                            f"vs PDT={daily_ind['prev_top']:.2f}, PDC={daily_ind['prev_center']:.2f}, PDL={daily_ind['prev_low']:.2f})"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ Daily CPR type error: {e}")

                # Weekly CPR type
                if weekly_ind and weekly_ind.get('prev_top') is not None:
                    try:
                        weekly_cpr_type = self.get_cpr_type(
                            pdt=weekly_ind['prev_top'],
                            pdc=weekly_ind['prev_center'],
                            pdl=weekly_ind['prev_low'],
                            cdt=weekly_ind['tc'],
                            cdc=weekly_ind['pivot'],
                            cdl=weekly_ind['bc'],
                        )
                        weekly_ind['cpr_type'] = weekly_cpr_type
                        indicators_logger.info(
                            f"📊 WEEKLY CPR Type: {weekly_cpr_type}"
                            f"(CWT={weekly_ind['tc']:.2f}, CWC={weekly_ind['pivot']:.2f}, CWL={weekly_ind['bc']:.2f} "
                            f"vs PWT={weekly_ind['prev_top']:.2f}, PWC={weekly_ind['prev_center']:.2f}, PWL={weekly_ind['prev_low']:.2f})"
                        )
                    except Exception as e:
                        logger.warning(f"⚠️ Weekly CPR type error: {e}")

                # Persist to SQLite
                try:
                    cpr_db_data = {
                        "trade_date": str(current_time.date()),
                        "symbol": symbol,
                        "daily_cpr_type":  daily_cpr_type,
                        "weekly_cpr_type": weekly_cpr_type,
                        # Current day CPR
                        "cd_top":    daily_ind['tc']     if daily_ind else None,
                        "cd_center": daily_ind['pivot']  if daily_ind else None,
                        "cd_low":    daily_ind['bc']     if daily_ind else None,
                        # Previous day CPR
                        "pd_top":    daily_ind.get('prev_top')    if daily_ind else None,
                        "pd_center": daily_ind.get('prev_center') if daily_ind else None,
                        "pd_low":    daily_ind.get('prev_low')    if daily_ind else None,
                        # Current week CPR
                        "cw_top":    weekly_ind['tc']    if weekly_ind else None,
                        "cw_center": weekly_ind['pivot'] if weekly_ind else None,
                        "cw_low":    weekly_ind['bc']    if weekly_ind else None,
                        # Previous week CPR
                        "pw_top":    weekly_ind.get('prev_top')    if weekly_ind else None,
                        "pw_center": weekly_ind.get('prev_center') if weekly_ind else None,
                        "pw_low":    weekly_ind.get('prev_low')    if weekly_ind else None,
                    }
                    log_cpr_market_data(cpr_db_data)
                    indicators_logger.info(
                        f"💾 CPR types saved to DB | Date: {current_time.date()} "
                        f"| Daily: {daily_cpr_type} | Weekly: {weekly_cpr_type}"
                    )
                except Exception as e:
                    logger.warning(f"⚠️ Failed to persist CPR types to DB: {e}")

            # Combined summary
            logger.info("╔═══════════════════════════")
            logger.info("📘 CPR SUMMARY OVERVIEW (All Active Timeframes)")
            for key, val in self.static_indicators.items():
                if val:
                    cpr_type_str = f" | CPR Type: {val['cpr_type']}" if val.get('cpr_type') else ""
                    logger.info(
                        f"{key:<8} | Pivot:{val['pivot']:.2f} | BC:{val['bc']:.2f} | TC:{val['tc']:.2f} | Width:{val['cpr_range']:.2f}{cpr_type_str}"
                    )
            logger.info("╚═══════════════════════════")
            return True

        except Exception as e:
            logger.error(f"❌ Error computing static indicators: {e}", exc_info=True)
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
                    max_retries=10,
                    delay=10,
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
                df = df.resample("W-SUN",  label="right", closed="right").agg({
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum"
                }).dropna()

            elif interval == "M":
                df = df.resample("ME").agg({
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
    @staticmethod
    def get_cpr_type(pdt, pdc, pdl, cdt, cdc, cdl):
        """
        Classifies the CPR width of the current day/period relative to the previous day/period.
        pdt: Previous period Top CPR (TC)
        pdc: Previous period Center CPR (Pivot)
        pdl: Previous period Low CPR (BC)
        cdt: Current period Top CPR (TC)
        cdc: Current period Center CPR (Pivot)
        cdl: Current period Low CPR (BC)

        Returns: 'extreme_narrow', 'narrow', 'wide', 'extreme_wide', or 'normal'
        """
        pd_full = abs(pdt - pdl)   # full width of previous period CPR
        pd_half = abs(pdt - pdc)   # half width of previous period CPR (top to center)
        cd_full = abs(cdt - cdl)   # full width of current period CPR
        cd_half = abs(cdt - cdc)   # half width of current period CPR (top to center)

        ratio = cd_full / pd_full if pd_full != 0 else 1

        if ratio <= 0.5:
            return "extreme_narrow"
        elif ratio <= 1.0:
            return "narrow"
        elif ratio >= 1.5:
            return "extreme_wide"
        elif ratio > 1.0:
            return "wide"
        else:
            return "normal"
        
        # Derived from vectorized logic:
        # Extreme Narrow: cd_full < pd_half
        # Narrow: pd_full >= cd_full AND cd_full >= pd_half (and not extreme_narrow)
        # Extreme Wide: cd_half >= pd_full
        # Wide: cd_full >= pd_full AND cd_half <= pd_full (and not extreme_wide)
        
        # # 1. Extreme Narrow
        # if cd_full < pd_half:
        #     return "extreme_narrow"
            
        # # 2. Extreme Wide
        # if cd_half >= pd_full:
        #     return "extreme_wide"
            
        # # 3. Narrow
        # # mask: (pd_full >= cd_full) & (cd_full <= pd_full) & (cd_full >= pd_half) & (~extreme_narrow)
        # # Simplified: pd_full >= cd_full >= pd_half
        # if pd_full >= cd_full and cd_full >= pd_half:
        #     return "narrow"
            
        # # 4. Wide
        # # mask: (cd_full >= pd_full) & (pd_full <= cd_full) & (cd_half <= pd_full) & (~extreme_wide)
        # # Simplified: cd_full >= pd_full and cd_half <= pd_full
        # if cd_full >= pd_full and cd_half <= pd_full:
        #     return "wide"
            
        # return "normal"
    #--------------------------
    # CPR - Daily, Weekly, Monthly
    #--------------------------     
    def compute_cpr(self, df, label: str):
        """Generic CPR computation used by daily, weekly, monthly methods.
        Also extracts the previous period's CPR band (prev_top, prev_center, prev_low)
        from df.iloc[-3] so callers can compute CPR type classification.
        """
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
        
        # ✅ Validate and fix BC/TC if swapped (BC should be <= TC)
        if bc > tc:
            bc, tc = tc, bc
            # indicators_logger.warning(f"⚠️ {label.upper()} CPR: BC > TC detected - values swapped. BC={bc:.2f}, TC={tc:.2f}")
        
        cpr_range = abs(tc - bc)
        r1 = (2 * pivot) - low
        s1 = (2 * pivot) - high
        r2 = pivot + (high - low)
        s2 = pivot - (high - low)
        r3 = high + 2 * (pivot - low)
        s3 = low - 2 * (high - pivot)
        r4 = r3 + (r2 - r1)
        s4 = s3 - (s1 - s2)

        # ── Previous period CPR (for CPR type classification) ────────────────
        # df.iloc[-2] = current period's source (yesterday / last week)
        # df.iloc[-3] = previous period's source (day-before-yesterday / week-before-last)
        prev_top = prev_center = prev_low = None
        if len(df) >= 3:
            pp = df.iloc[-3]   # previous period row
            pp_high, pp_low, pp_close = pp['high'], pp['low'], pp['close']
            pp_pivot = (pp_high + pp_low + pp_close) / 3
            pp_bc    = (pp_high + pp_low) / 2
            pp_tc    = 2 * pp_pivot - pp_bc
            
            # ✅ Validate and fix BC/TC if swapped (BC should be <= TC)
            if pp_bc > pp_tc:
                pp_bc, pp_tc = pp_tc, pp_bc
                # indicators_logger.warning(f"⚠️ {label.upper()} Previous Period CPR: BC > TC detected - values swapped. BC={pp_bc:.2f}, TC={pp_tc:.2f}")
            
            prev_top    = pp_tc
            prev_center = pp_pivot
            prev_low    = pp_bc
            indicators_logger.info(
                f"📅 {label.upper()} Prev Period CPR | TC:{pp_tc:.2f} | Pivot:{pp_pivot:.2f} | BC:{pp_bc:.2f}"
            )

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
            "timestamp": df.index[index_df],
            # Previous period CPR band (None if insufficient history)
            "prev_top":    prev_top,
            "prev_center": prev_center,
            "prev_low":    prev_low,
            "cpr_type":    None,   # filled in by compute_static_indicators for spot
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
        now_dt = get_now()
        # ---------------------------
        # ⚡ CACHE CHECK (Backtesting)
        # ---------------------------
        if BACKTESTING_MANAGER.active and self.dynamic_indicators_cache:
             if len(intraday_df) >= 2:
                 # The strategy uses the last completed candle (index -2)
                 target_ts = intraday_df.index[-2] 
                 if target_ts in self.dynamic_indicators_cache:
                      cached = self.dynamic_indicators_cache[target_ts]
                      # 🛡️ VALIDATION: Ensure we are serving cache for the same instrument (SPOT)
                      # by comparing Close price. Avoids serving Spot cache for Options.
                      cached_close = cached["last_candle"]["close"]
                      current_close = intraday_df["close"].iloc[-2]
                      if now_dt.time() == IB_CUTOFF_TIME:
                        logger.info(f"⏰ Triggering time-based condition update at {now_dt}")
                        self.ib_high = self.current_day_high
                        self.ib_low = self.current_day_low
                      if abs(cached_close - current_close) < 0.1:
                           # ✅ For RSI_STRATEGY: Build RSI_series from cached RSI values and intraday_df
                           # No need to recalculate - just use pre-calc values from cache
                           if RSI_STRATEGY:
                               for p in RSI_PERIODS:
                                   cached[f"RSI_{p}_series"] = self._build_aligned_rsi_series_from_precalc(
                                       p, intraday_df.index
                                   )
                           return cached

        df = intraday_df.copy()
        for col in ["open", "high", "low", "close"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")

        # Need at least 3 candles to have prev & last (last completed) reliably
        if len(df) < 3:
            indicators_logger.warning(f"Insufficient data for indicators: {len(df)} candles (need at least 3)")
            return dyn

        indicators_logger.debug(f"Computing dynamic indicators on {len(df)} candles")

        # Extract close series as a numpy array for fast computation
        close_array = df["close"].values

        # Compute EMAs
        for p in EMA_PERIODS:
            ema_arr = self._talib_ema_apply_func(close_array, p)
            dyn[f"EMA_{p}"] = float(ema_arr[-2]) if len(ema_arr) >= 2 else 0.0

        # Compute SMAs
        for p in SMA_PERIODS:
            if len(close_array) >= p + 1:
                sma_arr = self._calculate_sma_numpy(close_array, p)
                dyn[f"SMA_{p}"] = float(sma_arr[-2])
            else:
                dyn[f"SMA_{p}"] = 0.0

        # Compute RSI
        for p in RSI_PERIODS:
            if len(close_array) >= p + 2:
                # 🏃 OPTIMIZATION: Reuse pre-calculated RSI arrays if available in backtesting
                if BACKTESTING_MANAGER.active and RSI_STRATEGY and hasattr(self, '_precalc_rsi_arrays') and f"RSI_{p}" in self._precalc_rsi_arrays:
                    # Use pre-calculated RSI array to save computation time
                    try:
                        rsi_series = self._build_aligned_rsi_series_from_precalc(p, df.index)
                        dyn[f"RSI_{p}_series"] = rsi_series

                        rsi_curr = rsi_series.iloc[-2] if len(rsi_series) >= 2 else np.nan
                        rsi_prev = rsi_series.iloc[-3] if len(rsi_series) >= 3 else np.nan
                        dyn[f"RSI_{p}"] = float(rsi_curr) if not pd.isna(rsi_curr) else 0.0
                        dyn[f"RSI_PREV_{p}"] = float(rsi_prev) if not pd.isna(rsi_prev) else 0.0
                        continue  # Skip recalculation
                    except Exception as e:
                        indicators_logger.warning(f"Could not reuse pre-calc RSI_{p}: {e}. Recalculating...")
                
                # Fallback: Calculate RSI normally (live mode or pre-calc not available)
                rsi_arr = self._calculate_rsi_numpy(close_array, p)
                dyn[f"RSI_{p}_series"] = pd.Series(rsi_arr, index=df.index)
                dyn[f"RSI_{p}"] = float(rsi_arr[-2])
                dyn[f"RSI_PREV_{p}"] = float(rsi_arr[-3])
            else:
                dyn[f"RSI_{p}_series"] = pd.Series([float('nan')]*len(close_array), index=df.index)
                dyn[f"RSI_{p}"] = 0.0
                dyn[f"RSI_PREV_{p}"] = 0.0
        
        # ✅ Ensure RSI_STRATEGY has all required series (for backtesting with dynamic entry)
        if RSI_STRATEGY:
            for p in RSI_PERIODS:
                if f"RSI_{p}_series" not in dyn:
                    dyn[f"RSI_{p}_series"] = pd.Series([float('nan')]*len(close_array), index=df.index)

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

        logger.debug(
            f"📊 Current Day Levels | O={day_open} H={day_high} L={day_low}"
            )

        # Comprehensive logging of computed indicators
        logger.debug("=== Dynamic Indicators Computed ===")
        
        # Log EMA values
        ema_values = []
        for p in EMA_PERIODS:
            if f"EMA_{p}" in dyn:
                ema_values.append(f"EMA_{p}={dyn[f'EMA_{p}']:.2f}")
                # logger.debug(f"EMA Values: {', '.join(ema_values)}")
        
        # Log SMA values  
        sma_values = []
        for p in SMA_PERIODS:
            if f"SMA_{p}" in dyn:
                sma_values.append(f"SMA_{p}={dyn[f'SMA_{p}']:.2f}")
                # logger.debug(f"SMA Values: {', '.join(sma_values)}")
        
        # Log RSI values
        rsi_values = []
        for p in RSI_PERIODS:
            if f"RSI_{p}" in dyn:
                rsi_values.append(f"RSI_{p}={dyn[f'RSI_{p}']:.2f}")
                # logger.debug(f"RSI Values: {', '.join(rsi_values)}")

        # Log Dynamic data
        lc = dyn.get("last_candle", {})
        pc = dyn.get("prev_candle", {})
        if lc:
            logger.debug(f"Last Candle: O={lc.get('open',0):.2f}, H={lc.get('high',0):.2f}, L={lc.get('low',0):.2f}, C={lc.get('close',0):.2f}")
        if pc:
            logger.debug(f"Prev Candle: O={pc.get('open',0):.2f}, H={pc.get('high',0):.2f}, L={pc.get('low',0):.2f}, C={pc.get('close',0):.2f}")

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
            signal_logger.info(f"SL Level: {self.stoploss_price}, TP Level: {self.target_price}")
            
            
            dyn_option = self.compute_dynamic_indicators(intraday_df_opt)
            dyn = self.compute_dynamic_indicators(intraday_df)
            # Cache dynamic indicators and last close for reuse by CPR/TSL checks
            try:
                self.cached_dyn_spot = dyn
                self.cached_dyn_option = dyn_option
                spot_candle = dyn.get("last_candle", {})
                self.cached_last_close = float(spot_candle.get("close", 0.0))
            except Exception:
                self.cached_dyn_spot = None
                self.cached_dyn_option = None
                self.cached_last_close = None
            
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
                
                # Dynamic Reversal Check
                reversal_triggered = False
                is_spot_ema_exit = False
                is_opt_ema_exit = False
                if REVERSAL_MODE == "SPOT":
                    reversal_triggered = False
                    is_spot_ema_exit = spot_close < spot_ema20
                elif REVERSAL_MODE == "OPTIONS":
                    reversal_triggered = is_opt_reversal
                    is_opt_ema_exit = opt_close < opt_ema20
                elif REVERSAL_MODE == "SPOT_OPTIONS":
                    reversal_triggered = is_spot_reversal or is_opt_reversal
                    is_spot_ema_exit = spot_close < spot_ema20
                    is_opt_ema_exit = opt_close < opt_ema20
                
                if reversal_triggered:
                    if EXIT_LTP_BREAKOUT_ENABLED:
                        now = get_now()
                        mode = EXIT_LTP_BREAKOUT_MODE.upper()
                        payload = {
                            "type": "EXIT_BUY",
                            "trigger": "REVERSAL",
                            "mode": mode,
                            "reason": "REVERSAL_CANDLE_SPOT_CALL_SIDE"
                        }
                        # Include spot thresholds when mode allows SPOT
                        if mode in ("SPOT", "SPOT_OPTIONS"):
                            payload.update({
                                "low": float(spot_low - SL_BUFFER_POINT),
                                "high": float(spot_high + SL_BUFFER_POINT),
                                "reason": "REVERSAL_CANDLE_SPOT_CALL_SIDE"
                            })
                        # Include option thresholds when mode allows OPTIONS
                        if mode in ("OPTIONS", "SPOT_OPTIONS"):
                            payload.update({
                                "option_low": float(opt_low - SL_BUFFER_POINT),
                                "option_high": float(opt_high + SL_BUFFER_POINT),
                                "reason": "REVERSAL_CANDLE_OPTIONS_CALL_SIDE"
                            })
                        self.pending_exit_breakout = payload
                        minute = now.minute
                        next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                        self.pending_exit_breakout_expiry = (
                            now.replace(minute=0, second=0, microsecond=0)
                            + timedelta(minutes=next_multiple)
                        )
                        self.exit_breakout_side = "EXIT_BUY"
                        signal_logger.info(
                            f"⏳ EXIT BUY waiting for {self.pending_exit_breakout['reason']} LTP breakout < {self.pending_exit_breakout.get('low', 0):.2f} "
                            f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                        )
                        return None
                    signal_logger.info("🔴 EXIT: Reversal candle confirmed (CALL)")
                    return "REVERSAL_CANDLE_FORMATION"
                
                if REVERSAL_MODE != "NONE":   
                    # signal_logger.info(f"CALL Exit Check Spot: Close < EMA20 -> {spot_close:.2f} < {spot_ema20:.2f} = {is_spot_ema_exit}; CALL Exit Check Option: Close < EMA20 -> {opt_close:.2f} < {opt_ema20:.2f} = {is_opt_ema_exit}") 
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: EMA20 CROSSOVER (CALL Position)")
                    if is_spot_ema_exit:
                        return f"SPOT_CLOSE_BELOW_EMA20"
                    if is_opt_ema_exit:
                        return f"OPTION_CLOSE_BELOW_EMA20" 
            elif self.position == "SELL":
                signal_logger.info(f"Reversal Candle Check for PUT Position")
                is_spot_reversal = spot_prev_close > spot_prev_open and spot_close > spot_prev_high and spot_close > spot_open
                is_opt_reversal = opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open
                signal_logger.info(f"PUT Exit Check Spot: spot_prev_close > spot_prev_open and spot_close > spot_prev_high and spot_close > spot_open  -> {spot_prev_close:.2f} > {spot_prev_open:.2f} and {spot_close:.2f} > {spot_prev_high:.2f} and {spot_close:.2f} > {spot_open:.2f} = {is_spot_reversal}")
                signal_logger.info(f"PUT Exit Check Option: opt_prev_close < opt_prev_open and opt_close < opt_prev_low and opt_close < opt_open -> {opt_prev_close:.2f} < {opt_prev_open:.2f} and {opt_close:.2f} < {opt_prev_low:.2f} and {opt_close:.2f} < {opt_open:.2f} = {is_opt_reversal}") 
                
                # Dynamic Reversal Check
                reversal_triggered = False
                is_spot_ema_exit = False
                is_opt_ema_exit = False
                if REVERSAL_MODE == "SPOT":
                    reversal_triggered = False
                    is_spot_ema_exit = spot_close > spot_ema20
                elif REVERSAL_MODE == "OPTIONS":
                    reversal_triggered = is_opt_reversal
                    is_opt_ema_exit = opt_close < opt_ema20
                elif REVERSAL_MODE == "SPOT_OPTIONS":
                    reversal_triggered = is_spot_reversal or is_opt_reversal
                    is_spot_ema_exit = spot_close > spot_ema20
                    is_opt_ema_exit = opt_close < opt_ema20
                if reversal_triggered:
                    if EXIT_LTP_BREAKOUT_ENABLED:
                        now = get_now()
                        mode = EXIT_LTP_BREAKOUT_MODE.upper()
                        payload = {
                            "type": "EXIT_SELL",
                            "trigger": "REVERSAL",
                            "mode": mode,
                            "reason": "REVERSAL_CANDLE_SPOT_PUT_SIDE"
                        }
                        # Include spot thresholds when mode allows SPOT
                        if mode in ("SPOT", "SPOT_OPTIONS"):
                            payload.update({
                                "high": float(spot_high + SL_BUFFER_POINT),
                                "low": float(spot_low - SL_BUFFER_POINT),
                                "reason": "REVERSAL_CANDLE_SPOT_PUT_SIDE"
                            })
                        # Include option thresholds when mode allows OPTIONS
                        if mode in ("OPTIONS", "SPOT_OPTIONS"):
                            payload.update({
                                "option_low": float(opt_low - SL_BUFFER_POINT),
                                "option_high": float(opt_high + SL_BUFFER_POINT),
                                "reason": "REVERSAL_CANDLE_OPTIONS_PUT_SIDE"
                            })
                        self.pending_exit_breakout = payload
                        minute = now.minute
                        next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                        self.pending_exit_breakout_expiry = (
                            now.replace(minute=0, second=0, microsecond=0)
                            + timedelta(minutes=next_multiple)
                        )
                        self.exit_breakout_side = "EXIT_SELL"
                        signal_logger.info(
                            f"⏳ EXIT SELL waiting for {self.pending_exit_breakout['reason']} LTP breakout > {self.pending_exit_breakout.get('high', 0):.2f} "
                            f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                        )
                        return None
                    signal_logger.info("🔴 EXIT: Reversal candle confirmed (PUT)")
                    return "REVERSAL_CANDLE_FORMATION"
                if REVERSAL_MODE != "NONE":
                    # signal_logger.info(f"PUT Exit Check Spot: Close > EMA20 -> {spot_close:.2f} > {spot_ema20:.2f} = {is_spot_ema_exit} ; PUT Exit Check Option: Close < EMA20 -> {opt_close:.2f} < {opt_ema20:.2f} = {is_opt_ema_exit}")
                    if is_spot_ema_exit:
                        signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: EMA20 CROSSOVER (PUT Position)")
                        return f"SPOT_CLOSE_ABOVE_EMA20"
                    if is_opt_ema_exit:
                        signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: EMA20 CROSSOVER (PUT Position)")
                        return f"OPTION_CLOSE_BELOW_EMA20" 
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

    def classify_candle(self, open_p, high_p, low_p, close_p):
        """Classify a candle's body size relative to its entire range."""
        total_range = high_p - low_p
        if total_range == 0:
            return "Neutral"
        body = abs(close_p - open_p)
        body_pct = body / total_range
        direction = "Bullish" if close_p >= open_p else "Bearish"
        if body_pct > 0.6:
            strength = "Strong"
        elif body_pct > 0.3:
            strength = "Normal"
        else:
            strength = "Weak"
        return f"{strength} {direction}"

    # =========================================================
    # 📊 INSTITUTIONAL CONTEXT HELPERS (Gap / CPR / Open Zone)
    # =========================================================

    def compute_gap_type(self, intraday_df):
        """
        Classify today's market open vs. previous day's close into:
          'gap_up'   — open is >= GAP_THRESHOLD_PERCENT% above prev close
          'gap_down' — open is <= -GAP_THRESHOLD_PERCENT% below prev close
          'no_gap'   — within threshold

        Also builds self.first_15m_high / self.first_15m_low from intraday_df
        by aggregating candles whose timestamp falls in [09:15, 09:30).
        Result is cached in self.gap_type_cache for the day.
        """
        # Return cached result if already computed today
        if self.gap_type_cache is not None:
            return self.gap_type_cache

        try:
            # First calculate the current day open and prev day close
            current_day_open = getattr(self, 'current_day_open', None)
            prev_day_close = getattr(self, 'prev_day_close', None)

            if not current_day_open or not prev_day_close:
                # Fetch recent Daily candles to accurately get today's open and yesterday's close
                try:
                    daily_df = self.get_historical(
                        symbol=self.spot_symbol,
                        exchange=self.spot_exchange,
                        interval="D",
                        lookback=5,
                        instrument_type="SPOT"
                    )
                    
                    signal_logger.info(f"DEBUG GAP: daily_df empty? {daily_df.empty if daily_df is not None else True}")
                    if daily_df is not None and not daily_df.empty:
                        signal_logger.info(f"DEBUG GAP: daily_df index dates: {daily_df.index.date}")

                        df = daily_df.copy()
                        if df.index.tz is None:
                            df.index = df.index.tz_localize(IST)
                        else:
                            df.index = df.index.tz_convert(IST)
                            
                        # Sort to make sure it's chronological
                        df = df.sort_index()
                        
                        today = get_now().date()
                        
                        # Find the candle for today
                        today_candle = df[df.index.date == today]
                        past_candles = df[df.index.date < today]
                        
                        if not today_candle.empty:
                            current_day_open = float(today_candle.iloc[0]["open"])
                            self.current_day_open = current_day_open
                            
                        if not past_candles.empty:
                            prev_day_close = float(past_candles.iloc[-1]["close"])
                            self.prev_day_close = prev_day_close
                            
                except Exception as e:
                    signal_logger.warning(f"⚠️ Failed to fetch daily candles for gap type: {e}")

            if not current_day_open or not prev_day_close:
                # ⚠️ Data not ready yet (too early in day or daily fetch failed)
                signal_logger.warning(
                    f"⚠️ Gap type: current_day_open={current_day_open} prev_day_close={prev_day_close} "
                    f"— data not ready, will retry next candle (not cached)"
                )
                return "no_gap"  # transient default, NOT stored in cache

            gap_percent = ((float(current_day_open) - prev_day_close) / prev_day_close) * 100.0
            self.gap_percent = gap_percent

            if gap_percent >= GAP_THRESHOLD_PERCENT:
                gap_type = "gap_up"
            elif gap_percent <= -GAP_THRESHOLD_PERCENT:
                gap_type = "gap_down"
            else:
                gap_type = "no_gap"

            signal_logger.info(
                f"📊 Gap Analysis | Open={current_day_open:.2f} PrevClose={prev_day_close:.2f} "
                f"Gap%={gap_percent:.2f} → {gap_type}"
            )

            # Build first-15-minute candle range from intraday_df
            # (09:15 candle is the market open; 09:30 is the close of 3rd 5m candle)
            if intraday_df is not None and not intraday_df.empty:
                try:
                    df = intraday_df.copy()
                    if df.index.tz is None:
                        df.index = df.index.tz_localize(IST)
                    else:
                        df.index = df.index.tz_convert(IST)

                    today = get_now().date()
                    df_today = df[df.index.date == today]

                    # 09:15 to 09:30 window — first three 5-minute candles
                    window = df_today[
                        (df_today.index.time >= dt_time(9, 15)) &
                        (df_today.index.time < dt_time(9, 30))
                    ]

                    if not window.empty:
                        self.first_15m_high = float(window["high"].max())
                        self.first_15m_low  = float(window["low"].min())
                        self.first_15m_candle_type = self.classify_candle(
                            float(window.iloc[0]["open"]),
                            self.first_15m_high,
                            self.first_15m_low,
                            float(window.iloc[-1]["close"])
                        )
                        signal_logger.info(
                            f"📐 First 15-min candle | High={self.first_15m_high:.2f} Low={self.first_15m_low:.2f} Type={self.first_15m_candle_type} "
                            f"(from {len(window)} candle(s))"
                        )
                    else:
                        signal_logger.warning("⚠️ No candles found in 09:15-09:30 window for first_15m levels")
                except Exception as e:
                    signal_logger.warning(f"⚠️ Could not compute first_15m levels: {e}")

            # Only cache the gap type (and functionally lock the 15m levels) AFTER 09:30
            # so we don't prematurely lock in a partial 15m breakout range.
            if get_now().time() >= dt_time(9, 30):
                self.gap_type_cache = gap_type
                
            return gap_type

        except Exception as e:
            signal_logger.warning(f"⚠️ compute_gap_type failed: {e} — defaulting to no_gap")
            self.gap_type_cache = "no_gap"
            return "no_gap"

    def compute_cpr_relation(self):
        """
        Classify today's CPR relative to the previous day's CPR band.

        Values:
          'higher_value'  — today's entire CPR band is ABOVE yesterday's (bullish)
          'lower_value'   — today's entire CPR band is BELOW yesterday's (bearish)
          'inside'        — today's CPR is completely inside yesterday's CPR
          'overlapping'   — bands partially overlap (default)
        """
        try:
            daily_ind = (self.static_indicators or {}).get("DAILY", {})
            if not daily_ind:
                return None

            cdt = float(daily_ind.get("tc",          0.0))  # Current Day Top (TC)
            cdc = float(daily_ind.get("pivot",       0.0))  # Current Day Center (Pivot)
            cdl = float(daily_ind.get("bc",          0.0))  # Current Day Low (BC)

            pdt = float(daily_ind.get("prev_top",    0.0))  # Prev Day Top
            pdc = float(daily_ind.get("prev_center", 0.0))  # Prev Day Center
            pdl = float(daily_ind.get("prev_low",    0.0))  # Prev Day Low

            if not all([cdt, cdl, pdt, pdl]):
                return None

            if cdt > pdt and cdc > pdc and cdl > pdl:
                relation = "higher_value"
            elif cdt < pdt and cdc < pdc and cdl < pdl:
                relation = "lower_value"
            elif cdl >= pdl and cdt <= pdt:
                # Today's band fits entirely within yesterday's band
                relation = "inside"
            else:
                relation = "overlapping"

            signal_logger.info(
                f"📊 CPR Relation: {relation} | "
                f"Today(TC={cdt:.2f}, P={cdc:.2f}, BC={cdl:.2f}) vs "
                f"Prev(TC={pdt:.2f}, P={pdc:.2f}, BC={pdl:.2f})"
            )
            return relation

        except Exception as e:
            signal_logger.warning(f"⚠️ compute_cpr_relation failed: {e}")
            return None

    def compute_open_location(self):
        """
        Identify where the market opened relative to 13 price levels:
          PDL, S4, S3, S2, S1, BC, Pivot, TC, R1, R2, R3, R4, PDH

        Returns a descriptive zone string such as:
          'above_r4', 'between_r2_r3', 'inside_cpr', 'below_s4',
          'between_pdl_s4', 'at_pivot', 'above_pdh', etc.
        """
        try:
            open_price = float(getattr(self, 'current_day_open', 0.0) or 0.0)
            if not open_price:
                return None

            daily_ind = (self.static_indicators or {}).get("DAILY", {})
            if not daily_ind:
                return None

            # Build ordered named levels (name → value)
            levels = {
                "pdl":   float(daily_ind.get("low",    0.0)),
                "s4":    float(daily_ind.get("s4",     0.0)),
                "s3":    float(daily_ind.get("s3",     0.0)),
                "s2":    float(daily_ind.get("s2",     0.0)),
                "s1":    float(daily_ind.get("s1",     0.0)),
                "bc":    float(daily_ind.get("bc",     0.0)),
                "pivot": float(daily_ind.get("pivot",  0.0)),
                "tc":    float(daily_ind.get("tc",     0.0)),
                "r1":    float(daily_ind.get("r1",     0.0)),
                "r2":    float(daily_ind.get("r2",     0.0)),
                "r3":    float(daily_ind.get("r3",     0.0)),
                "r4":    float(daily_ind.get("r4",     0.0)),
                "pdh":   float(daily_ind.get("high",   0.0)),
            }

            # Remove zero values (missing data), then sort ascending
            levels = {k: v for k, v in levels.items() if v != 0.0}
            sorted_levels = sorted(levels.items(), key=lambda x: x[1])

            if not sorted_levels:
                return None

            # Check if open is below the lowest level
            lowest_name, lowest_val = sorted_levels[0]
            if open_price < lowest_val:
                return f"below_{lowest_name}"

            # Check if open is above the highest level
            highest_name, highest_val = sorted_levels[-1]
            if open_price > highest_val:
                return f"above_{highest_name}"

            # Find the two levels surrounding the open
            zone = None
            for i in range(len(sorted_levels) - 1):
                lo_name, lo_val = sorted_levels[i]
                hi_name, hi_val = sorted_levels[i + 1]

                if abs(open_price - lo_val) < 0.01:
                    zone = f"at_{lo_name}"
                    break
                if abs(open_price - hi_val) < 0.01:
                    zone = f"at_{hi_name}"
                    break

                if lo_val < open_price < hi_val:
                    # Use pivot as CPR midpoint divider:
                    #   bc → pivot  = between_bc_pivot  (lower half of CPR)
                    #   pivot → tc  = between_pivot_tc  (upper half of CPR)
                    # All other pairs use the standard between_lo_hi label.
                    zone = f"between_{lo_name}_{hi_name}"
                    break

            if zone is None:
                zone = "unknown"

            signal_logger.info(f"📍 Open Location: {open_price:.2f} → {zone}")
            return zone

        except Exception as e:
            signal_logger.warning(f"⚠️ compute_open_location failed: {e}")
            return None

    def compute_close_location(self, close_price):
        """
        Identify where the given close price is relative to 13 price levels.
        """
        try:
            close_price = float(close_price or 0.0)
            if not close_price:
                return None

            daily_ind = (self.static_indicators or {}).get("DAILY", {})
            if not daily_ind:
                return None

            # Build ordered named levels (name → value)
            levels = {
                "pdl":   float(daily_ind.get("low",    0.0)),
                "s4":    float(daily_ind.get("s4",     0.0)),
                "s3":    float(daily_ind.get("s3",     0.0)),
                "s2":    float(daily_ind.get("s2",     0.0)),
                "s1":    float(daily_ind.get("s1",     0.0)),
                "bc":    float(daily_ind.get("bc",     0.0)),
                "pivot": float(daily_ind.get("pivot",  0.0)),
                "tc":    float(daily_ind.get("tc",     0.0)),
                "r1":    float(daily_ind.get("r1",     0.0)),
                "r2":    float(daily_ind.get("r2",     0.0)),
                "r3":    float(daily_ind.get("r3",     0.0)),
                "r4":    float(daily_ind.get("r4",     0.0)),
                "pdh":   float(daily_ind.get("high",   0.0)),
            }

            # Remove zero values (missing data), then sort ascending
            levels = {k: v for k, v in levels.items() if v != 0.0}
            sorted_levels = sorted(levels.items(), key=lambda x: x[1])

            if not sorted_levels:
                return None

            # Check if close is below the lowest level
            lowest_name, lowest_val = sorted_levels[0]
            if close_price < lowest_val:
                return f"below_{lowest_name}"

            # Check if close is above the highest level
            highest_name, highest_val = sorted_levels[-1]
            if close_price > highest_val:
                return f"above_{highest_name}"

            # Find the two levels surrounding the close
            zone = None
            for i in range(len(sorted_levels) - 1):
                lo_name, lo_val = sorted_levels[i]
                hi_name, hi_val = sorted_levels[i + 1]

                if abs(close_price - lo_val) < 0.01:
                    zone = f"at_{lo_name}"
                    break
                if abs(close_price - hi_val) < 0.01:
                    zone = f"at_{hi_name}"
                    break

                if lo_val < close_price < hi_val:
                    zone = f"between_{lo_name}_{hi_name}"
                    break

            if zone is None:
                zone = "unknown"

            signal_logger.info(f"📍 Close Location: {close_price:.2f} → {zone}")
            return zone

        except Exception as e:
            signal_logger.warning(f"⚠️ compute_close_location failed: {e}")
            return None



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

            # ── 1H Close vs Weekly CPR ───────────────────────────────────────────────
            try:
                # Resample 5min to 60min. closed='right', label='right' matches standard 1H boundaries
                df_1h = intraday_df.resample('60min', closed='right', label='right').agg({
                    'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last'
                }).dropna()
                
                # Get the last fully closed 1-hour candle (second to last row)
                if len(df_1h) >= 2:
                    last_1h_close = float(df_1h.iloc[-2]['close'])
                    weekly_ind = (self.static_indicators or {}).get("WEEKLY", {})
                    wtc = float(weekly_ind.get("tc", 0.0))
                    wbc = float(weekly_ind.get("bc", 0.0))
                    
                    if wtc > 0 and wbc > 0:
                        higher_cpr_level = max(wtc, wbc)
                        lower_cpr_level = min(wtc, wbc)
                        if last_1h_close > higher_cpr_level:
                            self.current_hourly_vs_weekly_cpr = "above_weekly_cpr"
                        elif last_1h_close < lower_cpr_level:
                            self.current_hourly_vs_weekly_cpr = "below_weekly_cpr"
                        else:
                            self.current_hourly_vs_weekly_cpr = "inside_weekly_cpr"
                    else:
                        self.current_hourly_vs_weekly_cpr = None
                else:
                    self.current_hourly_vs_weekly_cpr = None # Not enough data
            except Exception as e:
                self.current_hourly_vs_weekly_cpr = None
                signal_logger.warning(f"⚠️ Failed to compute 1h vs weekly CPR: {e}")

            # ── Dynamic CPR Relation and Open Location ───────────────────────────────
            try:
                self.current_cpr_relation = self.compute_cpr_relation()
                self.current_open_location = self.compute_open_location()
            except Exception as e:
                self.current_cpr_relation = None
                self.current_open_location = None
                signal_logger.warning(f"⚠️ Failed to compute CPR relation/open location: {e}")


            # Get current LTP for logging context
            current_ltp = self.ltp if self.ltp else 0.0
            signal_logger.info(f"🔍 Current LTP: {current_ltp:.2f}")
            # ---------------------------
            # 🎯 Log current subscription status for debugging
            now = time.time()

            for sym in list(self.ws_subscriptions):
                last_time = self.last_ltp_time.get(sym)

                if not last_time:
                    signal_logger.debug(f"⚠️ NO TICK RECEIVED YET: {sym}")
                    continue

                # 🛡️ Auto-fix milliseconds
                if last_time > 1e12:   # clearly ms
                    last_time = last_time / 1000
                    self.last_ltp_time[sym] = last_time

                delta = now - last_time

                if delta > 10:
                    signal_logger.warning(f"⚠️ STALE DATA: {sym} last tick {delta:.1f}s ago")
                else:
                    signal_logger.debug(f"✅ FRESH DATA: {sym} last tick {delta:.1f}s ago")

            if ENTRY_LTP_BREAKOUT_ENABLED and self.pending_breakout:
                signal_logger.info(f"⏳ LTP breakout enabled for entry.")
                return None

            # ONLY FIRST TRADES REQUIRE PIVOT GATE
            if ENABLE_LTP_PIVOT_GATE and self.trade_count == 0:
                with self._pivot_gate_lock:
                    if not self.ltp_pivot_breakout:
                        signal_logger.info("❗ Entry BLOCKED → Pivot Breakout not happened yet for first trade.")
                        return None

            # ── 🚨 GAP FILTER GATE ──────────────────────────────────────────────────
            # Compute gap_type once; cached for the day after 09:15 data is available.
            _opening_breakout_expiry_day = False
            try:
                _cache_date = self.expiry_cache.get("date")
                _cache_is_expiry = self.expiry_cache.get("is_expiry")
                _today = get_now().date()
                if _cache_date == _today and _cache_is_expiry is not None:
                    _opening_breakout_expiry_day = bool(_cache_is_expiry)
                else:
                    _resolved_expiry = self.get_nearest_weekly_expiry()
                    if isinstance(_resolved_expiry, str):
                        _resolved_expiry = datetime.strptime(_resolved_expiry, "%Y-%m-%d").date()
                    _opening_breakout_expiry_day = isinstance(_resolved_expiry, date) and (_resolved_expiry == _today)
            except Exception as _exp_err:
                signal_logger.warning(f"Expiry check failed for opening-range filter: {_exp_err}")
                _opening_breakout_expiry_day = False

            _gap_type = "no_gap"  # transient default
            if GAP_FILTER_ENABLED or _opening_breakout_expiry_day:
                _gap_type = self.compute_gap_type(intraday_df)
                _gap_now_time = get_now().time()
                _gap_cutoff   = dt_time(9, 30)

                if _gap_type in ("gap_up", "gap_down") or _opening_breakout_expiry_day:
                    # Rule 1: Block ALL entries before 09:30 (first 15-min candle forming)
                    if _gap_now_time < _gap_cutoff:
                        signal_logger.info(
                            f"⛔ GAP FILTER: {_gap_type} | Time={_gap_now_time.strftime('%H:%M')} "
                            f"< 09:30 → NO entry until first 15-min candle closes"
                        )
                        return None

                    # Rule 2: After 09:30 – BOTH directions must break their respective level.
                    #   BUY  signal → close MUST be > first_15m_high  (regardless of gap direction)
                    #   SELL signal → close MUST be < first_15m_low   (regardless of gap direction)
                    # We evaluate this at the BUY/SELL return points below (see gap_filter checks).
                    if len(intraday_df) >= 2:
                        _gap_sig_close = float(intraday_df.iloc[-2].get("close", 0.0))
                    else:
                        _gap_sig_close = float(self.ltp or 0.0)

                    signal_logger.info(
                        f"📊 GAP FILTER active: {_gap_type} | sig_close={_gap_sig_close:.2f} "
                        f"| first_15m_H={self.first_15m_high} L={self.first_15m_low}"
                    )
                else:
                    _gap_sig_close = 0.0
                    signal_logger.debug("ℹ️ GAP FILTER: no_gap — normal rules apply")
            else:
                _gap_sig_close = 0.0
            # ── END GAP FILTER GATE (direction-specific checks at BUY/SELL returns) ──

            
            # Current Day High/Low Validation (Early Exit for Performance)
            now_dt = get_now()

            # Normalize configured validation time to a `time` object
            cmp_time = None
            try:
                if isinstance(DAY_HIGH_LOW_BREAKOUT_START_TIME, str) and self.trade_count >= DAY_HIGH_LOW_VALIDATION_FROM_TRADE:
                    cmp_time = datetime.strptime(DAY_HIGH_LOW_BREAKOUT_START_TIME, "%H:%M").time()
                elif isinstance(DAY_HIGH_LOW_BREAKOUT_START_TIME, dt_time):
                    cmp_time = DAY_HIGH_LOW_BREAKOUT_START_TIME
            except Exception:
                signal_logger.warning("Invalid DAY_HIGH_LOW_BREAKOUT_START_TIME format; expected 'HH:MM'. Skipping validation.")

            if cmp_time is not None and now_dt.time() >= cmp_time:
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
            ema200 = float(dyn.get("EMA_100", 0.0))
            rsi14 = float(dyn.get("RSI_14", 0.0))
            prev_rsi14 = float(dyn.get("RSI_PREV_14", 0.0))

            # Store the current attributes for trade logging
            self.current_rsi = rsi14
            self.current_candle_type = self.classify_candle(open_, high, low, close)
            self.current_close_location = self.compute_close_location(close)

            # ── Railway Track Pattern (RTP) ─────────────────────────────────────────
            try:
                if ema9 > 0 and ema20 > 0 and ema50 > 0 and ema200 > 0:
                    bullish_rtp = (ema9 > ema20 > ema50 > ema200)
                    bearish_rtp = (ema9 < ema20 < ema50 < ema200)
                    self.current_rtp = bool(bullish_rtp or bearish_rtp)
                else:
                    self.current_rtp = False
                    
                # Take snapshot at or after 09:30 exactly once per day
                now_time = get_now().time()
                if now_time >= dt_time(9, 30):
                    if getattr(self, "rtp_9_30", None) is None:
                        self.rtp_9_30 = self.current_rtp
                        signal_logger.info(f"🚂 Captured 09:30 RTP Snapshot: {self.rtp_9_30}")
            except Exception as e:
                self.current_rtp = False
                signal_logger.warning(f"⚠️ Failed to compute Railway Track Pattern: {e}")

            # static daily
            if not self.static_indicators:
                signal_logger.error("Daily indicators not available for signal evaluation")
                return None
            cpr_r1 = float(self.static_indicators.get("DAILY", {}).get("r1", 0.0))
            cpr_s1 = float(self.static_indicators.get("DAILY", {}).get("s1", 0.0))
            prev_day_high = float(self.static_indicators.get("DAILY", {}).get("high",0.0))
            prev_day_low = float(self.static_indicators.get("DAILY", {}).get("low",0.0))
            prev_day_close = float(self.static_indicators.get("DAILY", {}).get("close",0.0))
            daiily_pivot = float(self.static_indicators.get("DAILY", {}).get("pivot",0.0))
            weekly_pivot = float(self.static_indicators.get("WEEKLY", {}).get("pivot",0.0))
            # Log all key values for signal evaluation
            # 🚀 OPTIMIZATION (Phase 2): Reduce log verbosity in Backtesting
            if BACKTESTING_MANAGER.active:
                signal_logger.debug(f"📊 Signal Evaluation Data:")
                signal_logger.debug(f"  📈 Current Candle: Open={open_:.2f}, Close={close:.2f}")
                signal_logger.debug(f"  📉 Previous Candle: Open={prev_open:.2f}, High={prev_high:.2f}, Low={prev_low:.2f}, Close={prev_close:.2f}")
                signal_logger.debug(f"  📊 EMA Values: EMA9={ema9:.2f}, EMA20={ema20:.2f}, EMA50={ema50:.2f}")
                signal_logger.debug(f"  📐 CPR Levels: R1={cpr_r1:.2f}, S1={cpr_s1:.2f}")
                signal_logger.debug(f"  📊 Previous Day: High={prev_day_high:.2f}, Low={prev_day_low:.2f}")
            else:
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
            if is_expiry_day and EXPIRY_DAY_HL_BREAKOUT:
                    signal_logger.info(f"📅 TODAY IS EXPIRY DAY - Applying prev day/low breakout must for entry")
                    
                    now_time = get_now().time()
                    expiry_cutoff = EXPIRY_CUTOFF_TIME
                    
                    if now_time <= expiry_cutoff:
                        # Rule 1: Before 10:30 AM -> Must break Previous Day High OR Low (Generic)
                        broke_pdH = (close > self.gk_resistance_pivot)
                        broke_pdL = (close < self.gk_support_pivot)
                        
                        if broke_pdH or broke_pdL:
                             expiry_allowed = True
                             signal_logger.info(f"✅ 🎯 Expiry Rule (<10:30): Trading Allowed (Outside PD Range). Close={close:.2f} PDH={prev_day_high:.2f} PDL={prev_day_low:.2f}")
                        else:
                             expiry_allowed = False
                             signal_logger.info(f"⛔ Expiry Rule (<10:30): Trading Blocked (Inside PD Range). Close={close:.2f}")

                    else:
                        # Rule 2: After 10:30 AM -> Must break Current Day High OR Low (Generic)
                        broke_cdH = close > prev_day_high # close > current_day_high
                        broke_cdL = close < prev_day_low  # close < current_day_low
                        
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
            long_cond4 = (close > ema20)
            long_cond5 = (prev_close > prev_open)
            long_cond6 = (close > open_)
            long_cond7 = (close > ema50)
            long_cond8 = (close > ema200)
            long_cond9 = (current_ltp > high)
            long_cond10 = current_high_ok
            long_cond11 = (close > prev_high)
            long_cond12 = (close > self.gk_resistance_pivot) if getattr(self, "gk_resistance_pivot", None) is not None else False
            # long_cond14 = (rsi14 > 50 and rsi14 < 75)
            long_cond14 = True
            long_cond12 = True
            # Guard against None pivots introduced by check_ltp_pivot_break
            gk_res = getattr(self, "gk_resistance_pivot", None)
            try:
                long_cond13 = (gk_res is not None) and (close > float(gk_res))
            except Exception:
                long_cond13 = False
            # long_cond12 = (close > self.gk_resistance_pivot)

            # Evaluate Short conditions step by step
            short_cond1 = (close < weekly_pivot)
            short_cond2 = (close < daiily_pivot)
            short_cond3 = (close < ema9)
            short_cond4 = (close < ema20)
            short_cond5 = (prev_close < prev_open)
            short_cond6 = (close < open_)
            short_cond7 = (close < ema50)
            short_cond8 = (close < ema200)
            short_cond9 = (current_ltp < low)
            short_cond10 = current_low_ok
            short_cond11 = (close < prev_low)
            short_cond12 = (close < self.gk_support_pivot) if getattr(self, "gk_support_pivot", None) is not None else False
            
            # short_cond14 = (rsi14 < 50 and rsi14 > 25)
            short_cond14 = True
            short_cond12 = True
            # Guard for support pivot comparisons if used later
            gk_sup = getattr(self, "gk_support_pivot", None)
            try:
                short_cond13 = (gk_sup is not None) and (close < float(gk_sup))
            except Exception:
                short_cond13 = False
            # short_cond12 = (close < self.gk_support_pivot) 

            # Add two-candle breakout validation if enabled
            if TWOCANDLEBREAKOUT:
                # For LONG: require previous candle bullish (cond5) AND current close above previous high (cond11)
                # For SHORT: require previous candle bearish (cond5) AND current close below previous low (cond11)
                long_cond =  long_cond3 and long_cond4 and long_cond5 and long_cond6 and long_cond7 and long_cond8 and long_cond11 and long_cond14 and expiry_allowed and long_cond12
                short_cond = short_cond3 and short_cond4 and short_cond5 and short_cond6 and short_cond7 and short_cond8 and short_cond11 and short_cond14 and expiry_allowed and short_cond12
            else:
                # Default: without two-candle breakout requirement
                long_cond =  long_cond3 and long_cond4 and long_cond6 and long_cond7 and long_cond8 and long_cond12 and long_cond14 and expiry_allowed
                short_cond = short_cond3 and short_cond4 and short_cond6 and short_cond7 and short_cond8 and short_cond12 and expiry_allowed and short_cond14 and expiry_allowed


            
            log_func = logger.debug if BACKTESTING_MANAGER.active else logger.info
            if RSI_STRATEGY and now_dt.time() > dt_time(11, 15):
                # Extract the full RSI series from the dynamic indicators response
                rsi_series = dyn.get(f"RSI_{RSI_PERIODS[0]}_series", dyn.get("RSI_14_series"))
                if rsi_series is not None:
                    rsi_series = rsi_series.dropna()
                
                if rsi_series is not None and not rsi_series.empty:
                    long_cond14 = any(
                        rsi_v_setup(rsi_series, level, RSI_LOOKBACK)
                        for level in RSI_LEVELS
                    )
                    short_cond14 = any(
                        rsi_inverse_v_setup(rsi_series, level, RSI_LOOKBACK)
                        for level in RSI_LEVELS
                    )
                    long_cond_rsi = long_cond14  
                    short_cond_rsi = short_cond14
                    log_func(f"🔍 RSI Strategy Active: Long RSI Condition={long_cond_rsi}, Short RSI Condition={short_cond_rsi}")
                    long_cond =  long_cond_rsi
                    short_cond = short_cond_rsi
                else:
                    long_cond = False
                    short_cond = False

                
            # Log detailed condition evaluation (matching your reference)
            # 🎯 OPTIMIZATION: Reduce log verbosity in Backtesting
            if STRATETGY_MODE == "LIVE_TESTING":
                long_cond = long_cond6
                short_cond = short_cond6
            
            #Log detailed condition evaluation (matching your reference)
            log_func(f"=== 🟢 LONG Signal Conditions === {long_cond}{get_now()}")
            log_func(f"  1. Close > Weekly Pivot: {close:.2f} > {weekly_pivot:.2f} = {long_cond1}")
            log_func(f"  2. Close > Daily Pivot: {close:.2f} > {daiily_pivot:.2f} = {long_cond2}")
            log_func(f"  3. Close > EMA9: {close:.2f} > {ema9:.2f} = {long_cond3}")
            log_func(f"  4. EMA9 > EMA20: {ema9:.2f} > {ema20:.2f} = {long_cond4}")
            log_func(f"  5. Prev Close > Prev Open: {prev_close:.2f} > {prev_open:.2f} = {long_cond5}")
            log_func(f"  6. Close > Prev High: {close:.2f} > {prev_high:.2f} = {long_cond11}")
            log_func(f"  7. Close > Open: {close:.2f} > {open_:.2f} = {long_cond6}")
            log_func(f"  8. Close > EMA50: {close:.2f} > {ema50:.2f} = {long_cond7}")
            log_func(f"  9. Close > EMA200: {close:.2f} > {ema200:.2f} = {long_cond8}")
            log_func(f"  10. LTP > last candle high: {current_ltp:.2f} > {high:.2f} = {long_cond9}")
            log_func(f"  11. Close > current day high: {close:.2f} > {self.current_day_high} = {long_cond10}")
            log_func(f"  12. Expiry Day Rule: {expiry_allowed}")
            log_func(f"  13. Close > Gk Resistance Pivot: {close:.2f} > {self.gk_resistance_pivot} = {long_cond13}")
            long_cond14_log = f"  14. RSI Condition: Prev RSI14={prev_rsi14:.2f} < 50 and Current RSI14={rsi14:.2f} > 50 and Current RSI14 < 80 = {long_cond14}"
            log_func(long_cond14_log)  

            log_func(f"  🔴 SHORT Signal Conditions === {short_cond} {get_now()}")
            log_func(f"  1. Close < Weekly Pivot: {close:.2f} < {weekly_pivot:.2f} = {short_cond1}")
            log_func(f"  2. Close < Daily Pivot: {close:.2f} < {daiily_pivot:.2f} = {short_cond2}")
            log_func(f"  3. Close < EMA9: {close:.2f} < {ema9:.2f} = {short_cond3}")
            log_func(f"  4. EMA9 < EMA20: {ema9:.2f} < {ema20:.2f} = {short_cond4}")
            log_func(f"  5. Prev Close < Prev Open: {prev_close:.2f} < {prev_open:.2f} = {short_cond5}")
            log_func(f"  6. Close < Prev Low: {close:.2f} < {prev_low:.2f} = {short_cond11}")
            log_func(f"  7. Close < Open: {close:.2f} < {open_:.2f} = {short_cond6}")
            log_func(f"  8. Close < EMA50: {close:.2f} < {ema50:.2f} = {short_cond7}")
            log_func(f"  9. Close < EMA200: {close:.2f} < {ema200:.2f} = {short_cond8}")
            log_func(f"  10. LTP < last candle low: {current_ltp:.2f} < {low:.2f} = {short_cond9}")
            log_func(f"  11. Close < current day low: {close:.2f} < {self.current_day_low} = {short_cond10}")
            log_func(f"  12. Expiry Day Rule: {expiry_allowed}")
            log_func(f"  13. Close < Gk Support Pivot: {close:.2f} < {self.gk_support_pivot} = {short_cond13}")
            short_cond14_log = f"  14. RSI Condition: Prev RSI14={prev_rsi14:.2f} > 50 and Current RSI14={rsi14:.2f} < 50 and Current RSI14 > 25 = {short_cond14}"
            log_func(short_cond14_log)
            

            # === Breakout Filter Check ===
            if long_cond:
                last_candle = last
                # 🚨 GAP CHECK (BUY): on any gap day, close must break first_15m_high
                if (_gap_type in ("gap_up", "gap_down")) or _opening_breakout_expiry_day:
                    if close <= self.first_15m_high:
                        signal_logger.info(
                            f"⛔ GAP FILTER: BUY blocked on {_gap_type} day — "
                            f"close {close:.2f} <= first_15m_high {self.first_15m_high:.2f}"
                        )
                        return None
                if ENTRY_LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # --- UNIFIED DYNAMIC BREAKOUT SETUP (BUY) ---
                    # 1. Initialize Payload Base
                    self.pending_breakout = {
                        "type": "BUY",
                        "timestamp": now,
                        "mode": ENTRY_BREAKOUT_MODE,
                        # Spot levels are always relevant for reference if not validation
                        "spot_high": float(last_candle.get("high", 0)),
                        "spot_low": float(last_candle.get("low", 0)),
                        "high": float(last_candle.get("high", 0)), # Legacy shim
                        "low": float(last_candle.get("low", 0))    # Legacy shim
                    }
                    
                    # 2. Add Option Details if Mode requires OPTIONS or SPOT_OPTIONS
                    if ENTRY_BREAKOUT_MODE in ["OPTIONS", "SPOT_OPTIONS"]:
                        signal_logger.info("🕵️ Resolving option for BREAKOUT setup (BUY Signal)...")
                        # 2.1 Resolve Option
                        res = self.resolve_option_instrument("BUY")
                        if not res:
                            signal_logger.warning("❌ Could not resolve option for breakout setup. Aborting signal.")
                            return None
                        tradable_symbol, tradable_exchange, strike, opt_type, expiry = res
                        
                        # 2.2 Validate Option Signal (Pre-subscription check)
                        signal_logger.info(f"🔍 Validating option signal for {tradable_symbol}")
                        # Fetch option 5m intraday data (temporary, for validation only)
                        # NOTE: In backtest, this might need optimization, but logic implies we need to check if option chart actually agrees.
                        temp_opt_data = self.get_intraday(
                            days=LOOKBACK_DAYS,
                            symbol=tradable_symbol,
                            exchange=tradable_exchange,
                            instrument_type="OPTIONS"
                        )
                        
                        opt_signal_valid = False
                        if temp_opt_data is not None and not temp_opt_data.empty and len(temp_opt_data) >= 2:
                            dyn_option = self.compute_dynamic_indicators(temp_opt_data)
                            if dyn_option:
                                option_candle = dyn_option.get("last_candle", {})
                                option_prev_candle = dyn_option.get("prev_candle", {})
                                opt_close = float(option_candle.get("close", 0.0))
                                opt_prev_high = float(option_prev_candle.get("high", 0.0))
                                opt_prev_open = float(option_prev_candle.get("open", 0.0))
                                opt_ema20 = float(dyn_option.get("EMA_20", 0.0))
                                opt_vwap = float(dyn_option.get("VWAP", 0.0))
                                
                                # Validate LONG option signal
                                opt_signal_valid = (
                                       opt_close > opt_ema20 
                                    )
                                
                                signal_logger.info(f"Option Validation: Close={opt_close:.2f} > EMA20={opt_ema20:.2f} & VWAP={opt_vwap:.2f} = {opt_signal_valid}")
                        
                        if not opt_signal_valid:
                             signal_logger.info(f"❌ Option signal NOT valid for {tradable_symbol} - rejecting entry")
                             self.pending_breakout = None
                             return None

                        # 2.3 Option Signal Valid -> Enhance Payload
                        # Subscribe if Live
                        if not BACKTESTING_MANAGER.active:
                             order_logger.info("🛒 Subscribing to Live option LTP for Breakout Tracking...")
                             self.subscribe_option_ltp()
                        else:
                             # Ensure data is available for backtest replay
                             self._fetch_simulation_option_data()

                        # Add Option Levels to Payload
                        opt_breakout_high = float(option_candle.get("high", 0))
                        opt_breakout_low = float(option_candle.get("low", 0))
                        
                        self.pending_breakout.update({
                            "option_high": opt_breakout_high,
                            "option_low": opt_breakout_low,
                            "symbol": tradable_symbol,
                            "strike": strike,
                            "opt_type": opt_type,
                            "expiry": expiry,
                            "exchange": tradable_exchange
                        })
                        signal_logger.info(f"🪙 OPTION BREAKOUT INFO ADDED: {tradable_symbol} | High: {opt_breakout_high:.2f}")

                    # 3. Finalize Setup
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    self.pending_breakout_expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.breakout_side = "BUY"
                    
                    target_msg = f"SPOT > {self.pending_breakout['spot_high']:.2f}"
                    if ENTRY_BREAKOUT_MODE != "SPOT":
                        target_msg += f" | OPTION > {self.pending_breakout.get('option_high', 0):.2f}"
                        
                    signal_logger.info(f"🟢 🎯 BUY signal waiting for {ENTRY_BREAKOUT_MODE} breakout 📈 [{target_msg}] until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info(f"🟢 🎯 ENTRY SIGNAL GENERATED: BUY (no LTP breakout filter) {get_now()}")
                    return "BUY"

            if short_cond:
                last_candle = last
                # 🚨 GAP CHECK (SELL): on any gap day, close must break first_15m_low
                if (_gap_type in ("gap_up", "gap_down")) or _opening_breakout_expiry_day:
                    if close >= self.first_15m_low:
                        signal_logger.info(
                            f"⛔ GAP FILTER: SELL blocked on {_gap_type} day — "
                            f"close {close:.2f} >= first_15m_low {self.first_15m_low:.2f}"
                        )
                        return None
                if ENTRY_LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # --- UNIFIED DYNAMIC BREAKOUT SETUP (SELL) ---
                    # 1. Initialize Payload Base
                    self.pending_breakout = {
                        "type": "SELL",
                        "timestamp": now,
                        "mode": ENTRY_BREAKOUT_MODE,
                        "spot_high": float(last_candle.get("high", 0)),
                        "spot_low": float(last_candle.get("low", 0)),
                        "high": float(last_candle.get("high", 0)), # Legacy shim
                        "low": float(last_candle.get("low", 0))    # Legacy shim
                    }
                    
                    # 2. Add Option Details if Mode requires OPTIONS or SPOT_OPTIONS
                    if ENTRY_BREAKOUT_MODE in ["OPTIONS", "SPOT_OPTIONS"]:
                        signal_logger.info("🕵️ Resolving option for BREAKOUT setup (SELL Signal)...")
                        # 2.1 Resolve Option (PE for SELL signal)
                        res = self.resolve_option_instrument("SELL")
                        if not res:
                            signal_logger.warning("❌ Could not resolve option for breakout setup. Aborting signal.")
                            return None
                        tradable_symbol, tradable_exchange, strike, opt_type, expiry = res
                        
                        # 2.2 Validate Option Signal
                        signal_logger.info(f"🔍 Validating option signal for {tradable_symbol}")
                        temp_opt_data = self.get_intraday(
                            days=LOOKBACK_DAYS,
                            symbol=tradable_symbol,
                            exchange=tradable_exchange,
                            instrument_type="OPTIONS"
                        )
                        
                        opt_signal_valid = False
                        if temp_opt_data is not None and not temp_opt_data.empty and len(temp_opt_data) >= 2:
                            dyn_option = self.compute_dynamic_indicators(temp_opt_data)
                            if dyn_option:
                                option_candle = dyn_option.get("last_candle", {})
                                option_prev_candle = dyn_option.get("prev_candle", {})
                                opt_close = float(option_candle.get("close", 0.0))
                                opt_prev_high = float(option_prev_candle.get("high", 0.0))
                                opt_prev_open = float(option_prev_candle.get("open", 0.0))
                                opt_ema20 = float(dyn_option.get("EMA_20", 0.0))
                                opt_vwap = float(dyn_option.get("VWAP", 0.0))
                                
                                # Validate SHORT option signal (Buying PE)
                                opt_signal_valid = (
                                       opt_close > opt_ema20 
                                    )
                                
                                signal_logger.info(f"Option Validation: Close={opt_close:.2f} > EMA20={opt_ema20:.2f} & VWAP={opt_vwap:.2f} = {opt_signal_valid}")

                        if not opt_signal_valid:
                             signal_logger.info(f"❌ Option signal NOT valid for {tradable_symbol} - rejecting entry")
                             self.pending_breakout = None
                             return None

                        # 2.3 Option Signal Valid -> Enhance Payload
                        if not BACKTESTING_MANAGER.active:
                             order_logger.info("Subscribing to Live option LTP for Breakout Tracking...")
                             self.subscribe_option_ltp()
                        else:
                             self._fetch_simulation_option_data()
                        
                        opt_breakout_high = float(option_candle.get("high", 0))
                        opt_breakout_low = float(option_candle.get("low", 0))

                        self.pending_breakout.update({
                            "option_high": opt_breakout_high,
                            "option_low": opt_breakout_low,
                            "symbol": tradable_symbol,
                            "strike": strike,
                            "opt_type": opt_type,
                            "expiry": expiry,
                            "exchange": tradable_exchange
                        })
                        signal_logger.info(f"🪙 OPTION BREAKOUT INFO ADDED: {tradable_symbol} | High to Break: {opt_breakout_high:.2f}")

                    # 3. Finalize Setup
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    self.pending_breakout_expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.breakout_side = "SELL"
                    
                    target_msg = f"SPOT < {self.pending_breakout['spot_low']:.2f}"
                    if ENTRY_BREAKOUT_MODE != "SPOT":
                        target_msg += f" | OPTION > {self.pending_breakout.get('option_high', 0):.2f}"

                    signal_logger.info(f"📉 🎯 SELL signal waiting for {ENTRY_BREAKOUT_MODE} breakout 📉 [{target_msg}] until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
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
            
    def initialize_cpr_range_sl_tp_enhanced(self, signal, intraday_df, base_price=None, pivots=None, instrument_type="SPOT"):
        """
        Enhanced CPR initialization with exclusion filtering and dynamic risk:reward targeting
        Combines both CPR pivot exclusion and dynamic target selection features
        """
        try:
            position_logger.info(f"Initializing Enhanced CPR Range SL/TP for {signal} signal")
            position_logger.info(f"Features: Exclusion Filter={'ON' if CPR_EXCLUDE_PIVOTS else 'OFF'}, Dynamic Target={'ON' if USE_DYNAMIC_TARGET else 'OFF'}")
            position_logger.info("Option premimum: {:.2f}".format(float(base_price or 0.0)))
            # Get signal candle data for initial SL (last completed candle = -2).
            def _extract_signal_candle(df, source_tag):
                if df is None:
                    position_logger.warning(f"{source_tag}: intraday_df is None")
                    return None
                if not hasattr(df, "iloc"):
                    position_logger.warning(f"{source_tag}: intraday_df has no iloc (type={type(df)})")
                    return None
                try:
                    row_count = len(df)
                except Exception:
                    position_logger.warning(f"{source_tag}: unable to determine intraday_df length")
                    return None
                if row_count < 2:
                    position_logger.warning(
                        f"{source_tag}: insufficient candles for signal candle extraction (need>=2, got={row_count})"
                    )
                    return None
                try:
                    return df.iloc[-2]
                except Exception as e:
                    position_logger.warning(f"{source_tag}: failed to access signal candle at -2: {e}")
                    return None

            sig_candle = _extract_signal_candle(intraday_df, "initial")
            if sig_candle is None:
                position_logger.warning("Refreshing intraday data for CPR initialization")
                intraday_df = self.get_intraday(
                    days=LOOKBACK_DAYS,
                    symbol=self.spot_symbol if instrument_type == "SPOT" else getattr(self, "option_symbol", None),
                    exchange=self.spot_exchange if instrument_type == "SPOT" else OPTION_EXCHANGE,
                    timeframe=CANDLE_TIMEFRAME,
                    instrument_type=instrument_type)  
                sig_candle = _extract_signal_candle(intraday_df, "refreshed")
                if sig_candle is None:
                    position_logger.warning("Could not extract valid signal candle after refresh")
                    return False
            
            signal_candle_low = float(sig_candle.get('low', 0) or 0)
            signal_candle_high = float(sig_candle.get('high', 0) or 0)
            signal_candle_range = abs(signal_candle_high - signal_candle_low)
            # Merge pivot levels with exclusion filtering
                
            pivots_dict = pivots.copy() if pivots else self.static_indicators.copy()
            
            filtered_levels, level_mapping, excluded_ids = merge_pivot_levels_with_exclusion(pivots_dict, CPR_EXCLUDE_PIVOTS)
            
            # Store and log exclusion results
            self.excluded_pivots_info = excluded_ids
            if excluded_ids:
                position_logger.info(f"Excluded {len(excluded_ids)} unreliable pivot levels")
                
            if CPR_MIDPOINTS_ENABLED:
                position_logger.info("CPR Midpoint expansion enabled - generating midpoints for filtered levels")   
                expanded_levels = expand_with_midpoints(filtered_levels)
                self.cpr_levels_sorted = sorted(expanded_levels)
            else:
                position_logger.info("CPR Midpoint expansion disabled - using filtered levels only")
                self.cpr_levels_sorted = sorted(filtered_levels)
            
            if not self.cpr_levels_sorted:
                position_logger.warning("No CPR levels available after filtering")
                return False
                
            # Set entry price and initial SL
            if base_price is None:
                base_price = float(self.option_entry_price or 0.0)
            self.entry_price = float(base_price)

            # ✅ FIX: Determine effective side based on mode
            if USE_OPTION_FOR_SLTP:
                # For options, always use BUY logic (both CE/PE premiums increase similarly)
                effective_side = "BUY"
                position_logger.info(f"OPTION MODE: Using BUY logic for {signal} position (Option: {getattr(self, 'option_symbol', 'N/A')})")
            else:
                # For spot, use the actual signal side
                effective_side = signal.upper()
                position_logger.info(f"SPOT MODE: Using {effective_side} logic for {signal} position")
            self.cpr_side = effective_side
            
            # Calculate initial SL from signal candle
            position_logger.info(f"Calculating initial SL using signal candle range: Low={signal_candle_low:.2f}, High={signal_candle_high:.2f}, Range={signal_candle_range:.2f}")
            premium_based_range = self.entry_price * SL_PERCENT
            percent = SL_PERCENT * 100
            position_logger.info(f"{percent:.2f}% SL from entry premium {self.entry_price:.2f}: {premium_based_range:.2f}")
            position_logger.info(f"Minumum SL points: {MIN_SL_POINTS}")
            position_logger.info(f"Signal candle range: {signal_candle_range:.2f}")
            max_allowed_range = max(MIN_SL_POINTS, min(signal_candle_range, premium_based_range, MAX_RISK_POINTS))
            position_logger.info(f"Calculated max allowed SL range: {max_allowed_range:.2f}, (SignalCandleRange={signal_candle_range:.2f}, PremiumBasedRange={premium_based_range:.2f})")
            if self.cpr_side == "BUY":
                initial_sl = round(signal_candle_high - (max_allowed_range + SL_BUFFER_POINT), 2)
                # initial_sl = signal_candle_high - MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else initial_sl
            else:  # SELL
                initial_sl = round(signal_candle_low + (max_allowed_range + SL_BUFFER_POINT), 2)
                # initial_sl = signal_candle_low + MAX_SIGNAL_RANGE if signal_candle_range > MAX_SIGNAL_RANGE else initial_sl  
            self.cpr_sl = initial_sl
            self.stoploss_price = initial_sl
            self.initial_stoploss_price = initial_sl
            
            # Enhanced target selection with dynamic risk:reward
            if USE_DYNAMIC_TARGET:
                # Use dynamic calculation with filtered CPR levels
                dynamic_target, risk_dist, reward_dist, target_key, rationale = self.calculate_dynamic_cpr_target(
                    effective_side,  # ✅ Use effective_side instead of signal
                    base_price, 
                    initial_sl, 
                    self.cpr_levels_sorted, 
                    level_mapping
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
            position_logger.info(f"  Original Signal: {signal}")
            position_logger.info(f"  Effective Side (for SL/TP): {self.cpr_side}")
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
                    position_logger.info(f"First CPR hit → SL moved to Breakeven {self.cpr_sl:.2f}")
                    
                elif self.cpr_tp_hit_count > 1:
                    # Subsequent meaningful hits → SL to previous TP
                    previous_hit = self.cpr_tp_hits[-2]
                    self.cpr_sl = round(float(previous_hit), 2)
                    position_logger.info(
                        f"CPR hit #{self.cpr_tp_hit_count} → SL moved to prev TP {self.cpr_sl:.2f}"
                    )

                # Update SL: adjust sign based on side (BUY -> SL below, SELL -> SL above)
                try:
                    if getattr(self, 'cpr_side', 'BUY') == 'BUY':
                        self.stoploss_price = self.cpr_sl - SL_BUFFER_POINT
                    else:
                        self.stoploss_price = self.cpr_sl + SL_BUFFER_POINT
                    position_logger.info(f"Updated stoploss_price based on CPR SL: {self.stoploss_price:.2f}")
                except Exception:
                    # fallback to previous behavior on any issue
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
            # NOTE: Dynamic TSL enabling is evaluated only when a TP is hit
            # to avoid repeated intraday fetches / indicator recomputations.
            now = get_now()
            tsl_enable_cutoff = TSL_CUTOFF_TIME if isinstance(TSL_CUTOFF_TIME, datetime) else now.replace(hour=TSL_CUTOFF_TIME.hour, minute=TSL_CUTOFF_TIME.minute, second=0, microsecond=0)
            allow_dynamic_tsl_enable = now <= tsl_enable_cutoff 
          # Disable dynamic TSL enabling for now (can be enabled later after testing)

            side = self.cpr_side
            current_tp = self.get_current_cpr_tp()

            # ✅ FIX: Use correct LTP based on tracking mode
            if USE_OPTION_FOR_SLTP:
                tracking_ltp = getattr(self, "option_ltp", None) or current_ltp
                position_logger.debug(f"Using OPTION LTP for SL/TP check: {tracking_ltp:.2f}")
            else:
                tracking_ltp = self.ltp or current_ltp
                position_logger.debug(f"Using SPOT LTP for SL/TP check: {tracking_ltp:.2f}")

            # Use tracking_ltp for subsequent comparisons
            current_ltp = tracking_ltp
            
            # Check SL conditions with LTP breakout confirmation
            if side == "BUY" and current_ltp <= self.cpr_sl:
                position_logger.debug(f"CPR BUY | SL Hit: {current_ltp:.2f} <= {self.cpr_sl:.2f}")
                
                # Setup LTP breakout confirmation using CPR SL level
                if EXIT_LTP_BREAKOUT_ENABLED:
                    now = get_now()
                    
                    # Breakout level is the CPR SL itself
                    # handle_exit_ltp_breakout will confirm if option LTP breaks below this
                    self.pending_exit_breakout = {
                        "type": "CPR_SL_BUY",
                        "mode": EXIT_LTP_BREAKOUT_MODE, # Enforce SPOT mode for CPR levels
                        "low": float(self.cpr_sl - SL_BUFFER_POINT),
                        "high": float(self.cpr_sl + SL_BUFFER_POINT),
                        "trigger": "CPR_SL",
                        "reason": "CPR_SL_HIT_CALL_SIDE"
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
                position_logger.debug(f"CPR SELL | SL Hit: {current_ltp:.2f} >= {self.cpr_sl:.2f}")

                # Setup LTP breakout confirmation for SELL
                if EXIT_LTP_BREAKOUT_ENABLED:
                    now = get_now()

                    self.pending_exit_breakout = {
                        "type": "CPR_SL_SELL",
                        "mode": EXIT_LTP_BREAKOUT_MODE, # Enforce SPOT mode for CPR levels
                        "low": float(self.cpr_sl - SL_BUFFER_POINT),
                        "high": float(self.cpr_sl + SL_BUFFER_POINT),
                        "trigger": "CPR_SL",
                        "reason": "CPR_SL_HIT_PUT_SIDE"
                    }

                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    self.pending_exit_breakout_expiry = (
                        now.replace(minute=0, second=0, microsecond=0)
                        + timedelta(minutes=next_multiple)
                    )
                    self.exit_breakout_side = "EXIT_SELL"

                    ltp_type = "OPTION" if USE_OPTION_FOR_SLTP else "SPOT"
                    position_logger.info(
                        f"⏳ CPR SL waiting for {ltp_type} LTP breakup > {self.cpr_sl + SL_BUFFER_POINT:.2f} "
                        f"until {self.pending_exit_breakout_expiry.strftime('%H:%M:%S')}"
                    )
                    return "SL_PENDING_BREAKOUT"
                else:
                    return "SL_HIT"
                
            # Check TP conditions
            if current_tp is not None:
                if side == "BUY" and current_ltp >= current_tp:
                    position_logger.info(f"🟢 CPR BUY TP Hit: {current_ltp:.2f} >= {current_tp:.2f}")
                    # Evaluate dynamic TSL enable only when TP is hit (uses cached close if available)
                    if not getattr(self, 'tsl_enabled', False):
                        if allow_dynamic_tsl_enable:
                            try:
                                last_close = None
                                if getattr(self, 'cached_last_close', None) is not None:
                                    last_close = float(self.cached_last_close)
                                else:
                                    if USE_OPTION_FOR_SLTP:
                                        intraday_df = self.get_intraday(days=LOOKBACK_DAYS, symbol=self.option_symbol, exchange=OPTION_EXCHANGE, instrument_type="OPTIONS")
                                    else:
                                        intraday_df = self.get_intraday(days=LOOKBACK_DAYS, symbol=self.spot_symbol, exchange=self.spot_exchange, instrument_type="SPOT")
                                    dyn_for_trend = self.compute_dynamic_indicators(intraday_df) if intraday_df is not None and not intraday_df.empty else None
                                    if dyn_for_trend:
                                        last_close = float(dyn_for_trend.get("last_candle", {}).get("close", 0.0))

                                if last_close is not None:
                                    cpr_r1_local = float(self.static_indicators.get("DAILY", {}).get("pivot", 0.0)) if self.static_indicators else 0.0
                                    daily_pivot_local = float(self.static_indicators.get("DAILY", {}).get("pivot", 0.0)) if self.static_indicators else 0.0
                                    if last_close > daily_pivot_local and last_close > cpr_r1_local:
                                        position_logger.info("Enabling TSL dynamically (bullish CPR + above daily pivot + r1)")
                                        self.tsl_enabled = True
                                    else:
                                        position_logger.info("TSL remains disabled (conditions for dynamic enable not met) - last close: {:.2f}, daily pivot: {:.2f}, cpr r1: {:.2f}".format(last_close, daily_pivot_local, cpr_r1_local))
                            except Exception:
                                pass
                        else:
                            position_logger.info(
                                f"TSL dynamic enable skipped after cutoff (now={now.strftime('%H:%M:%S')}, cutoff=14:30:00)"
                            )

                    # Check TSL flag to determine behavior
                    if not getattr(self, 'tsl_enabled', False):
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
                    position_logger.info(f"🟢 CPR SELL TP Hit: {current_ltp:.2f} <= {current_tp:.2f}")
                    # Evaluate dynamic TSL enable only when TP is hit (uses cached close if available)
                    if not getattr(self, 'tsl_enabled', False):
                        if allow_dynamic_tsl_enable:
                            try:
                                last_close = None
                                if getattr(self, 'cached_last_close', None) is not None:
                                    last_close = float(self.cached_last_close)
                                else:
                                    intraday_df = self.get_intraday(days=LOOKBACK_DAYS, symbol=self.spot_symbol, exchange=self.spot_exchange, instrument_type="SPOT")
                                    dyn_for_trend = self.compute_dynamic_indicators(intraday_df) if intraday_df is not None and not intraday_df.empty else None
                                    if dyn_for_trend:
                                        last_close = float(dyn_for_trend.get("last_candle", {}).get("close", 0.0))

                                if last_close is not None:
                                    cpr_s1_local = float(self.static_indicators.get("DAILY", {}).get("pivot", 0.0)) if self.static_indicators else 0.0
                                    daily_pivot_local = float(self.static_indicators.get("DAILY", {}).get("pivot", 0.0)) if self.static_indicators else 0.0
                                    if last_close < daily_pivot_local and last_close < cpr_s1_local:
                                        position_logger.info("Enabling TSL dynamically (bearish CPR + daily pivot + s1)")
                                        self.tsl_enabled = True
                                    else:
                                        position_logger.info("TSL remains disabled (conditions for dynamic enable not met) - last close: {:.2f}, daily pivot: {:.2f}, cpr s1: {:.2f}".format(last_close, daily_pivot_local, cpr_s1_local))
                            except Exception:
                                pass
                        else:
                            position_logger.info(
                                f"TSL dynamic enable skipped after cutoff (now={now.strftime('%H:%M:%S')}, cutoff=14:30:00)"
                            )

                    # Check TSL flag to determine behavior
                    if not getattr(self, 'tsl_enabled', False):
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
                "option_price": float(f"{self.option_ltp:.2f}"),
                "action": f"TP_Hit_{self.cpr_tp_hit_count}",
                "quantity": QUANTITY,
                "order_id": "",
                "option_type": "",
                "strategy": STRATEGY,
                "leg_type": "TP_HIT",
                "reason": f"TP Hit: {target_level:.2f}",
                "stoploss": self.stoploss_price,
                "target": self.target_price,
                "pnl": f"{unrealized_pnl:.2f}",
                "leg_status": "TSL"
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
                # Use absolute minimum SL points as fallback risk (points),
                # not a subtraction from entry price which produced incorrect large values.
                risk = float(MIN_SL_POINTS)
                position_logger.warning(f"Fallback risk applied: {risk:.2f} points (MIN_SL_POINTS)")
            
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
                    reward = entry_price - level_value
                    # Ensure reward meets minimum (same direction logic as BUY)
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
            position_logger.info(f"  Available valid target count for {signal}: {len(valid_candidates)}")
            # Show only the top few valid candidates in the trade direction (BUY -> ascending, SELL -> descending)
            try:
                if signal == 'BUY':
                    sorted_by_price = sorted(valid_candidates, key=lambda x: x[0])
                else:
                    sorted_by_price = sorted(valid_candidates, key=lambda x: x[0], reverse=True)

                show_n = min(5, len(sorted_by_price))
                for level_val, level_key, dist in sorted_by_price[:show_n]:
                    position_logger.info(f"    {level_key}: {level_val:.2f} (dist: {dist:.2f})")
                if len(sorted_by_price) > show_n:
                    position_logger.info(f"    ... and {len(sorted_by_price) - show_n} more candidates (showing {show_n} in {signal} direction)")
            except Exception:
                # Fallback to previous verbose listing if sorting fails for any reason
                for level_val, level_key, dist in valid_candidates[:10]:
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
            # 🟢 BACKTESTING MODE: Lookup from EXPIRY_LIST_DDMMYYYY
            if BACKTESTING_MANAGER.active:
                current_date_val = get_now().date()
                
                # Sort and find the first expiry date >= current_date_val
                # Parse list into date objects
                try:
                     sorted_expiries = sorted(
                         [datetime.strptime(d, "%d-%m-%Y").date() for d in EXPIRY_LIST_DDMMYYYY]
                     )
                     
                     # Find nearest expiry
                     resolved_expiry_date = next(
                         (expiry for expiry in sorted_expiries if expiry >= current_date_val), 
                         None
                     )
                except Exception as e:
                     order_logger.error(f"Error parsing expiry list: {e}")
                     return None
                
                if resolved_expiry_date:
                     order_logger.info(f"🗓️ [SIMULATION] Resolved Expiry from List: {resolved_expiry_date}")
                     return resolved_expiry_date
                else:
                     logger.error(f"❌ No future expiry found in EXPIRY_LIST_DDMMYYYY for {current_date_val}")
                     return None

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
        max_order_attempts=5,
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
            action = "BUY"
            current_time = get_now()
            mock_order_id = f"{action}_Order_{current_time.strftime('%H:%M:%S')}"
            
            if self._fetch_simulation_option_data():
                self.option_ltp = self._update_option_ltp_from_df(current_time)
                exec_price = self.option_ltp
            else:
                return None, None
            # Use current LTP as execution price
            if exec_price is None:
                order_logger.warning(f"⚠️ Simulation Option LTP is not available, cannot execute order")
                return None, None
            exec_price = self.option_ltp
            order_logger.info(f"✅ 🎯 [SIMULATION] Order executed: ID={mock_order_id} | Price={exec_price}")
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

    def place_entry_order(self, signal):
        """
        Enhanced entry order placement with:
        1. Stop Loss Automation (25% rule)
        2. Option symbol resolution and SL/TP initialization

        BUGFIX: ensure self.exit_in_progress is cleared after SL/TP initialization
        so that on_ltp_update() will begin monitoring the newly opened position.
        """
        try:
            order_logger.info("=== 🛒 PLACING ENTRY ORDER ===")
            # Reset per-trade TSL flag at the start of a new order
            try:
                self.tsl_enabled = False
            except Exception:
                pass
            signal_side = "CALL" if signal == "BUY" else "PUT"
            order_logger.info(f"Buy Signal: {signal_side}")
            
            # 🛑 TIME CHECK: strict cutoff for new entries
            current_time = get_now().time()
            if current_time > ENTRY_CUTOFF_TIME:
                order_logger.warning(f"⛔ Entry skipped: Current time {current_time.strftime('%H:%M:%S')} > Cutoff {ENTRY_CUTOFF_TIME.strftime('%H:%M:%S')}")
                return False
            
            if current_time < TRADE_START_TIME:
                order_logger.warning(f"⛔ Entry skipped: Current time {current_time.strftime('%H:%M:%S')} < Start {TRADE_START_TIME.strftime('%H:%M:%S')}")
                return False
            

            if self.trade_count >= MAX_TRADES_PER_DAY:
                order_logger.warning(f"Daily trade limit reached ({self.trade_count}/{MAX_TRADES_PER_DAY})")
                
                # FIX: In Range Mode, do not kill the bot. Just stop taking new entries.
                if MODE == "BACKTESTING_RANGE":
                     logger.warning("Daily trade limit reached. Staying idle for remainder of the day (Range Mode).")
                     return False

                logger.warning("Daily trade limit reached (%d), will shutdown.", MAX_TRADES_PER_DAY)
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
                order_logger.error(f"Entry order failed due to order placement or option price not available.")
                return False
            order_logger.info(f"✅ 🎯 Order placed successfully. dynamic SL/TP calculation started {get_now()}")
            with self._state_lock:
                self.exit_in_progress = True
                self.position = signal
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
                    option_entry_type = "BUY"  # This is default for sl order since we are trading only in option buying.
                    sl_price = self.compute_pct_sl(exec_price, option_entry_type)

                    order_logger.info(f"Attempting to place linked SL order after entry confirmation...")
                    sl_order_id = self.place_and_confirm_stoploss(exit_symbol, exit_exchange, option_entry_type, sl_price)

                    if sl_order_id:
                        self.sl_order_id = sl_order_id
                        self.broker_sl = sl_price  # Store for real-time broker SL hit detection
                        order_logger.info(f"✅ 🎯 Stop Loss Order Placed Successfully | ID={sl_order_id} | SL={sl_price:.2f} | broker_sl set={self.broker_sl:.2f}")
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
                
                self.entry_price = round(base_price, 2)
                # Initialize SL/TP using configured method
                if SL_TP_METHOD == "Signal_candle_range_SL_TP":
                    intraday = self.get_intraday(
                            days=LOOKBACK_DAYS, 
                            symbol=self.spot_symbol, 
                            exchange=self.spot_exchange,
                            timeframe=CANDLE_TIMEFRAME,
                            instrument_type="SPOT")
                    self.initialize_signal_candle_sl_tp(signal, intraday, base_price=base_price)
                elif SL_TP_METHOD == "CPR_range_SL_TP":
                    risk_logger.info("CPR-based SL/TP calculation started")
                    cpr_success = False
                    if USE_OPTION_FOR_SLTP: 
                        spot_cpr = self.static_indicators.copy()
                        risk_logger.info("Calculating CPR for SL/TP")
                        self.compute_static_indicators(
                        symbol = self.option_symbol,
                        exchange = OPTION_EXCHANGE,
                        instrument_type="OPTIONS"
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
                                # ✅ FIX: Pass original signal, function will determine effective_side internally
                                cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal, intraday_df_opt, base_price=exec_price, pivots=cpr_source, instrument_type="OPTIONS")
                                # self.dynamic_indicators_option = self.compute_dynamic_indicators(intraday_df_opt)
                            else:
                                self.dynamic_indicators_option = None 
                                cpr_success = False
                                risk_logger.warning("Failed to fetch option intraday data for dynamic indicators after OPTION CPR SL")        
                    if USE_SPOT_FOR_SLTP and not cpr_success:
                        risk_logger.info("SPOT CPR SL/TP calculation enabled")
                        cpr_source =  self.static_indicators
                        intraday = self.get_intraday(
                            days=LOOKBACK_DAYS, 
                            symbol=self.spot_symbol, 
                            exchange=self.spot_exchange,
                            timeframe=CANDLE_TIMEFRAME,
                            instrument_type="SPOT")
                        cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal=signal, intraday_df=intraday, base_price=base_price, pivots=cpr_source, instrument_type="SPOT")
                    if not cpr_success:
                        # self.static_indicators = spot_cpr
                        cpr_source =  self.static_indicators
                        intraday = self.get_intraday(
                            days=LOOKBACK_DAYS, 
                            symbol=self.spot_symbol, 
                            exchange=self.spot_exchange,
                            timeframe=CANDLE_TIMEFRAME,
                            instrument_type="SPOT")
                        risk_logger.info("Option CPR initialization failed, Falling back to SPOT CPR")
                        cpr_success = self.initialize_cpr_range_sl_tp_enhanced(signal=signal, intraday_df=intraday, base_price=base_price, pivots=cpr_source, instrument_type="SPOT")
                    if not cpr_success:
                        self.initialize_signal_candle_sl_tp(signal=signal, intraday=intraday, base_price=base_price)
                else:
                    # fallback: fixed TP/SL
                    # ✅ FIX: Determine effective side for fallback logic
                    if USE_OPTION_FOR_SLTP:
                        effective_side = "BUY"
                        base_price = exec_price
                        risk_logger.info(f"Fallback: Using BUY logic for option (premium-based)")
                    else:
                        effective_side = signal.upper()
                        base_price = self.spot_entry_price
                        risk_logger.info(f"Fallback: Using {effective_side} logic for spot")
                    
                    if effective_side == "BUY":
                        self.stoploss_price = round(base_price - STOPLOSS, 2)
                        self.target_price = round(base_price + TARGET, 2)
                    else:  # SELL
                        self.stoploss_price = round(base_price + STOPLOSS, 2)
                        self.target_price = round(base_price - TARGET, 2)

                # finalize basic state vars
                self.entry_price = round(base_price, 2)
            
            # BUGFIX: Re-enable websocket-based monitoring now that SL/TP and position are initialized.
            with self._state_lock:
                self.exit_in_progress = False

            # Start verification for the option symbol if just establishment
            if not BACKTESTING_MANAGER.active:
                order_logger.info("Verifying option LTP subscription post-entry...")
                threading.Thread(target=self._verify_subscriptions_async, args=([self.option_symbol],), daemon=True).start()
        
            order_logger.info(f"Order ID: {entry_order_id}")

            # final logging and DB record
            order_logger.info("=== POSITION ESTABLISHED ===")
            order_logger.info(f"Position: {self.position}")
            order_logger.info(f"Entry Price: {self.entry_price:.2f}")
            order_logger.info(f"Stop Loss: {self.stoploss_price:.2f}")
            order_logger.info(f"Target: {self.target_price:.2f}")
            order_logger.info(f"Quantity: {QUANTITY}")
            order_logger.info(f"Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
            reason = f"Breakout"
            # log trade to DB (existing helper)     
            daily_ind  = (self.static_indicators or {}).get('DAILY')
            weekly_ind = (self.static_indicators or {}).get('WEEKLY')
            
            trade_data = {
                "timestamp": self.trade_start_time,
                "strategy": STRATEGY,
                "expiry_day": self.expiry_cache.get("is_expiry", False),
                "symbol": tradable_symbol,
                "spot_price": getattr(self, 'spot_entry_price', 0.0),
                "option_price": getattr(self, 'option_entry_price', 0.0),
                "option_type": "CALL" if signal.upper() == "BUY" else "PUT",
                "action": "BUY",
                "quantity": QUANTITY,
                "order_id": entry_order_id,
                "leg_type": "ENTRY",
                "stoploss": self.stoploss_price,
                "target": self.target_price,
                "leg_status": "Open",
                "reason": reason,
                "daily_cpr_type": (daily_ind  or {}).get('cpr_type'),
                "weekly_cpr_type": (weekly_ind or {}).get('cpr_type'),
                "hourly_vs_weekly_cpr": getattr(self, 'current_hourly_vs_weekly_cpr', None),
                "current_rtp": getattr(self, 'current_rtp', False),
                "rtp_9_30": getattr(self, 'rtp_9_30', False)
            }

            # ── 📊 INSTITUTIONAL CONTEXT (computed at order placement only) ──────────
            try:
                _ctx_gap      = self.gap_type_cache or self.compute_gap_type(None)
                _ctx_cpr_rel  = getattr(self, 'current_cpr_relation', None)
                _ctx_open_loc = getattr(self, 'current_open_location', None)
                trade_data.update({
                    "gap_type":      _ctx_gap,
                    "cpr_relation":  _ctx_cpr_rel,
                    "open_location": _ctx_open_loc,
                    "gap_pct": getattr(self, 'gap_percent', None),
                    "rsi_level": getattr(self, 'current_rsi', None),
                    "close_location": getattr(self, 'current_close_location', None),
                    "first_15m_candle_type": getattr(self, 'first_15m_candle_type', None),
                    "current_candle_type": getattr(self, 'current_candle_type', None),
                })
                order_logger.info(
                    f"📊 Institutional Context | "
                    f"Gap: {_ctx_gap} | CPR Rel: {_ctx_cpr_rel} | Open Zone: {_ctx_open_loc}"
                )
            except Exception as _ctx_err:
                order_logger.warning(f"⚠️ Institutional context computation failed (non-fatal): {_ctx_err}")
            # ── END INSTITUTIONAL CONTEXT ────────────────────────────────────────────

            order_logger.info(
                f"📊 CPR Types logged with entry | Daily: {trade_data['daily_cpr_type']} "
                f"| Weekly: {trade_data['weekly_cpr_type']}"
            )
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
        """
        Subscribe dynamically to option LTP once order is placed.
        Enhanced with retry logic and verification.
        """
        if not getattr(self, "option_symbol", None):
            order_logger.warning("❌ Option symbol not set yet")
            return
        
        # Avoid duplicate subscription
        if self.option_symbol in self.ws_subscriptions:
            order_logger.info(f"ℹ️ Option {self.option_symbol} already subscribed")
            return
        
        max_retries = 3
        subscribed = False
        
        for attempt in range(max_retries):
            try:
                with self.ws_subscription_lock:
                    self.ws_subscriptions.add(self.option_symbol)
                
                self.client.subscribe_ltp(
                    [{"exchange": OPTION_EXCHANGE, "symbol": self.option_symbol}],
                    on_data_received=self.on_ltp_update
                )
                order_logger.info(f"📡 Subscribed to Option {self.option_symbol} (attempt {attempt+1})")
                subscribed = True
                
                # Start verification for this option symbol
                threading.Thread(
                    target=self._verify_subscriptions_async,
                    args=([self.option_symbol],),
                    daemon=True
                ).start()
                break
                
            except Exception as e:
                order_logger.error(f"❌ Option subscribe failed for {self.option_symbol} (attempt {attempt+1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        
        if not subscribed:
            order_logger.error(f"🚨 Failed to subscribe to option {self.option_symbol} after {max_retries} attempts")
            with self.ws_subscription_lock:
                self.ws_subscriptions.discard(self.option_symbol)
    
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
            # 🚀 OPTIMIZATION: Check BacktestingManager Cache First (1m timeframe)
            sim_date = get_now() # Use full timestamp for slicing if needed, but here we just need the dataframe valid for today
            logger.info(f"Fetching 1-minute option data for backtesting: {self.option_symbol}")
            
            # Get simulation date range works for single day
            sim_date_only = sim_date.date()
            start_date = sim_date_only.strftime("%Y-%m-%d")
            end_date = sim_date_only.strftime("%Y-%m-%d")
            
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
            
            # 🚀 OPTIMIZATION: Cache the result for future use
            BACKTESTING_MANAGER.cache_option_data(self.option_symbol, "1m", df)

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

            try:
                self.unsubscribe_option_ltp(exit_symbol)
            except Exception as e:
                order_logger.error(f"Unable to unsubscribe option ltp")
                pass
                     
            if BACKTESTING_MANAGER.active:
                order_logger.info("📋 [SIMULATION] Exit order simulated in backtesting mode.")
                current_time = get_now().time()
                exit_order_id = f"{action}_Order_{current_time.strftime('%H:%M:%S')}"
                exit_price = self._update_option_ltp_from_df(get_now())

            if (exit_order_id is not None or exit_price is not None):
                order_logger.info(f"Exit Order Success: {exit_order_id}")
                if self.option_ltp is None:
                    self.option_ltp = self.option_entry_price
                if exit_price:
                    # Calculate actual P&L
                    if self.position == "BUY":
                        unrealized_pnl = unrealized_pnl
                    else:
                        unrealized_pnl = unrealized_pnl
                    
                    order_logger.info(f"EXIT EXECUTED: Price = {exit_price:.2f}")
                    order_logger.info(f"Unrealized P&L: {unrealized_pnl:.2f}")
                    
                    # Comprehensive trade summary
                    order_logger.info(f"=== TRADE {self.trade_count} COMPLETED ===") 
                    order_logger.info(f"Trade Start Time: {self.trade_start_time}")
                    order_logger.info(f"Spot Entry: {self.position} @ {self.spot_entry_price:.2f}")
                    order_logger.info(f"Spot Exit: {action} @ {spot_ltp}")
                    order_logger.info(f"Option Symbol: {self.option_symbol}")
                    order_logger.info(f"Option Entry: {self.position} @ {self.option_entry_price:.2f}")
                    order_logger.info(f"Option Exit: {action} @ {self.option_ltp:.2f}")
                    order_logger.info(f"Trade End Time: {trade_end_time}")
                    order_logger.info(f"Unrealized P&L: {unrealized_pnl:.2f}")
                    order_logger.info(f"Reason: {reason}")
                    order_logger.info(f"Duration: {trade_duration}")

                    log_trade_db({
                    "timestamp": trade_end_time,
                    "symbol": self.option_symbol,
                    "action": "SELL",
                    "stoploss": self.stoploss_price,
                    "target": self.target_price,
                    "spot_price": self.ltp if self.ltp else 0.0,
                    "option_price": float(f"{self.option_ltp:.2f}"),
                    "quantity": QUANTITY,
                    "option_type": "CALL" if self.position.upper() == "BUY" else "PUT",
                    "order_id": exit_order_id,
                    "strategy": STRATEGY,
                    "leg_type": "EXIT",
                    "reason": reason,
                    "leg_status": "Closed"
                    })

                    self.reconcile_trade_pnl({
                    "symbol": self.option_symbol,
                    "strategy": STRATEGY,
                    "option_price": float(f"{self.option_ltp:.2f}"),
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
            self.broker_sl = None          # Reset broker SL tracker for next trade
            
            # Reset trade-specific tracking properties per user request
            self.current_rsi = None
            self.current_candle_type = None
            self.current_close_location = None
            
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
            try:
                self.tsl_enabled = False
            except Exception:
                pass
            # Keep stored gatekeeper pivots across position exits so
            # `check_entry_signal` can still reference them. Do NOT clear
            # `gk_resistance_pivot` / `gk_support_pivot` here.
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
            if MODE in ["BACKTESTING", "BACKTESTING_RANGE"]:
                logger.info("📋 [SIMULATION] Skipping final order cancellation and position close API calls")
            else:
                logger.info("Attempting final order cancellations and position close via API")
                try:
                    cancel_result = client.cancelallorder(strategy=STRATEGY)
                    logger.info(f"Cancel all orders result: {cancel_result}")
                except Exception as e:
                    logger.warning(f"⚠️ Error cancelling all orders: {e}")
                
                try:
                    close_result = client.closeposition(strategy=STRATEGY)
                    logger.info(f"Close position result: {close_result}")
                except Exception as e:
                    logger.warning(f"⚠️ Error closing position via API (may already be closed or position not held): {e}")
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
            # Signal Replay Client to stop (simulation)
            # ---------------------------
            try:
                # Ensure BACKTESTING_MANAGER is marked inactive so replay loops can detect it
                # ONLY if NOT in BACKTESTING_RANGE (where we want to keep manager active for next day)
                if MODE != "BACKTESTING_RANGE":
                    try:
                        BACKTESTING_MANAGER.active = False
                    except Exception:
                        pass
            except Exception:
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
                    # Only attempt shutdown if scheduler is actually running
                    if self.scheduler.running:
                        self.scheduler.shutdown(wait=False)
                        main_logger.info("Scheduler shutdown completed")
                    else:
                        logger.debug("Scheduler is not running, skipping shutdown")
                except Exception as e:
                    # best-effort shutdown - log but don't fail
                    logger.debug(f"Scheduler shutdown error (may already be stopped): {e}")
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
        main_logger.warning("=== 🚀 STARTING MYALGO TRADING SESSION ===")
        session_start_time = datetime.now()
        main_logger.warning(f"⏰ Session Start Time: {session_start_time}")
        
        try:
            # websocket thread
            main_logger.info(f"Starting WebSocket thread for {MODE} data feed")
            ws_thread = threading.Thread(target=self.websocket_thread, daemon=True)
            ws_thread.start()
            time.sleep(1)
            # allow websocket to connect
            
            # strategy thread (contains scheduler)
            main_logger.info("Starting strategy thread with signal processing")
            strat_thread = threading.Thread(target=self.strategy_thread, daemon=True)
            strat_thread.start()
            
            main_logger.info("All threads started successfully - entering main loop")
            main_logger.info(f"System ready for trading | Max trades: {MAX_TRADES_PER_DAY} | Signal interval: {SIGNAL_CHECK_INTERVAL}min")
            
            try:
                while not self.stop_event.is_set() and self.running:
                    time.sleep(0.1)
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
            main_logger.warning("=== TRADING SESSION COMPLETED ===")
            main_logger.warning(f"Session Start: {session_start_time}")
            main_logger.warning(f"Session End: {session_end_time}")
            main_logger.warning(f"Total Duration: {session_duration}")
            main_logger.info(f"Total Trades Executed: {self.trade_count}")
            main_logger.info(f"Max Trades Allowed: {MAX_TRADES_PER_DAY}")
            main_logger.info(f"Final Status: {'Max trades reached' if self.trade_count >= MAX_TRADES_PER_DAY else 'Session ended normally'}")
        
            if MODE == "BACKTESTING":
                main_logger.warning("📋 [SIMULATION] Backtesting session ended")
                from backtesting_analysis import main
                main()
            
            logger.info("Bot stopped")
            
            # Exit system unless instructed otherwise
            if MODE == "BACKTESTING_RANGE":
                main_logger.info("✅ Multi-day backtesting completed successfully")
                # Run analysis after completion
                main_logger.info("🔍 Starting post-backtest analysis...")
                from backtesting_analysis import main as run_analysis
                run_analysis(strategy_name=STRATEGY, symbol=SYMBOL, timeframe="5m")
                sys.exit(0)
            else:
                send_telegram_alert(message="🔔 Trading session completed", username="valgo_live", priority=5)
                sys.exit()
# -------------------------
# main driver
# -------------------------
def _run_trading_engine():
    
    # ═════════════════════════════════════════════════════════════
    # MODE DETECTION & INITIALIZATION
    # ═════════════════════════════════════════════════════════════
    
    # ─────────────────────────────────────────────────────────────
    # MODE 1: BACKTESTING - DATE RANGE (NEW)
    # ─────────────────────────────────────────────────────────────
    # ─────────────────────────────────────────────────────────────
    # MODE 1: BACKTESTING - DATE RANGE (NEW - OPTIMIZED)
    # ─────────────────────────────────────────────────────────────
    if MODE == "BACKTESTING_RANGE":
        try:
            # Validate configuration
            if not START_DATE or not END_DATE:
                main_logger.error("❌ BACKTESTING_RANGE mode requires START_DATE and END_DATE")
                sys.exit(1)
            
            main_logger.warning(f"📅 BACKTESTING: {START_DATE} → {END_DATE}")
            
            # 1. Initialize Manager with Range
            BACKTESTING_MANAGER.start_range(
                start_date_str=START_DATE,
                end_date_str=END_DATE,
                symbol=SYMBOL,
                exchange=EXCHANGE,
                timeframe="1m"
            )
            
        except Exception as e:
            main_logger.exception(f"❌ Multi-day backtesting initialization failed: {e}")
            sys.exit(1)
    
    # ─────────────────────────────────────────────────────────────
    # MODE 2: BACKTESTING - SINGLE DAY (EXISTING)
    # ─────────────────────────────────────────────────────────────
    elif MODE == "BACKTESTING" and SIMULATION_DATE:
        try:
            BACKTESTING_MANAGER.start(SIMULATION_DATE, symbol=SYMBOL, exchange=EXCHANGE, timeframe="1m")
            main_logger.info(f"🎬 Simulation Mode Activated for {SIMULATION_DATE}")
        except Exception:
            main_logger.exception("❌ 🚨 Simulation startup failed; exiting")
            sys.exit(1)
    
    # ─────────────────────────────────────────────────────────────
    # MODE 3: LIVE TRADING (EXISTING)
    # ─────────────────────────────────────────────────────────────
    else:
        main_logger.info("🚀 Live Mode Activated")
        #Telegeram alert for live mode
        send_telegram_alert(message="🚀 MYALGO Bot started in LIVE mode", username="valgo_live", priority=5)
        if not market_is_open():
            logger.info("🛑 Market closed right now. Please schedule the bot to run during market hours.")
            sys.exit(1)
    
    # ═════════════════════════════════════════════════════════════
    # RUN BOT (ALL MODES)
    # ═════════════════════════════════════════════════════════════
    
    # In BACKTESTING_RANGE, bot.run() -> websocket_thread -> REPLAY_CLIENT.replay_range -> handles loop
    bot = MYALGO_TRADING_BOT()
    if MODE == "BACKTESTING_RANGE":
        bot._precalculate_strategy_indicators()
    bot.run()

if __name__ == "__main__":
    run()