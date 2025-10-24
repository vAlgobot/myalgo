# myalgo_trading_system.py
from sqlalchemy import true
from sqlalchemy.sql.functions import now
from dataclasses import dataclass
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import logging
from openalgo import api
import pandas as pd
from datetime import datetime, timedelta, date
import threading
import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

import pytz
import re
import sys
from dataclasses import dataclass
import os
import math

from typing import Dict, List, Optional

# ----------------------------
# Configuration
# ----------------------------
API_KEY = "112c4900f6b2d83b7d812921de13d36898116fd79a592d96cec666dfbbc389f8"
API_HOST = "http://127.0.0.1:5000"
WS_URL = "ws://127.0.0.1:8765"

client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

# Instrument
SYMBOL = "NIFTY"
EXCHANGE = "NSE_INDEX"
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
OPTION_STRIKE_SELECTION = "OTM8"  # "ATM", "ITM1", "OTM2", etc.
EXPIRY_LOOKAHEAD_DAYS = 30
LOT_QUANTITY =75
LOT = 1
QUANTITY = LOT * LOT_QUANTITY
SL_ORDER_ENABLED = False
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
TSL_ENABLED = True # enable trailing stoploss behavior
TSL_METHOD = "TSL_CPR_range_SL_TP" # trailing method for this enhancement

# ===============================================================
# 🔒 ENHANCEMENT: STOP LOSS AUTOMATION & ENTRY RESTRICTIONS
# ===============================================================
# Stop Loss automation constants
MAX_SL_RETRIES = 3              # Maximum retry attempts for SL confirmation
SL_RETRY_DELAY = 2              # Seconds between SL retry attempts
SL_BUFFER_PCT = 0.25            # 25% buffer for automatic SL placement

# Entry restrictions configuration
DAY_HIGH_LOW_VALIDATION_FROM_TRADE = 1  # Apply day high/low validation from this trade count onward

# ===============================================================
# 🎯 CPR PIVOT EXCLUSION CONFIGURATION
# ===============================================================
# Exclude unreliable or frequently false pivot levels from CPR targets
# CPR_EXCLUDE_PIVOTS = [
#     "DAILY_S3", "DAILY_S4", "DAILY_R3", "DAILY_R4",      # Extreme daily levels
#     "WEEKLY_S4", "WEEKLY_R4", "WEEKLY_TC",  "WEEKLY_BC",                              # Far weekly levels  
#     "MONTHLY_S3", "MONTHLY_S4", "MONTHLY_R3", "MONTHLY_R4"  # Extreme monthly levels
# ]

CPR_EXCLUDE_PIVOTS = [     # Extreme daily levels
     "WEEKLY_TC",  "WEEKLY_BC",                              # Far weekly levels  
    "MONTHLY_TC", "MONTHLY_BC",  # Extreme monthly levels
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
# Enhanced Logging System - Fix import path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from core.logger import get_logger, log_trade_execution

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
# 📘 SQLAlchemy Dataclass Setup for Trade Logging
# =========================================================

Base = declarative_base()

@dataclass
class TradeLog(Base):
    __tablename__ = "my_trade_logs"

    id: int = Column(Integer, primary_key=True, autoincrement=True)
    timestamp: datetime = Column(DateTime, default=datetime.now)
    instrument: str = Column(String(50), default=SYMBOL),
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

def get_nearest_level_above(price: float, levels: List[float]) -> Optional[float]:
    return next((lvl for lvl in sorted(levels) if lvl > price), None)

def get_nearest_level_below(price: float, levels: List[float]) -> Optional[float]:
    return next((lvl for lvl in sorted(levels, reverse=True) if lvl < price), None)
# ----------------------------
# MyAlgo Trdaing System
# ----------------------------
class MYALGO_TRADING_BOT:
    # ------------------------
    # Initialization
    # -------------------------
    def __init__(self):
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
        self._state_lock = threading.Lock() # state lock to avoid race between entry placement and websocket LTP handling
        # CPR_range_SL_TP specific attributes
        self.cpr_levels_sorted = []
        self.cpr_targets = []
        self.cpr_sl = 0.0
        self.cpr_side = None
        self.current_tp_index = 0
        self.cpr_tp_hit_count = 0
        self.cpr_active = False
        # Enhanced CPR attributes for dynamic targeting and exclusions
        self.dynamic_target_info = None
        self.excluded_pivots_info = None
        logger.info("Bot initialized")
        main_logger.info("=== MyAlgo Trading Bot Initialized ===")
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
        # Align start to next 5 seconds
        # self.scheduler.add_job(
        #     self.strategy_job,
        #     IntervalTrigger(seconds=5),
        #     id="strategy_signal_job",
        #     replace_existing=True,
        # )
        # self.scheduler.start()
        # logger.info("APScheduler started — aligned to 5-second intervals")
        # align to minute boundary; check every SIGNAL_CHECK_INTERVAL minutes at second=5 to allow candle to complete
        self.scheduler.add_job(
            self.strategy_job,
            CronTrigger(minute=f"*/{int(SIGNAL_CHECK_INTERVAL)}", second=5),
            id="strategy_signal_job",
            replace_existing=True,
        )
        self.scheduler.start()
        logger.info("APScheduler started — signal checks every %s minute(s)", SIGNAL_CHECK_INTERVAL)

    def strategy_job(self):
        """Main scheduled job — runs aligned to timeframe close calls."""
        try:
            # If reached daily trade limit, initiate graceful shutdown and skip signals
            if not self.position and self.trade_count >= MAX_TRADES_PER_DAY:
                logger.info("Max trades reached (%d). Initiating graceful shutdown.", MAX_TRADES_PER_DAY)
                self.shutdown_gracefully()
                return

            # Ensure static daily indicators are present and fresh
            if not self.static_indicators or self.static_indicators_date != date.today():
                # precompute once
                ok = self.compute_static_indicators()
                if not ok:
                    logger.warning("Daily indicators missing — skipping this tick")
                    return

            # ENTRY: only attempt if no open position and not exiting
            if not self.position and not self.exit_in_progress:
                intraday = self.get_intraday()
                if intraday.empty:
                    logger.debug("No intraday data")
                else:
                    signal = self.check_entry_signal(intraday)
                    if signal:
                        self.place_entry_order(signal, intraday)

            # EXIT: if a position exists, check exit conditions using latest intraday
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
        logger.info("Strategy scheduler starting")
        self.init_scheduler()
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
    # -------------------------
    # WebSocket - Real time data LTP 
    # -------------------------  
    def on_ltp_update(self, data):
        """Handle incoming websocket ticks/quote updates with comprehensive position tracking."""
        try:
            dtype = data.get("type")
            symbol = data.get("symbol") or (data.get("data") or {}).get("symbol")
            ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")
            if symbol == self.option_symbol:
                self.option_ltp = float(ltp)
                return
            elif symbol == SYMBOL:
                self.ltp = float(ltp)
            if symbol is None or ltp is None:
                position_logger.info("No LTP")
                return
            # update raw LTP immediately
            self.ltp = float(ltp)
            now = datetime.now().strftime("%H:%M:%S")
            # 🔍 Check LTP pending breakout confirmations
            if LTP_BREAKOUT_ENABLED and self.pending_breakout:
                signal_logger.info(f"✅ LTP breakout check")
                now_ts = datetime.now(IST)
                side = self.breakout_side
                high = self.pending_breakout.get("high", 0)
                low = self.pending_breakout.get("low", 0)

                if now_ts > self.pending_breakout_expiry:
                    signal_logger.info("❌ Breakout window expired — clearing pending signal.")
                    self.reset_pending_breakout()
                else:
                    if side == "BUY" and self.ltp > high:
                        signal_logger.info(f"✅ LTP breakout confirmed (BUY) at {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("BUY", intraday)
                        self.reset_pending_breakout()
                    elif side == "SELL" and self.ltp < low:
                        signal_logger.info(f"✅ LTP breakout confirmed (SELL) at {self.ltp:.2f}")
                        intraday = self.get_intraday()
                        self.place_entry_order("SELL", intraday)
                        self.reset_pending_breakout()
                    else:
                    # still waiting
                        print(f"\r⏳ Waiting for {side} breakout | LTP={self.ltp:.2f}", end="")
                # Don't process position logic while waiting
                return
            # Snapshot critical state under lock — use these locals for all monitoring decisions
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
                print(f"\r[{now}] {symbol} LTP={self.ltp:.2f} | No Active Position", end="")
                return
            # Choose LTP stream for monitoring (track_ltp). Do not re-read self.* after snapshot.
            track_ltp = None
            tracking_mode = "UNKNOWN"
            if USE_SPOT_FOR_SLTP:
                track_ltp = self.ltp
                tracking_mode = "SPOT"
                track_option_ltp = getattr(self, "option_ltp", None)
                if track_option_ltp is None:
                    position_logger.debug("Option LTP not available yet, using entry price for PnL placeholder")
                    track_option_ltp = option_entry_price if option_entry_price else entry_price
                self.option_ltp = track_option_ltp
            elif USE_OPTION_FOR_SLTP and option_symbol:
                try:
                    # q_opt = self.client.quotes(symbol=option_symbol, exchange=OPTION_EXCHANGE)
                    track_ltp = getattr(self, "option_ltp", None)
                    tracking_mode = "OPTION"
                    self.option_ltp = track_ltp
                except Exception:
                    track_ltp = None

            if track_ltp is None:
                print(f"\r[{now}] Waiting for LTP updates...", end="")
                return

            # Compute P&L and distances using snapshot values only
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
                    print(f"\n[{now}] CPR Stoploss hit at {track_ltp:.2f}")
                    with self._state_lock:
                        self.exit_in_progress = True
                    threading.Thread(target=self.place_exit_order, args=("CPR_STOPLOSS",), daemon=True).start()
                    return
                elif cpr_result == "FINAL_EXIT":
                    position_logger.info(f"🏁 CPR FINAL TARGET ACHIEVED at {track_ltp:.2f}")
                    print(f"\n[{now}] CPR Final target reached at {track_ltp:.2f}")
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
                        print(f"\n[{now}] Stoploss hit at {track_ltp:.2f}")
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
                        print(f"\n[{now}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return

                    if track_ltp <= target_price:
                        position_logger.info(f"🟢 TSL TP HIT: {track_ltp:.2f} <= {target_price:.2f} — advancing TSL")
                        self._handle_tsl_tp_hit(position, target_price)
                        return

            else:
                # Standard (non-TSL) behavior: exit immediately on TP or SL
                if position == "BUY":
                    if track_ltp <= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} <= {stoploss_price:.2f}")
                        print(f"\n[{now}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp >= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} >= {target_price:.2f}")
                        print(f"\n[{now}] Target hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return
                else:
                    if track_ltp >= stoploss_price:
                        position_logger.warning(f"🔴 STOP LOSS TRIGGERED: {track_ltp:.2f} >= {stoploss_price:.2f}")
                        print(f"\n[{now}] Stoploss hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("STOPLOSS",), daemon=True).start()
                        return
                    if track_ltp <= target_price:
                        position_logger.info(f"🟢 TARGET ACHIEVED: {track_ltp:.2f} <= {target_price:.2f}")
                        print(f"\n[{now}] Target hit at {track_ltp:.2f}")
                        with self._state_lock:
                            self.exit_in_progress = True
                        threading.Thread(target=self.place_exit_order, args=("TARGET",), daemon=True).start()
                        return

        except Exception:
            logger.exception("on_ltp_update error")
            position_logger.error("Error in LTP update processing", exc_info=True)

    def websocket_thread(self):
        try:
            logger.info("Connecting websocket...")
            self.client.connect()
            try:
                # SDK variations: try mode argument, otherwise call without
                self.client.subscribe_ltp(self.instrument, on_data_received=self.on_ltp_update)
            except TypeError:
                self.client.subscribe_ltp(self.instrument, on_data_received=self.on_ltp_update)
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
    
    def retry_api_call(self, func, max_retries=3, delay=2, description="API call", *args, **kwargs):
        """
        Generic retry mechanism for API calls.
        Retries the given function up to max_retries times if it fails or returns invalid/empty data.
        """
        for attempt in range(1, max_retries + 1):
            try:
                result = func(*args, **kwargs)

                # ✅ Validate non-empty DataFrame (if returned)
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

    def get_intraday(self, days=LOOKBACK_DAYS):
        """Fetch intraday OHLC data with retry mechanism for both intraday and daily intervals."""
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        # --- Intraday (5m, 1m etc.) ---
        raw = self.retry_api_call(
            func=self.client.history,
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

        # normalize
        df = pd.DataFrame(raw["data"]) if isinstance(raw, dict) and "data" in raw else raw.copy()
        for c in ["open", "high", "low", "close", "ltp", "volume"]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")

        # --- Daily OHLC (D interval) ---
        daily_raw = self.retry_api_call(
            func=self.client.history,
            max_retries=3,
            delay=2,
            description="Daily OHLC Fetch",
            symbol=SYMBOL,
            exchange=EXCHANGE,
            interval="D",
            start_date=datetime.now().strftime("%Y-%m-%d"),
            end_date=datetime.now().strftime("%Y-%m-%d")
        )

        if isinstance(daily_raw, pd.DataFrame) and not daily_raw.empty:
            latest = daily_raw.iloc[-1]
            self.current_day_high = float(latest.get("high", self.current_day_high or 0))
            self.current_day_low = float(latest.get("low", self.current_day_low or 0))
            self.current_day_open = float(latest.get("open", 0))
            self.current_day_close = float(latest.get("close", 0))
            data_logger.info(
                f"📅 Updated Current Day OHLC | O={self.current_day_open:.2f} "
                f"H={self.current_day_high:.2f} L={self.current_day_low:.2f} "
                f"C={self.current_day_close:.2f}"
            )
        else:
            logger.warning("⚠️ Could not update current day OHLC — daily data empty")

        return df
    # -------------------------
    # Static indicators 
    # -------------------------
    def compute_static_indicators(self):
        """Compute static CPR indicators for all enabled timeframes and store in one structure."""
        try:
            self.static_indicators = {}
            indicators_logger.info("════════════════════════════")
            indicators_logger.info("🔁 INITIALIZING MULTI-TIMEFRAME CPR COMPUTATION")
            indicators_logger.info(f"Enabled CPR Timeframes: {', '.join(CPR)}")
            indicators_logger.info("════════════════════════════")

            # DAILY CPR
            if 'DAILY' in CPR:
                dfdaily = self.get_historical("D",lookback=5)
                self.static_indicators['DAILY'] = self.compute_cpr(dfdaily, "DAILY")
                self.static_indicators_date = date.today()
            # WEEKLY CPR
            if 'WEEKLY' in CPR:
                dfweekly = self.get_historical("W",lookback=20)
                self.static_indicators['WEEKLY'] = self.compute_cpr(dfweekly, "WEEKLY")
            # MONTHLY CPR
            if 'MONTHLY' in CPR:
                dfmonthly = self.get_historical("M",lookback=70)
                self.static_indicators['MONTHLY'] = self.compute_cpr(dfmonthly, "MONTHLY")
            # Combined summary
            indicators_logger.info("═══════════════════════")
            indicators_logger.info("📘 CPR SUMMARY OVERVIEW (All Active Timeframes)")
            for key, val in self.static_indicators.items():
                if val:
                    indicators_logger.info(
                        f"{key:<8} | Pivot:{val['pivot']:.2f} | BC:{val['bc']:.2f} | TC:{val['tc']:.2f} | Width:{val['cpr_range']:.2f}"
                    )
            indicators_logger.info("═══════════════════════")
            return true

        except Exception as e:
            indicators_logger.error(f"❌ Error computing static indicators: {e}", exc_info=True)
    # -------------------------
    # Get Historical OHLCV 
    # -------------------------
    def get_historical(self, interval="D", lookback=90):
        """
        Dynamic reusable function to fetch historical OHLC data for any timeframe.
        Supports '5m', 'D' (Daily), 'W' (Weekly), 'M' (Monthly)
        Weekly and Monthly bars are computed by resampling Daily data.
        """
        try:
            end_date = datetime.now()

            # Determine start_date range
            if interval == "5m":
                start_date = end_date - timedelta(days=lookback)
            elif interval in ["D", "W", "M"]:
                start_date = end_date - timedelta(days=lookback)
            else:
                raise ValueError(f"Unsupported interval: {interval}")

            # # Fetch base data (OpenAlgo supports up to daily)
            # raw = self.client.history(
            #     symbol=SYMBOL,
            #     exchange=EXCHANGE,
            #     interval="D",   # ✅ always fetch daily for higher aggregation
            #     start_date=start_date.strftime("%Y-%m-%d"),
            #     end_date=end_date.strftime("%Y-%m-%d")
            #     )

            # Use retry_api_call wrapper for robustness
            description = f"Historical Fetch ({interval})"
            raw = self.retry_api_call(
                func=self.client.history,
                max_retries=3,
                delay=2,
                description=description,
                symbol=SYMBOL,
                exchange=EXCHANGE,
                interval="D" if interval in ["W", "M"] else interval,  # fetch daily base for resampling when needed
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

            # --- Resampling for Weekly and Monthly ---
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

        # Unified log style
        indicators_logger.info("═══════════════════════")
        indicators_logger.info(f"📊 {label.upper()} CPR INDICATOR LEVELS (Previous {label.title()} Data)")
        indicators_logger.info("───────────────────────")
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
        indicators_logger.info("═══════════════════════")

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
    def compute_dynamic_indicators(self, intraday_df):
        """
        Compute EMA, SMA, RSI on intraday_df and return dictionary `dyn`.
        Enhanced with comprehensive logging of all computed indicators.
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

        # Candles: last completed (-2) and prev (-3)
        last_candle = df.iloc[-2].to_dict()
        prev_candle = df.iloc[-3].to_dict()
        dyn["last_candle"] = last_candle
        dyn["prev_candle"] = prev_candle

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
        
        # Log candle data
        lc = dyn.get("last_candle", {})
        pc = dyn.get("prev_candle", {})
        if lc:
            indicators_logger.debug(f"Last Candle: O={lc.get('open',0):.2f}, H={lc.get('high',0):.2f}, L={lc.get('low',0):.2f}, C={lc.get('close',0):.2f}")
        if pc:
            indicators_logger.debug(f"Prev Candle: O={pc.get('open',0):.2f}, H={pc.get('high',0):.2f}, L={pc.get('low',0):.2f}, C={pc.get('close',0):.2f}")

        # Single-line print: numeric dyn entries + last/prev candle summary
        try:
            items = []
            # numeric dyn metrics
            for k, v in dyn.items():
                if isinstance(v, (int, float)):
                    items.append(f"{k}:{round(v,2)}")
            # include EMA/SMA/RSI keys explicitly to keep order
            for p in EMA_PERIODS:
                key = f"EMA_{p}"
                if key in dyn:
                    items.append(f"{key}:{round(dyn[key],2)}")
            for p in SMA_PERIODS:
                key = f"SMA_{p}"
                if key in dyn:
                    items.append(f"{key}:{round(dyn[key],2)}")
            for p in RSI_PERIODS:
                key = f"RSI_{p}"
                if key in dyn:
                    items.append(f"{key}:{round(dyn[key],2)}")
            # last & prev candle fields
            lc = dyn.get("last_candle", {})
            pc = dyn.get("prev_candle", {})
            if lc:
                items.append(f"last_o:{lc.get('open',0):.2f}")
                items.append(f"last_h:{lc.get('high',0):.2f}")
                items.append(f"last_l:{lc.get('low',0):.2f}")
                items.append(f"last_c:{lc.get('close',0):.2f}")
            if pc:
                items.append(f"prev_o:{pc.get('open',0):.2f}")
                items.append(f"prev_h:{pc.get('high',0):.2f}")
                items.append(f"prev_l:{pc.get('low',0):.2f}")
                items.append(f"prev_c:{pc.get('close',0):.2f}")
            # print single-line status (carriage return to overwrite)
            print("\r" + " | ".join(items) + "    ", end="")
        except Exception:
            # don't fail the indicator computation if print fails
            logger.exception("Dynamic indicators print failed")

        return dyn
    # -------------------------
    # Entry & Exit conditions check
    # -------------------------
    def check_exit_signal(self, intraday_df):
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
            
           # Check EMA200 crossover exit conditions
            if self.position == "BUY":
                ema_exit_condition = last_close < ema20
                signal_logger.info(f"BUY Exit Check: Close < EMA20 -> {last_close:.2f} < {ema20:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE BELOW EMA200 (BUY Position)")
                    signal_logger.info(f"Exit Context: LTP={current_ltp:.2f}, Close={last_close:.2f}, EMA20={ema20:.2f}, P&L={unrealized_pnl:.2f}")
                    return f"CLOSE_BELOW_EMA20 (close={last_close:.2f}, EMA200={ema20:.2f})"
                    
            elif self.position == "SELL":
                ema_exit_condition = last_close > ema20
                signal_logger.info(f"SELL Exit Check: Close > ema20 -> {last_close:.2f} > {ema20:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE ABOVE EMA200 (SELL Position)")
                    signal_logger.info(f"Exit Context: LTP={current_ltp:.2f}, Close={last_close:.2f}, EMA20={ema20:.2f}, P&L={unrealized_pnl:.2f}")
                    return f"CLOSE_ABOVE_EMA20 (close={last_close:.2f}, EMA20={ema20:.2f})"
            
            signal_logger.info("⚪ NO EXIT SIGNAL - Position maintained")
            return None
        except Exception:
            logger.exception("check_exit_signal failed")
            signal_logger.error("Exit signal evaluation failed", exc_info=True)
            return None

    def check_entry_signal(self, intraday_df):
        """
        Comprehensive entry signal evaluation with integrated day high/low validation:
        
        LONG CONDITIONS:
          - close > prev_day_high
          - close > cpr_r1
          - close > ema9
          - ema9 > ema20 > ema50
          - prev_close > prev_open
          - close > open
          
        SHORT CONDITIONS: (mirror of LONG)
          - close < prev_day_low
          - close < cpr_s1
          - close < ema9
          - ema9 < ema20 < ema50
          - prev_close < prev_open
          - close < open
          
        ENHANCEMENT - Day High/Low Validation (from trade #DAY_HIGH_LOW_VALIDATION_FROM_TRADE):
          - BUY: signal candle close > current_day_high
          - SELL: signal candle close < current_day_low
          
        Performance optimized: Day high/low pre-validation before expensive technical analysis.
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

                    # Check breakout confirmation condition
                last = intraday_df.iloc[-1] if len(intraday_df) > 0 else None
                if last is not None:
                    ltp = self.ltp or 0.0
                    if pb["signal"] == "BUY":
                        if ltp > pb["trigger_level"]:
                            signal_logger.info(f"✅ LTP breakout confirmed for BUY: {ltp:.2f} > {pb['trigger_level']:.2f}")
                            self.reset_pending_breakout()
                            return "BUY"
                        else:
                            signal_logger.info(f"⏳ Waiting BUY breakout: LTP={ltp:.2f} <= {pb['trigger_level']:.2f} | Expires={self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                            return None
                elif pb["signal"] == "SELL":
                    if ltp < pb["trigger_level"]:
                        signal_logger.info(f"✅ LTP breakout confirmed for SELL: {ltp:.2f} < {pb['trigger_level']:.2f}")
                        self.reset_pending_breakout()
                        return "SELL"
                    else:
                        signal_logger.info(f"⏳ Waiting SELL breakout: LTP={ltp:.2f} >= {pb['trigger_level']:.2f} | Expires={self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                        return None

            # ENHANCEMENT: Day High/Low Validation (Early Exit for Performance)
            # Perform this check BEFORE expensive technical analysis
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

            # dynamic
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
            long_cond1 = (close > prev_day_high)
            long_cond2 = (close > cpr_r1)
            long_cond3 = (close > ema9)
            long_cond4 = (ema9 > ema20)
            long_cond5 = (prev_close > prev_open)
            long_cond6 = (close > open_)
            long_cond7 = (close > ema50)
            long_cond8 = (close > ema200)
            long_cond9 = (current_ltp > high)
            long_cond10 = (close > self.current_day_high)
             
            long_cond = long_cond1 and long_cond2 and long_cond3 and long_cond4 and long_cond5 and long_cond6 and long_cond7 and long_cond8
            # long_cond = long_cond6
            # Evaluate Short conditions step by step
            short_cond1 = (close < prev_day_low)
            short_cond2 = (close < cpr_s1)
            short_cond3 = (close < ema9)
            short_cond4 = (ema9 < ema20)
            short_cond5 = (prev_close < prev_open)
            short_cond6 = (close < open_)
            short_cond7 = (close < ema50)
            short_cond8 = (close < ema200)
            short_cond9 = (current_ltp < low)
            short_cond10 = (close < self.current_day_low)

            short_cond =  short_cond1 and short_cond2 and short_cond3 and short_cond4 and short_cond5 and short_cond6 and short_cond7 and short_cond8  
            # short_cond = short_cond6
            # Log detailed condition evaluation
            signal_logger.info("=== LONG Signal Conditions ===")
            signal_logger.info(f"  1. Close > Prev Day High: {close:.2f} > {prev_day_high:.2f} = {long_cond1}")
            signal_logger.info(f"  2. Close > CPR R1: {close:.2f} > {cpr_r1:.2f} = {long_cond2}")
            signal_logger.info(f"  3. Close > EMA9: {close:.2f} > {ema9:.2f} = {long_cond3}")
            signal_logger.info(f"  4. EMA9 > EMA20: {ema9:.2f} > {ema20:.2f} = {long_cond4}")
            signal_logger.info(f"  5. Prev Close > Prev Open: {prev_close:.2f} > {prev_open:.2f} = {long_cond5}")
            signal_logger.info(f"  6. Close > Open: {close:.2f} > {open_:.2f} = {long_cond6}")
            signal_logger.info(f"  7. Close > EMA50: {close:.2f} > {ema50:.2f} = {long_cond7}")
            signal_logger.info(f"  8. Close > EMA200: {close:.2f} > {ema200:.2f} = {long_cond8}")
            signal_logger.info(f"  9. LTP > last candle high: {current_ltp:.2f} > {high:.2f} = {long_cond9}")
            signal_logger.info(f"  10. Close > current day high: {close:.2f} > {self.current_day_high :.2f} = {long_cond10}")
            signal_logger.info(f"  LONG Signal Valid: {long_cond}")

            signal_logger.info("=== SHORT Signal Conditions ===")
            signal_logger.info(f"  1. Close < Prev Day Low: {close:.2f} < {prev_day_low:.2f} = {short_cond1}")
            signal_logger.info(f"  2. Close < CPR S1: {close:.2f} < {cpr_s1:.2f} = {short_cond2}")
            signal_logger.info(f"  3. Close < EMA9: {close:.2f} < {ema9:.2f} = {short_cond3}")
            signal_logger.info(f"  4. EMA9 < EMA20: {ema9:.2f} < {ema20:.2f} = {short_cond4}")
            signal_logger.info(f"  5. Prev Close < Prev Open: {prev_close:.2f} < {prev_open:.2f} = {short_cond5}")
            signal_logger.info(f"  6. Close < Open: {close:.2f} < {open_:.2f} = {short_cond6}")
            signal_logger.info(f"  7. Close < EMA50: {close:.2f} < {ema50:.2f} = {short_cond7}")
            signal_logger.info(f"  8. Close < EMA200: {close:.2f} < {ema200:.2f} = {short_cond8}")
            signal_logger.info(f"  9. LTP < last candle low: {current_ltp:.2f} < {low:.2f} = {short_cond9}")
            signal_logger.info(f"  10. Close < current day low: {close:.2f} < {self.current_day_low :.2f} = {short_cond10}")
            signal_logger.info(f"  SHORT Signal Valid: {short_cond}")

            # === Breakout Filter Check ===
            if long_cond:
                last_candle = last
                if LTP_BREAKOUT_ENABLED:
                    self.pending_breakout = {
                        "type": "BUY",
                        "timestamp": datetime.now(IST),
                        "high": float(last_candle.get("high", 0)),
                        "low": float(last_candle.get("low", 0))
                        }
                    now = datetime.now(IST)
                    minute = now.minute
                    next_multiple = ((minute // LTP_BREAKOUT_INTERVAL_MIN) + 1) * LTP_BREAKOUT_INTERVAL_MIN
                    expiry = now.replace(minute=0, second=0, microsecond=0) + timedelta(minutes=next_multiple)
                    self.pending_breakout_expiry = expiry
                    self.breakout_side = "BUY"
                    signal_logger.info(f"📈 BUY signal waiting for LTP breakout > {self.pending_breakout['high']:.2f} until {self.pending_breakout_expiry.strftime('%H:%M:%S')}")
                    return None
                else:
                    signal_logger.info("🟢 ENTRY SIGNAL GENERATED: BUY (no LTP breakout filter)")
                    return "BUY"

            if short_cond:
                last_candle = dyn["last_candle"]
                if LTP_BREAKOUT_ENABLED:
                    self.pending_breakout = {
                        "type": "SELL",
                        "timestamp": datetime.now(IST),
                        "high": float(last_candle.get("high", 0)),
                        "low": float(last_candle.get("low", 0))
                    }
                    now = datetime.now(IST)
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

    def initialize_cpr_range_sl_tp(self, signal, intraday_df, base_price=None):
        """
        Initialize CPR-based dynamic SL/TP system:
        - Uses merged pivot levels (daily, weekly, monthly) as targets
        - Signal candle low/high for initial SL
        - Dynamic trailing after each TP hit
        """
        try:
            position_logger.info(f"Initializing CPR Range SL/TP for {signal} signal")
            
            # Get signal candle data for initial SL
            if intraday_df is None or intraday_df.empty or len(intraday_df) < 2:
                position_logger.warning("No signal candle data available for CPR initialization")
                return False
                
            sig_candle = intraday_df.iloc[-2]
            signal_candle_low = float(sig_candle.get('low', 0) or 0)
            signal_candle_high = float(sig_candle.get('high', 0) or 0)
            
            # Merge all available pivot levels
            if not self.static_indicators:
                position_logger.error("No static indicators available for CPR levels")
                return False
                
            pivots_dict = {}
            for timeframe in ['DAILY', 'WEEKLY', 'MONTHLY']:
                if timeframe in self.static_indicators:
                    pivots_dict[timeframe.lower()] = self.static_indicators[timeframe]
                    
            # Get merged and expanded pivot levels
            merged_levels = merge_pivot_levels(pivots_dict)
            expanded_levels = expand_with_midpoints(merged_levels)
            self.cpr_levels_sorted = sorted(expanded_levels)
            
            if not self.cpr_levels_sorted:
                position_logger.warning("No CPR levels found, falling back to signal candle method")
                return False
                
            # Set entry price and side
            if base_price is None:
                base_price = float(self.entry_price or 0.0)
            self.entry_price = float(base_price)
            self.cpr_side = signal.upper()
            
            # Initialize SL and targets based on signal direction

            if self.cpr_side == "BUY":
                # SL from signal candle low
                raw_sl = round(signal_candle_low - STOPLOSS, 2)
                self.cpr_sl = signal_candle_high - MAX_SIGNAL_RANGE if signal_candle_high - signal_candle_low > MAX_SIGNAL_RANGE else raw_sl
                # Targets: all levels above entry price
                self.cpr_targets = [lvl for lvl in self.cpr_levels_sorted if lvl > base_price]
            else:  # SELL
                # SL from signal candle high  
                raw_sl = round(signal_candle_high + STOPLOSS, 2)
                self.cpr_sl = signal_candle_low + MAX_SIGNAL_RANGE if signal_candle_high - signal_candle_low  > MAX_SIGNAL_RANGE else raw_sl
                # Targets: all levels below entry price (reversed order)
                self.cpr_targets = [lvl for lvl in sorted(self.cpr_levels_sorted, reverse=True) if lvl < base_price]
                
            # Initialize tracking variables
            self.current_tp_index = 0 if self.cpr_targets else -1
            self.cpr_tp_hit_count = 0
            self.cpr_active = True
            
            # Set initial target and SL for WebSocket monitoring
            if self.cpr_targets:
                self.target_price = round(self.cpr_targets[0], 2)
            else:
                # Fallback if no CPR targets available
                if signal == "BUY":
                    self.target_price = round(base_price + TARGET, 2)
                else:
                    self.target_price = round(base_price - TARGET, 2)
                    
            self.stoploss_price = self.cpr_sl
            
            position_logger.info(f"CPR Range SL/TP Initialized Successfully:")
            position_logger.info(f"  Side: {self.cpr_side}")
            position_logger.info(f"  Entry Price: {self.entry_price:.2f}")
            position_logger.info(f"  Initial SL: {self.cpr_sl:.2f}")
            position_logger.info(f"  Signal Candle: Low={signal_candle_low:.2f}, High={signal_candle_high:.2f}")
            position_logger.info(f"  Available CPR Targets: {len(self.cpr_targets)}")
            position_logger.info(f"  Next Target: {self.target_price:.2f}")
            position_logger.info(f"  Total CPR Levels: {len(self.cpr_levels_sorted)}")
            
            return True
            
        except Exception as e:
            position_logger.error(f"Error initializing CPR Range SL/TP: {e}", exc_info=True)
            self.cpr_active = False
            return False
            
    def initialize_cpr_range_sl_tp_enhanced(self, signal, intraday_df, base_price=None):
        """
        Enhanced CPR initialization with exclusion filtering and dynamic risk:reward targeting
        Combines both CPR pivot exclusion and dynamic target selection features
        """
        try:
            position_logger.info(f"Initializing Enhanced CPR Range SL/TP for {signal} signal")
            position_logger.info(f"Features: Exclusion Filter={'ON' if CPR_EXCLUDE_PIVOTS else 'OFF'}, Dynamic Target={'ON' if USE_DYNAMIC_TARGET else 'OFF'}")
            
            # Get signal candle data for initial SL
            if intraday_df is None or intraday_df.empty or len(intraday_df) < 2:
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
                position_logger.info(f"Excluded {len(excluded_ids)} unreliable pivot levels: {', '.join(excluded_ids[:5])}")
                if len(excluded_ids) > 5:
                    position_logger.info(f"... and {len(excluded_ids) - 5} more excluded levels")
            else:
                position_logger.info("No pivot levels excluded")
                
            # Expand with midpoints
            expanded_levels = expand_with_midpoints(filtered_levels)
            self.cpr_levels_sorted = sorted(expanded_levels)
            
            if not self.cpr_levels_sorted:
                position_logger.warning("No CPR levels available after filtering, falling back to signal candle method")
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
                "timestamp": datetime.now(),
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
            resp = self.client.expiry(symbol=SYMBOL, exchange=OPTION_EXCHANGE, instrumenttype="options")
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
            resp = self.client.search(query=candidate, exchange=OPTION_EXCHANGE)
            if isinstance(resp, dict) and resp.get("data"):
                for d in resp["data"]:
                    sym = (d.get("symbol") or d.get("tradingsymbol") or "").upper()
                # ✅ Only return if it *starts* with the correct symbol
                    if sym.startswith(SYMBOL.upper()) and suffix in sym and str(strike) in sym:
                        return sym

            # 🔹 Step 2: fallback to broader expiry search
            resp2 = self.client.search(query=f"{SYMBOL}{expiry_token}", exchange=OPTION_EXCHANGE)
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
        Poll order status up to 5 times (2s interval) until executed price is available.
        Returns float price or None if never filled.
        """
        for attempt in range(5):
            time.sleep(2)
            try:
                resp = self.client.orderstatus(order_id=order_id, strategy=STRATEGY)
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
        product="MIS",
        price_type="MARKET",
        max_order_attempts=3,
        max_exec_retries=5,
        retry_delay=2,
        ):
        """
        Places an order with full retry and execution confirmation.

         1️⃣ Retry API call if network or request fails.
         2️⃣ Poll order status until executed_price is available.
         3️⃣ Retry placing order again if still not executed after polling.

        Returns:
            (order_id, executed_price) or (None, None)
            """

        for order_attempt in range(1, max_order_attempts + 1):
            description = f"Placing order attempt {order_attempt} ({symbol} x {quantity})"

            # --- Step 1: Try placing order via retry_api_call
            try:
                order_resp = self.retry_api_call(
                    func=self.client.placeorder,
                    max_retries=3,
                    delay=retry_delay,
                    description=description,
                    strategy=strategy,
                    symbol=symbol,
                    exchange=exchange,
                    action=action,
                    quantity=quantity,
                    price_type=price_type,
                    product=product,
                )
            except Exception as e:
                order_logger.warning(f"⚠️ Attempt {order_attempt}: placeorder failed: {e}")
                continue
                
            # --- Step 2: Validate order response
            if not order_resp or "orderid" not in order_resp:
                order_logger.warning(f"⚠️ Attempt {order_attempt}: Missing orderid. Retrying...")
                time.sleep(retry_delay)
                continue

            order_id = order_resp["orderid"]
            order_logger.info(f"✅ Order response: {order_resp}")
            order_logger.info(f"✅ Order placed successfully (attempt {order_attempt}) ID={order_id}")

            # --- Step 3: Confirm executed price using get_executed_price
            exec_price = None
            for exec_attempt in range(max_exec_retries):
                exec_price = self.get_executed_price(order_id)
                if exec_price is not None and exec_price > 0:
                    order_logger.info(
                        f"✅ Executed price confirmed (order {order_id}) = {exec_price}"
                    )
                    return order_id, exec_price

                order_logger.warning(
                    f"⚠️ Exec check {exec_attempt+1}/{max_exec_retries}: No fill yet. Retrying in {retry_delay}s..."
                )
                time.sleep(retry_delay)

            # --- Step 4: Re-attempt order if execution failed
            order_logger.warning(
                f"⚠️ Order {order_id} did not execute after {max_exec_retries} polls. Retrying new order..."
            )
            time.sleep(retry_delay)

        # --- Final fallback
        order_logger.error("❌ All attempts failed: Order not executed after retries.")
        order_logger.error(f"❌ Order response: {order_resp}")
        return None, None
    
    def compute_25pct_sl(self, exec_price: float, side: str) -> float:
        """Compute 25% stop loss price based on executed price and side"""
        if side.upper() == "BUY":
            return round(exec_price * (1 - SL_BUFFER_PCT), 2)
        else:
            return round(exec_price * (1 + SL_BUFFER_PCT), 2)

    def place_and_confirm_stoploss(self, exit_symbol: str, exit_exchange: str, entry_side: str, sl_price: float) -> Optional[str]:
        """
        Place stop loss order and confirm execution with retry mechanism
        Returns SL order ID on success, None on failure
        """
        sl_action = "SELL" if entry_side.upper() == "BUY" else "BUY"
        order_logger.info(f"Placing SL order -> Action={sl_action} | Trigger/SL={sl_price:.2f} | Symbol={exit_symbol}")

        for attempt in range(1, MAX_SL_RETRIES + 1):
            try:
                resp = self.client.placeorder(
                    strategy=STRATEGY,
                    symbol=exit_symbol,
                    exchange=exit_exchange,
                    action=sl_action,
                    quantity=QUANTITY,
                    price_type="SL-M",
                    product=PRODUCT,
                    trigger_price=str(sl_price)
                )

                
                order_logger.info(f"SL place order response (attempt {attempt}): {resp}")

                if not isinstance(resp, dict) or resp.get("status") != "success":
                    time.sleep(SL_RETRY_DELAY)
                    continue

                sl_order_id = resp.get("orderid")
                if not sl_order_id:
                    time.sleep(SL_RETRY_DELAY)
                    continue

                # Wait for order confirmation
                for _ in range(6):
                    time.sleep(1)
                    st = self.client.orderstatus(order_id=sl_order_id, strategy=STRATEGY)
                    if isinstance(st, dict) and st.get("status") == "success":
                        data = st.get("data", {})
                        if data.get("order_status") == "complete":
                            order_logger.info(f"SL order confirmed complete: {sl_order_id}")
                            return sl_order_id

                time.sleep(SL_RETRY_DELAY)
            except Exception:
                order_logger.exception("place_and_confirm_stoploss exception")
                time.sleep(SL_RETRY_DELAY)

        order_logger.error(f"Failed to place/confirm SL after {MAX_SL_RETRIES} attempts")
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

            order_logger.info(f"Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
            current_ltp = self.ltp if self.ltp else 25980
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
                                      action=signal, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            # resp = self.client.placeorder(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
            #                           action=signal, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            if entry_order_id is None or exec_price is None:
                order_logger.error(f"Entry order failed")
                return False
            order_logger.info(f"Order placed successfully. dynamic SL/TP calculation started")
            self.trade_start_time = datetime.now()
            # mark that we are initializing (prevent concurrent monitoring during setup)
            with self._state_lock:
                self.exit_in_progress = True
                self.position = signal

                # determine base_price (spot/option/entry)
                base_price = self.entry_price
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
                    else:
                        risk_logger.info("Using Standard CPR method")
                        cpr_success = self.initialize_cpr_range_sl_tp(signal, intraday_df, base_price=base_price)

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
                self.entry_price = exec_price
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
            self.client.subscribe_ltp(
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
                
            # Get current LTP and P&L for logging
            spot_ltp = self.ltp if self.ltp else 0.0
            option_ltp = self.option_ltp if self.ltp else 0.0
            
            # Calculate P&L before exit
            if self.position == "BUY":
                unrealized_pnl = (option_ltp - self.option_entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.option_entry_price - option_ltp) * QUANTITY
                
            # Calculate trade duration
            trade_start = getattr(self, 'trade_start_time', datetime.now())
            trade_end_time = datetime.now()
            trade_duration = trade_end_time - trade_start
            
            order_logger.info(f"Trade Spot Position: {self.position} @ {self.spot_entry_price:.2f}")
            order_logger.info(f"Current Spot LTP: {spot_ltp:.2f}")
            order_logger.info(f"Trade Option Position: {self.position} @ {self.option_entry_price:.2f}")
            order_logger.info(f"Current Option LTP: {option_ltp:.2f}")
            order_logger.info(f"Unrealized P&L: {unrealized_pnl:.2f}")
            order_logger.info(f"Trade Duration: {trade_duration}")
            
            action = "SELL" if self.position == "BUY" else "BUY"
            
            # Use option symbol if available, otherwise fall back to spot
            exit_symbol = self.option_symbol if hasattr(self, 'option_symbol') and self.option_symbol else SYMBOL
            exit_exchange = OPTION_EXCHANGE if hasattr(self, 'option_symbol') and self.option_symbol else EXCHANGE
            
            order_logger.info(f"Exit Symbol: {exit_symbol}")
            order_logger.info(f"Exit Exchange: {exit_exchange}")
            order_logger.info(f"Exit Action: {action}")
            order_logger.info(f"Exit Quantity: {QUANTITY}")
            
            # # Place exit order
            # resp = self.client.placeorder(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
            #                               action=action, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)

            order_id, exit_price = self.place_order_with_execution_retry(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
                                      action=action, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)          

            if order_id is not None or exit_price is not None:
                order_logger.error(f"Exit Order Success: {order_id}")
                
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
                    order_logger.info("=== TRADE COMPLETED ===") 
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
                    "symbol": self.option_symbol,
                    "action": "PUT" if self.position == "SELL" else "CALL",
                    "spot_price": spot_ltp,
                    "quantity": QUANTITY,
                    "price": option_ltp,
                    "order_id": order_id,
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
            self.reset_pending_breakout()
            
            order_logger.info("Position cleared and reset (including CPR state)")
            main_logger.info(f"Position {previous_position} @ {previous_entry:.2f} exited due to: {reason}")
            
            return True
        except Exception:
            logger.exception("place_exit_order failed")
            order_logger.error(f"Failed to place exit order for reason: {reason}", exc_info=True)
            self.exit_in_progress = False
            return False



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
            
            # 1️⃣ Get current open positions from broker
            try:
                positions_resp = self.client.positionbook()
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
                    action = "SELL" if net_qty > 0 else "BUY"
                    product = pos.get("product", "MIS")
                    
                    order_logger.info(f"Processing position: {symbol} | Net Qty: {net_qty} | Action: {action}")

                    if qty == 0:
                        order_logger.warning(f"Skipping {symbol} - zero quantity")
                        continue

                    # 3️⃣ Place market exit order
                    try:
                        close_resp = self.client.placeorder(
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
                        orders_resp = self.client.orderbook()
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
                                        cancel_resp = self.client.cancelorder(
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
            orders_resp = self.client.orderbook()

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
                            cancel_resp = self.client.cancelorder(
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
        main_logger.info("=== GRACEFUL SHUTDOWN INITIATED ===")
        logger.info("Graceful shutdown started")
        
        # Log final system status
        main_logger.info(f"Final Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
        main_logger.info(f"Current Position: {self.position if self.position else 'None'}")
        
        if self.position:
            current_ltp = self.ltp if self.ltp else 0.0
            if self.position == "BUY":
                unrealized_pnl = (current_ltp - self.entry_price) * QUANTITY
            else:
                unrealized_pnl = (self.entry_price - current_ltp) * QUANTITY
            main_logger.info(f"Final Position Details: {self.position} @ {self.entry_price:.2f}, LTP: {current_ltp:.2f}, P&L: {unrealized_pnl:.2f}")
        
        # set flags
        self.stop_event.set()
        self.running = False
        main_logger.info("Shutdown flags set")
        
        # stop scheduler if running
        try:
            if self.scheduler:
                self.scheduler.shutdown(wait=False)
                main_logger.info("Scheduler shutdown completed")
                logger.info("Scheduler shutdown")
        except Exception:
            logger.exception("Error shutting down scheduler")
            main_logger.error("Error shutting down scheduler", exc_info=True)
            
        # Universal cleanup: close all positions and cancel all SL orders
        main_logger.info("Performing universal position cleanup before shutdown")
        logger.info("Performing universal cleanup before shutdown")
        try:
            cleanup_success = self.exit_all_positions()
            if cleanup_success:
                main_logger.info("✅ Universal cleanup completed successfully")
            else:
                main_logger.warning("⚠️ Universal cleanup encountered issues")
        except Exception:
            main_logger.error("❌ Universal cleanup failed", exc_info=True)
            logger.exception("Universal cleanup failed during shutdown")
                
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
            main_logger.info("Starting WebSocket thread for live data feed")
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
# -------------------------
# main
# -------------------------
if __name__ == "__main__":
    bot = MYALGO_TRADING_BOT()
    bot.run()
