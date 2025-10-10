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
import pytz
import re
import sys
from dataclasses import dataclass
import os
import argparse

from typing import Dict, List, Optional

# ----------------------------
# Configuration
# ----------------------------
API_KEY = "112c4900f6b2d83b7d812921de13d36898116fd79a592d96cec666dfbbc389f8"
API_HOST = "http://127.0.0.1:5000"
WS_URL = "ws://127.0.0.1:8765"

client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

# Speed multiplier for simulation:

SIMULATION_DATE: str | None = None  # e.g. "2025-06-02" for simulation, None for live  #1.0  = real time
SIMULATION_SPEED_MULTIPLIER: float = 60.0

_simulation_state = {
    "start_real_time": None,
    "start_simulated_time": None,
    "last_replayed_time": None,
    "replay_running": False
}

# Instrument
SYMBOL = "NIFTY"
EXCHANGE = "NSE_INDEX"
PRODUCT = "MIS"
CANDLE_TIMEFRAME = "1m"
LOOKBACK_DAYS = 3
SIGNAL_CHECK_INTERVAL = 1  # minutes (use integer minutes)

# Indicators to compute
EMA_PERIODS = [9, 20, 50, 200]
SMA_PERIODS = [9, 20, 50, 200]
RSI_PERIODS = [14, 21]
CPR = ['DAILY', 'WEEKLY','MONTHLY']

# Risk settings
STOPLOSS = 50
TARGET = 5

# Trade management
MAX_TRADES_PER_DAY = 3
STRATEGY = "myalgo_scalping"

# IST timezone
IST = pytz.timezone("Asia/Kolkata")

# Option trading config
OPTION_ENABLED = True
OPTION_EXCHANGE = "NFO"
STRIKE_INTERVAL = 50
OPTION_EXPIRY_TYPE = "WEEKLY"
OPTION_STRIKE_SELECTION = "ATM"  # "ATM", "ITM1", "OTM2", etc.
EXPIRY_LOOKAHEAD_DAYS = 30
LOT_QUANTITY =75
LOT = 2
QUANTITY = LOT * LOT_QUANTITY

# Websocket CONSTANTS
HEARTBEAT_INTERVAL = 10        # seconds
OPTION_POLL_INTERVAL = 1.0     # seconds (or higher if rate-limited)
RECONNECT_BASE = 1.0           # seconds
RECONNECT_MAX = 60.0           # seconds


# Stoploss/Target tracking configuration
USE_SPOT_FOR_SLTP = True      # If True → uses spot price for SL/TP tracking
USE_OPTION_FOR_SLTP = False   # If True → uses option position LTP for SL/TP tracking

# SL/TP Enhancement config
SL_TP_METHOD = "Signal_candle_range_SL_TP" # current active method #CPR_range_SL_TP #Signal_candle_range_SL_TP
TSL_ENABLED = True # enable trailing stoploss behavior
TSL_METHOD = "TSL_Signal_candle_range" # trailing method for this enhancement

# month map for option token if needed
MONTH_MAP = {
    1: "JAN", 2: "FEB", 3: "MAR", 4: "APR", 5: "MAY", 6: "JUN",
    7: "JUL", 8: "AUG", 9: "SEP", 10: "OCT", 11: "NOV", 12: "DEC"
}

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
# CLI Parser
# ----------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Run MYALGO_TRADING_BOT with optional simulation mode")
    parser.add_argument("--simulate", type=str, default=None, help="Date (YYYY-MM-DD) for simulation mode")
    parser.add_argument("--speed", type=float, default=60.0, help="Speed multiplier for replay (default 60x)")
    return parser.parse_args()
# ----------------------------
# Time helper (simulation aware)
# ----------------------------
def now() -> datetime:
    if not SIMULATION_DATE or not _simulation_state["replay_running"]:
        return datetime.now(IST)

    if _simulation_state["last_replayed_time"] is not None:
        return _simulation_state["last_replayed_time"]

    start_real = _simulation_state["start_real_time"]
    start_sim = _simulation_state["start_simulated_time"]
    if not start_real or not start_sim:
        return datetime.now(IST)

    elapsed_real = datetime.now(IST) - start_real
    elapsed_sim = timedelta(seconds=elapsed_real.total_seconds() * SIMULATION_SPEED_MULTIPLIER)
    return (start_sim + elapsed_sim).astimezone(IST)
# ----------------------------
# Intraday data wrapper
# ----------------------------
def get_intraday(symbol: str, exchange: str, interval: str = "1m", start_date: str | None = None, end_date: str | None = None):
    if SIMULATION_DATE and not start_date and not end_date:
        start_date = end_date = SIMULATION_DATE
    elif not start_date or not end_date:
        raise ValueError("start_date and end_date must be provided when not simulating")

    df = client.history(symbol=symbol, exchange=exchange, interval=interval, start_date=start_date, end_date=end_date)
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    if df.index.tz is None:
        df.index = df.index.tz_localize(IST)
    else:
        df.index = df.index.tz_convert(IST)
    return df

class ReplayWebsocket:
    def __init__(self, client_api, simulation_date: str, speed_multiplier: float = 60.0):
        self.client_api = client_api
        self.simulation_date = simulation_date
        self.speed_multiplier = speed_multiplier
        self._stop = threading.Event()
        self._thread = None
        self._callbacks = {}
        self._subscriptions = []

    def connect(self):
        print(f"[ReplayWebsocket] Connected (simulation {self.simulation_date})")

    def subscribe_ltp(self, instruments_list, on_data_received):
        for inst in instruments_list:
            key = f"{inst['exchange']}:{inst['symbol']}"
            self._callbacks[key] = on_data_received
            self._subscriptions.append(inst)
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def _loop(self):
        if not self._subscriptions:
            return
        inst = self._subscriptions[0]
        df = get_intraday(inst["symbol"], inst["exchange"], "1m", start_date=self.simulation_date, end_date=self.simulation_date)
        df = df.sort_index()

        _simulation_state["start_real_time"] = datetime.now(IST)
        _simulation_state["start_simulated_time"] = df.index[0]
        _simulation_state["replay_running"] = True

        for ts, row in df.iterrows():
            if self._stop.is_set():
                break
            msg = {
                "type": "market_data",
                "symbol": inst["symbol"],
                "exchange": inst["exchange"],
                "mode": 1,
                "data": {
                    "ltp": float(row.close),
                    "open": float(row.open),
                    "high": float(row.high),
                    "low": float(row.low),
                    "close": float(row.close),
                    "volume": float(row.volume),
                    "timestamp": int(ts.timestamp() * 1000),
                }
            }
            key = f"{inst['exchange']}:{inst['symbol']}"
            _simulation_state["last_replayed_time"] = ts
            cb = self._callbacks.get(key)
            if cb:
                try:
                    cb(msg)
                except Exception as e:
                    print(f"[ReplayWebsocket] Callback error: {e}")
            time.sleep(60 / SIMULATION_SPEED_MULTIPLIER)

        print("[ReplayWebsocket] Completed replay")
        _simulation_state["replay_running"] = False

    def unsubscribe_ltp(self, instruments_list):
        self._stop.set()

    def disconnect(self):
        self._stop.set()
        print("[ReplayWebsocket] Disconnected simulation")
# ----------------------------
# MyAlgo Trdaing System
# ----------------------------
class MYALGO_TRADING_BOT:
    # ------------------------
    # Initialization
    # -------------------------
    def __init__(self):
        self.client = None
        self.position = None
        self.entry_price = 0.0
        self.stoploss_price = 0.0
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
        self._state_lock = threading.Lock() # state lock to avoid race between entry placement and websocket LTP handling
        logger.info("Bot initialized")
        main_logger.info("=== MyAlgo Trading Bot Initialized ===")
        main_logger.info(f"Strategy: {STRATEGY} | Symbol: {SYMBOL} | Timeframe: {CANDLE_TIMEFRAME}")
        main_logger.info(f"Risk Management: SL={STOPLOSS}, TP={TARGET} | Max Trades: {MAX_TRADES_PER_DAY}")
        main_logger.info(f"Options Enabled: {OPTION_ENABLED} | Strike Selection: {OPTION_STRIKE_SELECTION}")
        main_logger.info(f"Signal Check Interval: {SIGNAL_CHECK_INTERVAL} minutes")

        if SIMULATION_DATE:
            print(f"[MYALGO_TRADING_BOT] Simulation mode active for {SIMULATION_DATE}")
            self.client = ReplayWebsocket(client_api=client, simulation_date=SIMULATION_DATE, speed_multiplier=SIMULATION_SPEED_MULTIPLIER)
        else:
            print("[MYALGO_TRADING_BOT] Live mode active")
            self.client = client

    def on_data_received(self, data):
        ts = datetime.fromtimestamp(data["data"]["timestamp"] / 1000, tz=IST)
        ltp = data["data"].get("ltp")
        print(f"[{ts}] LTP={ltp}")
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
                        self.place_entry_order(signal)

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
    # -------------------------
    # WebSocket - Real time data LTP 
    # -------------------------  
    def option_poller(self):
        """Periodically fetch option LTP (non-blocking to websocket)."""
        while not self.stop_event.is_set() and self.running:
            try:
                opt = getattr(self, 'option_symbol', None)
                if opt:
                    q_opt = self.client.quotes(symbol=opt, exchange=OPTION_EXCHANGE)
                    ltp = float((q_opt.get("data") or {}).get("ltp", 0) or 0)
                    with self._state_lock:
                        self.option_ltp = ltp
                time.sleep(1.0)
            except Exception:
                logger.exception("option_poller error")
                time.sleep(1)
    
    def _set_attr_threadsafe(self, name, value):
        with self._state_lock:
            setattr(self, name, value)

    def heartbeat_thread(self):
        """Keeps the websocket session alive using a lightweight quotes request."""
        while not self.stop_event.is_set() and self.running:
            try:
                # perform a simple quotes call as a keepalive
                self.client.quotes(symbol="NIFTY", exchange="NSE_INDEX")
                logger.debug("✅ Heartbeat successful (quotes ping).")
            except Exception as e:
                logger.exception("heartbeat ping failed")
            time.sleep(10)  # every 10 seconds

    # websocket_thread with reconnect/backoff and safe subscribe
    # def websocket_thread(self):
    #     backoff = RECONNECT_BASE
    #     logger.info("Starting websocket loop (with reconnect)")
    #     while not self.stop_event.is_set() and self.running:
    #         try:
    #             logger.info("Connecting websocket...")
    #             self.client.connect()     # may raise
    #             # try subscribing with mode param (handle SDK differences)
    #             try:
    #                 self.client.subscribe_ltp(self.instrument, on_data_received=self.on_ltp_update, mode="quote")
    #             except TypeError:
    #                 self.client.subscribe_ltp(self.instrument, on_data_received=self.on_ltp_update)
    #             logger.info("Subscribed to LTP")

    #             # start background helper threads (once connected)
    #             if not getattr(self, '_heartbeat_started', False):
    #                 threading.Thread(target=self.heartbeat_thread, daemon=True).start()
    #                 self._heartbeat_started = True

    #             if not getattr(self, '_option_poller_started', False):
    #                 threading.Thread(target=self.option_poller, daemon=True).start()
    #                 self._option_poller_started = True

    #             # reset backoff after successful connect
    #             backoff = RECONNECT_BASE

    #             # keep alive loop; when client.disconnect or server closes, an exception may be raised or subscribe returns
    #             # keep alive loop; when client.disconnect or server closes, an exception may be raised or subscribe returns
    #             while not self.stop_event.is_set() and self.running and self.client.connected:
    #                 time.sleep(0.5)
    #             # If loop breaks, try to cleanly unsubscribe and disconnect
    #             try:
    #                 self.client.unsubscribe_ltp(self.instrument)
    #             except Exception:
    #                 pass
    #             try:
    #                 self.client.disconnect()
    #             except Exception:
    #                 pass

    #             # if we reach here without stop_event, we'll reconnect (backoff)
    #             if not self.stop_event.is_set() and self.running:
    #                 logger.warning("Websocket disconnected unexpectedly; reconnecting in %.1f sec", backoff)
    #                 time.sleep(backoff)
    #                 backoff = min(backoff * 2, RECONNECT_MAX)

    #         except Exception:
    #             logger.exception("websocket connection error; retrying in %.1f sec", backoff)
    #             try:
    #                 self.client.disconnect()
    #             except Exception:
    #                 pass
    #             time.sleep(backoff)
    #             backoff = min(backoff * 2, RECONNECT_MAX)

    #     # final cleanup
    #     try:
    #         self.client.unsubscribe_ltp(self.instrument)
    #     except Exception:
    #         pass
    #     try:
    #         self.client.disconnect()
    #     except Exception:
    #         pass
    #     logger.info("websocket loop ended")

    def on_ltp_update(self, data):
        """Handle incoming websocket ticks/quote updates with comprehensive position tracking."""
        try:
            dtype = data.get("type")
            symbol = data.get("symbol") or (data.get("data") or {}).get("symbol")

            if dtype == "quote" and symbol == SYMBOL:
                q = data.get("data", {})
                high = q.get("high")
                low = q.get("low")
                if high is not None:
                    self.current_day_high = float(high)
                    data_logger.debug(f"Updated current day high: {self.current_day_high:.2f}")
                if low is not None:
                    self.current_day_low = float(low)
                    data_logger.debug(f"Updated current day low: {self.current_day_low:.2f}")
                ltp = q.get("ltp")
            else:
                ltp = data.get("ltp") or (data.get("data") or {}).get("ltp")

            if symbol is None or ltp is None:
                return

            # update raw LTP immediately
            self.ltp = float(ltp)
            now = datetime.now().strftime("%H:%M:%S")

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
                # print(f"\r[{now}] {symbol} LTP={self.ltp:.2f} | No Active Position", end="")
                return

            # Choose LTP stream for monitoring (track_ltp). Do not re-read self.* after snapshot.
            track_ltp = None
            tracking_mode = "UNKNOWN"
            if USE_SPOT_FOR_SLTP:
                track_ltp = self.ltp
                tracking_mode = "SPOT"
                q_opt = self.client.quotes(symbol=option_symbol, exchange=OPTION_EXCHANGE)
                track_option_ltp = float((q_opt.get("data") or {}).get("ltp", 0) or 0)
                self.option_ltp = track_option_ltp
            elif USE_OPTION_FOR_SLTP and option_symbol:
                try:
                    q_opt = self.client.quotes(symbol=option_symbol, exchange=OPTION_EXCHANGE)
                    track_ltp = float((q_opt.get("data") or {}).get("ltp", 0) or 0)
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
                position_logger.info(f"Spot Entry: {position} @ {entry_price:.2f}")
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
            # If TSL is enabled and the SL_TP_METHOD is Signal_candle_range_SL_TP, handle TP hits by trailing instead of exiting immediately.
            if TSL_ENABLED and SL_TP_METHOD == "Signal_candle_range_SL_TP":
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
                self.client.subscribe_ltp(self.instrument, on_data_received=self.on_ltp_update, mode="quote")
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
    # -------------------------
    # Get Intraday Candle
    # -------------------------
    def get_intraday(self, days=LOOKBACK_DAYS):
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            raw = self.client.history(symbol=SYMBOL, exchange=EXCHANGE, interval=CANDLE_TIMEFRAME,
                                      start_date=start_date.strftime("%Y-%m-%d"), end_date=end_date.strftime("%Y-%m-%d"))
            if isinstance(raw, pd.DataFrame):
                df = raw.copy()
            elif isinstance(raw, dict) and "data" in raw:
                df = pd.DataFrame(raw["data"])
            else:
                df = pd.DataFrame()
            # ensure numeric columns if present
            for c in ["open", "high", "low", "close", "ltp", "volume"]:
                if c in df.columns:
                    df[c] = pd.to_numeric(df[c], errors="coerce")
            return df
        except Exception:
            logger.exception("get_intraday failed")
            return pd.DataFrame()
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
                dfdaily = self.get_historical("D",lookback=3)
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

            # Fetch base data (OpenAlgo supports up to daily)
            raw = self.client.history(
                symbol=SYMBOL,
                exchange=EXCHANGE,
                interval="D",   # ✅ always fetch daily for higher aggregation
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
                ema_exit_condition = False
                signal_logger.info(f"BUY Exit Check: Close < EMA200 -> {last_close:.2f} < {ema200:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE BELOW EMA200 (BUY Position)")
                    signal_logger.info(f"Exit Context: LTP={current_ltp:.2f}, Close={last_close:.2f}, EMA20={ema20:.2f}, P&L={unrealized_pnl:.2f}")
                    return f"CLOSE_BELOW_EMA20 (close={last_close:.2f}, EMA200={ema20:.2f})"
                    
            elif self.position == "SELL":
                ema_exit_condition = False
                signal_logger.info(f"SELL Exit Check: Close > ema200 -> {last_close:.2f} > {ema20:.2f} = {ema_exit_condition}")
                if ema_exit_condition:
                    signal_logger.info("🔴 EXIT SIGNAL TRIGGERED: CLOSE ABOVE EMA200 (SELL Position)")
                    signal_logger.info(f"Exit Context: LTP={current_ltp:.2f}, Close={last_close:.2f}, EMA200={ema200:.2f}, P&L={unrealized_pnl:.2f}")
                    return f"CLOSE_ABOVE_EMA200 (close={last_close:.2f}, EMA200={ema200:.2f})"
            
            signal_logger.info("⚪ NO EMA EXIT SIGNAL - Position maintained")
            return None
        except Exception:
            logger.exception("check_exit_signal failed")
            signal_logger.error("Exit signal evaluation failed", exc_info=True)
            return None

    def check_entry_signal(self, intraday_df):
        """
        Implemented JSON-like entry condition with comprehensive logging:
        LONG:
          close > prev_high
          close > cpr_r1
          close > ema9
          ema9 > ema20 > ema50
          prev_close > prev_open
          close > open
          close > current_day_high (from quote)  <-- requires quote to have arrived
        SHORT: vice versa (mirror)
        """
        try:
            signal_logger.info("=== Evaluating Entry Signal ===")
            
            if intraday_df is None or intraday_df.empty:
                signal_logger.warning("No intraday data available for signal evaluation")
                return None

            # Get current LTP for logging context
            current_ltp = self.ltp if self.ltp else 0.0
            signal_logger.info(f"Current LTP: {current_ltp:.2f}")

            # dynamic
            dyn = self.compute_dynamic_indicators(intraday_df)
            if not dyn:
                signal_logger.warning("Failed to compute dynamic indicators")
                return None

            last = dyn["last_candle"]
            prev = dyn["prev_candle"]

            close = float(last.get("close", 0.0))
            open_ = float(last.get("open", 0.0))
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
            # prev_day_high = float(self.static_indicators.get("prev_day_high", 0.0))
            # prev_day_low = float(self.static_indicators.get("prev_day_low", 0.0))
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
             
            long_cond =  long_cond3 and long_cond6

            # Evaluate Short conditions step by step
            short_cond1 = (close < prev_day_low)
            short_cond2 = (close < cpr_s1)
            short_cond3 = (close < ema9)
            short_cond4 = (ema9 < ema20)
            short_cond5 = (prev_close < prev_open)
            short_cond6 = (close < open_)
            
            short_cond =   short_cond3 and short_cond6

            # Log detailed condition evaluation
            signal_logger.info("=== LONG Signal Conditions ===")
            signal_logger.info(f"  1. Close > Prev Day High: {close:.2f} > {prev_day_high:.2f} = {long_cond1}")
            signal_logger.info(f"  2. Close > CPR R1: {close:.2f} > {cpr_r1:.2f} = {long_cond2}")
            signal_logger.info(f"  3. Close > EMA9: {close:.2f} > {ema9:.2f} = {long_cond3}")
            signal_logger.info(f"  4. EMA9 > EMA20: {ema9:.2f} > {ema20:.2f} = {long_cond4}")
            signal_logger.info(f"  5. Prev Close > Prev Open: {prev_close:.2f} > {prev_open:.2f} = {long_cond5}")
            signal_logger.info(f"  6. Close > Open: {close:.2f} > {open_:.2f} = {long_cond6}")
            signal_logger.info(f"  LONG Signal Valid: {long_cond}")

            signal_logger.info("=== SHORT Signal Conditions ===")
            signal_logger.info(f"  1. Close < Prev Day Low: {close:.2f} < {prev_day_low:.2f} = {short_cond1}")
            signal_logger.info(f"  2. Close < CPR S1: {close:.2f} < {cpr_s1:.2f} = {short_cond2}")
            signal_logger.info(f"  3. Close < EMA9: {close:.2f} < {ema9:.2f} = {short_cond3}")
            signal_logger.info(f"  4. EMA9 < EMA20: {ema9:.2f} < {ema20:.2f} = {short_cond4}")
            signal_logger.info(f"  5. Prev Close < Prev Open: {prev_close:.2f} < {prev_open:.2f} = {short_cond5}")
            signal_logger.info(f"  6. Close < Open: {close:.2f} < {open_:.2f} = {short_cond6}")
            signal_logger.info(f"  SHORT Signal Valid: {short_cond}")

            if long_cond:
                signal_logger.info("🟢 ENTRY SIGNAL GENERATED: BUY")
                signal_logger.info(f"Entry Context: LTP={current_ltp:.2f}, Close={close:.2f}, EMA9={ema9:.2f}, EMA20={ema20:.2f}")
                logger.info("Entry signal -> BUY | close=%.2f prev_day_high=%.2f cpr_r1=%.2f ema9=%.2f ema20=%.2f", close, prev_day_high, cpr_r1, ema9, ema20)
                return "BUY"
            if short_cond:
                signal_logger.info("🔴 ENTRY SIGNAL GENERATED: SELL")
                signal_logger.info(f"Entry Context: LTP={current_ltp:.2f}, Close={close:.2f}, EMA9={ema9:.2f}, EMA20={ema20:.2f}")
                logger.info("Entry signal -> SELL | close=%.2f prev_day_low=%.2f cpr_s1=%.2f ema9=%.2f ema20=%.2f", close, prev_day_low, cpr_s1, ema9, ema20)
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
            sig_range = (sig_high - sig_low)
            TARGET = sig_range

            if base_price is None:
                base_price = float(self.entry_price or 0.0)

            if signal == "BUY":
                # SL from signal candle low, TP from base_price + configured TARGET
                self.stoploss_price = round(sig_low - STOPLOSS, 2)
                self.target_price = round(base_price + TARGET, 2)
            else:
                self.stoploss_price = round(sig_high + STOPLOSS, 2)
                self.target_price = round(base_price - TARGET, 2)

            # initialize trailing meta
            self.trailing_levels = []
            self.trailing_index = 0
            # step for future TP increments: use first TP - entry as the base step
            try:
                self.tsl_step = abs(self.target_price - base_price) if self.target_price is not None and base_price is not None else TARGET
                if self.tsl_step == 0:
                    self.tsl_step = TARGET
            except Exception:
                self.tsl_step = TARGET

            position_logger.info(f"Initialized Signal Candle Range SL/TP | SignalCandle H={sig_high:.2f} L={sig_low:.2f}")
            position_logger.info(f"SL={self.stoploss_price:.2f}, TP={self.target_price:.2f}, TSL_step={self.tsl_step:.2f}")
            return True
        except Exception:
            position_logger.exception("initialize_signal_candle_sl_tp failed")
            return False        
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
            # build expiry token like '30OCT25'
            expiry_token = f"{expiry_date.day:02d}{MONTH_MAP[expiry_date.month]}{str(expiry_date.year)[-2:]}"
            suffix = "CE" if opt_type == "CE" else "PE"
            candidate = f"{SYMBOL}{expiry_token}{strike}{suffix}"
            # use search API to find tradable symbol
            resp = self.client.search(query=candidate, exchange=OPTION_EXCHANGE)
            if isinstance(resp, dict) and resp.get("data"):
                # return first match's symbol (field names vary: 'symbol' or 'tradingsymbol')
                d = resp["data"][0]
                return d.get("symbol") or d.get("tradingsymbol")
            # fallback: try a looser search with symbol+expiry
            resp2 = self.client.search(query=f"{SYMBOL}{expiry_token}", exchange=OPTION_EXCHANGE)
            if isinstance(resp2, dict) and resp2.get("data"):
                # find nearest strike in returned list
                rows = resp2["data"]
                # find exact strike & type if present
                for r in rows:
                    name = (r.get("symbol") or r.get("tradingsymbol") or "").upper()
                    if str(strike) in name and (suffix in name):
                        return r.get("symbol") or r.get("tradingsymbol")
                # else return first row symbol
                return rows[0].get("symbol") or rows[0].get("tradingsymbol")
            return None
        except Exception:
            logger.exception("resolve_option_symbol_via_search failed")
            return None
    # -------------------------
    # Order functions - Place entry/exit order - Get existing order price
    # -------------------------
    def get_executed_price(self, order_id):
        for _ in range(5):
            time.sleep(2)
            try:
                resp = self.client.orderstatus(order_id=order_id, strategy=STRATEGY)
                if isinstance(resp, dict) and resp.get("status") == "success":
                    data = resp.get("data", {})
                    if data.get("order_status") == "complete":
                        return float(data.get("average_price", 0) or 0)
            except Exception:
                logger.exception("get_executed_price attempt failed")
        return None
                            
    def place_entry_order(self, signal):
        try:
            order_logger.info("=== PLACING ENTRY ORDER ===")
            order_logger.info(f"Signal: {signal}")

            if self.trade_count >= MAX_TRADES_PER_DAY:
                order_logger.warning(f"Daily trade limit reached ({self.trade_count}/{MAX_TRADES_PER_DAY})")
                logger.info("Daily trade limit reached (%d), will shutdown.", MAX_TRADES_PER_DAY)
                threading.Thread(target=self.shutdown_gracefully, daemon=True).start()
                return False

            order_logger.info(f"Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")
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
                    q = self.client.quotes(symbol=SYMBOL, exchange=EXCHANGE)
                    spot_ltp = float((q.get("data") or {}).get("ltp", 0) or 0)
                    order_logger.info(f"Fetched Spot LTP: {spot_ltp:.2f}")
                except Exception:
                    spot_ltp = self.ltp or 0.0
                    order_logger.warning(f"Failed to fetch spot LTP, using cached: {spot_ltp:.2f}")
                strike = self.get_option_strike(OPTION_STRIKE_SELECTION, spot_ltp, opt_type) # Strike selection
                order_logger.info(f"Resolved Strike: {strike} (selection: {OPTION_STRIKE_SELECTION})") 
                expiry = self.get_nearest_weekly_expiry() # Expiry date
                order_logger.info(f"Selected Expiry: {expiry}")
                opt_symbol = self.get_option_symbol_via_search_api(expiry, strike, opt_type) # Option symbol
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

            order_logger.info("=== ORDER PLACEMENT ===")
            order_logger.info(f"Symbol: {tradable_symbol}")
            order_logger.info(f"Exchange: {tradable_exchange}")
            order_logger.info(f"Action: {signal}")
            order_logger.info(f"Quantity: {QUANTITY}")
            order_logger.info(f"Order Type: MARKET")
            order_logger.info(f"Product: {PRODUCT}")
            order_logger.info(f"Strategy: {STRATEGY}")

            # MAX_RETRIES = 3 is pending

            resp = self.client.placeorder(strategy=STRATEGY, symbol=tradable_symbol, exchange=tradable_exchange,
                                          action=signal, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)
            order_logger.info(f"Order Response: {resp}")
            if not isinstance(resp, dict) or resp.get("status") != "success":
                order_logger.error(f"Entry order failed: {resp}")
                logger.warning("Entry order failed: %s", resp)
                return False
            self.trade_start_time = datetime.now()
            
            order_id = resp.get("orderid")
            order_logger.info(f"Order ID: {order_id}")
            order_logger.info("Waiting for order execution confirmation...")
            exec_price = self.get_executed_price(order_id)
            q_opt = self.client.quotes(symbol=self.option_symbol, exchange=OPTION_EXCHANGE)
            self.option_entry_price = float((q_opt.get("data") or {}).get("ltp", 0) or 0) # Option entry price

            if exec_price is None:
                order_logger.error("Could not confirm executed price for entry order")
                logger.warning("Could not confirm executed price for entry")
                return False
            order_logger.info(f"ORDER EXECUTED: Price = {self.option_entry_price:.2f}")

            # Grab intraday snapshot BEFORE locking (to avoid long-running IO inside lock)
            intraday = self.get_intraday()

            # --- Lock and initialize position & SL/TP atomically ---
            with self._state_lock:
                # guard monitoring while we initialize
                self.exit_in_progress = True
                self.position = signal
                self.entry_price = float(exec_price)

                # determine base_price (spot/option/entry)
                base_price = self.entry_price
                if USE_SPOT_FOR_SLTP:
                    try:
                        q = self.client.quotes(symbol=SYMBOL, exchange=EXCHANGE)
                        self.spot_entry_price = float((q.get("data") or {}).get("ltp", 0) or 0)
                        base_price = self.spot_entry_price
                        risk_logger.info(f"SL/TP tracking via SPOT | Spot Entry = {base_price:.2f}")
                    except Exception:
                        self.spot_entry_price = self.ltp or self.entry_price
                        base_price = self.spot_entry_price
                        risk_logger.warning(f"Failed to fetch spot for SL/TP, using cached: {base_price:.2f}")
                elif USE_OPTION_FOR_SLTP:
                    self.option_entry_price = float(exec_price)
                    base_price = self.option_entry_price
                    risk_logger.info(f"SL/TP tracking via OPTION | Option Entry = {base_price:.2f}")

                # Initialize SL/TP using the Signal Candle Range method if configured
                if SL_TP_METHOD == "Signal_candle_range_SL_TP":
                    self.initialize_signal_candle_sl_tp(signal, intraday, base_price=base_price)
                else:
                    # fallback: simple fixed SL/TP
                    if signal == "BUY":
                        self.stoploss_price = round(base_price - STOPLOSS, 2)
                        self.target_price = round(base_price + TARGET, 2)
                    else:
                        self.stoploss_price = round(base_price + STOPLOSS, 2)
                        self.target_price = round(base_price - TARGET, 2)

                self.trade_count += 1
                self.entry_price = round(base_price,2)
                # re-enable monitoring only after SL/TP set
                self.exit_in_progress = False

            # final logging
            order_logger.info("=== POSITION ESTABLISHED ===")
            order_logger.info(f"Position: {self.position}")
            order_logger.info(f"Entry Price: {self.entry_price:.2f}")
            order_logger.info(f"Stop Loss: {self.stoploss_price:.2f}")
            order_logger.info(f"Target: {self.target_price:.2f}")
            order_logger.info(f"Quantity: {QUANTITY}")
            order_logger.info(f"Trade Count: {self.trade_count}/{MAX_TRADES_PER_DAY}")

            risk_amount = abs(self.entry_price - (self.stoploss_price or 0))
            reward_amount = abs((self.target_price or 0) - self.entry_price)
            try:
                risk_logger.info(f"Risk Amount: {risk_amount:.2f}")
                risk_logger.info(f"Reward Amount: {reward_amount:.2f}")
                if risk_amount > 0:
                    risk_logger.info(f"Risk:Reward Ratio: 1:{reward_amount/risk_amount:.2f}")
            except Exception:
                pass

            log_trade_db({
            "timestamp": self.trade_start_time,
            "symbol": tradable_symbol,
            "spot_price": self.spot_entry_price,
            "action": "CALL" if signal.upper() == "BUY" else "PUT",
            "quantity": QUANTITY,
            "price": self.option_entry_price,
            "order_id": order_id,
            "strategy": STRATEGY,
            "leg_type": "ENTRY",
            "leg_status": "open"
            })

            logger.info("Entry executed: %s @ %.2f | trade_count=%d", self.position, self.entry_price, self.trade_count)
            return True

        except Exception:
            logger.exception("place_entry_order failed")
            order_logger.error("Failed to place entry order", exc_info=True)
            return False

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
            
            # Place exit order
            resp = self.client.placeorder(strategy=STRATEGY, symbol=exit_symbol, exchange=exit_exchange,
                                          action=action, quantity=QUANTITY, price_type="MARKET", product=PRODUCT)
            
            order_logger.info(f"Exit Order Response: {resp}")
            
            if isinstance(resp, dict) and resp.get("status") == "success":
                order_id = resp.get("orderid")
                order_logger.info(f"Exit Order ID: {order_id}")
                
                # Try to get execution price
                exit_price = self.get_executed_price(order_id)
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
                    order_logger.warning("Could not confirm exit execution price")
                    
            else:
                order_logger.error(f"Exit order failed: {resp}")
                
            logger.info("Exit order resp: %s", resp)
            
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
            
            order_logger.info("Position cleared and reset")
            # main_logger.info(f"Position {previous_position} @ {previous_entry:.2f} exited due to: {reason}")
            
            return True
        except Exception:
            logger.exception("place_exit_order failed")
            order_logger.error(f"Failed to place exit order for reason: {reason}", exc_info=True)
            self.exit_in_progress = False
            return False
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
            
        # place exit order if open
        if self.position and not self.exit_in_progress:
            main_logger.info("Closing open position before shutdown")
            logger.info("Closing open position before shutdown")
            self.exit_in_progress = True
            try:
                self.place_exit_order("Shutdown")
                main_logger.info("Position closed for shutdown")
            except Exception:
                main_logger.error("Failed to close position during shutdown", exc_info=True)
            finally:
                self.exit_in_progress = False
                
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
