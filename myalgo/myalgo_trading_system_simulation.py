"""
Patched MyAlgo Trading System with Historical Replay Simulation + CLI Control
-------------------------------------------------------------
Adds simulation capability via SIMULATION_DATE global config and optional CLI arguments.
Maintains complete backward compatibility with live trading.
"""

print("🔁 OpenAlgo Python Bot is running.")

from datetime import datetime, timedelta
import pytz
import time
import threading
import pandas as pd
import argparse
from openalgo import api

# ----------------------------
# Configuration
# ----------------------------
API_KEY = "112c4900f6b2d83b7d812921de13d36898116fd79a592d96cec666dfbbc389f8"
API_HOST = "http://127.0.0.1:5000"
WS_URL = "ws://127.0.0.1:8765"

SIMULATION_DATE: str = "2025-10-10"  # e.g. "2025-06-02" for simulation, None for live
SIMULATION_SPEED_MULTIPLIER: float = 60.0  # 1.0 = real time, 60.0 = 60x faster

IST = pytz.timezone("Asia/Kolkata")

_simulation_state = {
    "start_real_time": None,
    "start_simulated_time": None,
    "last_replayed_time": None,
    "replay_running": False
}

client = api(api_key=API_KEY, host=API_HOST, ws_url=WS_URL)

# ----------------------------
# CLI Parser
# ----------------------------
def parse_args():
    parser = argparse.ArgumentParser(description="Run MYALGO_TRADING_BOT with optional simulation mode")
    parser.add_argument("--simulate", type=str, default="2025-10-10", help="Date (YYYY-MM-DD) for simulation mode")
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

# ----------------------------
# Replay WebSocket (simulation)
# ----------------------------
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
        print(f"[ReplayWebsocket] Fetching 1m data for {inst['symbol']} on {self.simulation_date}")

        try:
            df = get_intraday(inst["symbol"], inst["exchange"], "1m",
                          start_date=self.simulation_date, end_date=self.simulation_date)
        except Exception as e:
            print(f"[ReplayWebsocket] ERROR fetching data: {e}")
            _simulation_state["replay_running"] = False
            return

        if df is None or df.empty:
            print(f"[ReplayWebsocket] No data returned for {inst['symbol']} on {self.simulation_date}")
            _simulation_state["replay_running"] = False
            return
        
        df = df.sort_index()
        print(f"[ReplayWebsocket] Loaded {len(df)} bars for replay")

        # Initialize simulation timing
        _simulation_state["start_real_time"] = datetime.now(IST)
        _simulation_state["start_simulated_time"] = df.index[0]
        _simulation_state["replay_running"] = True

        # Replay each bar
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
            
            # Sleep based on speed multiplier
            time.sleep(60.0 / self.speed_multiplier)

        print("[ReplayWebsocket] Completed replay")
        _simulation_state["replay_running"] = False

    def unsubscribe_ltp(self, instruments_list):
        self._stop.set()

    def disconnect(self):
        self._stop.set()
        print("[ReplayWebsocket] Disconnected simulation")

# ----------------------------
# MYALGO_TRADING_BOT (patched)
# ----------------------------
class MYALGO_TRADING_BOT:
    def __init__(self):
        self.symbol = "NIFTY"
        self.exchange = "NSE_INDEX"
        self.client = None

        if SIMULATION_DATE:
            print(f"[MYALGO_TRADING_BOT] Simulation mode active for {SIMULATION_DATE}")
            self.client = ReplayWebsocket(client_api=client, simulation_date=SIMULATION_DATE, 
                                        speed_multiplier=SIMULATION_SPEED_MULTIPLIER)
        else:
            print("[MYALGO_TRADING_BOT] Live mode active")
            self.client = client

    def on_data_received(self, data):
        ts = datetime.fromtimestamp(data["data"]["timestamp"] / 1000, tz=IST)
        ltp = data["data"].get("ltp")
        print(f"[{ts}] LTP={ltp}")
        # your existing signal, strategy, and indicator logic remains unchanged

    def run(self):
        try:
            self.client.connect()
            instruments = [{"exchange": self.exchange, "symbol": self.symbol}]
            self.client.subscribe_ltp(instruments, on_data_received=self.on_data_received)
            
            if SIMULATION_DATE:
                # Wait until the replay thread actually starts
                print("[Bot] Waiting for simulation thread to start...")
                timeout = 0
                while not _simulation_state.get("replay_running", False) and timeout < 30:
                    time.sleep(0.5)
                    timeout += 0.5
                
                if _simulation_state.get("replay_running", False):
                    print("[Bot] Simulation started successfully.")
                    # Now monitor until replay completes
                    while _simulation_state.get("replay_running", False):
                        time.sleep(1)
                    print("[Bot] Simulation completed.")
                else:
                    print("[Bot] ERROR: Simulation failed to start within timeout.")
            else:
                # Live mode - run indefinitely
                print("[Bot] Live mode running. Press Ctrl+C to stop.")
                while True:
                    time.sleep(1)
                    
        except KeyboardInterrupt:
            print("[Bot] Interrupted by user")
        finally:
            self.client.disconnect()

# ----------------------------
# main with CLI
# ----------------------------
if __name__ == "__main__":
    args = parse_args()

    if args.simulate:
        SIMULATION_DATE = args.simulate
        SIMULATION_SPEED_MULTIPLIER = args.speed
        print(f"[Config] Simulation mode ON for {SIMULATION_DATE} at {SIMULATION_SPEED_MULTIPLIER}x speed")
    else:
        SIMULATION_DATE = None
        print("[Config] Live mode ON")

    bot = MYALGO_TRADING_BOT()
    bot.run()