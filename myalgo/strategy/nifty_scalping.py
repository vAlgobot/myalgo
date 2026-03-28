# nifty_scalping.py
# ─────────────────────────────────────────────────────────────
# Strategy file for NIFTY Scalping.
# Upload this ONE file to the Orchestrator UI.
# The engine (myalgo_trading_system.py) must be deployed on
# the same server — this file only carries config data.
# ─────────────────────────────────────────────────────────────

CONFIG = {
    "strategy_name": "NIFTY_SCALPING",

    "instrument": {
        "symbol":   "NIFTY", 
        "exchange": "NSE_INDEX",
        "product":  "MIS",
    },

    "mode": {
        "run_mode":     "BACKTESTING_RANGE", # "LIVE" or "BACKTESTING" or "BACKTESTING_RANGE"
        "logging_mode": "fast",              # "fast" -> Only warnings/errors, "normal" -> INFO+warnings, "debug" -> Everything
        "strategy_mode": "SCALPING",         # "SCALPING" or "LIVE_TESTING"
        "start_date":   "13-03-2026",        # DD-MM-YYYY format
        "end_date":     "13-03-2026",        # DD-MM-YYYY format
        "simulation_date": "13-12-2025",     # DD-MM-YYYY or None
        "clear_trade_table": False           # Clear trade logs table before backtesting
    },

    "timeframe": {
        "candle":        "5m",               # Candle timeframe
        "lookback_days": 10,                 # Lookback days
        "signal_check_interval": 5           # minutes (use integer minutes)
    },

    "risk": {
        "sl_percent":        0.25,           # Percentage of option premium for Stoploss
        "risk_reward_ratio": 2.0,            # e.g., 1.5 = 1:1.5, 2.0 = 1:2 risk-reward
        "min_sl_points":     10,             # Absolute minimum SL points
        "max_risk_points":   30,             # Absolute safety on option premium
        "max_signal_range":  30,             # Maximum signal range
        "min_tp_separation_r": 0.5,          # Minimum TP separation ratio is to avoid closer pivot with previous target
        "min_reward_pct_of_risk": 0.5,       # At least 50% reward of risk
        "min_reward_for_sl_move": 0.5,       # TSL Move only if reward is at least 50% of initial target
        "min_absolute_reward": 15,           # Minimum absolute reward in points
        "sl_buffer_pct":     0.15,           # 15% buffer from premium automatic SL placement in broker
        "sl_buffer_point":   5,              # Buffer in points from premium in CPR SL placement
        "use_dynamic_target": True,          # Enable or disable dynamic target logic
        "dynamic_target_method": "NEAREST_VALID" # "NEAREST_VALID" or "FIRST_BEYOND"
    },

    "trade_management": {
        "max_trades_per_day": 5,             # Maximum trades per day
        "entry_confirm_seconds": 0,          # Seconds to confirm entry
        "exit_confirm_seconds":  0,          # Seconds to confirm exit
        "enable_seconds_confirmation_live": False, # For live mode, require confirmation seconds for entry
        "day_high_low_validation_from_trade": 2, # Apply day high/low validation from this trade count onward
        "expiry_day_hl_breakout": True       # If True, on expiry day entry requires breakout of previous day high/low
    },

    "strategy": {
        "rsi_enabled":                False, # Set to True to enable RSI-based entry conditions in strategy_job()
        "rsi_levels":                 [50],  # RSI Levels
        "rsi_lookback":               5,     # Lookback candles for RSI V setup validation
        "gap_filter_enabled":         True,  # If True, gap-up/gap-down days require 15-min candle breakout before entry
        "gap_threshold_percent":      0.3,   # Minimum gap % to classify a day as gap_up or gap_down
        "cpr_midpoints_enabled":      True,  # Enable midpoint levels for target consideration (CPR method)
        "enable_ltp_pivot_gate":      True,  # Master switch for this gatekeeper
        "candle_breakout_1m_enabled": True,  # If True, use 1m candle close instead of LTP for entry/exit breakout validation
        "tsl_enabled":                False, # Enable trailing stoploss behavior
        "sl_tp_method":               "CPR_range_SL_TP" # Current active method #CPR_range_SL_TP #Signal_candle_range_SL_TP
    },

    "breakout": {
        "entry_ltp_breakout_enabled":     True,  # Enable separate entry breakout confirmation
        "entry_breakout_mode":            "SPOT",# "SPOT" | "OPTIONS" | "SPOT_OPTIONS"
        "exit_ltp_breakout_enabled":      True,  # Enable separate exit breakout confirmation
        "exit_ltp_breakout_mode":         "SPOT",# "SPOT" | "OPTIONS" | "SPOT_OPTIONS" (For exit breakout validation)
        "exit_breakout_confirm_candles":  0,     # Backtesting confirmation candles for exit LTP breakout (1 for immediate)
        "entry_breakout_confirm_candles": 0,     # Backtesting confirmation candles for entry LTP breakout (1 for immediate)
        "reversal_mode":                  "NONE",# "SPOT" | "OPTIONS" | "SPOT_OPTIONS" | "NONE" (For reversal candle exit)
        "ltp_breakout_interval_min":      5      # minutes to wait for LTP breakout confirmation
    },

    "options": {
        "enabled":               True,       # Enable option trading
        "exchange":              "NFO",      # Option exchange
        "strike_interval":       50,         # Strike interval
        "expiry_type":           "WEEKLY",   # Option expiry type
        "strike_selection":      "OTM5",     # "ATM", "ITM1", "OTM1", etc.
        "expiry_lookahead_days": 30,         # Expiry look ahead days
        "lot_quantity":          65,         # Lot size
        "lot":                   1,          # Lot size
        "sl_order_enabled":      True,       # Enable SL order
        "use_spot_for_sltp":     True,       # If True -> uses spot price for SL/TP tracking
        "use_option_for_sltp":   False       # If True -> uses option position LTP for SL/TP tracking
    },

    "pivot": {
        "touch_buffer_pts": 0.5,             # Absolute buffer in price units (points). adjust for NIFTY ~0.5
        "touch_buffer_pct": None             # Optional: use percentage buffer (e.g. 0.001 for 0.1%)
    }
}

if __name__ == "__main__":
    import myalgo_trading_system as engine
    engine.run(config=CONFIG)
