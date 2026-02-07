# backtest_orchestrator.py
"""
BacktestOrchestrator - Multi-Day Backtesting Engine

Orchestrates date-range backtesting by:
1. Generating trading days (Mon-Fri, excluding holidays)
2. Running isolated single-day backtest sessions
3. Ensuring complete state reset between days
4. Handling errors gracefully per day
5. Aggregating results across the full range

CRITICAL DESIGN PRINCIPLES:
- Each day runs as a fresh trading session
- Zero state carryover between days
- Reuses existing single-day backtest machinery
- Does NOT modify LIVE trading flow
"""

import logging
from datetime import datetime, date, timedelta
import os
import sqlite3
from typing import List, Optional, Tuple
import time
import threading
import os
# Import existing components
from logger import get_logger

# Module logger
orchestration_logger = get_logger("backtest_orchestration")

# =========================================================
# TRADING CALENDAR UTILITIES
# =========================================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "myalgo.db")

def is_trading_day(check_date: date) -> bool:
    """
    Determine if a date is a valid trading day.
    
    Rules:
    - Monday to Friday only
    - Exclude known market holidays (extend this list as needed)
    
    Args:
        check_date: Date to check
        
    Returns:
        True if trading day, False otherwise
    """
    # Check weekday (0=Monday, 6=Sunday)
    if check_date.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # NSE/Indian market holidays for 2026 (EXTEND THIS LIST)
    # Format: (month, day)
    MARKET_HOLIDAYS_2026 = [
        (1, 26),   # Republic Day
        (1, 15),   # Pongal
        (3, 14),   # Holi
        (3, 30),   # Ram Navami
        (4, 2),    # Mahavir Jayanti
        (4, 10),   # Good Friday
        (4, 14),   # Ambedkar Jayanti
        (5, 1),    # May Day
        (8, 15),   # Independence Day
        (8, 27),   # Ganesh Chaturthi
        (10, 2),   # Gandhi Jayanti
        (10, 22),  # Dussehra
        (11, 11),  # Diwali
        (11, 12),  # Diwali (Balipratipada)
        (11, 25),  # Guru Nanak Jayanti
        (12, 25),  # Christmas
    ]
    
    if (check_date.month, check_date.day) in MARKET_HOLIDAYS_2026:
        return False
    
    return True



def generate_trading_days(start_date: date, end_date: date) -> List[date]:
    """
    Generate list of valid trading days between start and end dates (inclusive).
    
    Args:
        start_date: Start of date range
        end_date: End of date range
        
    Returns:
        List of trading dates
    """
    trading_days = []
    current = start_date
    
    while current <= end_date:
        if is_trading_day(current):
            trading_days.append(current)
        current += timedelta(days=1)
    
    return trading_days

def clear_trade_logs(db_path: str):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        cursor.execute("DELETE FROM my_trade_logs;")
        conn.commit()
        conn.close()
# =========================================================
# BACKTEST ORCHESTRATOR
# =========================================================

class BacktestOrchestrator:
    """
    Orchestrates multi-day backtesting sessions.
    
    Responsibilities:
    - Loop over date range
    - Ensure day isolation
    - Coordinate existing components
    - Handle errors gracefully
    - Track summary statistics
    """
    
    def __init__(self, start_date_str: str, end_date_str: str):
        """
        Initialize orchestrator with date range.
        
        Args:
            start_date_str: Start date in DD-MM-YYYY format
            end_date_str: End date in DD-MM-YYYY format
        """
        self.start_date = self._parse_date(start_date_str)
        self.end_date = self._parse_date(end_date_str)
        
        # Generate trading days
        self.trading_days = generate_trading_days(self.start_date, self.end_date)
        
        # Summary tracking
        self.total_days = len(self.trading_days)
        self.completed_days = 0
        self.failed_days = []
        self.skipped_days = []
        
        orchestration_logger.info("="*70)
        orchestration_logger.info("🎯 BACKTEST ORCHESTRATOR INITIALIZED")
        orchestration_logger.info("="*70)
        orchestration_logger.info(f"📅 Date Range: {start_date_str} → {end_date_str}")
        orchestration_logger.info(f"📊 Total Trading Days: {self.total_days}")
        orchestration_logger.info(f"📋 Trading Days: {[d.strftime('%d-%b-%Y') for d in self.trading_days]}")
        orchestration_logger.info("="*70)
    
    def _parse_date(self, date_str: str) -> date:
        """Parse date string in DD-MM-YYYY format."""
        for fmt in ("%d-%m-%Y", "%d/%m/%Y", "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Invalid date format: {date_str}. Expected DD-MM-YYYY")
    
    def run_backtest_range(self):
        """
        Main orchestration loop.
        
        Executes backtest for each trading day in sequence.
        Continues on errors to maximize data collection.
        """
        orchestration_logger.info("🚀 Starting multi-day backtest execution...")
        range_start_time = datetime.now()
        clear_trade_logs(DB_PATH)
        
        for idx, trading_day in enumerate(self.trading_days, 1):
            day_str = trading_day.strftime("%d-%m-%Y")
            
            orchestration_logger.info("")
            orchestration_logger.info("="*70)
            orchestration_logger.info(f"📆 DAY {idx}/{self.total_days}: {trading_day.strftime('%A, %d %B %Y')}")
            orchestration_logger.info("="*70)
            
            try:
                # Run single-day backtest
                success = self._run_single_day(trading_day)
                
                if success:
                    self.completed_days += 1
                    orchestration_logger.info(f"✅ Day {day_str} completed successfully")
                else:
                    self.failed_days.append(trading_day)
                    orchestration_logger.warning(f"⚠️ Day {day_str} incomplete")
                    
            except Exception as e:
                self.failed_days.append(trading_day)
                orchestration_logger.error(
                    f"❌ Day {day_str} failed with exception: {e}",
                    exc_info=True
                )
                orchestration_logger.info("Continuing to next day...")
            
            # Brief pause between days for cleanup
            # time.sleep(1)
        
        # Final summary
        self._print_final_summary(range_start_time)
    
    def _run_single_day(self, trading_day: date) -> bool:
        """
        Execute one day's backtest using existing machinery.
        
        Process:
        1. Reset BacktestingManager
        2. Load data for trading_day
        3. Create fresh bot instance
        4. Create fresh replay client
        5. Run bot.run() in controlled manner
        6. Wait for completion
        7. Cleanup
        
        Args:
            trading_day: Date to backtest
            
        Returns:
            True if successful, False otherwise
        """
        from myalgo_trading_system import (
            BACKTESTING_MANAGER,
            DateReplayClient,
            MYALGO_TRADING_BOT,
            SYMBOL,
            EXCHANGE,
            CANDLE_TIMEFRAME
        )
        
        day_str = trading_day.strftime("%d-%m-%Y")
        orchestration_logger.info(f"🔄 Initializing session for {day_str}")
        
        # ─────────────────────────────────────────────────────────
        # STEP 1: Reset BacktestingManager
        # ─────────────────────────────────────────────────────────
        try:
            orchestration_logger.info("🧹 Resetting BacktestingManager state...")
            BACKTESTING_MANAGER.stop()  # Clear previous state
            # time.sleep(0.5)  # Allow cleanup
        except Exception as e:
            orchestration_logger.error(f"Error resetting BacktestingManager: {e}")
            return False
        
        # ─────────────────────────────────────────────────────────
        # STEP 2: Load data for trading_day
        # ─────────────────────────────────────────────────────────
        try:
            orchestration_logger.info(f"📥 Loading market data for {day_str}...")
            BACKTESTING_MANAGER.start(
                simulate_date_str=day_str,
                symbol=SYMBOL,
                exchange=EXCHANGE,
                timeframe="1m"
            )
            
            # Verify data loaded
            if BACKTESTING_MANAGER.df is None or BACKTESTING_MANAGER.df.empty:
                orchestration_logger.warning(f"⚠️ No market data available for {day_str}, skipping...")
                self.skipped_days.append(trading_day)
                return False
                
            candle_count = len(BACKTESTING_MANAGER.df)
            orchestration_logger.info(f"✅ Loaded {candle_count} candles for {day_str}")
            
        except Exception as e:
            orchestration_logger.error(f"Data loading failed for {day_str}: {e}", exc_info=True)
            return False
        
        # ─────────────────────────────────────────────────────────
        # STEP 3 & 4: Create fresh instances
        # ─────────────────────────────────────────────────────────
        try:
            orchestration_logger.info("🆕 Creating fresh bot and replay client instances...")
            
            # Fresh replay client
            replay_client = DateReplayClient(BACKTESTING_MANAGER)
            
            # Fresh bot instance (all state reset via __init__)
            bot = MYALGO_TRADING_BOT()
            
            # Link components
            bot.client = replay_client
            replay_client.bot_ref = bot
            
            orchestration_logger.info("✅ Fresh instances created and linked")
            
        except Exception as e:
            orchestration_logger.error(f"Instance creation failed: {e}", exc_info=True)
            return False
        
        # ─────────────────────────────────────────────────────────
        # STEP 5: Run backtest (blocking until EOD)
        # ─────────────────────────────────────────────────────────
        try:
            orchestration_logger.info(f"▶️ Starting backtest execution for {day_str}...")
            
            # Run bot (this blocks until EOD or error)
            bot.run()
            
            orchestration_logger.info(f"⏹️ Backtest execution completed for {day_str}")
            success = True
        except Exception as e:
            orchestration_logger.error(f"Backtest execution error: {e}", exc_info=True)
            # Attempt cleanup even on error
            try:
                bot.shutdown_gracefully()
            except:
                pass
            return False
        
        # ─────────────────────────────────────────────────────────
        # # STEP 6: Verify completion
        # # ─────────────────────────────────────────────────────────
        # # try:
        # #     # Check if simulation completed naturally
        # #     if BACKTESTING_MANAGER.sim_index >= len(BACKTESTING_MANAGER.df):
        # #         orchestration_logger.info(f"✅ All candles processed for {day_str}")
        # #         success = True
        # #     else:
        # #         orchestration_logger.warning(
        # #             f"⚠️ Incomplete replay: {BACKTESTING_MANAGER.sim_index}/{len(BACKTESTING_MANAGER.df)} candles"
        # #         )
        # #         success = False
                
        # except Exception as e:
        #     orchestration_logger.error(f"Completion check error: {e}")
        #     success = False
        
        # ─────────────────────────────────────────────────────────
        # STEP 7: Cleanup (ensure resources released)
        # ─────────────────────────────────────────────────────────
        try:
            orchestration_logger.info("🧹 Cleaning up session resources...")
            
            # Disconnect replay client
            if hasattr(replay_client, 'disconnect'):
                replay_client.disconnect()
            
            # Final bot shutdown (if not already done)
            if hasattr(bot, 'shutdown_gracefully'):
                bot.shutdown_gracefully()
            
            # Explicit cleanup
            del bot
            del replay_client
            
            orchestration_logger.info("✅ Cleanup completed")
            
        except Exception as e:
            orchestration_logger.error(f"Cleanup error: {e}")
        
        return success
    
    def _print_final_summary(self, start_time: datetime):
        """Print final backtest summary."""
        end_time = datetime.now()
        duration = end_time - start_time
        
        orchestration_logger.info("")
        orchestration_logger.info("="*70)
        orchestration_logger.info("🏁 BACKTEST RANGE COMPLETED")
        orchestration_logger.info("="*70)
        orchestration_logger.info(f"📅 Date Range: {self.start_date.strftime('%d-%b-%Y')} → {self.end_date.strftime('%d-%b-%Y')}")
        orchestration_logger.info(f"⏱️ Total Duration: {duration}")
        orchestration_logger.info(f"📊 Total Trading Days: {self.total_days}")
        orchestration_logger.info(f"✅ Successfully Completed: {self.completed_days}")
        orchestration_logger.info(f"⚠️ Failed Days: {len(self.failed_days)}")
        orchestration_logger.info(f"⏭️ Skipped Days (No Data): {len(self.skipped_days)}")
        
        if self.failed_days:
            orchestration_logger.warning("Failed days:")
            for d in self.failed_days:
                orchestration_logger.warning(f"  - {d.strftime('%d-%b-%Y')}")
        
        if self.skipped_days:
            orchestration_logger.info("Skipped days (no market data):")
            for d in self.skipped_days:
                orchestration_logger.info(f"  - {d.strftime('%d-%b-%Y')}")
        
        orchestration_logger.info("="*70)
        orchestration_logger.info("💾 All trades saved to database: my_trade_logs")
        orchestration_logger.info("📈 Run backtesting_analysis.py for performance metrics")
        orchestration_logger.info("="*70)