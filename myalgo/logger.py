"""
MyAlgo Simplified Logging System

Provides simple, readable logging with timestamped files and standard Python format.
Single log file per session with easy module-based filtering.

Author: MyAlgo Trading System
"""

import logging
import logging.handlers
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional
import threading


class PerformanceLogger:
    """
    Context manager for performance logging.
    """
    
    def __init__(self, logger: logging.Logger, operation: str, **kwargs):
        self.logger = logger
        self.operation = operation
        self.kwargs = kwargs
        self.start_time = None
    
    def __enter__(self):
        self.start_time = datetime.now()
        extra_info = " - " + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) if self.kwargs else ""
        self.logger.debug(f"Starting {self.operation}{extra_info}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        end_time = datetime.now()
        duration = (end_time - self.start_time).total_seconds()
        
        extra_info = " - " + ", ".join(f"{k}={v}" for k, v in self.kwargs.items()) if self.kwargs else ""
        
        if exc_type:
            self.logger.error(f"Failed {self.operation} after {duration:.3f}s{extra_info}", exc_info=True)
        else:
            self.logger.info(f"Completed {self.operation} in {duration:.3f}s{extra_info}")


class LoggerManager:
    """
    Simplified logger manager for the trading system.
    Creates single timestamped log file per session.
    """
    
    _instance: Optional['LoggerManager'] = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if hasattr(self, 'initialized'):
            return
        
        self.initialized = True
         # ✅ FIXED BASE DIRECTORY (project root)
        BASE_DIR = Path(__file__).resolve().parents[1]

        today_folder = datetime.now().strftime("%d-%m-%y")
        self.logs_dir = BASE_DIR / "logs" / today_folder
    
        # Create timestamped log file for this session
        timestamp = datetime.now().strftime("%H-%M-%S")
        self.log_file = self.logs_dir / f"myalgo_{timestamp}.log"
        
        self.loggers: Dict[str, logging.Logger] = {}
        self._setup_directories()
        self._setup_root_logger()
    
    def _setup_directories(self):
        """Create logs directory if it doesn't exist."""
        self.logs_dir.mkdir(parents=True, exist_ok=True)
    
    def _setup_root_logger(self):
        """Setup root logger configuration."""
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)
        
        # Clear any existing handlers
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # Console handler for development
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)s in %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        console_handler.setFormatter(console_formatter)
        root_logger.addHandler(console_handler)
        
        # File handler - single file for all logs
        file_handler = logging.FileHandler(str(self.log_file), mode='w', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            '[%(asctime)s,%(msecs)03d] %(levelname)s in %(module)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)
    
    def get_logger(self, module_name: str, level: str = "INFO") -> logging.Logger:
        """
        Get or create a module-specific logger.
        
        Args:
            module_name: Module name (config, trading, strategy, etc.)
            level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            
        Returns:
            Configured logger instance
        """
        if module_name in self.loggers:
            return self.loggers[module_name]
        
        # Create logger
        logger = logging.getLogger(module_name)
        logger.setLevel(getattr(logging, level.upper()))
        
        # Note: Logger inherits handlers from root logger
        # No need to add handlers here
        
        self.loggers[module_name] = logger
        return logger
    
    def set_market_hours_level(self, market_level: str = "DEBUG", non_market_level: str = "INFO"):
        """
        Dynamically adjust log levels based on market hours.
        
        Args:
            market_level: Log level during market hours
            non_market_level: Log level during non-market hours
        """
        current_time = datetime.now().time()
        market_start = datetime.strptime("09:20", "%H:%M").time()
        market_end = datetime.strptime("15:00", "%H:%M").time()
        
        if market_start <= current_time <= market_end:
            target_level = market_level
            session = "trading"
        else:
            target_level = non_market_level
            session = "non-trading"
        
        # Update root logger level
        root_logger = logging.getLogger()
        root_logger.setLevel(getattr(logging, target_level.upper()))
        
        # Log the session change
        system_logger = self.get_logger('system')
        system_logger.info(f"Market session: {session}, log level: {target_level}")
    
    def log_trade_execution(self, trade_data: Dict[str, Any]):
        """
        Specialized method for logging trade executions.
        
        Args:
            trade_data: Trade information dictionary
        """
        logger = self.get_logger('trading')
        
        # Format trade data for readable logging
        symbol = trade_data.get('symbol', 'N/A')
        action = trade_data.get('action', 'N/A')
        leg_type = trade_data.get('leg_type', '')
        strike = trade_data.get('strike', '')
        quantity = trade_data.get('quantity', 0)
        price = trade_data.get('price', 0)
        
        trade_info = f"{action} {symbol}"
        if leg_type:
            trade_info += f" {leg_type}"
        if strike:
            trade_info += f" {strike}"
        if quantity:
            trade_info += f" qty={quantity}"
        if price:
            trade_info += f" price={price}"
        
        logger.info(f"Trade executed - {trade_info}")
    
    def log_performance(self, module_name: str, operation: str, **kwargs) -> PerformanceLogger:
        """
        Create performance logger context manager.
        
        Args:
            module_name: Module name
            operation: Operation name
            **kwargs: Additional context data
            
        Returns:
            PerformanceLogger context manager
        """
        logger = self.get_logger(module_name)
        return PerformanceLogger(logger, operation, **kwargs)
    
    def get_log_file_path(self) -> str:
        """Get the current log file path."""
        return str(self.log_file)
    
    def shutdown(self):
        """Shutdown all loggers and handlers."""
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            handler.close()
            root_logger.removeHandler(handler)
        self.loggers.clear()


# Global logger manager instance
_logger_manager = LoggerManager()

def get_logger(module_name: str, level: str = "INFO") -> logging.Logger:
    """
    Convenience function to get a module-specific logger.
    
    Args:
        module_name: Module name
        level: Log level
        
    Returns:
        Configured logger instance
    """
    return _logger_manager.get_logger(module_name, level)

def log_performance(module_name: str, operation: str, **kwargs) -> PerformanceLogger:
    """
    Convenience function for performance logging.
    
    Args:
        module_name: Module name
        operation: Operation name
        **kwargs: Additional context
        
    Returns:
        PerformanceLogger context manager
    """
    return _logger_manager.log_performance(module_name, operation, **kwargs)

def set_market_hours_level(market_level: str = "DEBUG", non_market_level: str = "INFO"):
    """Convenience function to set market-hours-aware log levels."""
    _logger_manager.set_market_hours_level(market_level, non_market_level)

def log_trade_execution(trade_data: Dict[str, Any]):
    """Convenience function for trade execution logging."""
    _logger_manager.log_trade_execution(trade_data)

def get_log_file_path() -> str:
    """Get the current log file path."""
    return _logger_manager.get_log_file_path()


if __name__ == "__main__":
    """
    Usage Examples and Testing
    """
    
    print("=== MyAlgo Simplified Logging System Examples ===\n")
    
    # Example 1: Basic module logging
    print("1. Basic Module Logging:")
    
    config_logger = get_logger('config')
    trading_logger = get_logger('trading')
    strategy_logger = get_logger('strategy')
    
    config_logger.info("All configurations loaded successfully")
    trading_logger.info("Order placed - NIFTY CALL ATM qty=75")
    strategy_logger.debug("Entry condition met - close > ema_20")
    
    print("   ✓ Created module loggers and logged sample messages")
    
    # Example 2: Performance logging
    print("\n2. Performance Logging:")
    
    with log_performance('config', 'load_all_configs'):
        # Simulate some work
        import time
        time.sleep(0.1)
        config_logger.info("Configuration validation completed")
    
    print("   ✓ Performance logging completed")
    
    # Example 3: Error logging with context
    print("\n3. Error Logging:")
    
    risk_logger = get_logger('risk')
    
    try:
        # Simulate an error
        raise ValueError("Stop loss calculation failed")
    except Exception as e:
        risk_logger.error(f"Risk calculation error - {str(e)}", exc_info=True)
    
    print("   ✓ Error logged with full context and stack trace")
    
    # Example 4: Market session awareness
    print("\n4. Market Session Awareness:")
    
    set_market_hours_level("DEBUG", "INFO")
    system_logger = get_logger('system')
    
    print("   ✓ Market session log levels adjusted")
    
    # Example 5: Trade execution logging
    print("\n5. Trade Execution Logging:")
    
    trade_data = {
        "trade_id": "T_001",
        "symbol": "NIFTY",
        "action": "BUY",
        "leg_type": "CALL",
        "strike": "ATM",
        "quantity": 75,
        "price": 185.50,
        "strategy": "EMA_Trend_Following"
    }
    
    log_trade_execution(trade_data)
    
    print("   ✓ Trade execution logged with readable format")
    
    # Example 6: Multiple module coordination
    print("\n6. Module Coordination:")
    
    market_data_logger = get_logger('market_data')
    indicators_logger = get_logger('indicators')
    
    # Simulate data flow through modules
    market_data_logger.debug("Received LTP update - NIFTY=18545.75")
    indicators_logger.debug("Updated indicators - ema_20=18520.30, rsi_14=62.5")
    strategy_logger.info("Signal generated - BUY with confidence=0.85")
    trading_logger.info("Order placed - ID=ORD_001, status=PENDING")
    
    print("   ✓ Module coordination logging completed")
    
    print(f"\n✅ All logging examples completed successfully!")
    print(f"📁 Log file created: {get_log_file_path()}")
    
    # Show sample log content
    print(f"\n📖 Sample log content:")
    try:
        with open(get_log_file_path(), 'r') as f:
            lines = f.readlines()
            for i, line in enumerate(lines[-3:], 1):  # Show last 3 lines
                print(f"   {line.strip()}")
    except Exception as e:
        print(f"   Could not read log file: {e}")
    
    # Cleanup for demo
    print("\n🧹 Shutting down loggers...")
    _logger_manager.shutdown()