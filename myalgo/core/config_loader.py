"""
MyAlgo Config Loader Module

Provides robust configuration loading with atomic operations, memory caching,
and comprehensive validation for the algo trading system.

Author: MyAlgo Trading System
"""

import json
import os
from pathlib import Path
from typing import Dict, Any, Optional
import hashlib


class ConfigLoader:
    """
    Enhanced configuration loader with caching and validation.
    
    Features:
    - Atomic loading: All configs load or none do
    - Memory caching for performance
    - Comprehensive validation
    - Clear error messages using built-in exceptions
    """
    
    _instance: Optional['ConfigLoader'] = None
    _configs_cache: Optional[Dict[str, Any]] = None
    _cache_hash: Optional[str] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        self.config_dir = Path("config")
        self.config_files = {
            "system_config": "1_system_config.json",
            "instruments": "2_instruments.json", 
            "indicators": "3_indicator_factory.json",
            "condition_keys": "4_condition_keys.json",
            "strategy": "5_strategy.json"
        }
        self.required_keys = {
            "system_config": ["modes"],
            "instruments": ["instruments"],
            "indicators": ["indicators_config"], 
            "condition_keys": ["all_available_keys"],
            "strategy": ["legs"]
        }
    
    def _calculate_files_hash(self) -> str:
        """Calculate hash of all config files for cache invalidation."""
        hash_md5 = hashlib.md5()
        
        for config_name, filename in self.config_files.items():
            file_path = self.config_dir / filename
            if file_path.exists():
                with open(file_path, 'rb') as f:
                    hash_md5.update(f.read())
        
        return hash_md5.hexdigest()
    
    def _validate_config_file_exists(self, config_name: str, file_path: Path) -> None:
        """Validate that config file exists."""
        if not file_path.exists():
            raise FileNotFoundError(
                f"Config file missing: {file_path}. "
                f"Required for {config_name} configuration."
            )
    
    def _load_json_file(self, file_path: Path) -> Dict[str, Any]:
        """Load and parse JSON file with error handling."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if not data:
                raise ValueError(f"Config file is empty: {file_path}")
            
            return data
            
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"Invalid JSON in {file_path}: {e.msg}",
                e.doc, e.pos
            )
        except Exception as e:
            raise ValueError(f"Error reading {file_path}: {str(e)}")
    
    def _validate_config_structure(self, config_name: str, config_data: Dict[str, Any]) -> None:
        """Validate that required keys exist in config."""
        required_keys = self.required_keys.get(config_name, [])
        
        for key in required_keys:
            if key not in config_data:
                raise KeyError(
                    f"Missing required key '{key}' in {config_name} configuration. "
                    f"Required keys: {required_keys}"
                )
        
        # Additional specific validations
        if config_name == "system_config":
            modes = config_data.get("modes", {})
            if not isinstance(modes, dict) or not modes:
                raise ValueError("'modes' must be a non-empty dictionary in system_config")
        
        elif config_name == "instruments":
            instruments = config_data.get("instruments", {})
            if not isinstance(instruments, dict) or not instruments:
                raise ValueError("'instruments' must be a non-empty dictionary")
        
        elif config_name == "strategy":
            legs = config_data.get("legs", [])
            if not isinstance(legs, list) or not legs:
                raise ValueError("'legs' must be a non-empty list in strategy configuration")
    
    def _load_all_configs_fresh(self) -> Dict[str, Any]:
        """Load all configuration files from disk with validation."""
        configs = {}
        loaded_configs = {}
        
        try:
            # Phase 1: Load all files (atomic operation - all or nothing)
            for config_name, filename in self.config_files.items():
                file_path = self.config_dir / filename
                
                # Check file exists
                self._validate_config_file_exists(config_name, file_path)
                
                # Load JSON
                config_data = self._load_json_file(file_path)
                loaded_configs[config_name] = config_data
            
            # Phase 2: Validate all configs (only after all files loaded successfully)
            for config_name, config_data in loaded_configs.items():
                self._validate_config_structure(config_name, config_data)
                configs[config_name] = config_data
            
            return configs
            
        except Exception as e:
            # Clear any partial loading
            configs.clear()
            loaded_configs.clear()
            raise e
    
    def load_all_configs(self, force_reload: bool = False) -> Dict[str, Any]:
        """
        Load all configuration files with caching.
        
        Args:
            force_reload: If True, bypass cache and reload from disk
            
        Returns:
            Dictionary containing all configs with keys:
            - system_config: System configuration
            - instruments: Instrument definitions  
            - indicators: Indicator configuration
            - condition_keys: Available condition keys
            - strategy: Strategy definition
            
        Raises:
            FileNotFoundError: If any config file is missing
            json.JSONDecodeError: If any JSON file is malformed
            ValueError: If any config file is empty or invalid
            KeyError: If required keys are missing
        """
        # Calculate current files hash
        current_hash = self._calculate_files_hash()
        
        # Return cached configs if available and valid
        if (not force_reload and 
            self._configs_cache is not None and 
            self._cache_hash == current_hash):
            return self._configs_cache.copy()
        
        # Load fresh configs
        configs = self._load_all_configs_fresh()
        
        # Update cache
        self._configs_cache = configs.copy()
        self._cache_hash = current_hash
        
        return configs
    
    def get_config(self, config_name: str) -> Dict[str, Any]:
        """
        Get a specific configuration.
        
        Args:
            config_name: Name of config (system_config, instruments, indicators, condition_keys, strategy)
            
        Returns:
            Specific configuration dictionary
            
        Raises:
            KeyError: If config_name is not valid
        """
        if config_name not in self.config_files:
            valid_names = list(self.config_files.keys())
            raise KeyError(f"Invalid config name '{config_name}'. Valid names: {valid_names}")
        
        configs = self.load_all_configs()
        return configs[config_name]
    
    def reload_configs(self) -> Dict[str, Any]:
        """Force reload all configs from disk, bypassing cache."""
        return self.load_all_configs(force_reload=True)
    
    def clear_cache(self) -> None:
        """Clear the configuration cache."""
        self._configs_cache = None
        self._cache_hash = None


# Global instance for easy access
_config_loader = ConfigLoader()

def load_all_configs(force_reload: bool = False) -> Dict[str, Any]:
    """
    Convenience function to load all configurations.
    
    Args:
        force_reload: If True, bypass cache and reload from disk
        
    Returns:
        Dictionary containing all validated configurations
    """
    return _config_loader.load_all_configs(force_reload)

def get_config(config_name: str) -> Dict[str, Any]:
    """
    Convenience function to get a specific configuration.
    
    Args:
        config_name: Name of config to retrieve
        
    Returns:
        Specific configuration dictionary
    """
    return _config_loader.get_config(config_name)

def reload_configs() -> Dict[str, Any]:
    """Convenience function to force reload all configs."""
    return _config_loader.reload_configs()


if __name__ == "__main__":
    """
    Usage Examples
    """
    
    print("=== MyAlgo Config Loader Usage Examples ===\n")
    
    try:
        # Example 1: Load all configurations
        print("1. Loading all configurations...")
        configs = load_all_configs()
        
        print(f"    Loaded {len(configs)} configurations:")
        for config_name in configs.keys():
            print(f"     - {config_name}")
        
        # Example 2: Access specific configurations safely
        print("\n2. Accessing specific configurations...")
        
        # Get system config
        system_config = configs['system_config']
        print(f"    System name: {system_config.get('system_name', 'N/A')}")
        print(f"    Version: {system_config.get('version', 'N/A')}")
        
        # Get trading mode
        live_trading = system_config.get('modes', {}).get('live_trading', {})
        if live_trading.get('enabled'):
            broker = live_trading.get('config', {}).get('broker', 'N/A')
            capital = live_trading.get('config', {}).get('capital', 'N/A')
            print(f"    Live trading enabled with broker: {broker}, capital: {capital}")
        
        # Get strategy info
        strategy = configs['strategy']
        print(f"    Strategy: {strategy.get('strategy', 'N/A')}")
        print(f"    Status: {strategy.get('status', 'N/A')}")
        print(f"    Number of legs: {len(strategy.get('legs', []))}")
        
        # Example 3: Using convenience functions
        print("\n3. Using convenience functions...")
        instruments = get_config('instruments')
        print(f"    Available instruments: {list(instruments.get('instruments', {}).keys())}")
        
        # Example 4: Cache performance test
        print("\n4. Testing cache performance...")
        import time
        
        start_time = time.time()
        configs1 = load_all_configs()  # First call (from cache)
        time1 = time.time() - start_time
        
        start_time = time.time()
        configs2 = load_all_configs()  # Second call (from cache)
        time2 = time.time() - start_time
        
        print(f"    First call: {time1:.4f}s")
        print(f"    Second call: {time2:.4f}s (cached)")
        print(f"    Cache speedup: {time1/time2:.1f}x faster" if time2 > 0 else "    Cache working")
        
        print("\n All examples completed successfully!")
        
    except FileNotFoundError as e:
        print(f"L Config file missing: {e}")
    except json.JSONDecodeError as e:
        print(f"L Invalid JSON: {e}")
    except KeyError as e:
        print(f"L Missing required key: {e}")
    except ValueError as e:
        print(f"L Validation error: {e}")
    except Exception as e:
        print(f"L Unexpected error: {e}")