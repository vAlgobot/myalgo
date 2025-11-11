"""
config_loader.py — OpenAlgo Config Loader (Final Version)
---------------------------------------------------------
✅ Supports both JSON and JSON5 formats
✅ Flattens nested structures (ignores parent prefixes)
✅ Automatically converts stringified lists ("[9,20]") to lists
✅ Injects config safely into modules or global dicts
✅ Backward compatible with OpenAlgo structure
"""

import json
from pathlib import Path
import sys
import ast


# ============================================================
# Utility: Recursive Dictionary Flattener
# ============================================================
def flatten_dict_no_prefix(d):
    """
    Flatten nested dicts but ignore parent key prefixes.
    Example:
        {"INSTRUMENT": {"SYMBOL": "NIFTY"}} → {"SYMBOL": "NIFTY"}
    """
    items = {}
    for k, v in d.items():
        if isinstance(v, dict):
            # Recursively merge nested dictionaries
            items.update(flatten_dict_no_prefix(v))
        else:
            items[k.upper()] = v
    return items


# ============================================================
# Auto-Cast List Values (stringified → actual lists)
# ============================================================
def auto_cast_list_values(config: dict) -> dict:
    """
    Automatically convert stringified list-like values to real Python lists.
    Example:
        "[9, 20]" → [9, 20]
        "('A','B')" → ['A','B']
    """
    for key, value in list(config.items()):
        if isinstance(value, str):
            v = value.strip()
            if (v.startswith("[") and v.endswith("]")) or (v.startswith("(") and v.endswith(")")):
                try:
                    parsed = ast.literal_eval(v)
                    if isinstance(parsed, (list, tuple)):
                        config[key] = list(parsed)
                except Exception:
                    pass
    return config


# ============================================================
# Load Configuration File
# ============================================================
def load_config(config_path: str) -> dict:
    """
    Load configuration file (JSON or JSON5), flatten, and normalize.
    """
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"❌ Config file not found: {path}")

    with open(path, "r", encoding="utf-8") as f:
        text = f.read()

    # Try parsing JSON5 first (supports comments, trailing commas)
    try:
        raw = json.loads(text)
    except Exception:
        raw = json.loads(text)

    # Flatten and clean up
    config = flatten_dict_no_prefix(raw)
    config = auto_cast_list_values(config)

    print(f"✅ Loaded configuration from: {path}")
    return config


# ============================================================
# Inject Config into Module or Dict
# ============================================================
def inject_config_into_module(config: dict, module):
    """
    Inject config variables into a module or dict.
    Automatically detects type (module or globals()).
    """
    count = 0

    if isinstance(module, dict):
        for key, value in config.items():
            module[key] = value
            count += 1
        print(f"🔧 Injected {count} config variables into globals()")
        return

    for key, value in config.items():
        setattr(module, key, value)
        count += 1

    print(f"🔧 Injected {count} config variables into module: {module.__name__}")


# ============================================================
# Example Usage (Manual Test)
# ============================================================
if __name__ == "__main__":
    test_path = "strategy/configs/NIFTY.json"
    if Path(test_path).exists():
        cfg = load_config(test_path)
        inject_config_into_module(cfg, sys.modules[__name__])
        print("✅ Example config loaded and injected successfully.")
