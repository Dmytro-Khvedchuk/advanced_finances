import yaml
from pathlib import Path

with open("configs/config.yaml") as f:
    cfg = yaml.safe_load(f)

# === PATHS ===
PROJECT_ROOT = Path(cfg["paths"]["project_root"])
DATA_PATH = PROJECT_ROOT / cfg["paths"]["data_dir"]

# === API CALLS ===
BINANCE_TRADES_LIMIT = 1000
MAX_RETRIES = 3
RETRY_DELAY = 2

# === LOGGER ===
LEVEL_MAP = {
    "DEBUG": 10,
    "INFO": 20,
    "WARNING": 30,
    "ERROR": 40,
    "CRITICAL": 50,
}

# === ENGINE ===
SYMBOL = cfg["engine"]["symbol"]
