from datetime import timedelta, datetime
from pathlib import Path
from yaml import safe_load

with open("configs/config.yaml") as f:
    cfg = safe_load(f)

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
BINANCE_EARLIEST_DATE = str(datetime(year=2017, month=8, day=17).date())
BINANCE_LATEST_DATE = str(datetime.now().date())
BINANCE_EARLIEST_ID = 1741118880
SYMBOL = cfg["engine"]["symbol"]
TIMEFRAME = cfg["engine"]["timeframe"]

TIMEFRAME_MAP = {
    "1m": timedelta(minutes=1),
    "5m": timedelta(minutes=5),
    "15m": timedelta(minutes=15),
    "30m": timedelta(minutes=30),
    "1h": timedelta(hours=1),
    "4h": timedelta(hours=4),
    "1D": timedelta(days=1),
}

MONTHS = {
    1: "jan",
    2: "feb",
    3: "mar",
    4: "apr",
    5: "may",
    6: "jun",
    7: "jul",
    8: "aug",
    9: "sep",
    10: "oct",
    11: "nov",
    12: "dec"
}

IMPACT_MAP = {
    "icon--ff-impact-gra": "Non-Economic",
    "icon--ff-impact-yel": "Low",
    "icon--ff-impact-ora": "Medium",
    "icon--ff-impact-red": "High",
}
