import yaml
from pathlib import Path

with open("configs/config.yaml") as f:
    cfg = yaml.safe_load(f)

PROJECT_ROOT = Path(cfg["paths"]["project_root"])
DATA_PATH = PROJECT_ROOT / cfg["paths"]["data_dir"]


BINANCE_TRADES_LIMIT = 1000
MAX_RETRIES = 3
RETRY_DELAY = 2
