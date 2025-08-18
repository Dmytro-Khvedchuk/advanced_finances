import yaml
from pathlib import Path

with open("configs/config.yaml") as f:
    cfg = yaml.safe_load(f)

PROJECT_ROOT = Path(cfg["paths"]["project_root"])
DATA_PATH = PROJECT_ROOT / cfg["paths"]["data_dir"]
