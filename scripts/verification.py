import bz2
import os
from pathlib import Path

repo_root = Path(__file__).resolve().parents[1]
data_dir = Path(os.environ.get("WIKI_HH_DATA_DIR", repo_root / "data"))
files = sorted(data_dir.glob("*.bz2"))
if not files:
    raise FileNotFoundError(f"No .bz2 files found under {data_dir}")
path = files[0]

with bz2.open(path, "rt", encoding="utf-8") as f:
    for _ in range(10):
        print(f.readline())