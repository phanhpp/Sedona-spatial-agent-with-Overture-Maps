"""
City configurations template — copy this file to city_configs.py and customise.

  cp src/city_configs.example.py src/city_configs.py

city_configs.py is gitignored so you can safely hardcode S3 paths or other
environment-specific values there.

Two ways to set enriched_file paths:
  1. Env var (recommended for CI): set S3_ENRICHED_BASE=s3a://your-bucket/enriched
     and leave enriched_file as the local default — the helper switches to S3 automatically.
  2. Hardcode directly in city_configs.py (for local dev):
     "enriched_file": "s3a://your-bucket/enriched/buildings_sydney_h3/",
"""

import os

_S3_BASE = os.environ.get("S3_ENRICHED_BASE", "").rstrip("/")


def _enriched_path(city: str, local_suffix: str) -> str:
    """Return the enriched path: S3 if S3_ENRICHED_BASE is set, otherwise local."""
    if _S3_BASE:
        return f"{_S3_BASE}/{city}_h3/"
    return local_suffix


CITY_CONFIGS = {
    "sydney": {
        "bbox": "150.5,-34.2,151.5,-33.5",
        "raw_file": "data/raw/buildings_sydney.parquet",
        "enriched_file": _enriched_path("buildings_sydney", "data/enriched/buildings_sydney_h3/"),
    },
    "melbourne": {
        "bbox": "144.5,-38.2,145.5,-37.5",
        "raw_file": "data/raw/buildings_melbourne.parquet",
        "enriched_file": _enriched_path("buildings_melbourne", "data/enriched/buildings_melbourne_h3/"),
    },
    "brisbane": {
        "bbox": "152.7,-27.8,153.4,-27.2",
        "raw_file": "data/raw/buildings_brisbane.parquet",
        "enriched_file": _enriched_path("buildings_brisbane", "data/enriched/buildings_brisbane_h3/"),
    },
    "perth": {
        "bbox": "115.7,-32.1,116.1,-31.7",
        "raw_file": "data/raw/buildings_perth.parquet",
        "enriched_file": _enriched_path("buildings_perth", "data/enriched/buildings_perth_h3/"),
    },
}
