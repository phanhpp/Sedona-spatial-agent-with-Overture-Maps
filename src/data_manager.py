"""
City-level Overture building data manager.

Data pipeline (per city):
  1. Raw: one .parquet file from Overture (downloaded via overturemaps CLI).
  2. Enriched: same data + pre-computed columns (centroid, h3_r7, h3_r8, h3_r9) saved under data/enriched/.
  3. In Spark: enriched data loaded into memory and registered as a temp view (e.g. buildings_sydney)
     so the agent can run SQL against it. This view lasts only for the current session.

When loading a city we:
  - If no raw file → download it, then enrich and save.
  - If raw exists but no enriched dir → enrich and save (no download).
  - If enriched dir exists → just load it into Spark (fast, no download or re-enrich).
"""

import os
import subprocess
from pathlib import Path
from pyspark.sql import functions as F
from src.sedona_client import get_sedona
from src.city_configs import CITY_CONFIGS
import logging
logger = logging.getLogger(__name__)

# Project root (parent of src/) — paths are resolved relative to this so tests/notebooks work.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


def _resolve(path: str) -> str:
    """Resolve a path: S3 URIs are returned as-is; local paths are resolved relative to project root."""
    if path.startswith("s3://") or path.startswith("s3a://"):
        return path
    return str(_PROJECT_ROOT / path)

# Cities currently loaded into Spark this session (temp view name + row count).
# Lost when the session ends; reload from disk next time.
_loaded_cities: dict = {}


def _view_name(city: str) -> str:
    """Return the Spark temp view name for a city (e.g. 'Sydney' -> 'buildings_sydney')."""
    return f"buildings_{city.lower().replace(' ', '_')}"


def _enriched_location(city: str) -> str | None:
    """
    Return where enriched GeoParquet exists: "s3", "local", or None if not present.

    Use this when you need to clearly inform whether data is on S3 or on local disk.
    On S3 credential or permission errors, returns None so the app can degrade gracefully.
    """
    enriched = _resolve(CITY_CONFIGS[city]["enriched_file"])
    if enriched.startswith("s3a://") or enriched.startswith("s3://"):
        logger.info(f"Checking S3 for {enriched}")
        try:
            import boto3
            from botocore.exceptions import ClientError
            path = enriched.split("://", 1)[1]
            bucket, prefix = path.split("/", 1)
            s3 = boto3.client("s3")
            resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
            return "s3" if resp.get("KeyCount", 0) > 0 else None
        except ClientError as e:
            logger.warning("S3 check failed for %s: %s", enriched, e.response.get("Error", {}).get("Code", e))
            return None
    if not os.path.exists(enriched):
        return None
    part_files = [f for f in os.listdir(enriched) if f.endswith(".parquet")]
    return "local" if part_files else None


def _enriched_exists(city: str) -> bool:
    """Check if enriched GeoParquet exists — on S3 or local disk depending on config."""
    return _enriched_location(city) is not None


def _download_if_needed(city: str) -> str:
    """
    Download raw Overture GeoParquet for the city if not on disk.

    Uses overturemaps CLI with the city's bbox from CITY_CONFIGS.
    Returns the path to the raw geoparquet file.
    """
    print(f"====Calling download_if_needed for {city}====")
    cfg = CITY_CONFIGS[city]
    raw_file = _resolve(cfg["raw_file"])
    if not os.path.exists(raw_file):
        os.makedirs(os.path.dirname(raw_file), exist_ok=True)
        print(f"Downloading {city} buildings from Overture...")
        subprocess.run([
            "overturemaps", "download",
            f"--bbox={cfg['bbox']}",
            "-f", "geoparquet", "--type=building",
            "-o", raw_file,
        ], check=True)
        print(f"✓ Downloaded {city}")
    else:
        print(f"✓ Raw file exists for {city}: {raw_file}")
    return raw_file


def _enrich_and_save(city: str) -> str:
    """
    Load raw GeoParquet, add centroid + H3 (r7, r8, r9) columns, save as enriched GeoParquet.

    One-time per city; subsequent loads use the saved enriched dir.
    Returns the path to the enriched output directory.
    """
    sd = get_sedona()
    cfg = CITY_CONFIGS[city]
    raw_path = _resolve(cfg["raw_file"])
    enriched_path = _resolve(cfg["enriched_file"])

    print(f"Enriching {city} with H3 indices (one-time operation)...")
    df_raw = sd.read.format("geoparquet").load(raw_path)

    # Sedona uses ST_H3CellIDs(geom, level, fullCover); element_at(..., 1) gets single cell for centroid.
    df_enriched = (
        df_raw
        .withColumn("centroid", F.expr("ST_Centroid(geometry)"))
        .withColumn("h3_r7", F.expr("element_at(ST_H3CellIDs(ST_Centroid(geometry), 7, true), 1)"))
        .withColumn("h3_r8", F.expr("element_at(ST_H3CellIDs(ST_Centroid(geometry), 8, true), 1)"))
        .withColumn("h3_r9", F.expr("element_at(ST_H3CellIDs(ST_Centroid(geometry), 9, true), 1)"))
    )

    os.makedirs(enriched_path, exist_ok=True)
    df_enriched.write.mode("overwrite").format("geoparquet").save(enriched_path)
    print(f"✓ Saved enriched {city} to {enriched_path}")

    return enriched_path


def _load_into_spark(city: str) -> int:
    """
    Load city building data into Spark: register temp view, cache, and materialise count.

    Fast path: enriched dir exists on disk → load only (~10s).
    Slow path: first time for city → download raw if needed then enrich (H3), save, then load (~3–6 min, once only).
    Returns the number of rows in the view.
    """
    print("====Calling load_into_spark - download and enrich if needed====")
    sd = get_sedona()
    cfg = CITY_CONFIGS[city]
    view = _view_name(city)

    enriched_path = _resolve(cfg["enriched_file"])
    if _enriched_exists(city):
        # Fast path — pre-enriched file already on disk or s3
        logger.info(f"Loading pre-enriched {city} from {enriched_path}...")
        df = sd.read.format("geoparquet").load(enriched_path)
    else:
        # Slow path — first time setup for this city
        logger.info(f"Slow path setup for {city}...")
        _download_if_needed(city)
        _enrich_and_save(city)
        df = sd.read.format("geoparquet").load(enriched_path)

    df.cache()
    count = df.count()  # materialise cache now
    df.createOrReplaceTempView(view)

    _loaded_cities[city] = {"view": view, "count": count}
    logger.info(f"✓ {city} ready: {count:,} buildings in Spark view '{view}'")
    return count


def city_status(city: str) -> dict:
    """
    Return status for a city so tools can decide what to tell the user.

    Args:
        city: City name (e.g. 'sydney', 'melbourne'). Normalised to lowercase.

    Returns:
        If unsupported: {"supported": False}.
        If supported: dict with:
          - in_spark: True if already loaded into Spark this session (can query now).
          - on_disk: True if enriched data exists (on S3 or local — fast reload next time).
          - enriched_location: "s3" | "local" | None — where enriched data lives; None if not yet created.
          - needs_download: True if we don't have raw data yet (slow path).
          - view, count: Spark view name and row count (only when in_spark).
    """
    city = city.lower().strip()
    if city not in CITY_CONFIGS:
        return {"supported": False}

    in_spark = city in _loaded_cities
    enriched_location = _enriched_location(city)
    on_disk = enriched_location is not None  # exists on S3 or local
    needs_download = not os.path.exists(_resolve(CITY_CONFIGS[city]["raw_file"])) and not on_disk

    return {
        "supported":         True,
        "in_spark":          in_spark,
        "on_disk":           on_disk,
        "enriched_location": enriched_location,
        "needs_download":    needs_download,
        "view":              _loaded_cities[city]["view"]  if in_spark else None,
        "count":             _loaded_cities[city]["count"] if in_spark else None,
    }


def load_city(city: str) -> tuple[bool, str]:
    """
    Load a city's building data so the agent can query it. Called by agent tools.

    - Already in Spark this session → no-op; returns success and current view/count.
    - Enriched data on disk → load from disk into Spark (fast).
    - No enriched data → download raw if needed, enrich and save, then load (slow first time).

    Args:
        city: City name (e.g. 'sydney'). Must be in CITY_CONFIGS.

    Returns:
        (success, message): True and a short status message, or False and an error message.
    """
    city = city.lower().strip()

    if city not in CITY_CONFIGS:
        available = ", ".join(CITY_CONFIGS.keys())
        return False, f"City '{city}' not supported. Available: {available}"

    if city in _loaded_cities:
        info = _loaded_cities[city]
        return True, (
            f"'{city}' already loaded in Spark view '{info['view']}' "
            f"with {info['count']:,} buildings."
        )

    try:
        count = _load_into_spark(city)
        view = _view_name(city)
        source = "disk cache" if _enriched_exists(city) else "freshly downloaded"
        return True, (
            f"✓ '{city}' loaded from {source}: {count:,} buildings in view '{view}'. "
            f"Pre-computed columns available: h3_r7, h3_r8, h3_r9, centroid."
        )
    except subprocess.CalledProcessError as e:
        return False, f"Download failed for '{city}': {e}"
    except Exception as e:
        return False, f"Failed to load '{city}': {e}"


def get_loaded_cities_schema() -> str:
    """
    Return a short schema summary of all cities currently loaded in Spark (this session).

    Used to inject up-to-date column info into agent tool docstrings so the
    model knows view names, row counts, and available columns (geometry,
    centroid, h3_r7/r8/r9, etc.).
    """
    if not _loaded_cities:
        return "No cities loaded yet. Call load_city_data first."

    lines = []
    for city, info in _loaded_cities.items():
        lines.append(f"View '{info['view']}' ({city}, {info['count']:,} rows):")
        lines.append("  Columns:")
        lines.append("    geometry       : geometry (polygon)")
        lines.append("    centroid       : geometry (point, pre-computed)")
        lines.append("    h3_r7          : string   (H3 res 7, pre-computed — USE THIS)")
        lines.append("    h3_r8          : string   (H3 res 8, pre-computed — USE THIS)")
        lines.append("    h3_r9          : string   (H3 res 9, pre-computed — USE THIS)")
        lines.append("    height         : double")
        lines.append("    class          : string   (e.g. 'residential', 'commercial')")
        lines.append("    names          : struct")
        lines.append("    bbox           : struct<xmin double, xmax double, ymin double, ymax double>")
        lines.append("")
    return "\n".join(lines)


def get_city_info(city: str) -> dict:
    "Get the status of cities loaded in Spark"
    return _loaded_cities[city]