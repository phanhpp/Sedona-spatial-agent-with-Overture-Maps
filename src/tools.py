from src.sedona_client import get_sedona
from typing import Annotated
from langchain.tools import tool, ToolRuntime
from src.data_manager import (
    CITY_CONFIGS,
    city_status,
    load_city,
    get_loaded_cities_schema,
)
from langgraph.types import Command
from src.data_manager import _resolve  # for resolving enriched path in check_city_status
import os
import re
import subprocess
from langchain_core.messages import SystemMessage, HumanMessage,ToolMessage


BUILDINGS_FILE = "/Users/dangphuonganh/Documents/UNsw/sedona/data/sydney_buildings.parquet"

# Keywords that are not allowed (DDL/DML) — only read-only queries (SELECT, WITH) allowed
_DDL_DML_KEYWORDS = frozenset({
    "CREATE", "DROP", "ALTER", "TRUNCATE", "INSERT", "DELETE", "UPDATE", "MERGE",
    "REFRESH", "CACHE", "UNCACHE", "CLEAR", "ADD", "SET", "RESET", "EXPLAIN",
    "ANALYZE", "REPAIR", "LOAD", "COPY",
})


def _normalize_sql_for_validation(sql: str) -> str:
    """Strip comments and collapse whitespace to get leading keyword."""
    # Remove single-line comments (-- and #)
    sql = re.sub(r"--[^\n]*", "", sql)
    sql = re.sub(r"#[^\n]*", "", sql)
    # Remove multi-line comments
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return " ".join(sql.split()).strip().upper()


def is_readonly_query(query: str) -> bool:
    """Return True if the query looks like read-only (SELECT or WITH); reject DDL/DML."""
    normalized = _normalize_sql_for_validation(query)
    if not normalized:
        return False
    first_token = normalized.split()[0]
    if first_token in _DDL_DML_KEYWORDS:
        return False
    return first_token in ("SELECT", "WITH", "SHOW", "DESCRIBE", "DESC")

@tool()
def execute_query(query: str) -> str:
    """Execute a read-only Sedona/Spark SQL query (SELECT, WITH, SHOW, DESCRIBE). DDL/DML is rejected."""
    if not is_readonly_query(query):
        return "Error: Only read-only queries (SELECT, WITH, SHOW, DESCRIBE) are allowed. DDL/DML is not permitted."
    sedona = get_sedona()
    try:
        res_df = sedona.sql(query)
        return res_df.limit(1000).toPandas().to_string()
    except Exception as e:
        return f"Query failed: {e}"

@tool()
def heat_proxy() -> str:
    """Run urban heat proxy query: aggregate buildings by H3 cell (res 7) and show top 15 cells by heat proxy score.

    Uses the 'buildings' temp view (call load_building_table first if needed). For each H3 cell, computes:
    - building_count: number of buildings
    - total_footprint_sqm: sum of building footprint areas (m², UTM 56S)
    - avg_building_sqm: average footprint size
    - heat_proxy_score: count × total footprint / 1e6 (proxy for impervious surface / heat island potential)

    Results are ordered by heat_proxy_score descending.
    """
    sedona = get_sedona()
    heat_proxy_df = sedona.sql("""
        SELECT
            element_at(ST_H3CellIDs(ST_Centroid(geometry), 7, true), 1) AS h3_cell,
            COUNT(*)                           AS building_count,
            ROUND(SUM(
                ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32756'))
            ), 0)                              AS total_footprint_sqm,
            ROUND(AVG(
                ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32756'))
            ), 1)                              AS avg_building_sqm,
            ROUND(
                COUNT(*) * SUM(
                    ST_Area(ST_Transform(geometry, 'EPSG:4326', 'EPSG:32756'))
                ) / 1000000
            , 2)                               AS heat_proxy_score
        FROM buildings
        GROUP BY h3_cell
        ORDER BY heat_proxy_score DESC
        LIMIT 15
    """)
    return heat_proxy_df.toPandas().to_string()

from src.data_manager import get_city_info
@tool()
def check_city_status(
    city: str,
    
    ) -> str:
    """Check if a city's building data is loaded in Spark, cached on disk, or needs downloading.
    Always call this before querying a city you haven't used yet this session.

    Args:
        city: City name e.g. 'sydney', 'melbourne', 'brisbane', 'perth'
    """
    city = city.lower().strip()
    status = city_status(city)

    print("city_status", status)
    if not status["supported"]:
        return f"'{city}' not supported. Available cities: {', '.join(CITY_CONFIGS.keys())}"

    if status["in_spark"]:
        return (
            f"✓ '{city}' is loaded in Spark as view '{status['view']}' "
            f"with {status['count']:,} buildings. Ready to query."
        )

    if status["on_disk"]:
        loc = status["enriched_location"]
        if loc == "s3":
            return (
                f"'{city}' enriched GeoParquet exists on S3 (H3 pre-computed). "
                f"Call load_city_data('{city}') to load into Spark (~10-20s, no download needed)."
            )
        # local
        enriched = _resolve(CITY_CONFIGS[city]["enriched_file"])
        size_mb = sum(
            os.path.getsize(os.path.join(enriched, f))
            for f in os.listdir(enriched)
            if f.endswith(".parquet")
        ) / 1_000_000
        return (
            f"'{city}' enriched GeoParquet exists on local disk ({size_mb:.0f}MB, H3 pre-computed). "
            f"Call load_city_data('{city}') to load into Spark (~10-20s, no download needed)."
        )

    if status["needs_download"]:
        return (
            f"'{city}' raw data not downloaded yet. Downloading + enriching will take ~3-6 minutes (one-time only). "
            f"Inform the user before calling load_city_data('{city}')."
        )
    
    return (f"'{city}' raw data exists but not yet enriched. Enriching only will take a few minutes (one-time). "
        f"Call load_city_data('{city}').")
   

@tool()
def load_city_data(city: str, runtime: ToolRuntime) -> Command:
    """Download (if needed) and load a city's building data into Spark with pre-computed H3 indices.
    The enriched file is saved to disk so future restarts only take ~10s (no re-download or re-enrichment).
    Only call this AFTER informing the user it may take several minutes if download is needed.

    Args:
        city: City name e.g. 'sydney', 'melbourne', 'brisbane', 'perth'
    """
    success, message = load_city(city)

    city_info = get_city_info(city)
    
    print("Updating state with loaded_cities:", {city: {"view": city_info["view"], "count": city_info["count"]}})
    return Command(
        update={
        "loaded_cities": {city: {"view": city_info["view"], "count": city_info["count"]}},
        "messages": [ToolMessage(content=message, tool_call_id=runtime.tool_call_id)]
        }
    )
    

tools = [
    execute_query,
    heat_proxy,
    check_city_status,
    load_city_data,
]

#@tool()
# def load_building_table(city: str) -> str:
#     """Load Overture buildings into Spark; skip if 'buildings' view already exists and is not empty."""
#     sedona = get_sedona()

#     # Spark: check if temp view exists and has data (no SQL "IF NOT EXISTS" for views)
#     if sedona.catalog.tableExists("buildings"):
#         buildings = sedona.table("buildings")
#         if not buildings.limit(1).isEmpty():
#             return "Table 'buildings' already loaded and not empty."
#         # view exists but empty — drop and reload below
#         sedona.catalog.dropTempView("buildings")

#     # Ensure parquet file exists (download via CLI if not)
#     if not os.path.exists(BUILDINGS_FILE):
#         # bbox = min_lon, min_lat, max_lon, max_lat
#         subprocess.run([
#             "overturemaps", "download",
#             "--bbox=150.5,-34.2,151.5,-33.5",
#             "-f", "geoparquet", "--type=building",
#             "-o", BUILDINGS_FILE,
#         ], check=True)
#         print("✓ Downloaded")
#     else:
#         print(f"✓ Using cached: {BUILDINGS_FILE}")

#     try:
#         buildings = sedona.read.format("geoparquet").load(BUILDINGS_FILE)
#         buildings.createOrReplaceTempView("buildings")
#         print(f"Total buildings: {buildings.count():,}")
#         print(f"Columns: {buildings.columns}")
#         return "Greater Sydney building table ready"
#     except Exception as e:
#         return f"Error loading building table: {e}"
