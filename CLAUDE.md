# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Install dependencies
```bash
pip install -r requirements.txt
```
Requires Java 11+ (`JAVA_HOME` must be set) for PySpark/Sedona.

### Run tests
```bash
pytest tests/ -v
```

### Run a single test
```bash
pytest tests/test_agent.py::test_is_readonly_query_allows -v
```

### Environment setup
Create a `.env` file with:
```
OPENAI_API_KEY=your_key_here
```

## Architecture

This is a LangGraph-based spatial query agent that lets users ask natural-language questions about building data for Australian cities (Sydney, Melbourne, Brisbane, Perth).

### Data pipeline (per city, in `src/data_manager.py`)

Three-stage pipeline managed by `CITY_CONFIGS`:
1. **Raw**: Downloaded from Overture Maps via `overturemaps` CLI as GeoParquet → `data/raw/buildings_{city}.parquet`
2. **Enriched**: Adds pre-computed columns (`centroid`, `h3_r7`, `h3_r8`, `h3_r9`) → `data/enriched/buildings_{city}_h3/` (Spark partitioned directory)
3. **In Spark**: Loaded into memory as a temp view `buildings_{city}` — session-only, re-loaded from disk on next start (~10s), re-downloaded only on first ever use (~3–6 min)

The in-memory state of loaded cities is tracked in `_loaded_cities` (module-level dict in `data_manager.py`).

### Agent (`src/agent.py`)

- Built with `langgraph` using `create_agent` and `InMemorySaver` checkpointer for conversation history
- Model: `gpt-4.1` via `init_chat_model`
- Uses a `CustomState` (extends `AgentState`) with a `loaded_cities` field — a dict of `{city: {view, count}}` kept in sync with `data_manager._loaded_cities`
- A `@dynamic_prompt` middleware injects the current `loaded_cities` into the system prompt at each turn so the model knows what's already loaded
- Entry point: `run_query(query, config)` — `config` carries the `thread_id` for conversation threading

### Tools (`src/tools.py`)

Four tools registered with the agent:
- `check_city_status(city)` — returns whether a city is in Spark, on disk, or needs downloading; always call before loading
- `load_city_data(city)` — triggers the data pipeline; returns a `Command` that updates `loaded_cities` in agent state
- `execute_query(sql)` — runs read-only Sedona/Spark SQL; DDL/DML is blocked by `is_readonly_query()`; results capped at 1000 rows
- `heat_proxy()` — fixed urban heat island proxy query (building count × footprint area aggregated by H3 r7 cell)

### Sedona client (`src/sedona_client.py`)

Singleton `get_sedona()` — initializes `SedonaContext` once per process with local Spark (`local[*]`), 4GB driver memory, and Ivy jar cache at `~/.ivy2`. Jars are downloaded on first run and cached.

### Key conventions

- All file paths in `CITY_CONFIGS` are **relative to project root**; always resolve via `_resolve()` (uses `Path(__file__).parent.parent`) so code works from any cwd (notebooks, pytest, etc.)
- Spark temp view names follow the pattern `buildings_{city}` (lowercase, underscores)
- Geometry is WGS84 (EPSG:4326); area calculations use `ST_Transform(..., 'EPSG:4326', 'EPSG:32756')` (UTM 56S)
- Pre-computed H3 columns (`h3_r7`, `h3_r8`, `h3_r9`) exist on all enriched datasets — use these instead of computing H3 inline
