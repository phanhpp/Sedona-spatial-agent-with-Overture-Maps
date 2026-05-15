# Sedona spatial query agent

An **AI agent** that answers natural-language questions about **building footprints** in Australian cities using **Apache Sedona** (Spark) and **Overture Maps** data. Ask in plain English; the agent loads the right city, writes read-only SQL, and returns clear answers.

---

## What the agent can do

| Capability | Example question |
|------------|------------------|
| **Count buildings** | “How many buildings are in the Sydney CBD?” |
| **Compare cities** | “Compare building count in Sydney CBD vs Melbourne CBD.” |
| **Spatial filters** | “List buildings in the Perth CBD with height &gt; 20 m.” |
| **Aggregations** | “Which H3 cells have the most buildings in Brisbane?” |
| **Urban heat proxy** | “Show me the top areas by heat proxy score in Sydney.” (built-in `heat_proxy` tool) |
| **Schema & status** | “Is Melbourne loaded?” / “What columns are in the Sydney view?” |

**Supported cities:** Sydney, Melbourne, Brisbane, Perth.  
The agent checks whether a city’s data is loaded, loads it if needed (from disk or S3), then runs **read-only Sedona/Spark SQL** over building polygons with pre-computed H3 indices (e.g. `buildings_sydney`, `buildings_melbourne`).

---

## Tech stack

- **LLM:** OpenAI (GPT-4) via LangChain
- **Orchestration:** LangGraph (agent with tools and state)
- **Spatial engine:** Apache Sedona (Spark SQL + H3, ST_*)
- **Data:** [Overture Maps](https://docs.overturemaps.org/) building footprints; optional enrichment and S3 for persistence

---

## Quick start

**1. Clone and install**

```bash
git clone https://github.com/YOUR_USERNAME/sedona.git
cd sedona
pip install -r requirements.txt
```

**2. Set environment**

Create a `.env` (or export) with:

- `OPENAI_API_KEY` — your OpenAI API key (required for the agent)

Optional, for loading data from S3 and for Spark/S3 access:

- `S3_ENRICHED_BASE` — e.g. `s3a://your-bucket/enriched`
- `AWS_PROFILE` or AWS credentials in env

**3. Run a query**

```python
from src.agent import run_query

run_query("How many buildings are in the Sydney CBD?")
```

The agent will:

1. Check if Sydney’s data is loaded (and load it if needed)
2. Run a Sedona SQL query (e.g. `SELECT count(*) ... WHERE ST_Intersects(geometry, ST_PolygonFromEnvelope(...))`)
3. Return a short summary and the result

**4. Example questions to try**

- “How many buildings in Melbourne CBD?”
- “Which H3 cells have the most buildings in Sydney?”
- “Compare building count in Sydney CBD vs Melbourne CBD.”
- “What’s the heat proxy score for the top 15 areas in Sydney?”

---

## Project layout

| Path | Purpose |
|------|--------|
| `src/agent.py` | LangGraph agent, `run_query()`, state |
| `src/tools.py` | Tools: `check_city_status`, `load_city_data`, `execute_query`, `heat_proxy` |
| `src/data_manager.py` | City configs, load/enrich, S3/local paths |
| `src/sedona_client.py` | Sedona/Spark session and S3 credential config |
| `src/prompts/system_prompt.py` | System prompt (workflow, view names, CBD bboxes) |
| `tests/` | Pytest tests (unit + optional integration) |

---

## Data and views

- **Source:** Overture Maps building layer (GeoParquet), per-city bboxes.
- **Enrichment:** Centroid and H3 (r7, r8, r9) are pre-computed and stored (local or S3).
- **Spark views:** `buildings_sydney`, `buildings_melbourne`, `buildings_brisbane`, `buildings_perth` — the agent uses these in `FROM` and only runs **read-only** SQL (SELECT / WITH); DDL/DML is rejected.

---

## Tests and CI

```bash
# Unit and non-Spark tests only (fast, no JVM)
pytest tests/ -v -m "not integration"

# With integration tests (requires Spark + optional S3)
pytest tests/ -v
```

CI (GitHub Actions) runs tests with `-m "not integration"` and optional Docker build/push; see [.github/workflows/test_sedona.yml](.github/workflows/test_sedona.yml).

---

## License and attribution

- **Data:** [Overture Maps](https://overturemaps.org/) building data subject to Overture’s license and attribution requirements.
