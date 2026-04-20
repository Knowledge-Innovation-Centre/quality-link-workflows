# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Prefect ETL pipeline workflows for the **Quality Link Pipeline (QL-Pipeline)** — a data integration platform that aggregates European higher education and course data from university APIs and the DEQAR quality assurance database into a unified semantic search system.

## Project Structure

```
flows/              — Prefect flow definitions (pipeline orchestration)
tasks/              — Prefect task definitions (business logic)
  loaders/          — Data loading tasks (fetch from APIs, SPARQL endpoints)
  transformers/     — Processing tasks (RDF conversion, enrichment, indexing)
  exporters/        — Output tasks (write to PostgreSQL, Jena, MinIO, Meilisearch)
  conditionals/     — Conditional check tasks
source_types/       — Framework-agnostic data source handlers (ELM, OOAPI, Edu-API)
schema/             — JSON-LD framing templates
prefect.yaml        — Deployment definitions with schedules and parameters
```

## Running Pipelines

### Local development

```bash
pip install -r requirements.txt

# Run a flow directly
python flows/provider_fetch.py

# Or deploy to Prefect server
prefect deploy --all
```

### Managing dependencies

Direct deps live in `requirements.in` (hand-edited). The pinned lockfile `requirements.txt` is generated via [`pip-tools`](https://pip-tools.readthedocs.io/) and is what Docker installs.

```bash
pip install pip-tools            # one-time
# add/remove entries in requirements.in, then:
pip-compile requirements.in      # regenerates requirements.txt
pip-compile --upgrade requirements.in   # bump all pins
```

Commit both files. Never edit `requirements.txt` by hand.

### Docker (self-hosted Prefect)

Two-container stack: `prefect-server` (UI + API on port 4200, SQLite-backed) and `prefect-worker` (runs all deployments from a `default-pool` process work pool).

```bash
cp .env.example .env   # fill in credentials for Postgres, MinIO, Fuseki, Meilisearch
docker compose up -d --build
```

On worker startup the entrypoint script (`docker/entrypoint-worker.sh`) waits for the server, creates the work pool if missing, runs `prefect deploy --all`, then starts the worker. Flow history persists in the `prefect-data` named volume.

The Prefect UI is at `http://localhost:4200`. Trigger `process-course-message` via `POST /api/deployments/{id}/create_flow_run` from your producer.

## Data Architecture (Bronze → Silver → Gold)

| Tier | Store | Description |
|------|-------|-------------|
| Bronze | MinIO | Raw versioned course files (RDF, TTL, JSON-LD) from provider URLs |
| Silver | Apache Jena Fuseki | RDF-enriched data with UUIDs and ingestion metadata; queryable via SPARQL |
| Gold | Meilisearch | Framed JSON-LD documents indexed for full-text search |

## Key Flows

**`provider_fetch_database_batch`** (`flows/provider_fetch.py`) — Fetches provider metadata from the DEQAR API → saves to PostgreSQL → converts to RDF → pushes to Jena Fuseki. Parameters: `api_base_url`, `limit`. Scheduled daily at 02:00.

**`process_course_message`** (`flows/process_course.py`) — Processes a single course source through bronze → silver → gold pipeline. Triggered per message via the Prefect API (replaces the old Redis/Dragonfly streaming pipeline). Parameters: `provider_uuid`, `source_uuid`, `source_version_uuid`.

**`vocab_fetch_jena_batch`** (`flows/vocab_fetch.py`) — Fetches EU controlled vocabularies from SPARQL endpoint → converts to RDF Turtle → pushes to Jena Fuseki. Parameter: `concept_scheme`. Multiple deployments for ISCED-F, languages, EQF, learning opportunity types.

**`push_private_keys`** (`flows/push_private_keys.py`) — Checks if an active RSA key pair exists; generates one if not.

**`reindex_courses`** (`flows/reindex_courses.py`) — Manually re-index courses in Meilisearch.

## Environment Variables

All service credentials are passed as environment variables:

```
POSTGRES_HOST, POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD
MINIO_HOST, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET_NAME
FUSEKI_URL, FUSEKI_USERNAME, FUSEKI_PASSWORD, FUSEKI_DATASET_NAME
MEILISEARCH_URL, MEILISEARCH_API_KEY, MEILISEARCH_INDEX
```

## Semantic Web Conventions

- Provider RDF uses the custom `ql:` namespace alongside standard ontologies: FOAF, SKOS, ELM, DCTERMS, ROV, ADMS
- Course entities are identified with UUID v5 under a project namespace
- `transform_index_gold.py` uses `pyld` for JSON-LD framing; the frame template is at `schema/frame.json`
- Jena Fuseki receives data via HTTP PUT/POST to the `/data` endpoint with `Content-Type: text/turtle`

## Code Conventions

- Each task file contains a single `@task`-decorated function plus any helper functions
- Tasks use emoji-prefixed `print()` statements for logging (e.g., `✅`, `❌`, `🔄`)
- Error handling follows a "skip and continue" pattern — failures are counted and reported as summary stats rather than raising exceptions that would halt the pipeline
- DEQAR API calls use 1-second delays between requests and up to 3 retries with 10-second delays
