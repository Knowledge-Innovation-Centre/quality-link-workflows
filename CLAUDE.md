# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

MageAI ETL pipeline workflows for the **Quality Link Pipeline (QL-Pipeline)** — a data integration platform that aggregates European higher education and course data from university APIs and the DEQAR quality assurance database into a unified semantic search system.

## MageAI Structure

MageAI pipelines are composed of reusable blocks stored in top-level directories:

- `data_loaders/` — Input blocks (fetch from APIs, databases, queues)
- `transformers/` — Processing blocks (data transformation, RDF conversion)
- `data_exporters/` — Output blocks (write to PostgreSQL, Jena, MinIO, Meilisearch)
- `conditionals/` — Branch logic blocks
- `pipelines/` — Pipeline definitions (each has a `metadata.yaml` declaring block relationships and variables)

Each pipeline's `metadata.yaml` defines which blocks it uses and in what order. Blocks are Python files with a decorated function signature expected by MageAI.

## Running Pipelines

There is no Makefile or test runner. Pipelines are managed through the MageAI UI or CLI. Dependencies are installed via:

```bash
pip install -r requirements.txt
```

## Data Architecture (Bronze → Silver → Gold)

| Tier | Store | Description |
|------|-------|-------------|
| Bronze | MinIO | Raw versioned course files (RDF, TTL, JSON-LD) from provider URLs |
| Silver | Apache Jena Fuseki | RDF-enriched data with UUIDs and ingestion metadata; queryable via SPARQL |
| Gold | Meilisearch | Framed JSON-LD documents indexed for full-text search |

## Key Pipelines

**`provider_fetch_database_batch`** — Fetches provider metadata from the DEQAR API → converts to RDF Turtle → saves to PostgreSQL + pushes to Jena Fuseki. Pipeline variables: `DEQAR_URL`, `LIMIT`.

**`course_fetch_datalake_stream`** — Streaming pipeline consuming Redis/Dragonfly queue (`provider_data_queue`) → stores versioned course files in MinIO.

**`course_fetch_datalake_jena_batch`** — Reads PostgreSQL transaction records → retrieves latest files from MinIO → enriches RDF with course UUIDs (UUID v5) → uploads to Jena Fuseki (Silver) → queries Jena via SPARQL CONSTRUCT → applies JSON-LD framing → uploads to Meilisearch (Gold).

**`push_private_keys`** — Generates RSA key pairs for encrypted manifest headers; conditional block skips generation if an active key already exists.

## Environment Variables

All service credentials are passed as environment variables (not MageAI secrets):

```
POSTGRES_HOST, POSTGRES_DB_NAME, POSTGRES_USER, POSTGRES_PASSWORD
MINIO_HOST, MINIO_ROOT_USER, MINIO_ROOT_PASSWORD, MINIO_BUCKET_NAME
FUSEKI_URL, FUSEKI_USERNAME, FUSEKI_PASSWORD, FUSEKI_DATASET_NAME
DRAGONFLY_HOST, DRAGONFLY_PASSWORD
MEILISEARCH_URL, MEILISEARCH_API_KEY, MEILISEARCH_INDEX
```

## Semantic Web Conventions

- Provider RDF uses the custom `ql:` namespace alongside standard ontologies: FOAF, SKOS, ELM, DCTERMS, ROV, ADMS
- Course entities are identified with UUID v5 under a project namespace
- `write_meili_gold.py` uses `pyld` for JSON-LD framing; the frame template is at `data_exporters/frame.json`
- Jena Fuseki receives data via HTTP PUT to the `/data` endpoint with `Content-Type: text/turtle`

## Block Conventions

- Each block file contains a single decorated function (e.g., `@data_loader`, `@transformer`, `@data_exporter`)
- Blocks use emoji-prefixed `print()` statements for logging (e.g., `✅`, `❌`, `🔄`)
- Error handling follows a "skip and continue" pattern — failures are counted and reported as summary stats rather than raising exceptions that would halt the pipeline
- DEQAR API calls use 1-second delays between requests and up to 3 retries with 10-second delays
