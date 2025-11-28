
# QL-Pipeline MageAI Workflows



This repository contains the **MageAI ETL pipeline configurations** for the **Quality Link Pipeline (QL-Pipeline)**, a comprehensive data integration platform that processes educational data from European universities and quality assurance organizations.



---



## Overview



The **QL-Pipeline** serves as a bridge between thousands of university APIs and quality assurance databases like **DEQAR**, creating a unified search and discovery system for:



- Higher education institutions  

- Learning opportunities  

- Course and programme data across Europe  



---



## Architecture



The workflows orchestrate data processing across multiple services:



- **PostgreSQL** — Relational data storage  

- **Apache Jena Fuseki** — RDF / semantic web data storage  

- **MinIO** — S3-compatible object storage (data lake)  

- **Redis / Dragonfly** — Caching and message queues  

- **DEQAR API** — Quality assurance data source  



---



## Pipelines



### 1. `provider_fetch_database_batch`

Extracts provider data from the **DEQAR API** using robust pagination, retry logic, and batch processing.  

Handles **new provider insertion** and **existing provider updates** in PostgreSQL.



---



### 2. `course_fetch_datalake_stream`

Streaming pipeline that:



- Consumes course data messages from Redis queues  

- Downloads course files from provider URLs  

- Stores and versions files in **MinIO**  

- Maintains manifest tracking for source metadata  



---



### 3. `course_fetch_jena_batch`

Batch pipeline that:



- Processes transaction records  

- Extracts the latest course files from MinIO  

- Uploads **RDF/TTL** formatted data to **Apache Jena Fuseki**  

- Enables semantic querying through SPARQL endpoints  



---



## Data Flow

```text

Provider Discovery → Fetch provider metadata from DEQAR API  

Manifest Processing → Discover course data sources via DNS / well-known URLs  

Data Ingestion → Stream course data files to MinIO data lake  

Semantic Processing → Transform and load RDF data into Jena Fuseki  

Search Integration → Unified search across European educational data  

```

---



## Key Features



- **Fault Tolerance**: Comprehensive error handling and retry mechanisms  

- **Scalability**: Designed to handle data from thousands of institutions  

- **Data Integrity**: UUID-based tracking and transaction logging  

- **Format Support**: RDF, Turtle, JSON course data formats  

- **Real-time Processing**: Streaming and batch processing capabilities  

