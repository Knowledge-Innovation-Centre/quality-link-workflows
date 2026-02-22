import os
from typing import Dict, List
import requests
import json
from pyld import jsonld
from rdflib import Namespace
from rdflib.namespace import RDF, DCTERMS, OWL

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer

QL = Namespace("http://data.quality-link.eu/ontology/v1#")
ELM = Namespace("http://data.europa.eu/snb/model/elm/")

@transformer
def transform(messages: List[Dict], *args, **kwargs):

    message = messages[0] if messages else None
    if message is None:
        print("⚠️ No message received")
        return None

    provider_uuid = message.get("provider_uuid")
    source_version_uuid = message.get("source_version_uuid")
    source_type = message.get("source_type", "unknown")
    course_uuids = message.get("course_uuids", [])

    print(f"🔄 Indexing {len(course_uuids)} courses for provider: {provider_uuid}")

    if not course_uuids:
        print("⚠️ No course_uuids to index")
        return {
            "provider_uuid": provider_uuid,
            "source_version_uuid": source_version_uuid,
            "source_type": source_type,
        }

    FUSEKI_URL = os.environ.get("FUSEKI_URL")
    FUSEKI_USERNAME = os.environ.get("FUSEKI_USERNAME")
    FUSEKI_PASSWORD = os.environ.get("FUSEKI_PASSWORD")
    DATASET_NAME = os.environ.get("FUSEKI_DATASET_NAME")
    MEILISEARCH_URL = os.environ.get("MEILISEARCH_URL")
    MEILISEARCH_API_KEY = os.environ.get("MEILISEARCH_API_KEY")
    INDEX_NAME = os.environ.get("MEILISEARCH_INDEX")

    auth = (FUSEKI_USERNAME, FUSEKI_PASSWORD) if FUSEKI_USERNAME and FUSEKI_PASSWORD else None
    query_url = f"{FUSEKI_URL}/{DATASET_NAME}/sparql"

    try:
        with open("ql/schema/frame.json", "r") as f:
            frame_config = json.load(f)
        print("✅ Loaded frame.json")
    except FileNotFoundError as e:
        print(f"❌ frame.json not found: {e}")
        raise

    meili_url = f"{MEILISEARCH_URL}/indexes/{INDEX_NAME}/documents"
    meili_headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MEILISEARCH_API_KEY}"
    }

    uploaded_count = 0
    failed_count = 0

    with requests.Session() as session:
        if auth:
            session.auth = auth

        for idx, course_uuid in enumerate(course_uuids, 1):
            # Step 1: SPARQL SELECT — find course URI
            query_course_by_uuid = f"""
            PREFIX rdf: <{RDF}>
            PREFIX ql: <{QL}>
            PREFIX elm: <{ELM}>
            PREFIX owl: <{OWL}>

            SELECT ?learningOpportunity
            WHERE {{
              VALUES ?type {{
                ql:LearningOpportunitySpecification
                elm:Qualification
                elm:LearningAchievementSpecification
              }}
              <urn:uuid:{course_uuid}> owl:sameAs ?learningOpportunity .
              ?learningOpportunity rdf:type ?type .
            }}
            """

            try:
                r = session.get(
                    query_url,
                    params={'query': query_course_by_uuid, 'format': 'application/sparql-results+json'},
                    timeout=30
                )
                r.raise_for_status()
                results = r.json()['results']['bindings']
                r.close()
                if not results:
                    print(f"   ⚠️ No URI found for course_uuid: {course_uuid}")
                    failed_count += 1
                    continue
                course_uri = results[0]['learningOpportunity']['value']
            except Exception as e:
                print(f"   ❌ URI lookup failed: {e}")
                failed_count += 1
                continue

            # Step 2: SPARQL CONSTRUCT — full subgraph
            query_full_data = f"""
            PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

            CONSTRUCT {{
              ?s ?p ?o .
            }}
            WHERE {{
              <{course_uri}> (<>|!<>)* ?s .
              ?s ?p ?o .
            }}
            """

            try:
                r = session.get(
                    query_url,
                    params={'query': query_full_data, 'format': 'application/ld+json'},
                    timeout=60
                )
                r.raise_for_status()
                raw_jsonld = r.json()
                r.close()
                if not raw_jsonld:
                    print(f"   ⚠️ No data returned for {course_uri}")
                    failed_count += 1
                    continue
            except Exception as e:
                print(f"   ❌ CONSTRUCT query failed: {e}")
                failed_count += 1
                continue

            # Step 3: JSON-LD framing
            try:
                framed = jsonld.frame(raw_jsonld, frame_config)
                if '@context' in framed:
                    del framed['@context']
                framed['id'] = course_uuid
            except Exception as e:
                print(f"   ❌ Framing failed: {e}")
                failed_count += 1
                continue

            # Step 5: Upload to Meilisearch
            try:
                r = requests.post(meili_url, headers=meili_headers, json=framed)
                r.raise_for_status()
                task_uid = r.json().get('taskUid', 'N/A')
                r.close()
                uploaded_count += 1
            except Exception as e:
                print(f"   ❌ Meilisearch upload failed: {e}")
                failed_count += 1
                continue

    print(f"\n📊 Indexing complete: {uploaded_count} uploaded, {failed_count} failed")

    return {
        "provider_uuid": provider_uuid,
        "source_version_uuid": source_version_uuid,
        "source_type": source_type,
    }
