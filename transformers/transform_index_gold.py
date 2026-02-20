import os
from typing import Dict, List
import requests
import json
from pyld import jsonld

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


def get_language_label(language_uri: str, query_url: str, auth: tuple) -> str:

    query = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>

    SELECT ?label
    WHERE {{
      <{language_uri}> skos:prefLabel ?label .
      FILTER(lang(?label) = "en")
    }}
    LIMIT 1
    """

    try:
        response = requests.get(
            query_url,
            params={'query': query, 'format': 'application/sparql-results+json'},
            auth=auth,
            timeout=10
        )
        response.raise_for_status()
        results = response.json()['results']['bindings']
        return results[0]['label']['value'] if results else None
    except Exception as e:
        print(f"      ⚠️ Failed to fetch label for {language_uri}: {e}")
        return None


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

    for idx, course_uuid in enumerate(course_uuids, 1):
        print(f"\n[{idx}/{len(course_uuids)}] Processing course_uuid: {course_uuid}")

        # Step 1: SPARQL SELECT — find course URI
        query_course_by_uuid = f"""
        PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
        PREFIX ql: <http://data.quality-link.eu/ontology/v1#>
        PREFIX dcterms: <http://purl.org/dc/terms/>

        SELECT ?learningOpportunity ?title ?course_uuid
        WHERE {{
          ?learningOpportunity rdf:type ql:LearningOpportunitySpecification .
          ?learningOpportunity ql:course_uuid ?course_uuid .
          OPTIONAL {{ ?learningOpportunity dcterms:title ?title }}

          FILTER (?course_uuid = "{course_uuid}")
        }}
        LIMIT 1
        """

        try:
            r = requests.get(
                query_url,
                params={'query': query_course_by_uuid, 'format': 'application/sparql-results+json'},
                auth=auth,
                timeout=30
            )
            r.raise_for_status()
            results = r.json()['results']['bindings']
            if not results:
                print(f"   ⚠️ No URI found for course_uuid: {course_uuid}")
                failed_count += 1
                continue
            course_uri = results[0]['learningOpportunity']['value']
            print(f"   ✅ URI: {course_uri}")
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
            r = requests.get(
                query_url,
                params={'query': query_full_data, 'format': 'application/ld+json'},
                auth=auth,
                timeout=60
            )
            r.raise_for_status()
            raw_jsonld = r.json()
            if not raw_jsonld or (isinstance(raw_jsonld, dict) and not raw_jsonld.get('@graph')):
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
            print(f"   ✅ Framed document")
        except Exception as e:
            print(f"   ❌ Framing failed: {e}")
            failed_count += 1
            continue

        # Step 4: Language label enrichment
        language_field = framed.get('dcterms:language')
        if language_field:
            if isinstance(language_field, list):
                labels = [get_language_label(uri, query_url, auth) for uri in language_field]
                labels = [l for l in labels if l]
                if labels:
                    framed['dcterms:languageLabel'] = labels
            else:
                label = get_language_label(language_field, query_url, auth)
                if label:
                    framed['dcterms:languageLabel'] = label

        # Step 5: Upload to Meilisearch
        try:
            r = requests.post(meili_url, headers=meili_headers, json=framed)
            r.raise_for_status()
            task_uid = r.json().get('taskUid', 'N/A')
            print(f"   ✅ Uploaded to Meilisearch (Task UID: {task_uid})")
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
