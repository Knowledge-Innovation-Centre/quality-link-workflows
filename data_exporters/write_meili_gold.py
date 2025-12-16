from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
import json
from pyld import jsonld

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

@data_exporter
def export_data(data, *args, **kwargs):
    FUSEKI_URL = get_secret_value("FUSEKI_URL")
    FUSEKI_USERNAME = get_secret_value("FUSEKI_USERNAME")
    FUSEKI_PASSWORD = get_secret_value("FUSEKI_PASSWORD")
    DATASET_NAME = "pipeline-data"
    
    MEILISEARCH_URL = get_secret_value("MEILISEARCH_URL")
    MEILISEARCH_API_KEY = get_secret_value("MEILISEARCH_API_KEY")
    INDEX_NAME = "education-entities"
    
    auth = (FUSEKI_USERNAME, FUSEKI_PASSWORD) if FUSEKI_USERNAME and FUSEKI_PASSWORD else None
    query_url = f"{FUSEKI_URL}/{DATASET_NAME}/sparql"
    
    print("✅ Configuration loaded")
    print(f"   Fuseki: {FUSEKI_URL}/{DATASET_NAME}")
    print(f"   Meilisearch Index: {INDEX_NAME}")
    
    course_uuids = data.get('course_uuids', [])
    
    if not course_uuids:
        print("⚠️  No course_uuids found in input data")
        return {
            "retrieved": 0,
            "framed": 0,
            "uploaded": 0,
            "failed": 0,
            "course_uuids": []
        }
    
    print(f"\n📋 Processing {len(course_uuids)} course UUIDs")
    print(f"   First 3: {course_uuids[:3]}")
    
    try:
        with open("ql/schema/frame.json", "r") as f:
            frame_config = json.load(f)
        print("✅ Frame configuration loaded from frame.json")
    except FileNotFoundError as e:
        print(f"❌ File not found: {e}")
        raise 
    
    
    print(f"\n{'='*60}")
    print("📋 STEP 1: Querying for course URIs by course_uuid")
    print(f"{'='*60}")
    
    course_uri_mapping = []
    
    for idx, course_uuid in enumerate(course_uuids, 1):
        print(f"\n[{idx}/{len(course_uuids)}] Looking up URI for course_uuid: {course_uuid}")
        
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
            response = requests.get(
                query_url,
                params={'query': query_course_by_uuid, 'format': 'application/sparql-results+json'},
                auth=auth,
                timeout=30
            )
            response.raise_for_status()
            
            results = response.json()['results']['bindings']
            
            if not results:
                print(f"   ⚠️  No course found for course_uuid: {course_uuid}")
                continue
            
            result = results[0]
            course_uri = result['learningOpportunity']['value']
            title = result.get('title', {}).get('value', 'No title')
            
            print(f"   ✅ Found course URI: {course_uri}")
            print(f"      Title: {title[:60]}...")
            
            course_uri_mapping.append({
                'course_uuid': course_uuid,
                'course_uri': course_uri,
                'title': title
            })
            
        except requests.RequestException as e:
            print(f"   ❌ Query failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text[:200]}")
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            continue
    
    print(f"\n✅ Successfully found URIs for {len(course_uri_mapping)}/{len(course_uuids)} courses")
    
    if not course_uri_mapping:
        print("\n⚠️  No course URIs found")
        return {
            "retrieved": 0,
            "framed": 0,
            "uploaded": 0,
            "failed": len(course_uuids),
            "course_uuids": course_uuids
        }
    
    
    print(f"\n{'='*60}")
    print("📥 STEP 2: Retrieving full course data via CONSTRUCT")
    print(f"{'='*60}")
    
    all_documents = []
    retrieval_failed = 0
    
    for idx, mapping in enumerate(course_uri_mapping, 1):
        course_uuid = mapping['course_uuid']
        course_uri = mapping['course_uri']
        title = mapping['title']
        
        print(f"\n[{idx}/{len(course_uri_mapping)}] Processing: {title[:50]}...")
        print(f"   URI: {course_uri}")
        print(f"   UUID: {course_uuid}")
        
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
            print(f"   🔽 Fetching complete course data...")
            response = requests.get(
                query_url,
                params={'query': query_full_data, 'format': 'application/ld+json'},
                auth=auth,
                timeout=60
            )
            response.raise_for_status()
            
            raw_jsonld = response.json()
            
            if not raw_jsonld or (isinstance(raw_jsonld, dict) and not raw_jsonld.get('@graph')):
                print(f"   ⚠️  No data found for course URI: {course_uri}")
                retrieval_failed += 1
                continue
            
            graph_size = len(raw_jsonld.get('@graph', [])) if isinstance(raw_jsonld, dict) else len(raw_jsonld)
            print(f"   ✅ Retrieved raw JSON-LD data ({graph_size} objects)")
            
            all_documents.append({
                'course_uuid': course_uuid,
                'course_uri': course_uri,
                'title': title,
                'raw_data': raw_jsonld
            })
            
        except requests.RequestException as e:
            print(f"   ❌ Failed to retrieve data: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text[:200]}")
            retrieval_failed += 1
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            import traceback
            traceback.print_exc()
            retrieval_failed += 1
            continue
    
    print(f"\n✅ Successfully retrieved data for {len(all_documents)}/{len(course_uri_mapping)} courses")
    print(f"❌ Failed retrievals: {retrieval_failed}")
    
    if not all_documents:
        print("\n⚠️  No documents to process")
        return {
            "retrieved": 0,
            "framed": 0,
            "uploaded": 0,
            "failed": len(course_uuids),
            "course_uuids": course_uuids
        }
    

    print(f"\n{'='*60}")
    print("🔄 STEP 3: Applying JSON-LD framing")
    print(f"{'='*60}")
    
    framed_documents = []
    framing_failed = 0
    
    for idx, doc in enumerate(all_documents, 1):
        course_uuid = doc['course_uuid']
        course_uri = doc['course_uri']
        title = doc['title']
        raw_jsonld = doc['raw_data']
        
        print(f"\n[{idx}/{len(all_documents)}] Framing: {title[:50]}...")
        print(f"   Course UUID: {course_uuid}")
        
        try:
            print(f"   🔄 Applying JSON-LD framing...")
            framed_json = jsonld.frame(raw_jsonld, frame_config)

            print(f"   ✅ Framing successful")
            
            if '@context' in framed_json:
                del framed_json['@context']
                print(f"   ✅ Removed @context")
            
            framed_json['id'] = course_uuid
            print(f"   ✅ Set Meilisearch ID: {course_uuid}")
            
            has_title = 'dcterms:title' in framed_json
            has_type = 'type' in framed_json or '@type' in framed_json
            has_course_uuid = 'ql:course_uuid' in framed_json
            has_ingested = 'ql:ingestedDate' in framed_json
            
            print(f"   📋 Verification:")
            print(f"      - Has title: {has_title}")
            print(f"      - Has type: {has_type}")
            print(f"      - Has course_uuid: {has_course_uuid}")
            print(f"      - Has ingestedDate: {has_ingested}")
            
            framed_documents.append(framed_json)
            
        except Exception as e:
            print(f"   ❌ Framing failed: {e}")
            import traceback
            traceback.print_exc()
            framing_failed += 1
            continue
    
    print(f"\n✅ Successfully framed {len(framed_documents)}/{len(all_documents)} documents")
    print(f"❌ Framing failures: {framing_failed}")
    
    if not framed_documents:
        print("\n⚠️  No framed documents to upload")
        return {
            "retrieved": len(all_documents),
            "framed": 0,
            "uploaded": 0,
            "failed": len(all_documents),
            "course_uuids": course_uuids
        }
    
    
    print(f"\n{'='*60}")
    print("🚀 STEP 4: Uploading to Meilisearch")
    print(f"{'='*60}")
    
    upload_url = f"{MEILISEARCH_URL}/indexes/{INDEX_NAME}/documents"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MEILISEARCH_API_KEY}"
    }
    
    uploaded_count = 0
    upload_failed = 0
    
    for idx, doc in enumerate(framed_documents, 1):
        course_uuid = doc.get('id', 'unknown')
        title = doc.get('dcterms:title', 'No title')
        
        if isinstance(title, dict) and '@value' in title:
            title = title['@value']
        elif isinstance(title, list) and len(title) > 0:
            title = title[0] if isinstance(title[0], str) else title[0].get('@value', 'No title')
        
        print(f"\n[{idx}/{len(framed_documents)}] Uploading: {str(title)[:60]}...")
        print(f"   Course UUID: {course_uuid}")
        
        try:
            response = requests.post(upload_url, headers=headers, json=doc)
            response.raise_for_status()
            
            task_info = response.json()
            task_uid = task_info.get('taskUid', 'N/A')
            print(f"   ✅ Uploaded successfully (Task UID: {task_uid})")
            uploaded_count += 1
            
        except requests.RequestException as e:
            print(f"   ❌ Upload failed: {e}")
            if hasattr(e, 'response') and e.response is not None:
                print(f"      Response: {e.response.text[:200]}")
            upload_failed += 1
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error: {e}")
            upload_failed += 1
            continue
    
    print(f"\n{'='*60}")
    print(f"📊 FINAL SUMMARY")
    print(f"{'='*60}")
    print(f"📥 Total course UUIDs:        {len(course_uuids)}")
    print(f"🔍 URIs found:                {len(course_uri_mapping)}")
    print(f"✅ Successfully retrieved:    {len(all_documents)}")
    print(f"🔄 Successfully framed:       {len(framed_documents)}")
    print(f"🚀 Successfully uploaded:     {uploaded_count}")
    print(f"❌ Failed operations:         {(len(course_uuids) - len(course_uri_mapping)) + retrieval_failed + framing_failed + upload_failed}")
    print(f"   - URI lookup failures:     {len(course_uuids) - len(course_uri_mapping)}")
    print(f"   - Retrieval failures:      {retrieval_failed}")
    print(f"   - Framing failures:        {framing_failed}")
    print(f"   - Upload failures:         {upload_failed}")
    print(f"{'='*60}")
    
    return {
        "total_course_uuids": len(course_uuids),
        "uris_found": len(course_uri_mapping),
        "retrieved": len(all_documents),
        "framed": len(framed_documents),
        "uploaded": uploaded_count,
        "failed_uri_lookups": len(course_uuids) - len(course_uri_mapping),
        "failed_retrievals": retrieval_failed,
        "failed_framing": framing_failed,
        "failed_uploads": upload_failed,
        "total_failed": (len(course_uuids) - len(course_uri_mapping)) + retrieval_failed + framing_failed + upload_failed,
        "course_uuids": course_uuids,
        "success_rate": f"{(uploaded_count / len(course_uuids) * 100):.1f}%" if course_uuids else "0%"
    }
