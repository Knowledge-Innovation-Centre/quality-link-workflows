if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

import requests
import json
import psycopg2
from psycopg2.extras import Json, DictCursor, execute_values
import re
from datetime import datetime
import time
import uuid
import pandas as pd
from io import StringIO
from mage_ai.data_preparation.shared.secrets import get_secret_value

@data_loader
def load_data(*args, **kwargs):

    API_BASE_URL = kwargs.get('API_BASE_URL', 'https://backend.testzone.eqar.eu/connectapi/v1/providers/')
    LIMIT = kwargs.get('LIMIT', 100)  
    INITIAL_OFFSET = kwargs.get('OFFSET', 0)  
    MAX_RETRIES = kwargs.get('MAX_RETRIES', 3)  
    RETRY_DELAY = kwargs.get('RETRY_DELAY', 10)  
    REQUEST_DELAY = kwargs.get('REQUEST_DELAY', 1)  
    LOG_FREQUENCY = kwargs.get('LOG_FREQUENCY', 10) 
    BATCH_SIZE = kwargs.get('BATCH_SIZE', 100)  
    
    DB_HOST = get_secret_value("POSTGRES_HOST")
    DB_NAME = get_secret_value("POSTGRES_DB_NAME")
    DB_USER = get_secret_value("POSTGRES_USER")
    DB_PASSWORD = get_secret_value("POSTGRES_PASSWORD")
    
    total_providers_processed = 0
    total_count = 0
    new_providers_count = 0
    updated_providers_count = 0
    error_providers_count = 0
    
    failed_pages = []
    
    print(f"🚀 Starting data collection from {API_BASE_URL}")
    print(f"📊 Using pagination with LIMIT={LIMIT}, starting at OFFSET={INITIAL_OFFSET}")
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        print("✅ Connected to PostgreSQL database")
        cursor = conn.cursor(cursor_factory=DictCursor)
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return {
            "success": False,
            "error": f"Database connection error: {e}"
        }
    
    offset = INITIAL_OFFSET
    more_pages = True
    
    while more_pages:
        url = f"{API_BASE_URL}?limit={LIMIT}&offset={offset}"
        print(f"🔍 Fetching: {url}")
        
        retry_count = 0
        request_successful = False
        
        while retry_count <= MAX_RETRIES and not request_successful:
            try:
                if retry_count > 0:
                    print(f"🔄 Retry attempt {retry_count}/{MAX_RETRIES} for offset {offset}. Waiting {RETRY_DELAY} seconds...")
                    time.sleep(RETRY_DELAY)
                
                response = requests.get(url)
                
                if response.status_code == 200:
                    request_successful = True
                    data = response.json()
                    
                    results = data.get("results", [])
                    
                    if len(results) == 0:
                        print(f"📊 No results found for offset {offset}. This could be the end of data.")
                        more_pages = False
                        break
                    
                    total_count = data.get("count", 0)
                    
                    provider_ids = [provider.get('id') for provider in results]
                    
                    cursor.execute(
                        "SELECT provider_uuid, base_id FROM provider WHERE base_id = ANY(%s)",
                        (provider_ids,)
                    )
                    existing_providers = {row['base_id']: row['provider_uuid'] for row in cursor.fetchall()}
                    
                    new_providers = []
                    update_providers = []
                    
                    for provider in results:
                        provider_id = provider.get('id')
                        if provider_id in existing_providers:
                            update_providers.append((provider, existing_providers[provider_id]))
                        else:
                            new_providers.append(provider)
                    
                    if new_providers:
                        success, count = batch_insert_providers(conn, cursor, new_providers)
                        if success:
                            new_providers_count += count
                        else:
                            for provider in new_providers:
                                if insert_provider(conn, cursor, provider):
                                    new_providers_count += 1
                                else:
                                    error_providers_count += 1
                    
                    if update_providers:
                        success, count = batch_update_providers(conn, cursor, update_providers)
                        if success:
                            updated_providers_count += count
                        else:
                            for provider, provider_uuid in update_providers:
                                if update_provider(conn, cursor, provider, provider_uuid):
                                    updated_providers_count += 1
                                else:
                                    error_providers_count += 1
                    
                    total_providers_processed += len(results)
                    
                    print(f"✅ Processed batch of {len(results)} providers. Progress: {total_providers_processed}/{total_count}")
                    print(f"📊 New: {new_providers_count}, Updated: {updated_providers_count}, Errors: {error_providers_count}")
                    
                    offset += LIMIT
                    
                    more_pages = data.get("next", False)
                    
                    time.sleep(REQUEST_DELAY)
                else:
                    print(f"❌ HTTP Error: {response.status_code} - {response.text}")
                    retry_count += 1
                    
                    if retry_count > MAX_RETRIES:
                        print(f"❌ Max retries ({MAX_RETRIES}) reached for offset {offset}. Skipping to next page.")
                        failed_pages.append({
                            "limit": LIMIT,
                            "offset": offset,
                            "error": f"HTTP Error: {response.status_code}",
                            "timestamp": datetime.now().isoformat()
                        })
                        offset += LIMIT  
            except Exception as e:
                print(f"❌ Error fetching data: {e}")
                retry_count += 1
                
                if retry_count > MAX_RETRIES:
                    print(f"❌ Max retries ({MAX_RETRIES}) reached for offset {offset}. Skipping to next page.")
                    failed_pages.append({
                        "limit": LIMIT,
                        "offset": offset,
                        "error": str(e),
                        "timestamp": datetime.now().isoformat()
                    })
                    offset += LIMIT  
    
    cursor.close()
    conn.close()
    print("🔌 Database connection closed")
    
    run_id = str(uuid.uuid4())
    timestamp = datetime.now().isoformat()
    
    if failed_pages:
        print("\n⚠️ FAILED PAGES SUMMARY ⚠️")
        print("The following pages failed after max retries and should be processed manually:")
        
        df = pd.DataFrame(failed_pages)
        print(df.to_string())
        
        summary_table = StringIO()
        df.to_csv(summary_table, index=False)
        failed_summary = summary_table.getvalue()
    else:
        failed_summary = "No failed pages"
        print("\n✅ All pages processed successfully!")
    
    print(f"\n✅ Processing completed!")
    print(f"📊 Total providers processed: {total_providers_processed}/{total_count}")
    print(f"📊 New providers: {new_providers_count}")
    print(f"📊 Updated providers: {updated_providers_count}")
    print(f"📊 Errors: {error_providers_count}")
    
    return {
        "success": True,
        "run_id": run_id,
        "timestamp": timestamp,
        "total_count": total_count,
        "providers_processed": total_providers_processed,
        "new_providers": new_providers_count,
        "updated_providers": updated_providers_count,
        "error_providers": error_providers_count,
        "failed_pages": failed_pages,
        "failed_summary": failed_summary
    }

def extract_schac_identifier(provider):
    identifiers = provider.get('identifiers', [])
    for identifier in identifiers:
        if identifier.get('resource') == 'SCHAC':
            return identifier.get('identifier')
    return None

def clean_website_url(url):
    if not url:
        return None
    
    url = re.sub(r'^https?://', '', url)
    
    url = re.sub(r'^www\.', '', url)
    
    url = url.rstrip('/')
    
    return url

def build_manifest_json(provider):
    manifest_json = []
    
    schac_id = extract_schac_identifier(provider)
    
    website_link = provider.get('website_link')
    clean_website = clean_website_url(website_link)
    
    if schac_id:
        manifest_json.append({"domain": schac_id, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": schac_id, "type": ".well-known", "check": False, "path": None})
    
    if website_link:
        manifest_json.append({"domain": website_link, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": website_link, "type": ".well-known", "check": False, "path": None})
    
    if clean_website and clean_website != website_link:
        manifest_json.append({"domain": clean_website, "type": "DNS", "check": False, "path": None})
        manifest_json.append({"domain": clean_website, "type": ".well-known", "check": False, "path": None})
    
    return manifest_json

def build_name_concat(provider):
    name_parts = []
    
    name_primary = provider.get('name_primary')
    if name_primary:
        name_parts.append(name_primary)
    
    names = provider.get('names', [])
    if names and len(names) > 0:
        name_official = names[0].get('name_official')
        if name_official:
            name_parts.append(name_official)
            
        name_transliterated = names[0].get('name_official_transliterated')
        if name_transliterated:
            name_parts.append(name_transliterated)
            
        name_english = names[0].get('name_english')
        if name_english:
            name_parts.append(name_english)
            
        acronym = names[0].get('acronym')
        if acronym:
            name_parts.append(acronym)
    
    return " ".join(name_parts)

def batch_insert_providers(conn, cursor, providers):

    try:
        print(f"🆕 Batch inserting {len(providers)} new providers")
        
        insert_data = []
        current_time = datetime.now()
        
        for provider in providers:
            provider_id = provider.get('id')
            deqar_id = provider.get('deqar_id')
            eter_id = provider.get('eter_id')
            manifest_json = build_manifest_json(provider)
            name_concat = build_name_concat(provider)
            provider_name = provider.get('name_primary', '')
            
            insert_data.append((
                deqar_id, eter_id, provider_id, Json(provider), Json(manifest_json),
                name_concat, provider_name, current_time,
                None, current_time, current_time
            ))
        
        execute_values(
            cursor,
            """
            INSERT INTO provider (
                deqar_id, eter_id, base_id, metadata, manifest_json, 
                name_concat, provider_name, last_deqar_pull, 
                last_manifest_pull, created_at, updated_at
            ) VALUES %s
            """,
            insert_data,
            template="""(
                %s, %s, %s, %s, %s, 
                %s, %s, %s, 
                %s, %s, %s
            )"""
        )
        
        conn.commit()
        
        print(f"✅ Successfully batch inserted {len(providers)} providers")
        
        return True, len(providers)
    except Exception as e:
        print(f"❌ Error batch inserting providers: {e}")
        conn.rollback()
        return False, 0

def batch_update_providers(conn, cursor, provider_data):

    try:
        print(f"🔄 Batch updating {len(provider_data)} existing providers")
        
        cursor.execute("""
            CREATE TEMP TABLE provider_updates (
                provider_uuid UUID,
                deqar_id VARCHAR,
                eter_id VARCHAR,
                metadata JSONB,
                name_concat VARCHAR,
                provider_name VARCHAR,
                last_deqar_pull TIMESTAMP WITH TIME ZONE,
                updated_at TIMESTAMP WITH TIME ZONE
            ) ON COMMIT DROP
        """)
        
        update_data = []
        current_time = datetime.now()
        
        for provider, provider_uuid in provider_data:
            deqar_id = provider.get('deqar_id')
            eter_id = provider.get('eter_id')
            name_concat = build_name_concat(provider)
            provider_name = provider.get('name_primary', '')
            
            update_data.append((
                provider_uuid, deqar_id, eter_id, Json(provider),
                name_concat, provider_name, current_time, current_time
            ))
        
        execute_values(
            cursor,
            """
            INSERT INTO provider_updates (
                provider_uuid, deqar_id, eter_id, metadata,
                name_concat, provider_name, last_deqar_pull, updated_at
            ) VALUES %s
            """,
            update_data,
            template="""(
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )"""
        )
        
        cursor.execute("""
            UPDATE provider p
            SET 
                deqar_id = u.deqar_id,
                eter_id = u.eter_id,
                metadata = u.metadata,
                name_concat = u.name_concat,
                provider_name = u.provider_name,
                last_deqar_pull = u.last_deqar_pull,
                updated_at = u.updated_at
            FROM provider_updates u
            WHERE p.provider_uuid = u.provider_uuid
        """)
        
        conn.commit()
        
        print(f"✅ Successfully batch updated {len(provider_data)} providers")
        
        return True, len(provider_data)
    except Exception as e:
        print(f"❌ Error batch updating providers: {e}")
        conn.rollback()
        return False, 0

def insert_provider(conn, cursor, provider, log_details=False):
    try:
        provider_id = provider.get('id')
        deqar_id = provider.get('deqar_id')
        eter_id = provider.get('eter_id')
        
        manifest_json = build_manifest_json(provider)
        name_concat = build_name_concat(provider)
        provider_name = provider.get('name_primary', '')
        
        current_time = datetime.now()
        
        print(f"🆕 Individual insert for provider: {provider_id} - {provider_name}")
        
        cursor.execute("""
            INSERT INTO provider (
                deqar_id, eter_id, base_id, metadata, manifest_json, 
                name_concat, provider_name, last_deqar_pull, 
                last_manifest_pull, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING provider_uuid
        """, (
            deqar_id, eter_id, provider_id, Json(provider), Json(manifest_json),
            name_concat, provider_name, current_time,
            None, current_time, current_time
        ))
        
        provider_uuid = cursor.fetchone()[0]
        
        conn.commit()
        
        print(f"✅ Individual insert successful for provider {provider_id}")
        
        return True
    except Exception as e:
        print(f"❌ Error inserting provider {provider.get('id', 'unknown')}: {e}")
        conn.rollback()
        return False

def update_provider(conn, cursor, provider, provider_uuid, log_details=False):
    try:
        provider_id = provider.get('id')
        deqar_id = provider.get('deqar_id')
        eter_id = provider.get('eter_id')
        
        name_concat = build_name_concat(provider)
        provider_name = provider.get('name_primary', '')
        
        current_time = datetime.now()
        
        print(f"🔄 Individual update for provider: {provider_id} - {provider_name}")
        
        cursor.execute("""
            UPDATE provider 
            SET 
                deqar_id = %s,
                eter_id = %s,
                metadata = %s,
                name_concat = %s,
                provider_name = %s,
                last_deqar_pull = %s,
                updated_at = %s
            WHERE provider_uuid = %s
        """, (
            deqar_id,
            eter_id,
            Json(provider),
            name_concat,
            provider_name,
            current_time,
            current_time,
            provider_uuid
        ))
        
        conn.commit()
        
        print(f"✅ Individual update successful for provider {provider_id}")
        
        return True
    except Exception as e:
        print(f"❌ Error updating provider {provider.get('id', 'unknown')}: {e}")
        conn.rollback()
        return False

@test
def test_output(output, *args) -> None:

    assert output is not None, 'The output is undefined'
    assert output.get("success", False), 'Operation was not successful'
    assert "providers_processed" in output, 'Processed providers count is missing'
    assert "new_providers" in output, 'New providers count is missing'
    assert "updated_providers" in output, 'Updated providers count is missing'
    assert "failed_pages" in output, 'Failed pages summary is missing'
    
    total = output.get("providers_processed", 0)
    new = output.get("new_providers", 0)
    updated = output.get("updated_providers", 0)
    errors = output.get("error_providers", 0)
    
    assert total == (new + updated + errors), 'Provider counts do not add up'