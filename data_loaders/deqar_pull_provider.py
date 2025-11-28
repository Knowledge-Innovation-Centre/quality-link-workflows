if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

from minio import Minio
from minio.error import S3Error
from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests
import json
from datetime import datetime
import io
import time
import psycopg2
from psycopg2.extras import Json, DictCursor

MINIO_HOST = get_secret_value("MINIO_HOST")
MINIO_ROOT_USER = get_secret_value("MINIO_ROOT_USER")
MINIO_ROOT_PASSWORD = get_secret_value("MINIO_ROOT_PASSWORD")
POSTGRES_USER = get_secret_value("POSTGRES_USER")
POSTGRES_PASSWORD = get_secret_value("POSTGRES_PASSWORD")
POSTGRES_HOST = get_secret_value("POSTGRES_HOST")
POSTGRES_DB_NAME = get_secret_value("POSTGRES_DB_NAME")

REQUEST_SLEEP_TIME = 1  
LOG_FREQUENCY = 100     
MAX_RETRIES = 3         
RETRY_WAIT_TIME = 10    

@data_loader
def load_data(*args, **kwargs):
 
    minio_client = connect_to_minio()
    if not minio_client:
        return {"success": False, "error": "Failed to connect to MinIO"}

    pg_conn = connect_to_postgres()
    if not pg_conn:
        return {"success": False, "error": "Failed to connect to PostgreSQL"}
    
    bucket_name = kwargs.get("QL_BUCKET_NAME", "quality-link-storage")
    deqar_base_url = kwargs.get("DEQAR_BASE_URL", "https://backend.deqar.eu/connectapi/v1/providers/")
    
    today = datetime.now()
    date_folder = today.strftime("%Y-%m-%d")
    folder_path = f"datalake/providers/{date_folder}"
    file_name = "providers_id.json"
    object_name = f"{folder_path}/{file_name}"
    
    try:
        print(f"📂 Reading provider IDs from {object_name}...")
        provider_ids = read_provider_ids_from_minio(minio_client, bucket_name, object_name)
        
        if not provider_ids:
            print("❌ No provider IDs found in MinIO")
            return {"success": False, "error": "No provider IDs found"}
        
        print(f"✅ Successfully loaded {len(provider_ids)} provider IDs")
        
        # Process each provider ID
        processed_count = 0  # New records inserted
        updated_count = 0    # Existing records updated
        skipped_count = 0    # Records completely skipped
        error_count = 0      # Records that failed processing
        
        for index, item in enumerate(provider_ids):
            provider_id = item.get("provider_id")
            if not provider_id:
                continue
            
            # Only log status every LOG_FREQUENCY records or for the first/last record
            if index % LOG_FREQUENCY == 0 or index == 0 or index == len(provider_ids) - 1:
                print(f"🔄 Processing provider ID: {provider_id} ({index + 1}/{len(provider_ids)})")
            
            # Check if provider already exists in database
            existing_uuid = check_provider_in_db(pg_conn, provider_id)
            
            if existing_uuid:
                # Provider exists, update it
                if index % LOG_FREQUENCY == 0 or index == 0 or index == len(provider_ids) - 1:
                    print(f"🔄 Provider ID {provider_id} exists, updating...")
                
                # Fetch provider details from DEQAR API with retry logic
                provider_data = fetch_provider_data_with_retry(deqar_base_url, provider_id)
                
                if provider_data:
                    # Update provider in database
                    if update_provider_in_db(pg_conn, existing_uuid, provider_id, provider_data):
                        updated_count += 1
                    else:
                        error_count += 1
                else:
                    error_count += 1
            else:
                # Provider doesn't exist, insert it
                if index % LOG_FREQUENCY == 0 or index == 0 or index == len(provider_ids) - 1:
                    print(f"🆕 Provider ID {provider_id} is new, inserting...")
                
                # Fetch provider details from DEQAR API with retry logic
                provider_data = fetch_provider_data_with_retry(deqar_base_url, provider_id)
                
                if provider_data:
                    # Insert provider into database
                    if insert_provider_to_db(pg_conn, provider_id, provider_data):
                        processed_count += 1
                    else:
                        error_count += 1
                else:
                    error_count += 1
            
            # Sleep between requests to prevent overloading the API
            time.sleep(REQUEST_SLEEP_TIME)
            
            # Print progress every LOG_FREQUENCY records
            if (index + 1) % LOG_FREQUENCY == 0 or index == len(provider_ids) - 1:
                print(f"📊 Progress: {index + 1}/{len(provider_ids)} | New: {processed_count} | Updated: {updated_count} | Errors: {error_count} | Completion: {((index + 1) / len(provider_ids)) * 100:.1f}%")
        
        print(f"\n✅ Processing complete!")
        print(f"   - New providers inserted: {processed_count}")
        print(f"   - Existing providers updated: {updated_count}")
        print(f"   - Providers skipped: {skipped_count}")
        print(f"   - Errors: {error_count}")
        
        return {
            "success": True,
            "new_count": processed_count,
            "updated_count": updated_count,
            "skipped_count": skipped_count,
            "error_count": error_count,
            "total_providers": len(provider_ids)
        }
        
    except Exception as e:
        print(f"❌ Error in main process: {str(e)}")
        return {"success": False, "error": str(e)}
    
    finally:
        # Close PostgreSQL connection
        if pg_conn:
            pg_conn.close()
            print("🔌 PostgreSQL connection closed")


def connect_to_minio():
    """Connect to MinIO server."""
    try:
        client = Minio(
            MINIO_HOST,
            access_key=MINIO_ROOT_USER,
            secret_key=MINIO_ROOT_PASSWORD,
            secure=False
        )
        print("✅ Connected to MinIO")
        return client
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {str(e)}")
        return None


def connect_to_postgres():
    """Connect to PostgreSQL database."""
    try:
        connection = psycopg2.connect(
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            host=POSTGRES_HOST,
            port=5432,
            database=POSTGRES_DB_NAME
        )
        print("✅ Connected to PostgreSQL")
        return connection
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {str(e)}")
        return None


def read_provider_ids_from_minio(client, bucket_name, object_name):
    """Read provider IDs from MinIO JSON file."""
    try:
        # Get the object from MinIO
        response = client.get_object(bucket_name, object_name)
        
        # Read and parse JSON
        provider_ids = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        
        return provider_ids
    except Exception as e:
        print(f"❌ Error reading provider IDs from MinIO: {str(e)}")
        return []


def check_provider_in_db(conn, provider_id):
    """
    Check if provider exists in database and return UUID if it does.
    Returns None if provider doesn't exist.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT provider_uuid FROM provider WHERE base_id = %s", (provider_id,))
        result = cursor.fetchone()
        cursor.close()
        
        if result:
            return result[0]  # Return the UUID
        return None
    except Exception as e:
        print(f"❌ Error checking provider in DB: {str(e)}")
        return None


def fetch_provider_data_with_retry(base_url, provider_id):
    """Fetch provider data from DEQAR API with retry logic."""
    url = f"{base_url}{provider_id}"
    
    # Initialize retry counter
    retry_count = 0
    
    while retry_count <= MAX_RETRIES:
        try:
            if retry_count > 0:
                print(f"🔄 Retry attempt {retry_count}/{MAX_RETRIES} for provider {provider_id}. Waiting {RETRY_WAIT_TIME} seconds...")
                time.sleep(RETRY_WAIT_TIME)
            
            response = requests.get(url)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"❌ HTTP Error for provider {provider_id}: {response.status_code} - {response.text}")
                retry_count += 1
                
        except Exception as e:
            print(f"❌ Error fetching data for provider {provider_id}: {str(e)}")
            retry_count += 1
    
    print(f"❌ Max retries ({MAX_RETRIES}) reached for provider {provider_id}. Moving to next provider.")
    return None


def extract_name_concat(provider_data):
    """Extract and concatenate name fields."""
    try:
        names = provider_data.get('names', [])
        if names:
            name_official = names[0].get('name_official', '')
            name_transliterated = names[0].get('name_official_transliterated', '')
            name_english = names[0].get('name_english', '')
            
            # Combine names, filtering out empty strings
            name_parts = [part for part in [name_official, name_transliterated, name_english] if part]
            return " ".join(name_parts)
        return ""
    except Exception as e:
        print(f"❌ Error extracting name concat: {str(e)}")
        return ""


def extract_provider_name(provider_data):
    """Extract provider name from response."""
    try:
        names = provider_data.get('names', [])
        if names:
            return names[0].get('name_official', '')
        return ""
    except Exception as e:
        print(f"❌ Error extracting provider name: {str(e)}")
        return ""


def insert_provider_to_db(conn, provider_id, provider_data):
    """Insert provider data into database."""
    try:
        # Create predefined manifest JSON
        manifest_json = [
            {"type": ".well-known", "domain": None},
            {"type": "DNS", "domain": None}
        ]
        
        # Extract fields from provider data
        deqar_id = f"DEQARINST{provider_id}"
        eter_id = provider_data.get('eter_id')
        name_concat = extract_name_concat(provider_data)
        provider_name = extract_provider_name(provider_data)
        current_time = datetime.now()
        
        # Create cursor
        cursor = conn.cursor()
        
        # Insert into providers table
        cursor.execute("""
            INSERT INTO provider (
                deqar_id, eter_id, base_id, metadata, manifest_json, 
                name_concat, provider_name, last_deqar_pull, 
                last_manifest_pull, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            deqar_id, eter_id, provider_id, Json(provider_data), Json(manifest_json),
            name_concat, provider_name, current_time,
            None, current_time, current_time
        ))
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error inserting provider {provider_id} to database: {str(e)}")
        # Rollback the transaction
        conn.rollback()
        return False


def update_provider_in_db(conn, provider_uuid, provider_id, provider_data):
    """Update existing provider record in database."""
    try:
        # Extract fields from provider data
        name_concat = extract_name_concat(provider_data)
        provider_name = extract_provider_name(provider_data)
        current_time = datetime.now()
        
        # Create cursor
        cursor = conn.cursor()
        
        # Update provider record
        cursor.execute("""
            UPDATE provider 
            SET 
                metadata = %s, 
                name_concat = %s, 
                provider_name = %s, 
                last_deqar_pull = %s, 
                updated_at = %s
            WHERE provider_uuid = %s
        """, (
            Json(provider_data),
            name_concat,
            provider_name,
            current_time,
            current_time,
            provider_uuid
        ))
        
        # Commit the transaction
        conn.commit()
        cursor.close()
        
        return True
        
    except Exception as e:
        print(f"❌ Error updating provider {provider_id} in database: {str(e)}")
        # Rollback the transaction
        conn.rollback()
        return False


@test
def test_output(output, *args) -> None:
    """
    Test the output of the block.
    """
    assert output is not None, 'The output is undefined'
    assert "success" in output, 'Success status missing in output'
    if output.get("success", False):
        total_processed = output.get("new_count", 0) + output.get("updated_count", 0)
        assert total_processed + output.get("error_count", 0) + output.get("skipped_count", 0) == output.get("total_providers", 0), 'Provider counts do not add up'