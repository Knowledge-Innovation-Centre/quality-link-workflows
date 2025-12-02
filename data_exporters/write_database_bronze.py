from mage_ai.data_preparation.shared.secrets import get_secret_value
from minio import Minio
from minio.error import S3Error
import requests
import psycopg2
from datetime import datetime
from typing import List
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    
    total_files = sum(len(transaction.get('files', [])) for transaction in data)
    print(f"📋 Processing {len(data)} transactions with {total_files} total files")
    
    success_count = 0
    failed_count = 0
    db_update_failed_count = 0
    unknown_file_type_count = 0
    
    try:
        minio_client = Minio(
            get_secret_value("MINIO_HOST"),
            access_key=get_secret_value("MINIO_ROOT_USER"),
            secret_key=get_secret_value("MINIO_ROOT_PASSWORD"),
            secure=False
        )
        print("✅ Connected to MinIO")
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")
        return {
            "success": 0,
            "failed": total_files,
            "db_update_failed": 0,
            "unknown_file_type": 0,
            "total": total_files
        }
    
    pg_conn = None
    pg_cursor = None
    try:
        pg_conn = psycopg2.connect(
            host=get_secret_value("POSTGRES_HOST"),
            database=get_secret_value("POSTGRES_DB_NAME"),
            user=get_secret_value("POSTGRES_USER"),
            password=get_secret_value("POSTGRES_PASSWORD")
        )
        pg_cursor = pg_conn.cursor()
        print("✅ Connected to PostgreSQL")
    except Exception as e:
        print(f"❌ Error connecting to PostgreSQL: {e}")
        print("⚠️ Continuing without database updates")
    
    fuseki_url = get_secret_value("FUSEKI_URL")
    fuseki_username = get_secret_value("FUSEKI_USERNAME")
    fuseki_password = get_secret_value("FUSEKI_PASSWORD")
    
    bucket_name = "quality-link-storage"
    dataset_name = "pipeline-data"
    upload_url = f"{fuseki_url}/{dataset_name}/data"
    
    auth = None
    if fuseki_username and fuseki_password:
        auth = (fuseki_username, fuseki_password)
    
    print(f"🎯 Uploading to Fuseki dataset: {dataset_name}")
    print(f"{'='*60}")
    
    processed_files = 0
    
    for transaction_idx, transaction in enumerate(data):
        trans_uuid = transaction.get('trans_uuid')
        provider_uuid = transaction.get('provider_uuid')
        source_version_uuid = transaction.get('source_version_uuid')
        files = transaction.get('files', [])
        
        print(f"\n📦 Transaction {transaction_idx + 1}/{len(data)}: {trans_uuid}")
        print(f"   Provider: {provider_uuid}")
        print(f"   Version: {source_version_uuid}")
        print(f"   Files to process: {len(files)}")
        
        for file_idx, file_info in enumerate(files):
            processed_files += 1
            source_uuid = file_info.get('source_uuid')
            file_path = file_info.get('file_path')
            
            if not source_uuid or not file_path:
                print(f"⚠️ [{processed_files}/{total_files}] Missing source_uuid or file_path, skipping")
                failed_count += 1
                continue
            
            print(f"\n🔄 [{processed_files}/{total_files}] Processing file:")
            print(f"   Source UUID: {source_uuid}")
            print(f"   Path: {file_path}")
            
            if file_path.endswith('.ttl'):
                content_type = 'text/turtle'
            elif file_path.endswith('.rdf'):
                content_type = 'application/rdf+xml'
            else:
                print(f"⚠️ Unknown file type for {file_path}, skipping")
                failed_count += 1
                unknown_file_type_count += 1
                continue
            
            try:
                response = minio_client.get_object(bucket_name, file_path)
                file_content = response.read()
                response.close()
                response.release_conn()
                print(f"📥 Downloaded from MinIO ({len(file_content)} bytes)")
            except S3Error as e:
                print(f"❌ MinIO error reading {file_path}: {e}")
                failed_count += 1
                continue
            except Exception as e:
                print(f"❌ Unexpected error downloading from MinIO: {e}")
                failed_count += 1
                continue
            
            headers = {"Content-Type": content_type}
            
            try:
                upload_response = requests.post(
                    upload_url,
                    data=file_content,
                    headers=headers,
                    auth=auth,
                    timeout=60
                )
                
                if upload_response.status_code == 200:
                    print(f"✅ Successfully uploaded to Fuseki")
                    success_count += 1
                    
                    if pg_conn and pg_cursor:
                        try:
                            filename = os.path.basename(file_path)
                            current_time = datetime.now()
                            
                            update_query = """
                                UPDATE source 
                                SET 
                                    last_file_pushed = %s,
                                    last_file_pushed_date = %s,
                                    last_file_pushed_path = %s,
                                    updated_at = %s
                                WHERE source_uuid = %s
                            """
                            
                            pg_cursor.execute(
                                update_query,
                                (filename, current_time, file_path, current_time, source_uuid)
                            )
                            pg_conn.commit()
                            
                            print(f"💾 Updated source record in database")
                            print(f"   Filename: {filename}")
                            print(f"   Timestamp: {current_time}")
                            
                        except Exception as db_error:
                            print(f"⚠️ PARTIAL SUCCESS: Jena upload succeeded but DB update failed")
                            print(f"   Error: {db_error}")
                            db_update_failed_count += 1
                            pg_conn.rollback()
                    else:
                        print(f"⚠️ Database connection unavailable, skipping record update")
                        db_update_failed_count += 1
                    
                else:
                    print(f"❌ Fuseki upload failed: {upload_response.status_code}")
                    print(f"   Response: {upload_response.text[:200]}")
                    failed_count += 1
                    
            except requests.RequestException as e:
                print(f"❌ Request error uploading to Fuseki: {e}")
                failed_count += 1
                continue
            except Exception as e:
                print(f"❌ Unexpected error during upload: {e}")
                failed_count += 1
                continue
    
    if pg_cursor:
        pg_cursor.close()
    if pg_conn:
        pg_conn.close()
        print("\n🔌 PostgreSQL connection closed")
    
    print(f"\n{'='*60}")
    print(f"📊 FINAL UPLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"✅ Successful uploads:        {success_count}")
    print(f"❌ Failed uploads:            {failed_count}")
    print(f"⚠️  DB update failures:        {db_update_failed_count}")
    print(f"📋 Unknown file types:        {unknown_file_type_count}")
    print(f"📈 Total files processed:     {total_files}")
    print(f"{'='*60}")
    print(f"✔️  Fully successful:          {success_count - db_update_failed_count}")
    print(f"⚠️  Partial success:           {db_update_failed_count}")
    print(f"{'='*60}")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "db_update_failed": db_update_failed_count,
        "unknown_file_type": unknown_file_type_count,
        "total": total_files,
        "fully_successful": success_count - db_update_failed_count,
        "partial_success": db_update_failed_count
    }