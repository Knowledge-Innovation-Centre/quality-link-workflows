from mage_ai.data_preparation.shared.secrets import get_secret_value
from minio import Minio
from minio.error import S3Error
import requests
from typing import List

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
 
    all_file_paths = []
    for transaction in data:
        files = transaction.get('files', [])
        for file_info in files:
            file_path = file_info.get('file_path')
            if file_path:
                all_file_paths.append(file_path)
    
    print(f"📋 Extracted {len(all_file_paths)} file paths from {len(data)} transactions")
    
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
        return {"success": 0, "failed": len(all_file_paths), "total": len(all_file_paths)}
    
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
    print(f"📊 Total files to process: {len(all_file_paths)}")
    
    success_count = 0
    failed_count = 0
    
    for file_path in all_file_paths:
        try:
            print(f"\n🔄 Processing: {file_path}")
            
            if file_path.endswith('.ttl'):
                content_type = 'text/turtle'
            elif file_path.endswith('.rdf'):
                content_type = 'application/rdf+xml'
            else:
                print(f"⚠️ Unknown file type for {file_path}, skipping")
                failed_count += 1
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
                else:
                    print(f"❌ Fuseki upload failed: {upload_response.status_code}")
                    print(f"   Response: {upload_response.text}")
                    failed_count += 1
                    
            except requests.RequestException as e:
                print(f"❌ Request error uploading to Fuseki: {e}")
                failed_count += 1
                continue
                
        except Exception as e:
            print(f"❌ Unexpected error processing {file_path}: {e}")
            failed_count += 1
            continue
    
    print(f"\n{'='*60}")
    print(f"📊 Upload Summary:")
    print(f"   ✅ Success: {success_count}")
    print(f"   ❌ Failed: {failed_count}")
    print(f"   📋 Total: {len(all_file_paths)}")
    print(f"{'='*60}")
    
    return {
        "success": success_count,
        "failed": failed_count,
        "total": len(all_file_paths)
    }