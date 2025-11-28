from typing import Dict, List
from minio import Minio
from minio.error import S3Error
from mage_ai.data_preparation.shared.secrets import get_secret_value
import json
from datetime import datetime
import requests
from io import BytesIO
import os
from urllib.parse import urlparse

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer


@transformer
def transform(messages: List[Dict], *args, **kwargs):

    client = None
    try:
        client = Minio(
            get_secret_value("MINIO_HOST"),   
            access_key=get_secret_value("MINIO_ROOT_USER"),      
            secret_key=get_secret_value("MINIO_ROOT_PASSWORD"),      
            secure=False                          
        )
        print("✅ Connected to MinIO")
        
        bucket_name = "quality-link-storage"
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
            print(f"📁 Created bucket: {bucket_name}")
        else:
            print(f"📁 Bucket {bucket_name} already exists")
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")
        return None
    
    today = datetime.now()
    date_str = today.strftime("%Y-%m-%d")
    datetime_str = today.strftime("%Y%m%d_%H%M%S")
    return_data = None

    for message in messages:
        try:
            provider_uuid = message["provider_uuid"]
            source_version_uuid = message["source_version_uuid"]
            source_uuid = message["source_uuid"]
            source_path = message["source_path"]
            
            print(f"🔄 Processing source: {source_uuid}")
            print(f"   Provider: {provider_uuid}")
            print(f"   Version: {source_version_uuid}")
            print(f"   Source Path: {source_path}")
            
            base_folder = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}"
            manifest_path = f"{base_folder}/source_manifest.json"
            date_folder = f"{base_folder}/{date_str}"
            
            manifest_exists = False
            manifest_data = {
                "dates": [date_str],
                "latest_date": date_str
            }
            
            try:
                response = client.get_object(bucket_name, manifest_path)
                manifest_content = response.read().decode('utf-8')
                manifest_data = json.loads(manifest_content)
                manifest_exists = True
                print(f"📄 Found existing manifest file")
                
                if manifest_data["latest_date"] != date_str:
                    if date_str not in manifest_data["dates"]:
                        manifest_data["dates"].append(date_str)
                    manifest_data["latest_date"] = date_str
                    print(f"📅 Updated manifest with new date: {date_str}")
                else:
                    print(f"📅 Current date {date_str} already in manifest")
                
                response.close()
                response.release_conn()
            except Exception as e:
                if "NoSuchKey" not in str(e):
                    raise e
                print(f"📄 No existing manifest found, will create new one")
                
            manifest_bytes = json.dumps(manifest_data, indent=4).encode('utf-8')
            client.put_object(
                bucket_name,
                manifest_path,
                BytesIO(manifest_bytes),
                length=len(manifest_bytes),
                content_type="application/json"
            )
            print(f"💾 Saved manifest file")
            
            try:
                print(f"🔽 Downloading file from: {source_path}")
                response = requests.get(source_path, timeout=60)
                response.raise_for_status()  
                
                file_extension = os.path.splitext(urlparse(source_path).path)[1]
                if not file_extension:
                    content_type = response.headers.get('content-type', '')
                    if 'application/rdf+xml' in content_type:
                        file_extension = '.rdf'
                    elif 'text/turtle' in content_type:
                        file_extension = '.ttl'
                    elif 'application/json' in content_type:
                        file_extension = '.json'
                    else:
                        file_extension = ''  
                
                target_filename = f"{date_folder}/{datetime_str}{file_extension}"
                
                file_bytes = response.content
                client.put_object(
                    bucket_name,
                    target_filename,
                    BytesIO(file_bytes),
                    length=len(file_bytes),
                    content_type=response.headers.get('content-type', 'application/octet-stream')
                )
                print(f"💾 Saved file to: {target_filename}")
                
                return_data = {
                    "provider_uuid": provider_uuid,
                    "source_version_uuid": source_version_uuid
                }

            except requests.RequestException as e:
                print(f"❌ Error downloading file: {e}")
                continue
                
        except KeyError as e:
            print(f"❌ Missing required field in message: {e}")
            continue
        except S3Error as e:
            print(f"❌ MinIO error: {e}")
            continue
        except Exception as e:
            print(f"❌ Unexpected error: {e}")
            continue



    return return_data