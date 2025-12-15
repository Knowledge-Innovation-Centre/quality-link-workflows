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

# from typing import Dict, List
# from minio import Minio
# from minio.error import S3Error
# from mage_ai.data_preparation.shared.secrets import get_secret_value
# import json
# from datetime import datetime, timezone  
# from io import BytesIO
# import os
# from urllib.parse import urlparse
# import requests

# from rdflib import Graph, Literal, Namespace, URIRef
# from rdflib.namespace import RDF, DCTERMS, XSD

# if 'transformer' not in globals():
#     from mage_ai.data_preparation.decorators import transformer


# def add_temporal_metadata_to_rdf(file_content: bytes, file_extension: str) -> tuple[bytes, int]:

#     try:
#         g = Graph()
        
#         if file_extension == '.ttl':
#             parse_format = 'turtle'
#             serialize_format = 'turtle'
#         elif file_extension == '.rdf':
#             parse_format = 'xml'
#             serialize_format = 'xml'
#         else:
#             return file_content, 0
        
#         g.parse(data=file_content, format=parse_format)
        
#         QL = Namespace("http://data.quality-link.eu/ontology/v1#")
        
#         g.bind("ql", QL)

#         now_datetime = datetime.now(timezone.utc)

#         now_full = Literal(now_datetime, datatype=XSD.dateTime)
#         now_date = Literal(now_datetime.strftime("%Y-%m-%d"), datatype=XSD.date)   

#         los_count = 0
#         for course_uri in g.subjects(RDF.type, QL.LearningOpportunitySpecification):
#             g.set((course_uri, QL.ingestedAt, now_full))
#             g.set((course_uri, QL.ingestedDate, now_date))

#             los_count += 1
        
#         if los_count > 0:
#             print(f"   ⏰ Added ingestion timestamps to {los_count} LearningOpportunitySpecification(s)")
#         else:
#             print(f"   ℹ️  No LearningOpportunitySpecifications found in file")
        
#         modified_content = g.serialize(format=serialize_format, encoding='utf-8')
        
#         return modified_content, los_count
        
#     except Exception as e:
#         print(f"   ⚠️  Error adding temporal metadata: {e}")
#         print(f"   📦 Saving original file without modifications")
#         return file_content, 0


# @transformer
# def transform(messages: List[Dict], *args, **kwargs):
#     client = None
#     try:
#         client = Minio(
#             get_secret_value("MINIO_HOST"),   
#             access_key=get_secret_value("MINIO_ROOT_USER"),      
#             secret_key=get_secret_value("MINIO_ROOT_PASSWORD"),      
#             secure=False                          
#         )
#         print("✅ Connected to MinIO")
        
#         bucket_name = "quality-link-storage"
#         if not client.bucket_exists(bucket_name):
#             client.make_bucket(bucket_name)
#             print(f"📁 Created bucket: {bucket_name}")
#         else:
#             print(f"📁 Bucket {bucket_name} already exists")
#     except Exception as e:
#         print(f"❌ Error connecting to MinIO: {e}")
#         return None
    
#     today = datetime.now()
#     date_str = today.strftime("%Y-%m-%d")
#     datetime_str = today.strftime("%Y%m%d_%H%M%S")
#     return_data = None
    
#     total_files_processed = 0
#     total_courses_timestamped = 0
    
#     for message in messages:
#         try:
#             provider_uuid = message["provider_uuid"]
#             source_version_uuid = message["source_version_uuid"]
#             source_uuid = message["source_uuid"]
#             source_path = message["source_path"]
            
#             print(f"\n{'='*60}")
#             print(f"🔄 Processing source: {source_uuid}")
#             print(f"   Provider: {provider_uuid}")
#             print(f"   Version: {source_version_uuid}")
#             print(f"   Source Path: {source_path}")
            
#             base_folder = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}"
#             manifest_path = f"{base_folder}/source_manifest.json"
#             date_folder = f"{base_folder}/{date_str}"
            
#             manifest_exists = False
#             manifest_data = {
#                 "dates": [date_str],
#                 "latest_date": date_str
#             }
            
#             try:
#                 response = client.get_object(bucket_name, manifest_path)
#                 manifest_content = response.read().decode('utf-8')
#                 manifest_data = json.loads(manifest_content)
#                 manifest_exists = True
#                 print(f"📄 Found existing manifest file")
                
#                 if manifest_data["latest_date"] != date_str:
#                     if date_str not in manifest_data["dates"]:
#                         manifest_data["dates"].append(date_str)
#                     manifest_data["latest_date"] = date_str
#                     print(f"📅 Updated manifest with new date: {date_str}")
#                 else:
#                     print(f"📅 Current date {date_str} already in manifest")
                
#                 response.close()
#                 response.release_conn()
#             except Exception as e:
#                 if "NoSuchKey" not in str(e):
#                     raise e
#                 print(f"📄 No existing manifest found, will create new one")
                
#             manifest_bytes = json.dumps(manifest_data, indent=4).encode('utf-8')
#             client.put_object(
#                 bucket_name,
#                 manifest_path,
#                 BytesIO(manifest_bytes),
#                 length=len(manifest_bytes),
#                 content_type="application/json"
#             )
#             print(f"💾 Saved manifest file")
            
#             try:
#                 print(f"🔽 Downloading file from: {source_path}")
#                 response = requests.get(source_path, timeout=60)
#                 response.raise_for_status()  
                
#                 file_extension = os.path.splitext(urlparse(source_path).path)[1]
#                 if not file_extension:
#                     content_type = response.headers.get('content-type', '')
#                     if 'application/rdf+xml' in content_type:
#                         file_extension = '.rdf'
#                     elif 'text/turtle' in content_type:
#                         file_extension = '.ttl'
#                     elif 'application/json' in content_type:
#                         file_extension = '.json'
#                     else:
#                         file_extension = ''  
                
#                 target_filename = f"{date_folder}/{datetime_str}{file_extension}"
                
#                 file_bytes = response.content
#                 original_size = len(file_bytes)
#                 print(f"📥 Downloaded file ({original_size} bytes)")
                
#                 courses_timestamped = 0
#                 if file_extension in ['.ttl', '.rdf']:
#                     print(f"🔧 Processing RDF file: adding temporal metadata...")
#                     file_bytes, courses_timestamped = add_temporal_metadata_to_rdf(
#                         file_bytes, 
#                         file_extension
#                     )
#                     modified_size = len(file_bytes)
#                     print(f"📝 Modified content size: {modified_size} bytes (Δ {modified_size - original_size:+d})")
                    
#                     if courses_timestamped > 0:
#                         total_courses_timestamped += courses_timestamped
#                 else:
#                     print(f"ℹ️  Non-RDF file, skipping temporal metadata")
                
#                 client.put_object(
#                     bucket_name,
#                     target_filename,
#                     BytesIO(file_bytes),
#                     length=len(file_bytes),
#                     content_type=response.headers.get('content-type', 'application/octet-stream')
#                 )
#                 print(f"💾 Saved file to: {target_filename}")
                
#                 total_files_processed += 1
                
#                 return_data = {
#                     "provider_uuid": provider_uuid,
#                     "source_version_uuid": source_version_uuid,
#                     "files_processed": total_files_processed,
#                     "courses_timestamped": total_courses_timestamped
#                 }
                
#             except requests.RequestException as e:
#                 print(f"❌ Error downloading file: {e}")
#                 continue
                
#         except KeyError as e:
#             print(f"❌ Missing required field in message: {e}")
#             continue
#         except S3Error as e:
#             print(f"❌ MinIO error: {e}")
#             continue
#         except Exception as e:
#             print(f"❌ Unexpected error: {e}")
#             continue
    
#     if total_files_processed > 0:
#         print(f"\n{'='*60}")
#         print(f"📊 PROCESSING SUMMARY")
#         print(f"{'='*60}")
#         print(f"📁 Total files processed:       {total_files_processed}")
#         print(f"⏰ Total courses timestamped:   {total_courses_timestamped}")
#         print(f"{'='*60}")
    
#     return return_data