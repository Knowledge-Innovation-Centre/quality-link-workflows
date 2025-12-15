from typing import Dict, List
from minio import Minio
from minio.error import S3Error
from mage_ai.data_preparation.shared.secrets import get_secret_value
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(curr_data, *args, **kwargs):

    try:

        client = Minio(
            get_secret_value("MINIO_HOST"),
            access_key=get_secret_value("MINIO_ROOT_USER"),
            secret_key=get_secret_value("MINIO_ROOT_PASSWORD"),
            secure=False
        )
        print("✅ Connected to MinIO")
    except Exception as e:
        print(f"❌ Error connecting to MinIO: {e}")
        return []
    
    bucket_name = "quality-link-storage"
    
    data = curr_data if isinstance(curr_data, list) else [curr_data]
    
    print(f"📋 Processing {len(data)} transaction records")
    
    results = []
    
    for transaction in data:
        try:
            provider_uuid = transaction["provider_uuid"]
            source_version_uuid = transaction["source_version_uuid"]
            trans_uuid = transaction["trans_uuid"]
            
            if isinstance(transaction["created_at_date"], str):
                date_str = transaction["created_at_date"]
            else:
                date_str = transaction["created_at_date"].strftime("%Y-%m-%d")
            
            print(f"\n🔄 Processing transaction: {trans_uuid}")
            print(f"   Provider: {provider_uuid}")
            print(f"   Version: {source_version_uuid}")
            print(f"   Date: {date_str}")
            
            base_prefix = f"datalake/courses/{provider_uuid}/{source_version_uuid}/"
            
            source_uuids = []
            try:
                objects = client.list_objects(bucket_name, prefix=base_prefix, recursive=False)
                for obj in objects:
                    parts = obj.object_name.rstrip('/').split('/')
                    if len(parts) >= 5:
                        source_uuid = parts[4]
                        if source_uuid and source_uuid not in source_uuids:
                            source_uuids.append(source_uuid)
                
                print(f"📁 Found {len(source_uuids)} source folders")
            except S3Error as e:
                print(f"❌ Error listing source folders: {e}, skipping transaction")
                continue
            
            latest_files = []
            
            for source_uuid in source_uuids:
                try:
                    date_folder_prefix = f"datalake/courses/{provider_uuid}/{source_version_uuid}/{source_uuid}/{date_str}/"
                    
                    objects = client.list_objects(bucket_name, prefix=date_folder_prefix, recursive=True)
                    file_list = []
                    
                    for obj in objects:
                        file_list.append({
                            "full_path": obj.object_name,
                            "last_modified": obj.last_modified
                        })
                    
                    if not file_list:
                        print(f"⚠️ No files found for source {source_uuid} on {date_str}, skipping")
                        continue
                    
                    sorted_files = sorted(file_list, key=lambda x: x["last_modified"])
                    latest_file = sorted_files[-1]
                    
                    latest_files.append({
                        "source_uuid": source_uuid,
                        "file_path": latest_file["full_path"],
                        "last_modified": latest_file["last_modified"].isoformat()
                    })
                    
                    print(f"✅ Latest file for {source_uuid}: {latest_file['full_path']}")
                    
                except S3Error as e:
                    print(f"⚠️ Error processing source {source_uuid}: {e}, skipping")
                    continue
                except Exception as e:
                    print(f"⚠️ Unexpected error for source {source_uuid}: {e}, skipping")
                    continue
            
            result = {
                "trans_uuid": trans_uuid,
                "provider_uuid": provider_uuid,
                "source_version_uuid": source_version_uuid,
                "date": date_str,
                "files": latest_files,
                "file_count": len(latest_files)
            }
            
            results.append(result)
            print(f"📊 Retrieved {len(latest_files)} latest files for transaction {trans_uuid}")
            
        except KeyError as e:
            print(f"❌ Missing field in transaction: {e}, skipping")
            continue
        except Exception as e:
            print(f"❌ Unexpected error processing transaction: {e}, skipping")
            continue
    
    print(f"\n✅ Completed processing {len(results)} transactions")
    return results


# @test
# def test_output(output, *args) -> None:

#     assert output is not None, 'The output is undefined'
#     assert isinstance(output, dict), 'Output should be a dict (single transaction result)'
#     assert 'files' in output, 'Output should contain files list'
#     assert isinstance(output['files'], list), 'Files should be a list'
#     assert 'trans_uuid' in output, 'Output should contain trans_uuid'
    
#     total_results = 1 + len(args)
#     total_files = output['file_count'] + sum(arg.get('file_count', 0) for arg in args)
    
#     print(f"✅ Test passed: {total_results} transaction(s) processed with {total_files} total files")