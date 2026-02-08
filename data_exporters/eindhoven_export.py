if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import requests
from mage_ai.data_preparation.shared.secrets import get_secret_value
from typing import Dict, Any


@data_exporter
def export_data(data, *args, **kwargs) -> Dict[str, Any]:
    """
    Export RDF data to Apache Jena Fuseki.
    
    Args:
        data: Output from transformer containing rdf_content and metrics
    
    Returns:
        Dict with upload metrics matching existing pipeline format
    """
    # Extract from transformer output
    rdf_content = data.get('rdf_content')
    course_uuids = data.get('course_uuids', [])
    triples_count = data.get('triples_created', 0)
    total_courses = data.get('total', 0)
    
    if not rdf_content:
        print("❌ No RDF content to upload")
        return {
            "success": 0,
            "failed": 1,
            "total": 1,
            "triples_uploaded": 0,
            "course_uuids": []
        }
    
    print(f"📤 Preparing to upload RDF to Fuseki")
    print(f"{'='*60}")
    print(f"   Courses: {total_courses}")
    print(f"   Triples: {triples_count}")
    print(f"   Content size: {len(rdf_content)} bytes")
    
    try:
        fuseki_url = get_secret_value("FUSEKI_URL")
        fuseki_username = get_secret_value("FUSEKI_USERNAME")
        fuseki_password = get_secret_value("FUSEKI_PASSWORD")
    except Exception as e:
        print(f"❌ Failed to get Fuseki credentials: {e}")
        return {
            "success": 0,
            "failed": 1,
            "total": 1,
            "triples_uploaded": 0,
            "course_uuids": course_uuids
        }
    
    dataset_name = "pipeline-data"
    upload_url = f"{fuseki_url}/{dataset_name}/data"
    
    auth = None
    if fuseki_username and fuseki_password:
        auth = (fuseki_username, fuseki_password)
    
    print(f"🎯 Uploading to Fuseki dataset: {dataset_name}")
    print(f"   URL: {upload_url}")
    print(f"{'='*60}")
    
    headers = {"Content-Type": "text/turtle"}
    
    try:
        upload_response = requests.post(
            upload_url,
            data=rdf_content,
            headers=headers,
            auth=auth,
            timeout=60
        )
        
        if upload_response.status_code == 200:
            print(f"✅ Successfully uploaded to Fuseki")
            print(f"   Status: {upload_response.status_code}")
            print(f"   Triples uploaded: {triples_count}")
            print(f"   Course UUIDs: {len(course_uuids)}")
            
            print(f"\n{'='*60}")
            print(f"📊 UPLOAD SUMMARY")
            print(f"{'='*60}")
            print(f"✅ Successful uploads:        1")
            print(f"❌ Failed uploads:            0")
            print(f"📈 Total upload attempts:     1")
            print(f"🔢 Triples uploaded:          {triples_count}")
            print(f"🆔 Courses uploaded:          {total_courses}")
            print(f"{'='*60}")
            
            return {
                "success": 1,
                "failed": 0,
                "total": 1,
                "triples_uploaded": triples_count,
                "course_count": total_courses,
                "course_uuids": course_uuids
            }
        else:
            print(f"❌ Fuseki upload failed: {upload_response.status_code}")
            print(f"   Response: {upload_response.text[:200]}")
            
            print(f"\n{'='*60}")
            print(f"📊 UPLOAD SUMMARY")
            print(f"{'='*60}")
            print(f"✅ Successful uploads:        0")
            print(f"❌ Failed uploads:            1")
            print(f"📈 Total upload attempts:     1")
            print(f"{'='*60}")
            
            return {
                "success": 0,
                "failed": 1,
                "total": 1,
                "triples_uploaded": 0,
                "course_count": total_courses,
                "course_uuids": course_uuids,
                "error_code": upload_response.status_code,
                "error_message": upload_response.text[:200]
            }
            
    except requests.RequestException as e:
        print(f"❌ Request error uploading to Fuseki: {e}")
        
        print(f"\n{'='*60}")
        print(f"📊 UPLOAD SUMMARY")
        print(f"{'='*60}")
        print(f"✅ Successful uploads:        0")
        print(f"❌ Failed uploads:            1")
        print(f"📈 Total upload attempts:     1")
        print(f"{'='*60}")
        
        return {
            "success": 0,
            "failed": 1,
            "total": 1,
            "triples_uploaded": 0,
            "course_count": total_courses,
            "course_uuids": course_uuids,
            "error": str(e)
        }
    except Exception as e:
        print(f"❌ Unexpected error during upload: {e}")
        
        return {
            "success": 0,
            "failed": 1,
            "total": 1,
            "triples_uploaded": 0,
            "course_count": total_courses,
            "course_uuids": course_uuids,
            "error": str(e)
        }