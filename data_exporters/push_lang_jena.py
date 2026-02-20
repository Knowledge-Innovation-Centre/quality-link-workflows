if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import requests


@data_exporter
def export_data(data, *args, **kwargs):

    scheme_uri = kwargs.get("CONCEPT_SCHEME")

    fuseki_url = os.environ.get("FUSEKI_URL")
    fuseki_username = os.environ.get("FUSEKI_USERNAME")
    fuseki_password = os.environ.get("FUSEKI_PASSWORD")
    
    dataset_name = os.environ.get("FUSEKI_DATASET_NAME")
    upload_url = f"{fuseki_url}/{dataset_name}/data"
    
    auth = None
    if fuseki_username and fuseki_password:
        auth = (fuseki_username, fuseki_password)
    
    headers = {
        "Content-Type": "text/turtle; charset=utf-8"
    }
    
    try:
        upload_response = requests.post(
            upload_url,
            data=data.encode("utf-8"),
            headers=headers,
            auth=auth,
            timeout=60
        )
        
        if upload_response.status_code == 200:
            print(f"✅ Successfully uploaded vocabulary <{scheme_uri}> to Fuseki")
            print(f"{'='*60}")
            return {
                "success": True,
                "bytes_uploaded": len(data),
                "dataset": dataset_name
            }
        else:
            print(f"❌ Fuseki upload failed: {upload_response.status_code}")
            print(f"Response: {upload_response.text[:500]}")
            print(f"{'='*60}")
            return {
                "success": False,
                "error": f"HTTP {upload_response.status_code}",
                "response": upload_response.text[:500]
            }
            
    except requests.RequestException as e:
        print(f"❌ Request error uploading to Fuseki: {e}")
        print(f"{'='*60}")
        return {
            "success": False,
            "error": str(e)
        }