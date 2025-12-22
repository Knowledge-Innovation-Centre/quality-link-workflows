if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

from mage_ai.data_preparation.shared.secrets import get_secret_value
import requests


@data_exporter
def export_data(data, *args, **kwargs):

    fuseki_url = get_secret_value("FUSEKI_URL")
    fuseki_username = get_secret_value("FUSEKI_USERNAME")
    fuseki_password = get_secret_value("FUSEKI_PASSWORD")
    
    dataset_name = "pipeline-data"
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
            print(f"✅ Successfully uploaded language vocabulary to Fuseki")
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