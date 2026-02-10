if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import os
import requests

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Write provider data to Jena

    Args:
        data: array of RDF Turtle data ready for pushing to Jena

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """

    fuseki_url = os.environ.get("FUSEKI_URL")
    fuseki_username = os.environ.get("FUSEKI_USERNAME")
    fuseki_password = os.environ.get("FUSEKI_PASSWORD")

    dataset_name = os.environ.get("FUSEKI_DATASET_NAME")
    upload_url = f"{fuseki_url}/{dataset_name}/data"

    auth = None
    if fuseki_username and fuseki_password:
        auth = (fuseki_username, fuseki_password)

    print(f"🎯 Uploading to Fuseki dataset: {dataset_name}")
    print(f"{'='*60}")

    success_count = 0
    failed_count = 0

    for row in data:
        headers = {"Content-Type": "text/turtle"}

        try:
            upload_response = requests.post(
                upload_url,
                data=row,
                headers=headers,
                auth=auth,
                timeout=60
            )

            if upload_response.status_code == 200:
                success_count += 1
            else:
                print(f"   ❌ Fuseki upload failed: {upload_response.status_code}")
                print(f"      Response: {upload_response.text[:200]}")
                failed_count += 1

        except requests.RequestException as e:
            print(f"   ❌ Request error uploading to Fuseki: {e}")
            failed_count += 1
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error during upload: {e}")
            failed_count += 1
            continue

    print(f"✅ {success_count} records successfully uploaded to Fuseki")
    print(f"❌ {failed_count} errors during upload")

    return {
        "success": success_count,
        "failed": failed_count,
    }